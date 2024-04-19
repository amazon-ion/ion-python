# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the
# License.

import base64
from decimal import Decimal
from collections import defaultdict, deque
from enum import IntEnum
from functools import partial
from typing import Optional, NamedTuple, Iterator, Callable, Tuple

from amazon.ion.core import Transition, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, IonType, IonEvent, \
    IonEventType, IonThunkEvent, TimestampPrecision, timestamp, ION_VERSION_MARKER_EVENT
from amazon.ion.exceptions import IonException
from amazon.ion.reader import BufferQueue, reader_trampoline, ReadEventType, CodePointArray, CodePoint
from amazon.ion.sliceable_buffer import SliceableBuffer
from amazon.ion.symbols import SymbolToken, TEXT_ION_1_0
from amazon.ion.util import coroutine, _next_code_point, CodePoint


def _illegal_character(c, ctx, message=''):
    """Raises an IonException upon encountering the given illegal character in the given context.

    Args:
        c (int|None): Ordinal of the illegal character.
        ctx (_HandlerContext):  Context in which the illegal character was encountered.
        message (Optional[str]): Additional information, as necessary.

    """
    container_type = ctx.container.ion_type is None and 'top-level' or ctx.container.ion_type.name
    value_type = ctx.ion_type is None and 'unknown' or ctx.ion_type.name
    if c is None:
        header = 'Illegal token'
    else:
        c = 'EOF' if BufferQueue.is_eof(c) else chr(c)
        header = 'Illegal character %s' % (c,)
    raise IonException('%s at position %d in %s value contained in %s. %s Pending value: %s'
                       % (header, ctx.queue.position, value_type, container_type, message, ctx.value))


def _defaultdict(dct, fallback=_illegal_character):
    """Wraps the given dictionary such that the given fallback function will be called when a nonexistent key is
    accessed.
    """
    out = defaultdict(lambda: fallback)
    for k, v in iter(dct.items()):
        out[k] = v
    return out


def _merge_mappings(*args):
    """Merges a sequence of dictionaries and/or tuples into a single dictionary.

    If a given argument is a tuple, it must have two elements, the first of which is a sequence of keys and the second
    of which is a single value, which will be mapped to from each of the keys in the sequence.
    """
    dct = {}
    for arg in args:
        if isinstance(arg, dict):
            merge = arg
        else:
            assert isinstance(arg, tuple)
            keys, value = arg
            merge = dict(zip(keys, [value]*len(keys)))
        dct.update(merge)
    return dct


def _seq(s):
    """Converts bytes to a sequence of integer code points."""
    return tuple(iter(s))


_ENCODING = 'utf-8'

# NOTE: the following are stored as sequences of integer code points. This simplifies dealing with inconsistencies
# between how bytes objects are handled in python 2 and 3, and simplifies logic around comparing multi-byte characters.
_WHITESPACE_NOT_NL = _seq(b' \t\v\f')
_WHITESPACE = _WHITESPACE_NOT_NL + _seq(b'\n\r')
_VALUE_TERMINATORS = _seq(b'{}[](),\"\' \t\n\r/')
_SYMBOL_TOKEN_TERMINATORS = _WHITESPACE + _seq(b'/:')
_DIGITS = _seq(b'0123456789')
_BINARY_RADIX = _seq(b'Bb')
_BINARY_DIGITS = _seq(b'01')
_HEX_RADIX = _seq(b'Xx')
_HEX_DIGITS = _DIGITS + _seq(b'abcdefABCDEF')
_DECIMAL_EXPS = _seq(b'Dd')
_FLOAT_EXPS = _seq(b'Ee')
_SIGN = _seq(b'+-')
_TIMESTAMP_YEAR_DELIMITERS = _seq(b'-T')
_TIMESTAMP_DELIMITERS = _seq(b'-:+.')
_TIMESTAMP_OFFSET_INDICATORS = _seq(b'Z+-')
_LETTERS = _seq(b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
_BASE64_DIGITS = _LETTERS + _DIGITS + _seq(b'+/')
_IDENTIFIER_STARTS = _LETTERS + _seq(b'_')  # Note: '$' is dealt with separately.
_IDENTIFIER_CHARACTERS = _IDENTIFIER_STARTS + _DIGITS + _seq(b'$')
_OPERATORS = _seq(b'!#%&*+-./;<=>?@^`|~')
_COMMON_ESCAPES = _seq(b'abtnfrv?0\'"/\\')
_NEWLINES = _seq(b'\r\n')

_UNDERSCORE = ord(b'_')
_DOT = ord(b'.')
_COMMA = ord(b',')
_COLON = ord(b':')
_SLASH = ord(b'/')
_ASTERISK = ord(b'*')
_BACKSLASH = ord(b'\\')
_CARRIAGE_RETURN = ord(b'\r')
_NEWLINE = ord(b'\n')
_DOUBLE_QUOTE = ord(b'"')
_SINGLE_QUOTE = ord(b'\'')
_DOLLAR_SIGN = ord(b'$')
_PLUS = ord(b'+')
_MINUS = ord(b'-')
_HYPHEN = _MINUS
_T = ord(b'T')
_Z = ord(b'Z')
_T_LOWER = ord(b't')
_N_LOWER = ord(b'n')
_F_LOWER = ord(b'f')
_ZERO = _DIGITS[0]
_OPEN_BRACE = ord(b'{')
_OPEN_BRACKET = ord(b'[')
_OPEN_PAREN = ord(b'(')
_CLOSE_BRACE = ord(b'}')
_CLOSE_BRACKET = ord(b']')
_CLOSE_PAREN = ord(b')')
_BASE64_PAD = ord(b'=')
_QUESTION_MARK = ord(b'?')
_UNICODE_ESCAPE_2 = ord(b'x')
_UNICODE_ESCAPE_4 = ord(b'u')
_UNICODE_ESCAPE_8 = ord(b'U')

_ESCAPED_NEWLINE = u''  # An escaped newline expands to nothing.

_MAX_TEXT_CHAR = 0x10ffff
_MAX_CLOB_CHAR = 0x7f
_MIN_QUOTED_CHAR = 0x20

# The following suffixes are used for comparison when a token is found that starts with the first letter in
# the keyword. For example, when a new token starts with 't', the next three characters must match those in
# _TRUE_SUFFIX, followed by an acceptable termination character, in order for the token to match the 'true' keyword.
_TRUE_SUFFIX = _seq(b'rue')
_FALSE_SUFFIX = _seq(b'alse')
_NAN_SUFFIX = _seq(b'an')
_INF_SUFFIX = _seq(b'inf')
_IVM_PREFIX = _seq(b'$ion_')

_IVM_EVENTS = {
    TEXT_ION_1_0: ION_VERSION_MARKER_EVENT,
}

_POS_INF = float('+inf')
_NEG_INF = float('-inf')
_NAN = float('nan')


def _ends_value(c):
    return c in _VALUE_TERMINATORS or BufferQueue.is_eof(c)


class _NullSequence:
    """Contains the terminal character sequence for the typed null suffix of the given IonType, starting with the first
    character after the one which disambiguated the type.

    For example, SYMBOL's _NullSequence contains the characters 'mbol' because 'null.s' is ambiguous until 'y' is found,
    at which point it must end in 'mbol'.

    Instances are used as leaves of the typed null prefix tree below.
    """
    def __init__(self, ion_type, sequence):
        self.ion_type = ion_type
        self.sequence = sequence

    def __getitem__(self, item):
        return self.sequence[item]

_NULL_SUFFIX = _NullSequence(IonType.NULL, _seq(b'ull'))
_NULL_SYMBOL_SUFFIX = _NullSequence(IonType.SYMBOL, _seq(b'mbol'))
_NULL_SEXP_SUFFIX = _NullSequence(IonType.SEXP, _seq(b'xp'))
_NULL_STRING_SUFFIX = _NullSequence(IonType.STRING, _seq(b'ng'))
_NULL_STRUCT_SUFFIX = _NullSequence(IonType.STRUCT, _seq(b'ct'))
_NULL_INT_SUFFIX = _NullSequence(IonType.INT, _seq(b'nt'))
_NULL_FLOAT_SUFFIX = _NullSequence(IonType.FLOAT, _seq(b'loat'))
_NULL_DECIMAL_SUFFIX = _NullSequence(IonType.DECIMAL, _seq(b'ecimal'))
_NULL_CLOB_SUFFIX = _NullSequence(IonType.CLOB, _seq(b'lob'))
_NULL_LIST_SUFFIX = _NullSequence(IonType.LIST, _seq(b'ist'))
_NULL_BLOB_SUFFIX = _NullSequence(IonType.BLOB, _seq(b'ob'))
_NULL_BOOL_SUFFIX = _NullSequence(IonType.BOOL, _seq(b'ol'))
_NULL_TIMESTAMP_SUFFIX = _NullSequence(IonType.TIMESTAMP, _seq(b'imestamp'))


# The following implements a prefix tree used to determine whether a typed null keyword has been found (see
# _typed_null_handler). The leaves of the tree (enumerated above) are the terminal character sequences for the 13
# possible suffixes to 'null.'. Any other suffix to 'null.' is an error. _NULL_STARTS is entered when 'null.' is found.

_NULL_STR_NEXT = {
    ord(b'i'): _NULL_STRING_SUFFIX,
    ord(b'u'): _NULL_STRUCT_SUFFIX
}

_NULL_ST_NEXT = {
    ord(b'r'): _NULL_STR_NEXT
}

_NULL_S_NEXT = {
    ord(b'y'): _NULL_SYMBOL_SUFFIX,
    ord(b'e'): _NULL_SEXP_SUFFIX,
    ord(b't'): _NULL_ST_NEXT
}

_NULL_B_NEXT = {
    ord(b'l'): _NULL_BLOB_SUFFIX,
    ord(b'o'): _NULL_BOOL_SUFFIX
}

_NULL_STARTS = {
    ord(b'n'): _NULL_SUFFIX,  # null.null
    ord(b's'): _NULL_S_NEXT,  # null.string, null.symbol, null.struct, null.sexp
    ord(b'i'): _NULL_INT_SUFFIX,  # null.int
    ord(b'f'): _NULL_FLOAT_SUFFIX,  # null.float
    ord(b'd'): _NULL_DECIMAL_SUFFIX,  # null.decimal
    ord(b'b'): _NULL_B_NEXT,  # null.bool, null.blob
    ord(b'c'): _NULL_CLOB_SUFFIX,  # null.clob
    ord(b'l'): _NULL_LIST_SUFFIX,  # null.list
    ord(b't'): _NULL_TIMESTAMP_SUFFIX,  # null.timestamp
}


class _ContainerContext(NamedTuple):
    """A description of an Ion container, including the container's IonType and its textual delimiter and end character,
    if applicable.

    This is tracked as part of the current token's context, and is useful when certain lexing decisions depend on
    which container the token is a member of. For example, ending a numeric token with ']' is not legal unless that
    token is contained in a list.

    Args:
        end (tuple): Tuple containing the container's end character, if any.
        delimiter (tuple): Tuple containing the container's delimiter character, if any.
        ion_type (Optional[IonType]): The container's IonType, if any.
        is_delimited (bool): True if delimiter is not empty; otherwise, False.
    """
    end: tuple
    delimiter: tuple
    ion_type: Optional[IonType]
    is_delimited: bool

_C_TOP_LEVEL = _ContainerContext((), (), None, False)
_C_STRUCT = _ContainerContext((_CLOSE_BRACE,), (_COMMA,), IonType.STRUCT, True)
_C_LIST = _ContainerContext((_CLOSE_BRACKET,), (_COMMA,), IonType.LIST, True)
_C_SEXP = _ContainerContext((_CLOSE_PAREN,), (), IonType.SEXP, False)

def invalid_char(c: int):
    raise ValueError(f"Invalid char {c}")

def open_brace_handler(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    # todo: handle blob/clob


tlv_table = [invalid_char] * 256
tlv_table[_OPEN_BRACE] = open_brace_handler
tlv_table[_OPEN_BRACKET] = open_bracket_handler


class _ContextFrame(NamedTuple):
    parser: Callable[[SliceableBuffer], Tuple[IonEvent, SliceableBuffer]]
    ion_type: Optional[IonType]
    depth: int


def _whitespace(byte):
    return byte in bytearray(b" \n\t\r\f")

def _tlv_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    (_, buffer) = buffer.read_while(_whitespace)
    if buffer.is_eof():
        return ION_STREAM_END_EVENT, buffer
    if


def _list_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    (_, buffer) = buffer.read_while(lambda b: b in bytearray(b" \n\t\r\f"))
    if buffer.is_eof():
        raise IonException("Unexpected end of file")
    if buffer.read_byte() == _CLOSE_BRACKET:
        # todo: i guess i need to pass the depth here too!
        return IonEvent(IonEventType.CONTAINER_END, IonType.LIST, 0), buffer

    # XXX: we expect the _tlv_parser to trim trailing whitespace, I guess?S?!?
    (event, buffer) = _tlv_parser(buffer)

    # if buffer is empty... durn we need to wait for comma i guess? erg.
    # if comma, consume it
    if buffer.read_byte() == _COMMA:
        pass
    elif buffer.read_byte() == _CLOSE_BRACKET:
        pass
    else:
        error!


def _struct_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    pass

def _sexp_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    pass

_parser_table = [
    None,  # NULL = 0
    None,  # BOOL = 1
    None,  # INT = 2
    None,  # FLOAT = 3
    None,  # DECIMAL = 4
    None,  # TIMESTAMP = 5
    None,  # SYMBOL = 6
    None,  # STRING = 7
    None,  # CLOB = 8
    None,  # BLOB = 9
    _list_parser,  # LIST = 10
    _sexp_parser,  # SEXP = 11
    _struct_parser,  # STRUCT = 12
]

@coroutine
def stream_handler():
    """
    Handler for an Ion Text value-stream.
    """
    buffer: SliceableBuffer = SliceableBuffer.empty()
    context_stack = deque([_ContextFrame(_tlv_parser, None, 0)])
    ion_event = None
    skip_or_next = ReadEventType.NEXT
    expect_data = False

    while True:
        read_event = yield ion_event
        assert read_event is not None

        # part 1: handle user's read event
        if expect_data:
            if read_event.type is not ReadEventType.DATA:
                raise TypeError("Data expected")
            buffer = buffer.extend(read_event.data)
        else:
            if read_event.type is ReadEventType.DATA:
                raise TypeError("Next or Skip expected")
            skip_or_next = read_event.type

        if skip_or_next is ReadEventType.SKIP:
            raise NotImplementedError("Skip is not supported")

        # part 2: do some lexxing
        (parser, ctx_type, depth) = context_stack[-1]

        (ion_event, buffer) = parser(buffer)
        event_type = ion_event.type
        ion_type = ion_event.ion_type

        if event_type is IonEventType.STREAM_END:
            expect_data = True
        elif event_type is IonEventType.INCOMPLETE:
            # todo: flushable/commit or something something
            raise NotImplementedError("Incomplete is not supported")
        elif event_type is IonEventType.CONTAINER_START:
            parser = _parser_table[ion_type]
            context_stack.append(_ContextFrame(parser, ion_type, depth + 1))
        elif event_type is IonEventType.CONTAINER_END:
            assert ion_type is ctx_type
            assert depth > 0
            context_stack.pop()


