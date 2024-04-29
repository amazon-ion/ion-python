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

from collections import deque
from decimal import Decimal
from typing import Optional, NamedTuple, Tuple

from amazon.ion.core import IonType, IonEvent, \
    IonEventType, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, IonThunkEvent, ION_VERSION_MARKER_EVENT, Timestamp
from amazon.ion.exceptions import IonException
from amazon.ion.protons import *
from amazon.ion.reader import ReadEventType
from amazon.ion.sliceable_buffer import SliceableBuffer
from amazon.ion.symbols import SymbolToken
from amazon.ion.util import coroutine

def _whitespace(byte):
    return byte in bytearray(b" \t\n\r\v\f")


_stop = peek(
    alt(
        one_of(b"{}[](),\"\' \t\n\r\v\f"),
        is_eof()))


def tag_stop(tag_value):
    return terminated(tag(tag_value), _stop)


def is_empty(buffer: SliceableBuffer):
    if buffer.size:
        return ParseResult(ResultType.FAILURE, buffer, None)
    else:
        return ParseResult(ResultType.SUCCESS, buffer, None)


# todo: not nan, null, true or false for field keys and annotations
_identifier_symbol = \
    preceded(
        peek(one_of(b"$_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")),
        take_while(lambda b: b in bytearray(b"$_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")))

_quoted_symbol = delim(
        tag(b"'"),
        take_while(lambda b: b != ord(b"'")),
        tag(b"'"))

_quoted_string = delim(
        tag(b'"'),
        take_while(lambda b: b != ord(b'"')),
        tag(b'"'))

def _is_digit(byte):
    return byte in bytearray(b"0123456789")

# todo: negative numbers
_integer = terminated(
    alt(
        tag(b"0"),
        preceded(
            peek(one_of(b"123456789")),
            take_while(_is_digit))),
    _stop)

_timestamp = pair(
    take_while_n(4, _is_digit),
    alt(
        tag_stop(b"T"),
        preceded(
            tag(b"-"),
            pair(
                take_while_n(2, _is_digit),
                alt(
                    tag_stop(b"T"),
                    preceded(
                        tag(b"-"),
                        pair(
                            take_while_n(2, _is_digit),
                            alt(
                                tag_stop(b"T"),
                                _stop))))))))

# todo: lazy
def _to_timestamp_event(value):
    (year, rem1) = value
    if type(rem1) is tuple:
        (month, rem2) = rem1
        if type(rem2) is tuple:
            (day, rem3) = rem2
            return IonEvent(IonEventType.SCALAR, IonType.TIMESTAMP, Timestamp(int(bytes(year)), int(bytes(month)), int(bytes(day))))
        else:
            return IonEvent(IonEventType.SCALAR, IonType.TIMESTAMP, Timestamp(int(bytes(year)), int(bytes(month))))
    else:
        return IonEvent(IonEventType.SCALAR, IonType.TIMESTAMP, Timestamp(int(bytes(year)),))

_decimal = terminated(
    delim_pair(
        alt(
            tag(b"0"),
            preceded(
                peek(one_of(b"123456789")),
                take_while(_is_digit))),
        # todo: this is a huge hack, but it works for my hkc dataset
        alt(tag(b"."), tag(b"d")),
        take_while(_is_digit)),
    _stop)


# todo: lazy
def _to_decimal_event(value):
    (integer, fraction) = value
    dec_str = bytes(integer) + b"." + bytes(fraction)
    #print(dec_str)
    return IonEvent(IonEventType.SCALAR, IonType.DECIMAL, Decimal(dec_str.decode("utf-8")))

_value_parsec = alt(
        constant(is_empty, ION_STREAM_END_EVENT),
        constant(tag_stop(b"nan"), IonEvent(IonEventType.SCALAR, IonType.FLOAT, float("nan"))),
        constant(tag_stop(b"+inf"), IonEvent(IonEventType.SCALAR, IonType.FLOAT, float("+inf"))),
        constant(tag_stop(b"-inf"), IonEvent(IonEventType.SCALAR, IonType.FLOAT, float("-inf"))),

        constant(tag_stop(b"null"), IonEvent(IonEventType.SCALAR, IonType.NULL, None)),
        constant(tag_stop(b"true"), IonEvent(IonEventType.SCALAR, IonType.BOOL, True)),
        constant(tag_stop(b"false"), IonEvent(IonEventType.SCALAR, IonType.BOOL, False)),

        # ignore(delim(tag_stop(b"//"), take_while(lambda b: b != ord(b"\n")), ),
        map_value(_timestamp, _to_timestamp_event),
        map_value(_decimal, _to_decimal_event),

        map_value(_integer, lambda v: IonThunkEvent(IonEventType.SCALAR, IonType.INT, lambda: int(bytes(v)))),


        # must rule out annotation before doing symbol values, uggh.

        # must come after the above
        map_value(alt(_identifier_symbol, _quoted_symbol),
                  lambda v: IonThunkEvent(IonEventType.SCALAR, IonType.SYMBOL, lambda: SymbolToken(bytes(v).decode('utf-8'), None))),

        map_value(_quoted_string,
              lambda v: IonThunkEvent(IonEventType.SCALAR, IonType.STRING,
                                      lambda: bytes(v).decode('utf-8'), None)),

        constant(peek(tag(b"{")), IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT)),
        constant(tag(b"["), IonEvent(IonEventType.CONTAINER_START, IonType.LIST)),
        constant(tag(b"("), IonEvent(IonEventType.CONTAINER_START, IonType.SEXP)),

        constant(tag(b"}"), IonEvent(IonEventType.CONTAINER_END, IonType.STRUCT)),
        constant(tag(b"]"), IonEvent(IonEventType.CONTAINER_END, IonType.LIST)),
        constant(tag(b")"), IonEvent(IonEventType.CONTAINER_END, IonType.SEXP)),
)

_tlv_parsec = alt(constant(tag_stop(b"$ion_1_0"), ION_VERSION_MARKER_EVENT), _value_parsec)

_take_while_whitespace = take_while(_whitespace)


def _trim_whitespace(buffer: SliceableBuffer):
    result = _take_while_whitespace(buffer)
    return result.buffer


def whitespace_then(parser: Parser) -> Parser:
    return preceded(_take_while_whitespace, parser)


def _map_result(result: ParseResult, buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    if result.type is ResultType.SUCCESS:
        return result.value, result.buffer
    if result.type is ResultType.INCOMPLETE:
        if not buffer.size:
            return ION_STREAM_END_EVENT, buffer
        return ION_STREAM_INCOMPLETE_EVENT, buffer
    if result.type is ResultType.FAILURE:
        if not buffer.size and buffer.is_eof():
            return ION_STREAM_END_EVENT, buffer
        raise IonException(f"Parse failed on {buffer}")


def _tlv_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    buffer = _trim_whitespace(buffer)
    result = _tlv_parsec(buffer)

    return _map_result(result, buffer)


_list_parsec = alt(
    constant(tag(b"]"), IonEvent(IonEventType.CONTAINER_END, IonType.LIST)),
    terminated(
        _value_parsec,
        whitespace_then(alt(tag(b','), peek(tag(b']'))))))


def _list_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    buffer = _trim_whitespace(buffer)
    result = _list_parsec(buffer)

    return _map_result(result, buffer)

_field_name = alt(
    _identifier_symbol,
    _quoted_symbol,
    # _quoted_string,
    # todo: long quoted string, because why!?!?!?
)

def opt(parser: Parser) -> Parser:
    def p(buffer: SliceableBuffer):
        result = parser(buffer)
        if result.type is ResultType.SUCCESS:
            return result
        if result.type is ResultType.FAILURE:
            return ParseResult(ResultType.SUCCESS, result.buffer)
        return result
    return p

# todo: this is all eff'ed, basically it doesn't work when the _value_parsec does not fully consume the
#       buffer. I didn't build this for these to be re-entrant so it's both an issue with expecting a comma or brace after a value
#       and an issue when we close the container, then we're at a place where we have a whitespace and comma from a previous invocation.
# _struct_parsec = alt(
#     constant(tag(b"}"), IonEvent(IonEventType.CONTAINER_END, IonType.STRUCT)),
#     map_value(
#         terminated(
#             delim_pair(
#                 _field_name,
#                 whitespace_then(tag(b':')),
#                 whitespace_then(_value_parsec)),
#             # todo: i think there's a bug here with whitespace after a trailing comma
#             debug("struct end", whitespace_then(alt(tag(b','), peek(tag(b'}')))))),
#         # todo: lazy field name
#         lambda pair: pair[1].derive_field_name(SymbolToken(bytes(pair[0]).decode("utf-8"), None))))

# todo: empty structs don't work, because they're not parsed as a container_end event
_struct_parsec = alt(
    whitespace_then(constant(tag(b"}"), IonEvent(IonEventType.CONTAINER_END, IonType.STRUCT))),
    preceded(
        whitespace_then(alt(tag(b"{"), tag(b","))),
        map_value(
                delim_pair(
                    whitespace_then(_field_name),
                    whitespace_then(tag(b':')),
                    whitespace_then(_value_parsec)),
            # todo: lazy field name
            lambda pair: pair[1].derive_field_name(SymbolToken(bytes(pair[0]).decode("utf-8"), None)))))


def _struct_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    buffer = _trim_whitespace(buffer)
    result = _struct_parsec(buffer)

    return _map_result(result, buffer)

# todo this is far from complete
_operators = b"!#%&*+-./;<=>?@^`|~"
_whitespace_or_operator = alt(one_of(b" "), one_of(_operators))

_sexp_parsec = alt(
    constant(tag(b")"), IonEvent(IonEventType.CONTAINER_END, IonType.SEXP)),
    terminated(
        whitespace_then(_value_parsec),
            alt(_whitespace_or_operator, peek(tag(b')')))))

def _sexp_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    buffer = _trim_whitespace(buffer)
    result = _sexp_parsec(buffer)

    return _map_result(result, buffer)


_container_parsers = [
    _list_parser,
    _sexp_parser,
    _struct_parser,
]


class _ContextFrame(NamedTuple):
    parser: Callable[[SliceableBuffer], Tuple[IonEvent, SliceableBuffer]]
    ion_type: Optional[IonType]
    depth: int


@coroutine
def text_stream_handler():
    """
    Handler for an Ion Text value-stream.
    """
    buffer: SliceableBuffer = SliceableBuffer.empty()
    context_stack = deque([_ContextFrame(_tlv_parser, None, 0)])
    ion_event = None
    skip_or_next = ReadEventType.NEXT
    expect_data = False
    incomplete = False

    while True:
        read_event = yield ion_event
        assert read_event is not None

        # part 1: handle user's read event
        if expect_data:
            if read_event.type is not ReadEventType.DATA:
                # flush it
                if incomplete:
                    # todo: a cleaner way to do this is to set a flush flag
                    #       and pass it to the parser as context...
                    buffer = buffer.eof()
                else:
                    raise TypeError("Data expected")
            else:
                # todo: this doesn't seem great.
                data = read_event.data.encode("utf-8") if type(read_event.data) is str else read_event.data
                buffer = buffer.extend(data)
        else:
            if read_event.type is ReadEventType.DATA:
                raise TypeError("Next or Skip expected")
            skip_or_next = read_event.type

        if skip_or_next is ReadEventType.SKIP:
            raise NotImplementedError("Skip is not supported")

        # part 2: do some lexxing
        (parser, ctx_type, depth) = context_stack[-1]
        (ion_event, buffer) = parser(buffer)

        # part 3: mutate state
        event_type = ion_event.event_type

        if event_type.is_stream_signal:
            expect_data = True
            incomplete = event_type is IonEventType.INCOMPLETE
        else:
            expect_data = False
            incomplete = False
            ion_type = ion_event.ion_type

            if event_type is IonEventType.CONTAINER_START:
                parser = _container_parsers[ion_type - IonType.LIST]
                context_stack.append(_ContextFrame(parser, ion_type, depth + 1))
            elif event_type is IonEventType.CONTAINER_END:
                assert ion_type is ctx_type
                assert depth > 0
                depth -= 1
                context_stack.pop()

            ion_event = ion_event.derive_depth(depth)
