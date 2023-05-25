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
from datetime import timedelta
from decimal import Decimal, localcontext
from enum import IntEnum
from functools import partial
from io import BytesIO
from struct import unpack
from typing import NamedTuple, Optional, Sequence, Callable, Tuple, List

from .core import ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, ION_VERSION_MARKER_EVENT, \
    IonEventType, IonType, IonEvent, IonThunkEvent, \
    TimestampPrecision, Timestamp, OffsetTZInfo
from .exceptions import IonException
from .sliceable_buffer import SliceableBuffer, IncompleteReadError
from .util import coroutine
from .reader import ReadEventType
from .symbols import SYMBOL_ZERO_TOKEN, SymbolToken


class _TypeID(IntEnum):
    """Type IDs in the binary encoding which is distinct from the :class:`IonType` enum."""
    NULL = 0
    BOOL = 1
    POS_INT = 2
    NEG_INT = 3
    FLOAT = 4
    DECIMAL = 5
    TIMESTAMP = 6
    SYMBOL = 7
    STRING = 8
    CLOB = 9
    BLOB = 10
    LIST = 11
    SEXP = 12
    STRUCT = 13
    ANNOTATION = 14


# Mappings from type code to value type.
_TID_VALUE_TYPE_TABLE = (
    IonType.NULL,
    IonType.BOOL,
    IonType.INT,  # Positive integer
    IonType.INT,  # Negative integer
    IonType.FLOAT,
    IonType.DECIMAL,
    IonType.TIMESTAMP,
    IonType.SYMBOL,
    IonType.STRING,
    IonType.CLOB,
    IonType.BLOB,
    IonType.LIST,
    IonType.SEXP,
    IonType.STRUCT,
    None,  # Annotations do not have an Ion type.
)

# Streams are infinite.
_STREAM_REMAINING = Decimal('Inf')

_VAR_INT_VALUE_MASK = 0b01111111
_VAR_INT_VALUE_BITS = 7
_VAR_INT_SIGN_MASK = 0b01000000
_VAR_INT_SIGN_VALUE_MASK = 0b00111111
_VAR_INT_SIGNAL_MASK = 0b10000000

_SIGNED_INT_SIGN_MASK = 0b10000000
_SIGNED_INT_SIGN_VALUE_MASK = 0b01111111

_LENGTH_LN_MAX = 0xD
_LENGTH_FIELD_FOLLOWS = 0xE
_ALL_LENGTH_LNS = tuple(range(0, _LENGTH_FIELD_FOLLOWS + 1))
_NON_ZERO_LENGTH_LNS = tuple(range(1, _LENGTH_FIELD_FOLLOWS + 1))
_ANNOTATION_LENGTH_LNS = tuple(range(3, _LENGTH_FIELD_FOLLOWS + 1))

_IVM_START_OCTET = 0xE0
_IVM_TAIL = b'\x01\x00\xEA'
_IVM_TAIL_LEN = len(_IVM_TAIL)

# Type IDs for value types that are nullable.
_NULLABLE_TIDS = tuple(range(0, 14))
_NULL_LN = 0xF

_STATIC_SCALARS = (
    # Boolean
    (0x10, IonType.BOOL, False),
    (0x11, IonType.BOOL, True),

    # Zero-values
    (0x20, IonType.INT, 0),
    (0x40, IonType.FLOAT, 0.0),
    (0x50, IonType.DECIMAL, Decimal()),
    (0x70, IonType.SYMBOL, SYMBOL_ZERO_TOKEN),

    # Empty string/clob/blob
    (0x80, IonType.STRING, u''),
    (0x90, IonType.CLOB, b''),
    (0xA0, IonType.BLOB, b''),
)

# Mapping of valid LNs and the struct.unpack format strings
_FLOAT_LN_TABLE = {
    0x4: '>f',
    0x8: '>d'
}

_CONTAINER_TIDS = (_TypeID.LIST, _TypeID.SEXP, _TypeID.STRUCT)


def _gen_type_octet(hn, ln):
    """Generates a type octet from a high nibble and low nibble."""
    return (hn << 4) | ln


def _parse_var_int_components(buf, signed):
    """Parses a ``VarInt`` or ``VarUInt`` field from a file-like object."""
    value = 0
    sign = 1
    while True:
        ch = buf.read(1)
        if ch == '':
            raise IonException('Variable integer under-run')
        octet = ord(ch)
        if signed:
            if octet & _VAR_INT_SIGN_MASK:
                sign = -1
            value = octet & _VAR_INT_SIGN_VALUE_MASK
            signed = False
        else:
            value <<= _VAR_INT_VALUE_BITS
            value |= octet & _VAR_INT_VALUE_MASK

        if octet & _VAR_INT_SIGNAL_MASK:
            break
    return sign, value


def _parse_var_int(buf, signed):
    sign, value = _parse_var_int_components(buf, signed)
    return sign * value


def _parse_signed_int_components(buf):
    """Parses the remainder of a file-like object as a signed magnitude value.

    Returns:
        Returns a pair of the sign bit and the unsigned magnitude.
    """
    sign_bit = 0
    value = 0

    first = True
    while True:
        ch = buf.read(1)
        if ch == b'':
            break
        octet = ord(ch)
        if first:
            if octet & _SIGNED_INT_SIGN_MASK:
                sign_bit = 1
            value = octet & _SIGNED_INT_SIGN_VALUE_MASK
            first = False
        else:
            value <<= 8
            value |= octet

    return sign_bit, value


def _parse_decimal(buf):
    """Parses the remainder of a file-like object as a decimal."""
    exponent = _parse_var_int(buf, signed=True)
    sign_bit, coefficient = _parse_signed_int_components(buf)

    if coefficient == 0:
        # Handle the zero cases--especially negative zero
        value = Decimal((sign_bit, (0,), exponent))
    else:
        coefficient *= sign_bit and -1 or 1
        with localcontext() as context:
            # Adjusting precision for taking into account arbitrarily
            # large/small numbers
            context.prec = len(str(coefficient))
            value = Decimal(coefficient).scaleb(exponent)

    return value


def _parse_sid_iter(data):
    """Parses the given :class:`bytes` data as a list of :class:`SymbolToken`"""
    limit = len(data)
    buf = BytesIO(data)
    while buf.tell() < limit:
        sid = _parse_var_int(buf, signed=False)
        yield SymbolToken(None, sid)


class _ParserContext(NamedTuple):
    buffer: SliceableBuffer
    depth: int


def _invalid_handler(type_octet, ctx):
    """Placeholder handler for invalid type codes."""
    raise IonException(f'Invalid type octet: {type_octet}')


def _var_uint_parser(buffer):
    """
    Parse a Var UInt.

    Return (value, byte_ct) where value is the integer value of the VarUInt
    and byte_ct is the length of the VarUInt.
    """
    value = 0
    while True:
        (octet, buffer) = buffer.read_byte()
        value <<= _VAR_INT_VALUE_BITS
        value |= octet & _VAR_INT_VALUE_MASK
        if octet & _VAR_INT_SIGNAL_MASK:
            break
    return value, buffer


def _var_uint_field_handler(handler, context: _ParserContext):
    """
    Parse the VarUInt length of a field, then delegate to the handler in order
    to parse the field.

    Return the parse result from the field handler.
    """
    (buffer, depth) = context
    length, buffer = _var_uint_parser(buffer)
    return handler(length, _ParserContext(buffer, depth))


def _ivm_handler(context: _ParserContext):
    (buffer, depth) = context
    if depth != 0:
        raise IonException("Ion version markers are only valid at the top-level!")

    (ivm_tail, buffer) = buffer.read_slice(_IVM_TAIL_LEN)
    if _IVM_TAIL != ivm_tail:
        raise IonException('Invalid version marker: %r' % ivm_tail)

    return ION_VERSION_MARKER_EVENT, buffer


def _nop_pad_handler(_, length, context: _ParserContext):
    if not length:
        return None, context.buffer

    (skipped, buffer) = context.buffer.skip(length)
    if skipped < length:
        raise IncompleteReadError("Couldn't complete skip!")

    return None, buffer


def _static_scalar_handler(ion_type, value, context: _ParserContext):
    return IonEvent(IonEventType.SCALAR, ion_type, value, depth=context.depth), context.buffer


def _length_scalar_handler(scalar_factory, ion_type, length, context: _ParserContext):
    """Handles scalars, ``scalar_factory`` is a function that returns a value or thunk."""
    (buffer, depth) = context
    if length == 0:
        data = b''
    else:
        (data, buffer) = buffer.read_slice(length)

    scalar = scalar_factory(data)
    if callable(scalar):
        event = IonThunkEvent(IonEventType.SCALAR, ion_type, scalar, depth=context.depth)
    else:
        event = IonEvent(IonEventType.SCALAR, ion_type, scalar, depth=context.depth)

    return event, buffer


def _annotation_handler(_, length, context: _ParserContext):
    (buffer, depth) = context
    init_size = buffer.size
    anno_length, buffer = _var_uint_parser(buffer)

    if anno_length < 1:
        raise IonException('Invalid annotation length subfield; annotation wrapper must have at least one annotation.')

    (anno_bytes, buffer) = buffer.read_slice(anno_length)
    annotations = tuple(_parse_sid_iter(anno_bytes))

    if length - (init_size - buffer.size) < 1:
        raise IonException("Invalid annotation length subfield; annotation wrapper must wrap non-zero length value.")

    event, buffer = _tlv_parser(_ParserContext(buffer, depth))

    # nop padding comes back as none
    if event is None:
        raise IonException("Cannot annotate nop padding!")

    if event.annotations:
        raise IonException("Cannot nest annotations!")

    actual_length = init_size - buffer.size
    if event.event_type is IonEventType.CONTAINER_START:
        actual_length += event.value
    if actual_length != length:
        raise IonException(f"Expected wrapped value to have length of {length} \
                but was {actual_length}")

    return event.derive_annotations(annotations), buffer


def _ordered_struct_start_handler(length, context: _ParserContext):
    if length < 2:
        raise IonException('Ordered structs (type ID 0xD1) must have at least one field name/value pair.')
    return IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT, value=length, depth=context.depth), context.buffer


def _container_start_handler(ion_type, length, context: _ParserContext):
    # todo: consider extension event to smuggle limit out!
    return IonEvent(IonEventType.CONTAINER_START, ion_type, value=length, depth=context.depth), context.buffer


def _ivm_parser(context: _ParserContext):
    """
    Parse and verify an IVM; used only at start of stream.
    """
    (buffer, depth) = context[0:3]
    (type_octet, buffer) = buffer.read_byte()
    if type_octet != _IVM_START_OCTET:
        raise IonException(
            f'Expected binary version marker, got: {bytes(type_octet)}')

    return _ivm_handler(_ParserContext(buffer, depth))


def _tlv_parser(context: _ParserContext):
    """
    Parse any acceptable top-level or sequence value.

    Validation that IVMs are only at the top-level is in the _ivm_handler.
    """
    (buffer, depth) = context
    (tid, buffer) = buffer.read_byte()
    return _HANDLER_DISPATCH_TABLE[tid](_ParserContext(buffer, depth))


def _struct_item_parser(context: _ParserContext):
    """
    Parse the field and value for an item in a struct.
    """
    (buffer, depth) = context
    field_sid, buffer = _var_uint_parser(buffer)
    event, buffer = _tlv_parser(_ParserContext(buffer, depth))

    if not event:
        return event, buffer
    else:
        return event.derive_field_name(SymbolToken(None, field_sid)), buffer


class _ContextFrame(NamedTuple):
    parser: Callable[[_ParserContext], Tuple[IonEvent, SliceableBuffer]]
    ion_type: Optional[IonType]
    depth: int
    limit: int


@coroutine
def stream_handler():
    """
    Handler for an Ion Binary value-stream.
    """
    buffer = SliceableBuffer.empty()
    # top-level context limit is -1 to denote no limit
    context_stack = deque([_ContextFrame(_tlv_parser, None, 0, -1)])
    cursor = 0
    ion_event = None
    skip_or_next = ReadEventType.NEXT
    expect_data = False
    # will get swapped out for tlv or struct parser in main loop
    parser = _ivm_parser
    parent_type = None
    depth = 0
    limit = -1

    # This is the main event loop for the parser coroutine.
    #
    # Each iteration begins by responding with prior ion_event (None initially)
    # and receiving the user's read event.
    #
    # Then there are two main parts:
    # 1/ handles the users' request, checking invariants and extending the
    #    buffer and/or skipping.
    # 2/ parses if possible then mutates state based on the results of parsing.
    #
    # You should think of them as distinct functions, inlined to avoid stack
    # push/pop overhead.
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

        ion_event = None
        if skip_or_next is ReadEventType.SKIP:
            if parent_type is None:
                raise TypeError("Skip is only allowed within an Ion Container (Struct, List, S-expression)")

            to_skip = limit - cursor
            (skipped, buffer) = buffer.skip(to_skip)
            cursor += skipped
            if cursor < limit:
                ion_event = ION_STREAM_INCOMPLETE_EVENT

        # part 2: parse and reset state

        # loop is to consume but suppress nop padding
        while not ion_event:
            # we might be at end of container
            if cursor == limit:
                ion_event = IonEvent(
                    IonEventType.CONTAINER_END,
                    parent_type,
                    depth=depth - 1)
            # parsing is fun, let's do that!
            else:
                try:
                    (ion_event, new_buff) = parser(_ParserContext(buffer, depth))
                    cursor += buffer.size - new_buff.size
                    buffer = new_buff
                    if 0 < limit < cursor:
                        raise IonException("Passed limit of current container!")
                except IncompleteReadError:
                    if depth == 0 and buffer.size == 0:
                        ion_event = ION_STREAM_END_EVENT
                    else:
                        ion_event = ION_STREAM_INCOMPLETE_EVENT

        event_type = ion_event.event_type
        if event_type.is_stream_signal:
            expect_data = True
        else:
            expect_data = False

            if event_type is IonEventType.CONTAINER_START:
                if ion_event.ion_type is IonType.STRUCT:
                    parser = _struct_item_parser
                else:
                    parser = _tlv_parser
                # we're appropriating "value" for the length of the container
                frame = _ContextFrame(parser, ion_event.ion_type, depth + 1, cursor + ion_event.value)
                context_stack.append(frame)
                ion_event = ion_event.derive_value(None)
            elif event_type is IonEventType.CONTAINER_END:
                context_stack.pop()

            (parser, parent_type, depth, limit) = context_stack[-1]


#
# Scalar Factories
#


def _rslice(data, rem, size):
    start = -rem
    end = start + size
    if end >= 0:
        end = None
    return data[slice(start, end)]


def _int_factory(sign, data):
    def parse_int():
        value = 0
        length = len(data)
        while length >= 8:
            segment = _rslice(data, length, 8)
            value <<= 64
            value |= unpack('>Q', segment)[0]
            length -= 8
        if length >= 4:
            segment = _rslice(data, length, 4)
            value <<= 32
            value |= unpack('>I', segment)[0]
            length -= 4
        if length >= 2:
            segment = _rslice(data, length, 2)
            value <<= 16
            value |= unpack('>H', segment)[0]
            length -= 2
        if length == 1:
            value <<= 8
            value |= data[-length]
        return sign * value
    return parse_int


def _float_factory(data):
    fmt = _FLOAT_LN_TABLE.get(len(data))
    if fmt is None:
        raise ValueError('Invalid data length for float: %d' % len(data))

    return lambda: unpack(fmt, data)[0]


def _decimal_factory(data):
    def parse_decimal():
        return _parse_decimal(BytesIO(data))

    return parse_decimal


def _timestamp_factory(data):
    def parse_timestamp():
        end = len(data)
        buf = BytesIO(data)

        precision = TimestampPrecision.YEAR
        off_sign, off_value = _parse_var_int_components(buf, signed=True)
        off_value *= off_sign
        if off_sign == -1 and off_value == 0:
            # -00:00 (unknown UTC offset) is a naive datetime.
            tz = None
        else:
            tz = OffsetTZInfo(timedelta(minutes=off_value))
        year = _parse_var_int(buf, signed=False)

        if buf.tell() == end:
            month = 1
        else:
            month = _parse_var_int(buf, signed=False)
            precision = TimestampPrecision.MONTH

        if buf.tell() == end:
            day = 1
        else:
            day = _parse_var_int(buf, signed=False)
            precision = TimestampPrecision.DAY

        if buf.tell() == end:
            hour = 0
            minute = 0
        else:
            hour = _parse_var_int(buf, signed=False)
            minute = _parse_var_int(buf, signed=False)
            precision = TimestampPrecision.MINUTE

        if buf.tell() == end:
            second = 0
        else:
            second = _parse_var_int(buf, signed=False)
            precision = TimestampPrecision.SECOND

        if buf.tell() == end:
            fraction = None
        else:
            fraction = _parse_decimal(buf)
            fraction_exponent = fraction.as_tuple().exponent
            if fraction == 0 and fraction_exponent > -1:
                # According to the spec, fractions with coefficients of zero and exponents >= zero are ignored.
                fraction = None

        return Timestamp.adjust_from_utc_fields(
            year, month, day,
            hour, minute, second, None,
            tz,
            precision=precision, fractional_precision=None, fractional_seconds=fraction
        )

    return parse_timestamp


def _symbol_factory(data):
    parse_sid = _int_factory(1, data)

    def parse_symbol():
        sid = parse_sid()
        return SymbolToken(None, sid)

    return parse_symbol


def _string_factory(data):
    return lambda: str(data, 'utf-8')


def _lob_factory(data):
    # Lobs are a trivial return of the byte data.
    return data


#
# Binding Functions
#


# Handler table for type octet to handler function, initialized with the
# invalid handler for all octets.
_HANDLER_DISPATCH_TABLE: List[Callable] = [partial(_invalid_handler, i) for i in range(256)]


def _bind_null_handlers():
    for tid in _NULLABLE_TIDS:
        type_octet = _gen_type_octet(tid, _NULL_LN)
        ion_type = _TID_VALUE_TYPE_TABLE[tid]
        _HANDLER_DISPATCH_TABLE[type_octet] = partial(_static_scalar_handler, ion_type, None)


def _bind_static_scalar_handlers():
    for type_octet, ion_type, value in _STATIC_SCALARS:
        _HANDLER_DISPATCH_TABLE[type_octet] = partial(_static_scalar_handler, ion_type, value)


def _bind_length_handlers(tids, user_handler, lns):
    """Binds a set of handlers with the given factory.

    Args:
        tids (Sequence[int]): The Type IDs to bind to.
        user_handler (Callable): A function that takes as its parameters
            :class:`IonType`, ``length``, and the ``ctx`` context
            returning a ParseResult.
        lns (Sequence[int]): The low-nibble lengths to bind to.
    """
    for tid in tids:
        for ln in lns:
            type_octet = _gen_type_octet(tid, ln)
            ion_type = _TID_VALUE_TYPE_TABLE[tid]
            if ln == 1 and ion_type is IonType.STRUCT:
                handler = partial(_var_uint_field_handler, _ordered_struct_start_handler)
            elif ln < _LENGTH_FIELD_FOLLOWS:
                # Directly partially bind length.
                handler = partial(user_handler, ion_type, ln)
            else:
                # Delegate to length field parsing first.
                handler = partial(_var_uint_field_handler, partial(user_handler, ion_type))
            _HANDLER_DISPATCH_TABLE[type_octet] = handler


def _bind_length_scalar_handlers(tids, scalar_factory, lns=_NON_ZERO_LENGTH_LNS):
    """Binds a set of scalar handlers for an inclusive range of low-nibble values.

    Args:
        tids (Sequence[int]): The Type IDs to bind to.
        scalar_factory (Callable): The factory for the scalar parsing function.
            This function can itself return a function representing a thunk to defer the
            scalar parsing or a direct value.
        lns (Sequence[int]): The low-nibble lengths to bind to.
    """
    handler = partial(_length_scalar_handler, scalar_factory)
    return _bind_length_handlers(tids, handler, lns)


# Populate the actual handlers.
_HANDLER_DISPATCH_TABLE[_IVM_START_OCTET] = _ivm_handler
_bind_null_handlers()
_bind_static_scalar_handlers()
_bind_length_scalar_handlers([_TypeID.POS_INT], partial(_int_factory, 1))
_bind_length_scalar_handlers([_TypeID.NEG_INT], partial(_int_factory, -1))
_bind_length_scalar_handlers([_TypeID.FLOAT], _float_factory, lns=_FLOAT_LN_TABLE.keys())
_bind_length_scalar_handlers([_TypeID.DECIMAL], _decimal_factory)
_bind_length_scalar_handlers([_TypeID.TIMESTAMP], _timestamp_factory)
_bind_length_scalar_handlers([_TypeID.STRING], _string_factory)
_bind_length_scalar_handlers([_TypeID.SYMBOL], _symbol_factory)
_bind_length_scalar_handlers([_TypeID.CLOB, _TypeID.BLOB], _lob_factory)
_bind_length_handlers(_CONTAINER_TIDS, _container_start_handler, _ALL_LENGTH_LNS)
_bind_length_handlers([_TypeID.ANNOTATION], _annotation_handler, _ANNOTATION_LENGTH_LNS)
_bind_length_handlers([_TypeID.NULL], _nop_pad_handler, _ALL_LENGTH_LNS)

# Make immutable.
_HANDLER_DISPATCH_TABLE = tuple(_HANDLER_DISPATCH_TABLE)

binary_reader = stream_handler
