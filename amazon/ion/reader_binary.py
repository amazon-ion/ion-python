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

from datetime import timedelta
from decimal import Decimal, localcontext
from enum import IntEnum
from functools import partial
from io import BytesIO
from struct import unpack

from .core import ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, ION_VERSION_MARKER_EVENT,\
                  IonEventType, IonType, IonEvent, IonThunkEvent, Transition, \
                  TimestampPrecision, Timestamp, OffsetTZInfo
from .exceptions import IonException
from .util import coroutine, record
from .reader import reader_trampoline, BufferQueue, ReadEventType
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
    from decimal import localcontext
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


class _HandlerContext(record(
        'position', 'limit', 'queue', 'field_name', 'annotations', 'depth', 'whence'
    )):
    """A context for a handler co-routine.

    Args:
        position (int): The offset of the *start* of the data being parsed.
        limit (Optional[int]): The logical offset that represents the *end* of the container.
        queue (BufferQueue): The data source for the handler.
        field_name (Optional[SymbolToken]): The token representing the field name for the handled
            value.
        annotations (Optional[Sequence[SymbolToken]]): The sequence of annotations tokens
            for the value to be parsed.
        depth (int): the depth of the parser.
        whence (Coroutine): The reference to the co-routine that this handler should delegate
            back to when the handler is logically done.
    """
    @property
    def remaining(self):
        """Determines how many bytes are remaining in the current context."""
        if self.depth == 0:
            return _STREAM_REMAINING
        return self.limit - self.queue.position

    def read_data_transition(self, length, whence=None,
                             skip=False, stream_event=ION_STREAM_INCOMPLETE_EVENT):
        """Returns an immediate event_transition to read a specified number of bytes."""
        if whence is None:
            whence = self.whence

        return Transition(
            None, _read_data_handler(length, whence, self, skip, stream_event)
        )

    def event_transition(self, event_cls, event_type,
                         ion_type=None, value=None, annotations=None, depth=None, whence=None):
        """Returns an ion event event_transition that yields to another co-routine.

        If ``annotations`` is not specified, then the ``annotations`` are the annotations of this
        context.
        If ``depth`` is not specified, then the ``depth`` is depth of this context.
        If ``whence`` is not specified, then ``whence`` is the whence of this context.
        """
        if annotations is None:
            annotations = self.annotations
        if annotations is None:
            annotations = ()
        if not (event_type is IonEventType.CONTAINER_START) and \
                annotations and (self.limit - self.queue.position) != 0:
            # This value is contained in an annotation wrapper, from which its limit was inherited. It must have
            # reached, but not surpassed, that limit.
            raise IonException('Incorrect annotation wrapper length.')

        if depth is None:
            depth = self.depth

        if whence is None:
            whence = self.whence

        return Transition(
            event_cls(event_type, ion_type, value, self.field_name, annotations, depth),
            whence
        )

    def immediate_transition(self, delegate=None):
        """Returns an immediate transition to another co-routine.

        If ``delegate`` is not specified, then ``whence`` is the delegate.
        """
        if delegate is None:
            delegate = self.whence

        return Transition(None, delegate)

    def derive_container_context(self, length, add_depth=1):
        new_limit = self.queue.position + length
        return _HandlerContext(
            self.position,
            new_limit,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth + add_depth,
            self.whence
        )

    def derive_child_context(self, position, field_name, annotations, whence):
        return _HandlerContext(
            position,
            self.limit,
            self.queue,
            field_name,
            annotations,
            self.depth,
            whence
        )


#
# Handler Co-routine Factories
#


def _create_delegate_handler(delegate):
    """Creates a handler function that creates a co-routine that can yield once with the given
    positional arguments to the delegate as a transition.

    Args:
        delegate (Coroutine): The co-routine to delegate to.

    Returns:
        A :class:`callable` handler that returns a co-routine that ignores the data it receives
        and sends with the arguments given to the handler as a :class:`Transition`.
    """
    @coroutine
    def handler(*args):
        yield
        yield delegate.send(Transition(args, delegate))

    return handler


@coroutine
def _read_data_handler(length, whence, ctx, skip=False, stream_event=ION_STREAM_INCOMPLETE_EVENT):
    """Creates a co-routine for retrieving data up to a requested size.

    Args:
        length (int): The minimum length requested.
        whence (Coroutine): The co-routine to return to after the data is satisfied.
        ctx (_HandlerContext): The context for the read.
        skip (Optional[bool]): Whether the requested number of bytes should be skipped.
        stream_event (Optional[IonEvent]): The stream event to return if no bytes are read or
            available.
    """
    trans = None
    queue = ctx.queue

    if length > ctx.remaining:
        raise IonException('Length overrun: %d bytes, %d remaining' % (length, ctx.remaining))

    # Make sure to check the queue first.
    queue_len = len(queue)
    if queue_len > 0:
        # Any data available means we can only be incomplete.
        stream_event = ION_STREAM_INCOMPLETE_EVENT
    length -= queue_len

    if skip:
        # For skipping we need to consume any remnant in the buffer queue.
        if length >= 0:
            queue.skip(queue_len)
        else:
            queue.skip(queue_len + length)

    while True:
        data_event, self = (yield trans)
        if data_event is not None and data_event.data is not None:
            data = data_event.data
            data_len = len(data)
            if data_len > 0:
                # We got something so we can only be incomplete.
                stream_event = ION_STREAM_INCOMPLETE_EVENT
            length -= data_len
            if not skip:
                queue.extend(data)
            else:
                pos_adjustment = data_len
                if length < 0:
                    pos_adjustment += length
                    # More data than we need to skip, so make sure to accumulate that remnant.
                    queue.extend(data[length:])
                queue.position += pos_adjustment
        if length <= 0:
            # We got all the data we need, go back immediately
            yield Transition(None, whence)

        trans = Transition(stream_event, self)


@coroutine
def _invalid_handler(type_octet, ctx):
    """Placeholder co-routine for invalid type codes."""
    yield
    raise IonException('Invalid type octet: 0x%02X' % type_octet)


@coroutine
def _var_uint_field_handler(handler, ctx):
    """Handler co-routine for variable unsigned integer fields that.

    Invokes the given ``handler`` function with the read field and context,
    then immediately yields to the resulting co-routine.
    """
    _, self = yield
    queue = ctx.queue
    value = 0
    while True:
        if len(queue) == 0:
            # We don't know when the field ends, so read at least one byte.
            yield ctx.read_data_transition(1, self)
        octet = queue.read_byte()
        value <<= _VAR_INT_VALUE_BITS
        value |= octet & _VAR_INT_VALUE_MASK
        if octet & _VAR_INT_SIGNAL_MASK:
            break
    yield ctx.immediate_transition(handler(value, ctx))


@coroutine
def _ivm_handler(ctx):
    _, self = yield

    if ctx.depth != 0:
        raise IonException('IVM encountered below top-level')

    yield ctx.read_data_transition(_IVM_TAIL_LEN, self)
    ivm_tail = ctx.queue.read(_IVM_TAIL_LEN)
    if _IVM_TAIL != ivm_tail:
        raise IonException('Invalid IVM tail: %r' % ivm_tail)
    yield Transition(ION_VERSION_MARKER_EVENT, ctx.whence)


@coroutine
def _nop_pad_handler(ion_type, length, ctx):
    yield

    if ctx.field_name is not None and ctx.field_name != SYMBOL_ZERO_TOKEN:
        raise IonException(
            'Cannot have NOP pad with non-zero symbol field, field SID %d' % ctx.field_name)

    if length > 0:
        yield ctx.read_data_transition(length, ctx.whence, skip=True)

    # Nothing to skip, so we just go back from whence we came...
    yield ctx.immediate_transition()


@coroutine
def _static_scalar_handler(ion_type, value, ctx):
    yield
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ion_type, value)


@coroutine
def _length_scalar_handler(scalar_factory, ion_type, length, ctx):
    """Handles scalars, ``scalar_factory`` is a function that returns a value or thunk."""
    _, self = yield
    if length == 0:
        data = b''
    else:
        yield ctx.read_data_transition(length, self)
        data = ctx.queue.read(length)

    scalar = scalar_factory(data)
    event_cls = IonEvent
    if callable(scalar):
        # TODO Wrap the exception to get context position.
        event_cls = IonThunkEvent
    yield ctx.event_transition(event_cls, IonEventType.SCALAR, ion_type, scalar)


@coroutine
def _start_type_handler(field_name, whence, ctx, expects_ivm=False, at_top=False, annotations=None):
    _, self = yield

    child_position = ctx.queue.position

    # Read type byte.
    if at_top:
        incomplete_event = ION_STREAM_END_EVENT
    else:
        incomplete_event = ION_STREAM_INCOMPLETE_EVENT
    yield ctx.read_data_transition(1, self, stream_event=incomplete_event)
    type_octet = ctx.queue.read_byte()

    if expects_ivm and type_octet != _IVM_START_OCTET:
        raise IonException(
            'Expected binary version marker, got: %02X' % type_octet)

    handler = _HANDLER_DISPATCH_TABLE[type_octet]
    child_ctx = ctx.derive_child_context(child_position, field_name, annotations, whence)
    yield ctx.immediate_transition(handler(child_ctx))


@coroutine
def _annotation_handler(ion_type, length, ctx):
    """Handles annotations.  ``ion_type`` is ignored."""
    _, self = yield
    self_handler = _create_delegate_handler(self)

    if ctx.annotations is not None:
        raise IonException('Annotation cannot be nested in annotations')

    # We have to replace our context for annotations specifically to encapsulate the limit
    ctx = ctx.derive_container_context(length, add_depth=0)
    # Immediately read the length field and the annotations
    (ann_length, _), _ = yield ctx.immediate_transition(
        _var_uint_field_handler(self_handler, ctx)
    )

    if ann_length < 1:
        raise IonException('Invalid annotation length subfield; annotation wrapper must have at least one annotation.')

    # Read/parse the annotations.
    yield ctx.read_data_transition(ann_length, self)
    ann_data = ctx.queue.read(ann_length)
    annotations = tuple(_parse_sid_iter(ann_data))

    if ctx.limit - ctx.queue.position < 1:
        # There is no space left for the 'value' subfield, which is required.
        raise IonException('Incorrect annotation wrapper length.')

    # Go parse the start of the value but go back to the real parent container.
    yield ctx.immediate_transition(
        _start_type_handler(ctx.field_name, ctx.whence, ctx, annotations=annotations)
    )


@coroutine
def _ordered_struct_start_handler(handler, ctx):
    """Handles the special case of ordered structs, specified by the type ID 0xD1.

    This coroutine's only purpose is to ensure that the struct in question declares at least one field name/value pair,
    as required by the spec.
    """
    _, self = yield
    self_handler = _create_delegate_handler(self)
    (length, _), _ = yield ctx.immediate_transition(
        _var_uint_field_handler(self_handler, ctx)
    )
    if length < 2:
        # A valid field name/value pair is at least two octets: one for the field name SID and one for the value.
        raise IonException('Ordered structs (type ID 0xD1) must have at least one field name/value pair.')
    yield ctx.immediate_transition(handler(length, ctx))


@coroutine
def _container_start_handler(ion_type, length, ctx):
    """Handles container delegation."""
    _, self = yield

    container_ctx = ctx.derive_container_context(length)
    if ctx.annotations and ctx.limit != container_ctx.limit:
        # 'ctx' is the annotation wrapper context. `container_ctx` represents the wrapper's 'value' subfield. Their
        # limits must match.
        raise IonException('Incorrect annotation wrapper length.')
    delegate = _container_handler(ion_type, container_ctx)

    # We start the container, and transition to the new container processor.
    yield ctx.event_transition(
        IonEvent, IonEventType.CONTAINER_START, ion_type, value=None, whence=delegate
    )


@coroutine
def _container_handler(ion_type, ctx):
    """Handler for the body of a container (or the top-level stream).

    Args:
        ion_type (Optional[IonType]): The type of the container or ``None`` for the top-level.
        ctx (_HandlerContext): The context for the container.
    """
    transition = None
    first = True
    at_top = ctx.depth == 0
    while True:
        data_event, self = (yield transition)
        if data_event is not None and data_event.type is ReadEventType.SKIP:
            yield ctx.read_data_transition(ctx.remaining, self, skip=True)

        if ctx.queue.position == ctx.limit:
            # We are at the end of the container.
            # Yield the close event and go to enclosing container.
            yield Transition(
                IonEvent(IonEventType.CONTAINER_END, ion_type, depth=ctx.depth-1),
                ctx.whence
            )

        if ion_type is IonType.STRUCT:
            # Read the field name.
            self_handler = _create_delegate_handler(self)
            (field_sid, _), _ = yield ctx.immediate_transition(
                _var_uint_field_handler(self_handler, ctx)
            )
            field_name = SymbolToken(None, field_sid)
        else:
            field_name = None

        expects_ivm = first and at_top
        transition = ctx.immediate_transition(
            _start_type_handler(field_name, self, ctx, expects_ivm, at_top=at_top)
        )
        first = False


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
    return lambda: data.decode('utf-8')


def _lob_factory(data):
    # Lobs are a trivial return of the byte data.
    return data


#
# Binding Functions
#


# Handler table for type octet to handler co-routine.
_HANDLER_DISPATCH_TABLE = [None] * 256


def _bind_invalid_handlers():
    """Seeds the co-routine table with all invalid handlers."""
    for type_octet in range(256):
        _HANDLER_DISPATCH_TABLE[type_octet] = partial(_invalid_handler, type_octet)


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
            returning a co-routine.
        lns (Sequence[int]): The low-nibble lengths to bind to.
    """
    for tid in tids:
        for ln in lns:
            type_octet = _gen_type_octet(tid, ln)
            ion_type = _TID_VALUE_TYPE_TABLE[tid]
            if ln == 1 and ion_type is IonType.STRUCT:
                handler = partial(_ordered_struct_start_handler, partial(user_handler, ion_type))
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

# First seed all type byte handlers with invalid.
_bind_invalid_handlers()

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


def raw_reader(queue=None):
    """Returns a raw binary reader co-routine.

    Args:
        queue (Optional[BufferQueue]): The buffer read data for parsing, if ``None`` a
            new one will be created.

    Yields:
        IonEvent: parse events, will have an event type of ``INCOMPLETE`` if data is needed
            in the middle of a value or ``STREAM_END`` if there is no data **and** the parser
            is not in the middle of parsing a value.

            Receives :class:`DataEvent`, with :class:`ReadEventType` of ``NEXT`` or ``SKIP``
            to iterate over values, or ``DATA`` if the last event was a ``INCOMPLETE``
            or ``STREAM_END`` event type.

            ``SKIP`` is only allowed within a container. A reader is *in* a container
            when the ``CONTAINER_START`` event type is encountered and *not in* a container
            when the ``CONTAINER_END`` event type for that container is encountered.
    """
    if queue is None:
        queue = BufferQueue()
    ctx = _HandlerContext(
        position=0,
        limit=None,
        queue=queue,
        field_name=None,
        annotations=None,
        depth=0,
        whence=None
    )

    return reader_trampoline(_container_handler(None, ctx))

binary_reader = raw_reader
