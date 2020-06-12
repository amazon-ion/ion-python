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

"""Writer for raw binary Ion values, without symbol table management."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from datetime import datetime
from decimal import Decimal, localcontext
from functools import partial

import six
import struct

from amazon.ion.equivalence import _is_float_negative_zero
from amazon.ion.symbols import SymbolToken
from .core import IonEventType, IonType, DataEvent, Transition, TimestampPrecision, TIMESTAMP_FRACTION_PRECISION_FIELD, \
    MICROSECOND_PRECISION, TIMESTAMP_PRECISION_FIELD, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, Timestamp
from .util import coroutine, total_seconds, Enum
from .writer import NOOP_WRITER_EVENT, WriteEventType, \
                    writer_trampoline, partial_transition, serialize_scalar, \
                    validate_scalar_value, illegal_state_null
from .writer_binary_raw_fields import _write_varuint, _write_uint, _write_varint, _write_int


class _TypeIds(Enum):
    NULL = 0x00
    BOOL_FALSE = 0x10
    BOOL_TRUE = 0x11
    POS_INT = 0x20
    NEG_INT = 0x30
    FLOAT = 0x40
    DECIMAL = 0x50
    TIMESTAMP = 0x60
    SYMBOL = 0x70
    STRING = 0x80
    CLOB = 0x90
    BLOB = 0xA0
    LIST = 0xB0
    SEXP = 0xC0
    STRUCT = 0xD0
    ANNOTATION_WRAPPER = 0xE0


class _Zeros(Enum):
    """Single-octet encodings, represented by the type ID and a length nibble of zero.

    Notes:
        Blob, clob, list, and sexp also have single-octet encodings, but the current
        implementation handles these implicitly.
    """
    INT = _TypeIds.POS_INT  # Int zero is always encoded using the positive type ID.
    FLOAT = _TypeIds.FLOAT  # Represents zero.
    DECIMAL = _TypeIds.DECIMAL  # Represents 0d0.
    STRING = _TypeIds.STRING  # Represents the zero-length (empty) string.
    SYMBOL = _TypeIds.SYMBOL  # Represents symbol zero.
    STRUCT = _TypeIds.STRUCT  # Represents a struct with zero fields.

_VARINT_NEG_ZERO = 0xC0  # This refers to the variable-length signed integer subfield.
_INT_NEG_ZERO = 0x80  # This refers to the fixed-length signed integer subfield.

_LENGTH_FLOAT_64 = 0x08
_LENGTH_FIELD_THRESHOLD = 14
_LENGTH_FIELD_INDICATOR = 0x0E
_NULL_INDICATOR = 0x0F


def _null(tid):
    return bytearray([tid | _NULL_INDICATOR])


_NULLS = [
    _null(_TypeIds.NULL),
    _null(_TypeIds.BOOL_FALSE),
    _null(_TypeIds.POS_INT),
    _null(_TypeIds.FLOAT),
    _null(_TypeIds.DECIMAL),
    _null(_TypeIds.TIMESTAMP),
    _null(_TypeIds.SYMBOL),
    _null(_TypeIds.STRING),
    _null(_TypeIds.CLOB),
    _null(_TypeIds.BLOB),
    _null(_TypeIds.LIST),
    _null(_TypeIds.SEXP),
    _null(_TypeIds.STRUCT)
]

_BOOL_TRUE = bytearray([_TypeIds.BOOL_TRUE])
_BOOL_FALSE = bytearray([_TypeIds.BOOL_FALSE])


def _serialize_bool(ion_event):
    if ion_event.value:
        return _BOOL_TRUE
    else:
        return _BOOL_FALSE


def _write_length(buf, length, tid):
    if length < _LENGTH_FIELD_THRESHOLD:
        buf.append(tid | length)
    else:
        buf.append(tid | _LENGTH_FIELD_INDICATOR)
        _write_varuint(buf, length)


def _write_int_value(buf, tid, value):
    value_buf = bytearray()
    length = _write_uint(value_buf, value)
    _write_length(buf, length, tid)
    buf.extend(value_buf)


def _serialize_int(ion_event):
    buf = bytearray()
    value = ion_event.value
    validate_scalar_value(value, six.integer_types)
    if value == 0:
        buf.append(_Zeros.INT)
    else:
        if value < 0:
            value = -value
            tid = _TypeIds.NEG_INT
        else:
            tid = _TypeIds.POS_INT
        _write_int_value(buf, tid, value)
    return buf


def _serialize_float(ion_event):
    buf = bytearray()
    float_value = ion_event.value
    validate_scalar_value(float_value, float)
    # TODO Assess whether abbreviated encoding of zero is beneficial; it's allowed by spec.
    if float_value.is_integer() and float_value == 0.0 and not _is_float_negative_zero(float_value):
        buf.append(_Zeros.FLOAT)
    else:
        # TODO Add an option for 32-bit representation (length=4) per the spec.
        buf.append(_TypeIds.FLOAT | _LENGTH_FLOAT_64)
        encoded = struct.pack('>d', float_value)
        buf.extend(encoded)
    return buf


def _write_decimal_value(buf, exponent, coefficient, sign=0):
    length = _write_varint(buf, exponent)
    if coefficient:
        # The coefficient is non-zero, so the coefficient field is required.
        length += _write_int(buf, coefficient)
    elif sign:
        # The coefficient is negative zero.
        buf.append(_INT_NEG_ZERO)
        length += 1
    # Else the coefficient is positive zero and the field is omitted.
    return length


def _write_timestamp_fractional_seconds(buf, value):
    sign, digits, exponent = value.as_tuple()
    coefficient = int(value.scaleb(-exponent).to_integral_value())
    if coefficient == 0 and exponent >= 0:
        length = 0
    else:
        length = _write_decimal_value(buf, exponent, coefficient, sign)
    return length


def _serialize_decimal(ion_event):
    buf = bytearray()
    value = ion_event.value
    validate_scalar_value(value, Decimal)
    sign, digits, exponent = value.as_tuple()
    with localcontext() as context:
        # Adjusting precision for taking into account arbitrarily large/small
        # numbers
        context.prec = len(digits)
        coefficient = int(value.scaleb(-exponent).to_integral_value())
    if not sign and not exponent and not coefficient:
        # The value is 0d0; other forms of zero will fall through.
        buf.append(_Zeros.DECIMAL)
    else:
        value_buf = bytearray()
        length = _write_decimal_value(value_buf, exponent, coefficient, sign)
        _write_length(buf, length, _TypeIds.DECIMAL)
        buf.extend(value_buf)
    return buf


def _serialize_string(ion_event):
    buf = bytearray()
    value = ion_event.value
    validate_scalar_value(value, six.text_type)
    if not value:
        buf.append(_Zeros.STRING)
    else:
        value_buf = value.encode('utf-8')
        _write_length(buf, len(value_buf), _TypeIds.STRING)
        buf.extend(value_buf)
    return buf


def _serialize_symbol(ion_event):
    buf = bytearray()
    token = ion_event.value
    validate_scalar_value(token, SymbolToken)
    sid = token.sid
    if sid == 0:
        buf.append(_Zeros.SYMBOL)
    else:
        _write_int_value(buf, _TypeIds.SYMBOL, sid)
    return buf


def _serialize_lob_value(event, tid):
    buf = bytearray()
    value = event.value
    _write_length(buf, len(value), tid)
    buf.extend(value)
    return buf


_serialize_blob = partial(_serialize_lob_value, tid=_TypeIds.BLOB)
_serialize_clob = partial(_serialize_lob_value, tid=_TypeIds.CLOB)


def _serialize_timestamp(ion_event):
    buf = bytearray()
    dt = ion_event.value
    precision = getattr(dt, TIMESTAMP_PRECISION_FIELD, TimestampPrecision.SECOND)
    if precision is None:  # TODO should this defaulting be pushed into Timestamp itself?
        precision = TimestampPrecision.SECOND
    validate_scalar_value(dt, datetime)
    value_buf = bytearray()
    if dt.tzinfo is None:
        value_buf.append(_VARINT_NEG_ZERO)  # This signifies an unknown local offset.
        length = 1
    else:
        # Normalize to UTC and write the offset field.
        offset = dt.utcoffset()
        dt -= offset
        length = _write_varint(value_buf, int(total_seconds(offset) // 60))
    length += _write_varuint(value_buf, dt.year)
    if precision.includes_month:
        length += _write_varuint(value_buf, dt.month)
    if precision.includes_day:
        length += _write_varuint(value_buf, dt.day)
    if precision.includes_minute:
        length += _write_varuint(value_buf, dt.hour)
        length += _write_varuint(value_buf, dt.minute)
    if precision.includes_second:
        length += _write_varuint(value_buf, dt.second)
        if isinstance(ion_event.value, Timestamp):
            fractional_seconds = getattr(ion_event.value, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, None)
            if fractional_seconds is not None:
                length += _write_timestamp_fractional_seconds(value_buf, fractional_seconds)
        else:
            # This must be a normal datetime, which always has a range-validated microsecond value.
            length += _write_decimal_value(value_buf, -MICROSECOND_PRECISION, dt.microsecond)

    _write_length(buf, length, _TypeIds.TIMESTAMP)
    buf.extend(value_buf)
    return buf


_SERIALIZE_SCALAR_JUMP_TABLE = {
    IonType.NULL: illegal_state_null,
    IonType.BOOL: _serialize_bool,
    IonType.INT: _serialize_int,
    IonType.FLOAT: _serialize_float,
    IonType.DECIMAL: _serialize_decimal,
    IonType.TIMESTAMP: _serialize_timestamp,
    IonType.SYMBOL: _serialize_symbol,
    IonType.STRING: _serialize_string,
    IonType.CLOB: _serialize_clob,
    IonType.BLOB: _serialize_blob,
}


_serialize_scalar = partial(
    serialize_scalar, jump_table=_SERIALIZE_SCALAR_JUMP_TABLE, null_table=_NULLS
)


def _serialize_annotation_wrapper(output_buf, annotations):
    value_length = output_buf.current_container_length
    annot_length_buf = bytearray()
    annot_length = 0
    for annotation in annotations:
        annot_length += _write_varuint(annot_length_buf, annotation.sid)
    header = bytearray()
    length_buf = bytearray()
    length = _write_varuint(length_buf, annot_length) + annot_length + value_length
    _write_length(header, length, _TypeIds.ANNOTATION_WRAPPER)
    header.extend(length_buf)
    header.extend(annot_length_buf)
    output_buf.end_container(header)


def _serialize_container(output_buf, ion_event):
    ion_type = ion_event.ion_type
    length = output_buf.current_container_length
    header = bytearray()
    if ion_type is IonType.STRUCT:
        if length == 0:
            header.append(_Zeros.STRUCT)
        else:
            # TODO Support sorted field name symbols, per the spec.
            header.append(_TypeIds.STRUCT | _LENGTH_FIELD_INDICATOR)
            _write_varuint(header, length)
    else:
        tid = _TypeIds.LIST
        if ion_type is IonType.SEXP:
            tid = _TypeIds.SEXP
        _write_length(header, length, tid)
    output_buf.end_container(header)


_WRITER_EVENT_NEEDS_INPUT_EMPTY = DataEvent(WriteEventType.NEEDS_INPUT, b'')


@coroutine
def _raw_writer_coroutine(writer_buffer, depth=0, container_event=None,
                          whence=None, pending_annotations=None):

    def fail():
        raise TypeError('Invalid event: %s at depth %d' % (ion_event, depth))

    write_result = None
    while True:
        ion_event, self = (yield write_result)
        delegate = self
        curr_annotations = ion_event.annotations
        writer_event = _WRITER_EVENT_NEEDS_INPUT_EMPTY
        if depth > 0 and container_event.ion_type is IonType.STRUCT \
                and ion_event.event_type.begins_value:
            # A field name symbol ID is required at this position.
            sid_buffer = bytearray()
            _write_varuint(sid_buffer, ion_event.field_name.sid)  # Write the field name's symbol ID.
            writer_buffer.add_scalar_value(sid_buffer)
        if ion_event.event_type.begins_value and curr_annotations:
            writer_buffer.start_container()
        if ion_event.event_type is IonEventType.SCALAR:
            scalar_buffer = _serialize_scalar(ion_event)
            writer_buffer.add_scalar_value(scalar_buffer)
            if curr_annotations:
                _serialize_annotation_wrapper(writer_buffer, curr_annotations)
        elif ion_event.event_type is IonEventType.STREAM_END:
            if depth != 0:
                fail()
            for partial_value in writer_buffer.drain():
                yield partial_transition(partial_value, self)
            writer_event = NOOP_WRITER_EVENT
        elif ion_event.event_type is IonEventType.CONTAINER_START:
            if not ion_event.ion_type.is_container:
                raise TypeError('Expected container type')
            writer_buffer.start_container()
            delegate = _raw_writer_coroutine(writer_buffer, depth + 1,
                                             ion_event, self, curr_annotations)
        elif ion_event.event_type is IonEventType.CONTAINER_END:
            if depth < 1:
                fail()
            _serialize_container(writer_buffer, container_event)
            if pending_annotations:
                _serialize_annotation_wrapper(writer_buffer, pending_annotations)
            pending_annotations = None
            delegate = whence
        else:
            fail()
        write_result = Transition(writer_event, delegate)


def _raw_binary_writer(writer_buffer):
    """Returns a raw binary writer co-routine.

    Yields:
        DataEvent: serialization events to write out

        Receives :class:`amazon.ion.core.IonEvent`.
    """
    return writer_trampoline(_raw_writer_coroutine(writer_buffer))
