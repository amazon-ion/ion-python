# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

"""Writer for raw binary Ion values, without symbol table management."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import struct
from decimal import Decimal
from datetime import datetime
from functools import partial

import six

from .writer_binary_raw_fields import _write_varuint, _write_uint, _write_varint, _write_int
from .core import IonEventType, IonType, DataEvent, Transition
from .util import coroutine, Enum
from .writer import writer_trampoline, partial_transition, WriteEventType


class _TypeIds(Enum):
    null = 0x00
    bool_false = 0x10
    bool_true = 0x11
    pos_int = 0x20
    neg_int = 0x30
    float = 0x40
    decimal = 0x50
    timestamp = 0x60
    symbol = 0x70
    string = 0x80
    clob = 0x90
    blob = 0xA0
    list = 0xB0
    sexp = 0xC0
    struct = 0xD0
    annotation_wrapper = 0xE0


class _Zeros(Enum):
    """Single-octet encodings, represented by the type ID and a length nibble of zero.

    Notes:
        Blob, clob, list, and sexp also have single-octet encodings, but the current implementation handles these
        implicitly.
    """
    int = _TypeIds.pos_int | 0x0  # Int zero is always encoded using the positive type ID.
    float = _TypeIds.float | 0x0  # Represents zero.
    decimal = _TypeIds.decimal | 0x0  # Represents 0d0.
    string = _TypeIds.string | 0x0  # Represents the zero-length (empty) string.
    symbol = _TypeIds.symbol | 0x0  # Represents symbol zero.
    struct = _TypeIds.struct | 0x0  # Represents a struct with zero fields.

_VARINT_NEG_ZERO = 0xC0  # This refers to the variable-length signed integer subfield.
_INT_NEG_ZERO = 0x80  # This refers to the fixed-length signed integer subfield.

_LENGTH_FLOAT_64 = 0x08
_LENGTH_FIELD_THRESHOLD = 14
_LENGTH_FIELD_INDICATOR = 0x0E
_NULL_INDICATOR = 0x0F

_NULLS = [
    _TypeIds.null | _NULL_INDICATOR,
    _TypeIds.bool_false | _NULL_INDICATOR,
    _TypeIds.pos_int | _NULL_INDICATOR,
    _TypeIds.float | _NULL_INDICATOR,
    _TypeIds.decimal | _NULL_INDICATOR,
    _TypeIds.timestamp | _NULL_INDICATOR,
    _TypeIds.symbol | _NULL_INDICATOR,
    _TypeIds.string | _NULL_INDICATOR,
    _TypeIds.clob | _NULL_INDICATOR,
    _TypeIds.blob | _NULL_INDICATOR,
    _TypeIds.list | _NULL_INDICATOR,
    _TypeIds.sexp | _NULL_INDICATOR,
    _TypeIds.struct | _NULL_INDICATOR
]


def _serialize_null(buf, ion_event):
    buf.append(_NULLS[ion_event.ion_type])


def _serialize_bool(buf, ion_event):
    if ion_event.value:
        buf.append(_TypeIds.bool_true)
    else:
        buf.append(_TypeIds.bool_false)


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


# TODO This can be consolidated with writer_text's _serialize_scalar_from_string_representation_factory validation.
def _validate_scalar_value(value, expected_type):
    if not isinstance(value, expected_type):
        raise TypeError('Expected type %s, found %s.' % (expected_type, type(value)))


def _serialize_int(buf, ion_event):
    value = ion_event.value
    _validate_scalar_value(value, six.integer_types)
    if value == 0:
        buf.append(_Zeros.int)
        return
    if value < 0:
        value = -value
        tid = _TypeIds.neg_int
    else:
        tid = _TypeIds.pos_int
    _write_int_value(buf, tid, value)


def _serialize_float(buf, ion_event):
    float_value = ion_event.value
    _validate_scalar_value(float_value, float)
    # TODO Assess whether abbreviated encoding of zero is beneficial; it's allowed by spec.
    if float_value.is_integer() and float_value == 0:
        buf.append(_Zeros.float)
        return
    # TODO Add an option for 32-bit representation (length=4) per the spec.
    buf.append(_TypeIds.float | _LENGTH_FLOAT_64)
    encoded = struct.pack('>d', float_value)  # '>' specifies big-endian encoding.
    buf.extend(encoded)


def _write_decimal_value(buf, exponent, coefficient, sign=0):
    length = _write_varint(buf, exponent)
    if coefficient:  # The coefficient is non-zero, so the coefficient field is required.
        length += _write_int(buf, coefficient)
    elif sign:  # The coefficient is negative zero.
        buf.append(_INT_NEG_ZERO)
        length += 1
    # Else the coefficient is positive zero and the field is omitted.
    return length


def digits_to_coefficient(digits):
    """List of integers representing digits to the base-10 integer representation of those digits."""
    i = 0
    integer = 0
    for digit in reversed(digits):
        integer += digit * pow(10, i)
        i += 1
    return integer


def _serialize_decimal(buf, ion_event):
    value = ion_event.value
    _validate_scalar_value(value, Decimal)
    dec_tuple = value.as_tuple()
    exponent = dec_tuple.exponent
    magnitude = digits_to_coefficient(dec_tuple.digits)
    sign = dec_tuple.sign
    if not sign and not exponent and not magnitude:  # The value is 0d0; other forms of zero will fall through.
        buf.append(_Zeros.decimal)
        return
    coefficient = sign and -magnitude or magnitude
    value_buf = bytearray()
    length = _write_decimal_value(value_buf, exponent, coefficient, sign)
    _write_length(buf, length, _TypeIds.decimal)
    buf.extend(value_buf)


def _serialize_string(buf, ion_event):
    value = ion_event.value
    _validate_scalar_value(value, six.text_type)
    if not value:
        buf.append(_Zeros.string)
        return
    value_buf = value.encode('utf-8')
    _write_length(buf, len(value_buf), _TypeIds.string)
    buf.extend(value_buf)


def _serialize_symbol(buf, ion_event):
    sid = ion_event.value
    _validate_scalar_value(sid, six.integer_types)
    if sid == 0:
        buf.append(_Zeros.symbol)
        return
    _write_int_value(buf, _TypeIds.symbol, sid)


def _serialize_lob_value(buf, event, tid):
    value = event.value
    _write_length(buf, len(value), tid)
    buf.extend(value)

_serialize_blob = partial(_serialize_lob_value, tid=_TypeIds.blob)
_serialize_clob = partial(_serialize_lob_value, tid=_TypeIds.clob)


_MICROSECOND_DECIMAL_EXPONENT = -6  # There are 1e6 microseconds per second.


def _serialize_timestamp(buf, ion_event):
    dt = ion_event.value
    _validate_scalar_value(dt, datetime)
    value_buf = bytearray()
    if dt.tzinfo is None:
        value_buf.append(_VARINT_NEG_ZERO)  # This signifies an unknown local offset.
        length = 1
    else:
        length = _write_varint(value_buf, int(dt.utcoffset().total_seconds()) // 60)
    length += _write_varuint(value_buf, dt.year)
    # The lack of validation here is because datetime defaults these to 0, not None, and None is not an
    # acceptable value for any of the following fields. As such, all timestamps generated here will have
    # at least second precision. If a different object is used to represent timestamps, additional
    # validation logic may be required.
    length += _write_varuint(value_buf, dt.month)
    length += _write_varuint(value_buf, dt.day)
    length += _write_varuint(value_buf, dt.hour)
    length += _write_varuint(value_buf, dt.minute)
    length += _write_varuint(value_buf, dt.second)
    microsecond = dt.microsecond
    if microsecond != 0:
        length += _write_decimal_value(value_buf, _MICROSECOND_DECIMAL_EXPONENT, microsecond)
    _write_length(buf, length, _TypeIds.timestamp)
    buf.extend(value_buf)

_SERIALIZE_SCALAR_JUMP_TABLE = {
    IonType.NULL: _serialize_null,
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


# TODO This can be consolidated into a common implementation for text/binary.
def _serialize_scalar(buf, ion_event):
    ion_type = ion_event.ion_type
    if ion_event.value is None:
        return _serialize_null(buf, ion_event)
    if ion_type.is_container:
        raise TypeError('Expected scalar type in event: %s' % (ion_event,))
    _SERIALIZE_SCALAR_JUMP_TABLE[ion_type](buf, ion_event)


def _serialize_annotation_wrapper(output_buf, annotations):
    value_length = output_buf.current_container_length
    annot_length_buf = bytearray()
    annot_length = 0
    for annotation in annotations:
        annot_length += _write_varuint(annot_length_buf, annotation)
    header = bytearray()
    length_buf = bytearray()
    length = _write_varuint(length_buf, annot_length) + annot_length + value_length
    _write_length(header, length, _TypeIds.annotation_wrapper)
    header.extend(length_buf)
    header.extend(annot_length_buf)
    output_buf.end_container(header)


def _serialize_container(output_buf, ion_event):
    ion_type = ion_event.ion_type
    length = output_buf.current_container_length
    header = bytearray()
    if ion_type is IonType.STRUCT:
        if length == 0:
            header.append(_Zeros.struct)
        else:
            # TODO Support sorted field name symbols, per the spec.
            header.append(_TypeIds.struct | _LENGTH_FIELD_INDICATOR)
            _write_varuint(header, length)
    else:
        tid = _TypeIds.list
        if ion_type is IonType.SEXP:
            tid = _TypeIds.sexp
        _write_length(header, length, tid)
    output_buf.end_container(header)


_WRITER_EVENT_NEEDS_INPUT_EMPTY = DataEvent(WriteEventType.NEEDS_INPUT, b'')
_WRITER_EVENT_COMPLETE_EMPTY = DataEvent(WriteEventType.COMPLETE, b'')


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
        if depth > 0 and container_event.ion_type is IonType.STRUCT and ion_event.event_type.begins_value:
            # A field name symbol ID is required at this position.
            sid_buffer = bytearray()
            _write_varuint(sid_buffer, ion_event.field_name)  # Write the field name's symbol ID.
            writer_buffer.add_scalar_value(sid_buffer)
        if ion_event.event_type.begins_value and curr_annotations:
            writer_buffer.start_container()
        if ion_event.event_type is IonEventType.SCALAR:
            scalar_buffer = bytearray()
            _serialize_scalar(scalar_buffer, ion_event)
            writer_buffer.add_scalar_value(scalar_buffer)
            if curr_annotations:
                _serialize_annotation_wrapper(writer_buffer, curr_annotations)
        elif ion_event.event_type is IonEventType.STREAM_END:
            if depth != 0:
                fail()
            for partial_value in writer_buffer.drain():
                yield partial_transition(partial_value, self)
            writer_event = _WRITER_EVENT_COMPLETE_EMPTY
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
