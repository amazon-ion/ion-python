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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
from io import BytesIO

from decimal import Decimal
from pytest import raises

from amazon.ion.core import IonEvent, IonType
from amazon.ion.core import IonEventType
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary_raw import _raw_binary_writer
from amazon.ion.writer_buffer import BufferTree
from tests import parametrize, UTCOffset


def new_writer():
    out = BytesIO()
    return out, blocking_writer(_raw_binary_writer(BufferTree()), out)


def finish(writer):
    writer.send(IonEvent(IonEventType.STREAM_END))


def test_container_end_at_top_level_fails():
    out, writer = new_writer()
    with raises(TypeError):
        writer.send(IonEvent(IonEventType.CONTAINER_END))


def test_container_start_with_scalar_fails():
    out, writer = new_writer()
    with raises(TypeError):
        writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.BOOL, False))


def test_stream_end_not_at_top_level_fails():
    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.LIST))
    writer.send(IonEvent(IonEventType.SCALAR, IonType.STRING, u'foo'))
    with raises(TypeError):
        finish(writer)


def test_write_scalar_with_container_fails():
    out, writer = new_writer()
    with raises(TypeError):
        writer.send(IonEvent(IonEventType.SCALAR, IonType.STRUCT, u'foo'))


def to_bytes(arr):
    expected = BytesIO()
    expected.write(bytearray(arr))
    return expected.getvalue()


class ValueParameter():
    def __init__(self, ion_type, expected, data=None):
        self.ion_type = ion_type
        self.expected = expected
        self.data = data

    def __str__(self):
        return "[" + str(self.ion_type) + ", " + str(self.expected) + ", " + str(self.data) + "]"


@parametrize(
    ValueParameter(IonType.LIST, 0xB0),
    ValueParameter(IonType.SEXP, 0xC0),
    ValueParameter(IonType.STRUCT, 0xD0)
)
def test_empty_container(p):
    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.CONTAINER_START, p.ion_type))
    writer.send(IonEvent(IonEventType.CONTAINER_END))
    finish(writer)
    assert out.getvalue() == to_bytes([p.expected])


@parametrize(
    ValueParameter(IonType.NULL, [0x0F]),
    ValueParameter(IonType.BOOL, [0x10], False),
    ValueParameter(IonType.BOOL, [0x11], True),
    ValueParameter(IonType.INT, [0x20], 0),
    ValueParameter(IonType.FLOAT, [0x40], 0e0),
    ValueParameter(IonType.DECIMAL, [0x50], Decimal(0)),
    # TODO clarify whether there's a valid zero-length Timestamp representation
    ValueParameter(IonType.SYMBOL, [0x70], 0),
    ValueParameter(IonType.STRING, [0x80], u''),
    ValueParameter(IonType.CLOB, [0x90], u''),
    ValueParameter(IonType.BLOB, [0xA0], b'')

)
def test_empty_scalar(p):
    assert_scalar(p)


def test_negative_zero_decimal():
    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.SCALAR, IonType.DECIMAL, Decimal(0).copy_negate()))
    finish(writer)
    assert out.getvalue() == to_bytes([0x52, 0x80, 0x80])


def assert_scalar(p):
    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.SCALAR, p.ion_type, p.data))
    finish(writer)
    assert out.getvalue() == to_bytes(p.expected)


# THE FOLLOWING ARE BASICALLY HERE UNTIL WE ADD ROUNDTRIP TESTING


@parametrize(
    ValueParameter(IonType.INT, [0x21, 0x01], 1),
    ValueParameter(IonType.INT, [0x31, 0x01], -1),
    ValueParameter(IonType.DECIMAL, [0x52, 0x81, 0x01], Decimal("1e1")),
    ValueParameter(IonType.DECIMAL, [0x52, 0x80, 0x01], Decimal("1e0")),
    ValueParameter(IonType.DECIMAL, [0x52, 0xC1, 0x01], Decimal("1e-1")),
    ValueParameter(IonType.DECIMAL, [0x51, 0xC1], Decimal("0e-1")),
    ValueParameter(IonType.DECIMAL, [0x51, 0x81], Decimal("0e1")),
    ValueParameter(IonType.TIMESTAMP, [0x67, 0xC0, 0x81, 0x81, 0x81, 0x80, 0x80, 0x80],
                   datetime.datetime(year=1, month=1, day=1)),
    ValueParameter(IonType.TIMESTAMP, [0x67, 0x81, 0x81, 0x81, 0x81, 0x80, 0x80, 0x80],
                   datetime.datetime(year=1, month=1, day=1, tzinfo=UTCOffset(datetime.timedelta(minutes=1)))),
    ValueParameter(IonType.TIMESTAMP, [0x69, 0xC0, 0x81, 0x81, 0x81, 0x80, 0x80, 0x80, 0xC6, 0x01],
                   datetime.datetime(year=1, month=1, day=1, hour=0, minute=0, second=0, microsecond=1)),
    ValueParameter(IonType.SYMBOL, [0x71, 0x01], 1)
)
def test_basic_scalar(p):
    assert_scalar(p)


__ION_ENCODED_INT_ZERO = 0x20
__VARUINT_END_BYTE = 0x80


def test_basic_annotation_wrapper():
    annot_length = 2  # 10 and 11 each fit in one VarUInt byte
    annot_length_length = 1  # 2 fits in one VarUInt byte
    value_length = 1  # Ion int 0 fits in one byte (see test_empty_scalar)
    annotation_tid = 0xE0

    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.SCALAR, IonType.INT, 0, annotations=[10, 11]))
    finish(writer)
    assert out.getvalue() == to_bytes([annotation_tid | (annot_length + annot_length_length + value_length),
                                       __VARUINT_END_BYTE | annot_length,
                                       __VARUINT_END_BYTE | 10,
                                       __VARUINT_END_BYTE | 11,
                                       __ION_ENCODED_INT_ZERO])


def test_basic_struct():
    struct_tid = 0xDE  # the lower nibble may vary by implementation. It does not indicate actual length unless it's 0.
    length = 2  # field name 10 and value 0 each fit in 1 byte

    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT))
    writer.send(IonEvent(IonEventType.SCALAR, IonType.INT, 0, field_name=10))
    writer.send(IonEvent(IonEventType.CONTAINER_END))
    finish(writer)
    assert out.getvalue() == to_bytes([struct_tid,
                                       __VARUINT_END_BYTE | length,
                                       __VARUINT_END_BYTE | 10,
                                       __ION_ENCODED_INT_ZERO])


@parametrize(
    ValueParameter(IonType.LIST, 0xB0),
    ValueParameter(IonType.SEXP, 0xC0)
)
def test_basic_sequence(sequence):
    length = 1  # 0 fits in 1 byte

    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.CONTAINER_START, sequence.ion_type))
    writer.send(IonEvent(IonEventType.SCALAR, IonType.INT, 0))
    writer.send(IonEvent(IonEventType.CONTAINER_END))
    finish(writer)
    assert out.getvalue() == to_bytes([sequence.expected | length,
                                       __ION_ENCODED_INT_ZERO])