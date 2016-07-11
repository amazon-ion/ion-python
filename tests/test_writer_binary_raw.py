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

from functools import partial
from io import BytesIO

from itertools import chain

from datetime import datetime, timedelta
from decimal import Decimal

from amazon.ion.core import OffsetTZInfo, IonEvent, IonType, IonEventType
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary_raw import _raw_binary_writer, _write_length
from amazon.ion.writer_buffer import BufferTree
from tests import parametrize
from tests.writer_util import assert_writer_events, WriterParameter, generate_scalars, generate_containers

_D = Decimal
_DT = datetime

_E = IonEvent
_IT = IonType
_ET = IonEventType
_P = WriterParameter

_P_FAILURES = [
    _P(
        desc="CONTAINER END AT TOP LEVEL",
        events=[
            _E(_ET.CONTAINER_END)
        ],
        expected=TypeError
    ),
    _P(
        desc="CONTAINER START WITH SCALAR VALUE",
        events=[
            _E(_ET.CONTAINER_START, _IT.BOOL, False)
        ],
        expected=TypeError
    ),
    _P(
        desc="STREAM END BELOW TOP LEVEL",
        events=[
            _E(_ET.CONTAINER_START, _IT.LIST),
            _E(_ET.SCALAR, _IT.STRING, u'foo'),
            _E(_ET.STREAM_END)
        ],
        expected=TypeError
    ),
    _P(
        desc="SCALAR WITH CONTAINER VALUE",
        events=[
            _E(_ET.SCALAR, _IT.STRUCT, u'foo')
        ],
        expected=TypeError
    ),
    _P(
        desc="NULL WITH VALUE",
        events=[
            _E(_ET.SCALAR, _IT.NULL, u'foo')
        ],
        expected=TypeError
    ),
    _P(
        desc="INCORRECT TYPE FOR INT",
        events=[
            _E(_ET.SCALAR, _IT.INT, 1.23e4)
        ],
        expected=TypeError
    ),
    _P(
        desc="INCORRECT TYPE FOR FLOAT",
        events=[
            _E(_ET.SCALAR, _IT.FLOAT, _D(42))
        ],
        expected=TypeError
    ),
    _P(
        desc="INCORRECT TYPE FOR DECIMAL",
        events=[
            _E(_ET.SCALAR, _IT.DECIMAL, 1.23e4)
        ],
        expected=TypeError
    ),
    _P(
        desc="INCORRECT TYPE FOR TIMESTAMP",
        events=[
            _E(_ET.SCALAR, _IT.TIMESTAMP, 123456789)
        ],
        expected=TypeError
    ),
    _P(
        desc="INCORRECT TYPE FOR SYMBOL",
        events=[
            _E(_ET.SCALAR, _IT.SYMBOL, u'symbol')
        ],
        expected=TypeError
    ),
    _P(
        desc="INCORRECT TYPE FOR STRING",
        events=[
            _E(_ET.SCALAR, _IT.STRING, 42)
        ],
        expected=TypeError
    ),
    _P(
        desc="IVM EVENT",  # The IVM is handled by the managed binary writer.
        events=[
            _E(_ET.VERSION_MARKER)
        ],
        expected=TypeError
    ),
]

_SIMPLE_SCALARS_MAP = {
    _IT.NULL: (
        (None, b'\x0F'),
    ),
    _IT.BOOL: (
        (None, b'\x1F'),
        (False, b'\x10'),
        (True, b'\x11')
    ),
    _IT.INT: (
        (None, b'\x2F'),
        (0, b'\x20'),
        (1, b'\x21\x01'),
        (-1, b'\x31\x01'),
        (0xFFFFFFFF, b'\x24\xFF\xFF\xFF\xFF'),
        (-0xFFFFFFFF, b'\x34\xFF\xFF\xFF\xFF'),
        (0xFFFFFFFFFFFFFFFF, b'\x28\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
        (-0xFFFFFFFFFFFFFFFF, b'\x38\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
        (0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
         b'\x2E\x90\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
        (-0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
         b'\x3E\x90\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
    ),
    _IT.FLOAT: (
        (None, b'\x4F'),
        (0e0, b'\x40'),
        (0e-0, b'\x40'),
        (1e0, b'\x48\x3F\xF0\x00\x00\x00\x00\x00\x00'),
        (1e-0, b'\x48\x3F\xF0\x00\x00\x00\x00\x00\x00'),
        (-1e0, b'\x48\xBF\xF0\x00\x00\x00\x00\x00\x00'),
        (1e1, b'\x48\x40\x24\x00\x00\x00\x00\x00\x00'),
        (-1e1, b'\x48\xC0\x24\x00\x00\x00\x00\x00\x00'),
        (float('inf'), b'\x48\x7F\xF0\x00\x00\x00\x00\x00\x00'),
        (1e400, b'\x48\x7F\xF0\x00\x00\x00\x00\x00\x00'),
        (float('-inf'), b'\x48\xFF\xF0\x00\x00\x00\x00\x00\x00'),
        (-1e400, b'\x48\xFF\xF0\x00\x00\x00\x00\x00\x00'),
    ),
    _IT.DECIMAL: (
        (None, b'\x5F'),
        (_D(0), b'\x50'),
        (_D(0).copy_negate(), b'\x52\x80\x80'),
        (_D("1e1"), b'\x52\x81\x01'),
        (_D("1e0"), b'\x52\x80\x01'),
        (_D("1e-1"), b'\x52\xC1\x01'),
        (_D("0e-1"), b'\x51\xC1'),
        (_D("0e1"), b'\x51\x81'),
        (_D("-1e1"), b'\x52\x81\x81'),
        (_D("-1e0"), b'\x52\x80\x81'),
        (_D("-1e-1"), b'\x52\xC1\x81'),
        (_D("-0e-1"), b'\x52\xC1\x80'),
        (_D("-0e1"), b'\x52\x81\x80'),
    ),
    _IT.TIMESTAMP: (
        (None, b'\x6F'),
        # TODO Clarify whether there's a valid zero-length Timestamp representation.
        (_DT(year=1, month=1, day=1), b'\x67\xC0\x81\x81\x81\x80\x80\x80'),
        (_DT(year=1, month=1, day=1, tzinfo=OffsetTZInfo(timedelta(minutes=1))),
         b'\x67\x81\x81\x81\x81\x80\x80\x80'),
        (_DT(year=1, month=1, day=1, hour=0, minute=0, second=0, microsecond=1),
         b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC6\x01'),
    ),
    _IT.SYMBOL: (
        (None, b'\x7F'),
        (0, b'\x70'),
        (1, b'\x71\x01'),
    ),
    _IT.STRING: (
        (None, b'\x8F'),
        (u'', b'\x80'),
        (u'abc', b'\x83abc'),
        (u'abcdefghijklmno', b'\x8E\x8Fabcdefghijklmno'),
        (u'a\U0001f4a9c', b'\x86' + bytearray([b for b in u'a\U0001f4a9c'.encode('utf-8')])),
        (u'a\u0009\x0a\x0dc', b'\x85' + bytearray([b for b in 'a\t\n\rc'.encode('utf-8')])),
    ),
    _IT.CLOB: (
        (None, b'\x9F'),
        (b'', b'\x90'),
        (b'abc', b'\x93' + b'abc'),
        (b'abcdefghijklmno', b'\x9E\x8Fabcdefghijklmno'),
    ),
    _IT.BLOB: (
        (None, b'\xAF'),
        (b'', b'\xA0'),
        (b'abc', b'\xA3' + b'abc'),
        (b'abcdefghijklmno', b'\xAE\x8Fabcdefghijklmno'),
    ),
    _IT.LIST: (
        (None, b'\xBF'),
    ),
    _IT.SEXP: (
        (None, b'\xCF'),
    ),
    _IT.STRUCT: (
        (None, b'\xDF'),
    ),
}

_ION_ENCODED_INT_ZERO = 0x20
_VARUINT_END_BYTE = 0x80


_SIMPLE_CONTAINER_MAP = {
    _IT.LIST: (
        (
            (),
            b'\xB0'
        ),
        (
            (_E(_ET.SCALAR, _IT.INT, 0),),
            bytearray([
                0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                _ION_ENCODED_INT_ZERO
            ])
        ),
    ),
    _IT.SEXP: (
        (
            (),
            b'\xC0'
        ),
        (
            (_E(_ET.SCALAR, _IT.INT, 0),),
            bytearray([
                0xC0 | 0x01,  # Int value 0 fits in 1 byte.
                _ION_ENCODED_INT_ZERO
            ])
        ),
    ),
    _IT.STRUCT: (
        (
            (),
            b'\xD0'
        ),
        (
            (_E(_ET.SCALAR, _IT.INT, 0, field_name=10),),
            bytearray([
                0xDE,  # The lower nibble may vary by implementation. It does not indicate actual length unless it's 0.
                _VARUINT_END_BYTE | 2,  # # Field name 10 and value 0 each fit in 1 byte.
                _VARUINT_END_BYTE | 10,
                _ION_ENCODED_INT_ZERO
            ])
        ),
    ),
}


_generate_simple_scalars = partial(generate_scalars, _SIMPLE_SCALARS_MAP, True)
_generate_simple_containers = partial(generate_containers, _SIMPLE_CONTAINER_MAP, True)


def _generate_annotated_values():
    for value_p in chain(_generate_simple_scalars(), _generate_simple_containers()):
        events = (value_p.events[0].derive_annotations([10, 11]),) + value_p.events[1:]
        annot_length = 2  # 10 and 11 each fit in one VarUInt byte
        annot_length_length = 1  # 2 fits in one VarUInt byte
        value_length = len(value_p.expected)
        length_field = annot_length + annot_length_length + value_length
        wrapper = []
        _write_length(wrapper, length_field, 0xE0)
        wrapper.extend([
            _VARUINT_END_BYTE | annot_length,
            _VARUINT_END_BYTE | 10,
            _VARUINT_END_BYTE | 11
        ])
        yield _P(
            desc='ANN %s' % value_p.desc,
            events=events + (_E(_ET.STREAM_END),),
            expected=bytearray(wrapper) + value_p.expected,
        )


def new_writer():
    out = BytesIO()
    return out, blocking_writer(_raw_binary_writer(BufferTree()), out)


@parametrize(
    *tuple(chain(
        _P_FAILURES,
        _generate_simple_scalars(),
        _generate_simple_containers(),
        _generate_annotated_values()
    ))
)
def test_raw_writer(p):
    assert_writer_events(p, new_writer)
