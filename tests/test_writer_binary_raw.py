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
from functools import partial
from io import BytesIO

from itertools import chain

from amazon.ion.core import OffsetTZInfo
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary_raw import _raw_binary_writer, _write_length
from amazon.ion.writer_buffer import BufferTree
from tests import parametrize
from tests.writer_util import assert_writer_events, _D, _E, _ET, _IT, _P, _generate_scalars, _generate_containers


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
        (None, [0x0F]),
    ),
    _IT.BOOL: (
        (None, [0x1F]),
        (False, [0x10]),
        (True, [0x11])
    ),
    _IT.INT: (
        (None, [0x2F]),
        (0, [0x20]),
        (1, [0x21, 0x01]),
        (-1, [0x31, 0x01]),
        (0xFFFFFFFF, [0x24, 0xFF, 0xFF, 0xFF, 0xFF]),
        (-0xFFFFFFFF, [0x34, 0xFF, 0xFF, 0xFF, 0xFF]),
        (0xFFFFFFFFFFFFFFFF, [0x28, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
        (-0xFFFFFFFFFFFFFFFF, [0x38, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
        (0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, [0x2E, 0x90, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                              0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
        (-0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, [0x3E, 0x90, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                               0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
    ),
    _IT.FLOAT: (
        (None, [0x4F]),
        (0e0, [0x40]),
        (0e-0, [0x40]),
        (1e0, [0x48, 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (1e-0, [0x48, 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (-1e0, [0x48, 0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (1e1, [0x48, 0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (-1e1, [0x48, 0xC0, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (float('inf'), [0x48, 0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (1e400, [0x48, 0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (float('-inf'), [0x48, 0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (-1e400, [0x48, 0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
    ),
    _IT.DECIMAL: (
        (None, [0x5F]),
        (_D(0), [0x50]),
        (_D(0).copy_negate(), [0x52, 0x80, 0x80]),
        (_D("1e1"), [0x52, 0x81, 0x01]),
        (_D("1e0"), [0x52, 0x80, 0x01]),
        (_D("1e-1"), [0x52, 0xC1, 0x01]),
        (_D("0e-1"), [0x51, 0xC1]),
        (_D("0e1"), [0x51, 0x81]),
    ),
    _IT.TIMESTAMP: (
        (None, [0x6F]),
        # TODO Clarify whether there's a valid zero-length Timestamp representation.
        (datetime.datetime(year=1, month=1, day=1), [0x67, 0xC0, 0x81, 0x81, 0x81, 0x80, 0x80, 0x80]),
        (datetime.datetime(year=1, month=1, day=1, tzinfo=OffsetTZInfo(datetime.timedelta(minutes=1))),
            [0x67, 0x81, 0x81, 0x81, 0x81, 0x80, 0x80, 0x80]),
        (datetime.datetime(year=1, month=1, day=1, hour=0, minute=0, second=0, microsecond=1),
            [0x69, 0xC0, 0x81, 0x81, 0x81, 0x80, 0x80, 0x80, 0xC6, 0x01]),
    ),
    _IT.SYMBOL: (
        (None, [0x7F]),
        (0, [0x70]),
        (1, [0x71, 0x01]),
    ),
    _IT.STRING: (
        (None, [0x8F]),
        (u'', [0x80]),
        (u'abc', [0x83] + [b for b in u'abc'.encode('utf-8')]),
        (u'abcdefghijklmno', [0x8E, 0x8F] + [b for b in u'abcdefghijklmno'.encode('utf-8')]),
        (u'a\U0001f4a9c', [0x86] + [b for b in u'a\U0001f4a9c'.encode('utf-8')]),
        (u'a\u0009\x0a\x0dc', [0x85] + [b for b in 'a\t\n\rc'.encode('utf-8')]),
    ),
    _IT.CLOB: (
        (None, [0x9F]),
        (b'', [0x90]),
        (b'abc', [0x93] + [b for b in b'abc']),
        (b'abcdefghijklmno', [0x9E, 0x8F] + [b for b in b'abcdefghijklmno']),
    ),
    _IT.BLOB: (
        (None, [0xAF]),
        (b'', [0xA0]),
        (b'abc', [0xA3] + [b for b in b'abc']),
        (b'abcdefghijklmno', [0xAE, 0x8F] + [b for b in b'abcdefghijklmno']),
    ),
    _IT.LIST: (
        (None, [0xBF]),
    ),
    _IT.SEXP: (
        (None, [0xCF]),
    ),
    _IT.STRUCT: (
        (None, [0xDF]),
    ),
}

_ION_ENCODED_INT_ZERO = 0x20
_VARUINT_END_BYTE = 0x80


_SIMPLE_CONTAINER_MAP = {
    _IT.LIST: (
        (
            (),
            [0xB0]
        ),
        (
            (_E(_ET.SCALAR, _IT.INT, 0),),
            [
                0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                _ION_ENCODED_INT_ZERO
            ]
        ),
    ),
    _IT.SEXP: (
        (
            (),
            [0xC0]
        ),
        (
            (_E(_ET.SCALAR, _IT.INT, 0),),
            [
                0xC0 | 0x01,  # Int value 0 fits in 1 byte.
                _ION_ENCODED_INT_ZERO
            ]
        ),
    ),
    _IT.STRUCT: (
        (
            (),
            [0xD0]
        ),
        (
            (_E(_ET.SCALAR, _IT.INT, 0, field_name=10),),
            [
                0xDE,  # The lower nibble may vary by implementation. It does not indicate actual length unless it's 0.
                _VARUINT_END_BYTE | 2,  # # Field name 10 and value 0 each fit in 1 byte.
                _VARUINT_END_BYTE | 10,
                _ION_ENCODED_INT_ZERO
            ]
        ),
    ),
}


_generate_simple_scalars = partial(_generate_scalars, _SIMPLE_SCALARS_MAP, True)
_generate_simple_containers = partial(_generate_containers, _SIMPLE_CONTAINER_MAP, True)


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
            expected=wrapper + value_p.expected,
        )


def new_writer():
    out = BytesIO()
    return out, blocking_writer(_raw_binary_writer(BufferTree()), out)


def to_bytes(arr):
    expected = BytesIO()
    expected.write(bytearray(arr))
    return expected.getvalue()


@parametrize(
    *tuple(chain(
        _P_FAILURES,
        _generate_simple_scalars(),
        _generate_simple_containers(),
        _generate_annotated_values()
    ))
)
def test_raw_writer(p):
    assert_writer_events(p, new_writer, to_bytes)
