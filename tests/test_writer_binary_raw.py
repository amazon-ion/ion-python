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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from functools import partial
from io import BytesIO
from itertools import chain
from datetime import datetime
from decimal import Decimal

from amazon.ion.symbols import SymbolToken
from tests import parametrize
from tests.writer_util import assert_writer_events, generate_scalars, generate_containers, \
                              WriterParameter, SIMPLE_SCALARS_MAP_BINARY, ION_ENCODED_INT_ZERO, VARUINT_END_BYTE

from amazon.ion.core import IonEvent, IonType, IonEventType, timestamp, TimestampPrecision
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary_raw import _raw_binary_writer, _write_length
from amazon.ion.writer_buffer import BufferTree

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
    )
]

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
                ION_ENCODED_INT_ZERO
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
                ION_ENCODED_INT_ZERO
            ])
        ),
    ),
    _IT.STRUCT: (
        (
            (),
            b'\xD0'
        ),
        (
            (_E(_ET.SCALAR, _IT.INT, 0, field_name=SymbolToken(None, 10)),),
            bytearray([
                0xDE,  # The lower nibble may vary by implementation. It does not indicate actual length unless it's 0.
                VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                VARUINT_END_BYTE | 10,
                ION_ENCODED_INT_ZERO
            ])
        ),
    ),
}


_generate_simple_scalars = partial(generate_scalars, SIMPLE_SCALARS_MAP_BINARY, True)
_generate_simple_containers = partial(generate_containers, _SIMPLE_CONTAINER_MAP, True)


def _generate_annotated_values():
    for value_p in chain(_generate_simple_scalars(), _generate_simple_containers()):
        events = (value_p.events[0].derive_annotations(
            [SymbolToken(None, 10), SymbolToken(None, 11)]),) + value_p.events[1:]
        annot_length = 2  # 10 and 11 each fit in one VarUInt byte
        annot_length_length = 1  # 2 fits in one VarUInt byte
        final_expected = ()
        if isinstance(value_p.expected, (list, tuple)):
            expecteds = value_p.expected
        else:
            expecteds = (value_p.expected,)

        for one_expected in expecteds:
            value_length = len(one_expected)
            length_field = annot_length + annot_length_length + value_length
            wrapper = []
            _write_length(wrapper, length_field, 0xE0)
            wrapper.extend([
                VARUINT_END_BYTE | annot_length,
                VARUINT_END_BYTE | 10,
                VARUINT_END_BYTE | 11
            ])

            exp = bytearray(wrapper) + one_expected
            final_expected += (exp, )

        yield _P(
            desc='ANN %s' % value_p.desc,
            events=events + (_E(_ET.STREAM_END),),
            expected=final_expected,
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
