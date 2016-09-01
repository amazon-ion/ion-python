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
from io import BytesIO

from decimal import Decimal
from itertools import chain

import six

from amazon.ion.writer_binary import _IVM
from amazon.ion.core import IonType
from amazon.ion.simple_types import IonPyDict, IonPyText, IonPyList, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, \
    IonPyDecimal, IonPyTimestamp, IonPyBytes, IonPySymbol
from amazon.ion.simpleion import dump, load
from amazon.ion.util import record
from tests.writer_util import VARUINT_END_BYTE, ION_ENCODED_INT_ZERO, SIMPLE_SCALARS_MAP
from tests import parametrize


class Parameter(record('desc', 'obj', 'expected', 'has_symbols', ('stream', False))):
    def __str__(self):
        return self.desc

_TYPE_TABLE = [
    IonPyNull,
    IonPyBool,
    IonPyInt,
    IonPyFloat,
    IonPyDecimal,
    IonPyTimestamp,
    IonPySymbol,
    IonPyText,
    IonPyBytes,
    IonPyBytes,
    IonPyDict,
    IonPyList,
    IonPyList
]

_SIMPLE_CONTAINER_MAP = {
    IonType.LIST: (
        (
            ([],),
            b'\xB0'
        ),
        (
            ([0],),
            bytearray([
                0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                ION_ENCODED_INT_ZERO
            ])
        ),
    ),
    IonType.SEXP: (
        (
            (IonPyList.from_value(IonType.SEXP, []),),
            b'\xC0'
        ),
        (
            (IonPyList.from_value(IonType.SEXP, [0]),),
            bytearray([
                0xC0 | 0x01,  # Int value 0 fits in 1 byte.
                ION_ENCODED_INT_ZERO
            ])
        ),
    ),
    IonType.STRUCT: (
        (
            ({},),
            b'\xD0'
        ),
        (
            ({u'foo': 0},),
            bytearray([
                0xDE,  # The lower nibble may vary by implementation. It does not indicate actual length unless it's 0.
                VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                VARUINT_END_BYTE | 10,
                ION_ENCODED_INT_ZERO
            ])
        ),
    ),
}


def generate_scalars(scalars_map):
    for ion_type, values in six.iteritems(scalars_map):
        for native, expected in values:
            native_expected = expected
            has_symbols = False
            if native is None:
                # An un-adorned 'None' doesn't contain enough information to determine its Ion type
                native_expected = b'\x0f'
            elif ion_type is IonType.CLOB:
                # All six.binary_type are treated as BLOBs unless wrapped by an _IonNature
                tid = expected[0] + 0x10  # increment upper nibble for clob -> blob; keep lower nibble
                native_expected = bytearray([tid]) + expected[1:]
            elif ion_type is IonType.SYMBOL and native is not None:
                has_symbols = True
            elif ion_type is IonType.STRING:
                # Encode all strings as symbols too. Since they're encoded as individual values, all will have SID 10
                yield Parameter(IonType.SYMBOL.name + ' ' + native, IonPyText.from_value(IonType.SYMBOL, native), b'\x71\x0a', True)
            yield Parameter(ion_type.name + ' ' + str(native), native, native_expected, has_symbols)
            wrapper = _TYPE_TABLE[ion_type].from_value(ion_type, native)  # TODO add some annotations
            yield Parameter(repr(wrapper), wrapper, expected, has_symbols)


def generate_containers(container_map):
    for ion_type, container in six.iteritems(container_map):
        for test_tuple in container:
            obj = test_tuple[0]
            encoded = test_tuple[1]
            has_symbols = False
            for elem in obj:
                if isinstance(elem, dict) and len(elem) > 0:
                    has_symbols = True
            yield Parameter(IonType.SYMBOL.name + ' ' + repr(obj), obj, encoded, has_symbols, True)


def test_roundtrip():
    sym = IonPyText.from_value(IonType.SYMBOL, 'sym')
    bar = IonPyText.from_value(IonType.STRING, 'bar', annotations=('str',))
    lst = IonPyList.from_value(IonType.LIST, [True, None, 1.23e4, sym])
    sxp = IonPyList.from_value(IonType.SEXP, [False, IonPyNull.from_value(IonType.STRUCT, None, ('class',)), Decimal('5.678')])
    dct = IonPyDict.from_value(IonType.STRUCT, {"foo": bar, "baz": 123, "lst": lst, "sxp": sxp}, annotations=('annot1', 'annot2'))
    out = BytesIO()
    dump(dct, out)
    out.seek(0)
    res = load(out)
    print("%r" % (res,))


@parametrize(
    *tuple(chain(
        generate_scalars(SIMPLE_SCALARS_MAP),
        generate_containers(_SIMPLE_CONTAINER_MAP)
    ))
)
def test_scalars_dump(p):
    out = BytesIO()
    dump(p.obj, out, sequence_as_stream=p.stream)
    res = out.getvalue()
    if not p.has_symbols:
        assert (_IVM + p.expected) == res
    else:
        # The payload contains a LST. The value comes last, so compare the end bytes.
        assert p.expected == res[len(res) - len(p.expected):]


@parametrize(
    *tuple(chain(
        generate_scalars(SIMPLE_SCALARS_MAP)
    ))
)
def test_scalars_load(p):
    out = BytesIO()
    dump(p.obj, out)
    out.seek(0)
    res = load(out)
    if p.obj is None:
        assert isinstance(res, IonPyNull)
    else:
        assert p.obj == res
