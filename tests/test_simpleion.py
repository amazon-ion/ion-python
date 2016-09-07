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
from datetime import datetime, timedelta
from io import BytesIO

from decimal import Decimal
from itertools import chain

import six
from pytest import raises

from amazon.ion.exceptions import IonException
from amazon.ion.symbols import SymbolToken
from amazon.ion.writer_binary import _IVM
from amazon.ion.core import IonType, IonEvent, IonEventType, OffsetTZInfo
from amazon.ion.simple_types import IonPyDict, IonPyText, IonPyList, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, \
    IonPyDecimal, IonPyTimestamp, IonPyBytes, IonPySymbol, _IonNature
from amazon.ion.simpleion import dump, load, _ion_type, _FROM_ION_TYPE
from amazon.ion.util import record
from amazon.ion.writer_binary_raw import _serialize_symbol, _write_length
from tests.writer_util import VARUINT_END_BYTE, ION_ENCODED_INT_ZERO, SIMPLE_SCALARS_MAP
from tests import parametrize


class Parameter(record('desc', 'obj', 'expected', 'has_symbols', ('stream', False))):
    def __str__(self):
        return self.desc


_SIMPLE_CONTAINER_MAP = {
    IonType.LIST: (
        (
            [[], ],
            [b'\xB0', ]
        ),
        (
            [IonPyList.from_value(IonType.LIST, []), ],
            [b'\xB0', ]
        ),
        (
            [[0], ],
            [
                bytearray([
                    0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
            ]
        ),
        (
            [IonPyList.from_value(IonType.LIST, [0]), ],
            [
                bytearray([
                    0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
            ]
        ),
    ),
    IonType.SEXP: (
        (
            [IonPyList.from_value(IonType.SEXP, []), ],
            [b'\xC0', ]
        ),
        (
            [IonPyList.from_value(IonType.SEXP, [0]), ],
            [
                bytearray([
                    0xC0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
            ]
        ),
    ),
    IonType.STRUCT: (
        (
            [{}, ],
            [b'\xD0', ]
        ),
        (
            [IonPyDict.from_value(IonType.STRUCT, {}), ],
            [b'\xD0', ]
        ),
        (
            [{u'foo': 0}, ],
            [
                bytearray([
                    0xDE,  # The lower nibble may vary. It does not indicate actual length unless it's 0.
                    VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                    VARUINT_END_BYTE | 10,
                    ION_ENCODED_INT_ZERO
                ]),
            ]
        ),
        (
            [IonPyDict.from_value(IonType.STRUCT, {u'foo': 0}), ],
            [
                bytearray([
                    0xDE,  # The lower nibble may vary. It does not indicate actual length unless it's 0.
                    VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                    VARUINT_END_BYTE | 10,
                    ION_ENCODED_INT_ZERO
                ]),
            ]
        ),
    ),
}


def generate_scalars(scalars_map, preceding_symbols=0):
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
                # Encode all strings as symbols too.
                symbol_expected = _serialize_symbol(
                    IonEvent(IonEventType.SCALAR, IonType.SYMBOL, SymbolToken(None, 10 + preceding_symbols)))
                yield Parameter(IonType.SYMBOL.name + ' ' + native,
                                IonPyText.from_value(IonType.SYMBOL, native), symbol_expected, True)
            yield Parameter(ion_type.name + ' ' + str(native), native, native_expected, has_symbols)
            wrapper = _FROM_ION_TYPE[ion_type].from_value(ion_type, native)  # TODO add some annotations
            yield Parameter(repr(wrapper), wrapper, expected, has_symbols)


def generate_containers(container_map, preceding_symbols=0):
    for ion_type, container in six.iteritems(container_map):
        for test_tuple in container:
            obj = test_tuple[0]
            expecteds = test_tuple[1]
            has_symbols = False
            for elem in obj:
                if isinstance(elem, dict) and len(elem) > 0:
                    has_symbols = True
            if has_symbols and preceding_symbols:
                for expected in expecteds:
                    field_sid = expected[-2] & (~VARUINT_END_BYTE)
                    expected[-2] = VARUINT_END_BYTE | (preceding_symbols + field_sid)
            yield Parameter(IonType.SYMBOL.name + ' ' + repr(obj), obj, b''.join(expecteds), has_symbols, True)


def generate_annotated_values(scalars_map, container_map):
    for value_p in chain(generate_scalars(scalars_map, preceding_symbols=2),
                         generate_containers(container_map, preceding_symbols=2)):
        obj = value_p.obj
        if not isinstance(obj, _IonNature):
            continue
        obj.ion_annotations = (SymbolToken(u'annot1', None), SymbolToken(u'annot2', None),)
        annot_length = 2  # 10 and 11 each fit in one VarUInt byte
        annot_length_length = 1  # 2 fits in one VarUInt byte
        value_length = len(value_p.expected)
        length_field = annot_length + annot_length_length + value_length
        wrapper = []
        _write_length(wrapper, length_field, 0xE0)
        wrapper.extend([
            VARUINT_END_BYTE | annot_length,
            VARUINT_END_BYTE | 10,
            VARUINT_END_BYTE | 11
        ])
        yield Parameter(
            desc='ANNOTATED %s' % value_p.desc,
            obj=obj,
            expected=bytearray(wrapper) + value_p.expected,
            has_symbols=True,
            stream=value_p.stream
        )

@parametrize(
    *tuple(chain(
        generate_scalars(SIMPLE_SCALARS_MAP),
        generate_containers(_SIMPLE_CONTAINER_MAP),
        generate_annotated_values(SIMPLE_SCALARS_MAP, _SIMPLE_CONTAINER_MAP),
    ))
)
def test_dump_load(p):
    # test dump
    out = BytesIO()
    dump(p.obj, out, sequence_as_stream=p.stream)
    res = out.getvalue()
    if not p.has_symbols:
        assert (_IVM + p.expected) == res
    else:
        # The payload contains a LST. The value comes last, so compare the end bytes.
        assert p.expected == res[len(res) - len(p.expected):]
    # test load
    out.seek(0)
    res = load(out, single_value=(not p.stream))
    if p.obj is None:
        assert isinstance(res, IonPyNull)
    else:
        assert p.obj == res


_ROUNDTRIPS = [
    None,
    IonPyNull.from_value(IonType.NULL, None),
    IonPyNull.from_value(IonType.BOOL, None),
    IonPyNull.from_value(IonType.INT, None),
    IonPyNull.from_value(IonType.FLOAT, None),
    IonPyNull.from_value(IonType.DECIMAL, None),
    IonPyNull.from_value(IonType.TIMESTAMP, None),
    IonPyNull.from_value(IonType.SYMBOL, None),
    IonPyNull.from_value(IonType.STRING, None),
    IonPyNull.from_value(IonType.CLOB, None),
    IonPyNull.from_value(IonType.BLOB, None),
    IonPyNull.from_value(IonType.LIST, None),
    IonPyNull.from_value(IonType.SEXP, None),
    IonPyNull.from_value(IonType.STRUCT, None),
    True,
    False,
    IonPyInt.from_value(IonType.BOOL, 0),
    IonPyInt.from_value(IonType.BOOL, 1),
    0,
    -1,
    1.23,
    1.23e4,
    -1.23,
    -1.23e-4,
    Decimal(0),
    Decimal('-1.23'),
    datetime(year=1, month=1, day=1, tzinfo=OffsetTZInfo(timedelta(minutes=-1))),
    u'',
    u'abc',
    u'abcdefghijklmno',
    u'a\U0001f4a9c',
    u'a\u0009\x0a\x0dc',
    b'abcd',
    IonPyBytes.from_value(IonType.CLOB, b'abcd'),
    [[[]]],
    [[],[],[]],
    [{}, {}, {}],
    {'foo': [], 'bar': [], 'baz': []},
    {'foo': {'foo': {}}},
    [{'foo': [{'foo': []}]}],
    {'foo': [{'foo': []}]},
    {
         "foo": IonPyText.from_value(IonType.STRING, 'bar', annotations=('str',)),
         "baz": 123,
         "lst": IonPyList.from_value(IonType.LIST,
                                     [
                                         True,
                                         None,
                                         1.23e4,
                                         IonPyText.from_value(IonType.SYMBOL, 'sym')
                                     ]),
         "sxp": IonPyList.from_value(IonType.SEXP,
                                     [
                                         False,
                                         IonPyNull.from_value(IonType.STRUCT, None, ('class',)),
                                         Decimal('5.678')
                                     ])
    },

]


def _generate_roundtrips(roundtrips):
    for obj in roundtrips:
        yield obj
        if not isinstance(obj, _IonNature):
            ion_type = _ion_type(obj)
            yield _FROM_ION_TYPE[ion_type].from_value(ion_type, obj)
        else:
            ion_type = obj.ion_type
        if isinstance(obj, IonPyNull):
            obj = None
        yield _FROM_ION_TYPE[ion_type].from_value(ion_type, obj, annotations=('annot1', 'annot2'))
        if isinstance(obj, list):
            yield _FROM_ION_TYPE[ion_type].from_value(IonType.SEXP, obj)
            yield _FROM_ION_TYPE[ion_type].from_value(IonType.SEXP, obj, annotations=('annot1', 'annot2'))


def _assert_roundtrip(before, after):
    # All loaded Ion values extend _IonNature, even if they were dumped from primitives. This recursively
    # wraps each input value in _IonNature for comparison against the output.
    def _to_ion_nature(obj):
        out = obj
        if not isinstance(out, _IonNature):
            ion_type = _ion_type(out)
            out = _FROM_ION_TYPE[ion_type].from_value(ion_type, out)
        if isinstance(out, dict):
            update = {}
            for field, value in six.iteritems(out):
                update[field] = _to_ion_nature(value)
            out = update
        elif isinstance(out, list):
            update = []
            for value in out:
                update.append(_to_ion_nature(value))
            out = update
        return out
    assert _to_ion_nature(before) == after


@parametrize(
    *tuple(_generate_roundtrips(_ROUNDTRIPS))
)
def test_roundtrip(obj):
    out = BytesIO()
    dump(obj, out)
    out.seek(0)
    res = load(out)
    _assert_roundtrip(obj, res)


def test_single_value_with_stream_fails():
    out = BytesIO()
    dump(['foo', 123], out, sequence_as_stream=True)
    out.seek(0)
    with raises(IonException):
        load(out, single_value=True)


def test_unknown_object_type_fails():
    class Dummy:
        pass
    out = BytesIO()
    with raises(TypeError):
        dump(Dummy(), out)
