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
from functools import partial
from io import BytesIO

from decimal import Decimal
from itertools import chain
from math import isnan

import six
from pytest import raises

from amazon.ion.exceptions import IonException
from amazon.ion.symbols import SymbolToken, SYSTEM_SYMBOL_TABLE
from amazon.ion.writer_binary import _IVM
from amazon.ion.core import IonType, IonEvent, IonEventType, OffsetTZInfo
from amazon.ion.simple_types import IonPyDict, IonPyText, IonPyList, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, \
    IonPyDecimal, IonPyTimestamp, IonPyBytes, IonPySymbol, _IonNature
from amazon.ion.simpleion import dump, load, _ion_type, _FROM_ION_TYPE
from amazon.ion.util import record
from amazon.ion.writer_binary_raw import _serialize_symbol, _write_length
from tests.writer_util import VARUINT_END_BYTE, ION_ENCODED_INT_ZERO, SIMPLE_SCALARS_MAP_BINARY, SIMPLE_SCALARS_MAP_TEXT
from tests import parametrize


_st = partial(SymbolToken, sid=None, location=None)


class _Parameter(record('desc', 'obj', 'expected', 'has_symbols', ('stream', False))):
    def __str__(self):
        return self.desc


class _Expected:
    def __init__(self, binary, text):
        self.binary = [binary]
        self.text = [text]

_SIMPLE_CONTAINER_MAP = {
    IonType.LIST: (
        (
            [[], ],
            _Expected(b'\xB0', b'[]')
        ),
        (
            [IonPyList.from_value(IonType.LIST, []), ],
            _Expected(b'\xB0', b'[]')
        ),
        (
            [[0], ],
            _Expected(
                bytearray([
                    0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
                b'[0]'
            )
        ),
        (
            [IonPyList.from_value(IonType.LIST, [0]), ],
            _Expected(
                bytearray([
                    0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
                b'[0]'
            )
        ),
    ),
    IonType.SEXP: (
        (
            [IonPyList.from_value(IonType.SEXP, []), ],
            _Expected(b'\xC0', b'()')
        ),
        (
            [IonPyList.from_value(IonType.SEXP, [0]), ],
            _Expected(
                bytearray([
                    0xC0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
                b'(0)'
            )
        ),
    ),
    IonType.STRUCT: (
        (
            [{}, ],
            _Expected(b'\xD0', b'{}')
        ),
        (
            [IonPyDict.from_value(IonType.STRUCT, {}), ],
            _Expected(b'\xD0', b'{}')
        ),
        (
            [{u'foo': 0}, ],
            _Expected(
                bytearray([
                    0xDE,  # The lower nibble may vary. It does not indicate actual length unless it's 0.
                    VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                    VARUINT_END_BYTE | 10,
                    ION_ENCODED_INT_ZERO
                ]),
                b"{'foo':0}"
            )
        ),
        (
            [IonPyDict.from_value(IonType.STRUCT, {u'foo': 0}), ],
            _Expected(
                bytearray([
                    0xDE,  # The lower nibble may vary. It does not indicate actual length unless it's 0.
                    VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                    VARUINT_END_BYTE | 10,
                    ION_ENCODED_INT_ZERO
                ]),
                b"{'foo':0}"
            )
        ),
    ),
}


def generate_scalars_binary(scalars_map, preceding_symbols=0):
    for ion_type, values in six.iteritems(scalars_map):
        for native, expected in values:
            native_expected = expected
            has_symbols = False
            if native is None:
                # An un-adorned 'None' doesn't contain enough information to determine its Ion type
                native_expected = b'\x0f'
            elif ion_type is IonType.CLOB:
                # All six.binary_type are treated as BLOBs unless wrapped by an _IonNature
                tid = six.byte2int(expected) + 0x10  # increment upper nibble for clob -> blob; keep lower nibble
                native_expected = bytearray([tid]) + expected[1:]
            elif ion_type is IonType.SYMBOL and native is not None:
                has_symbols = True
            elif ion_type is IonType.STRING:
                # Encode all strings as symbols too.
                symbol_expected = _serialize_symbol(
                    IonEvent(IonEventType.SCALAR, IonType.SYMBOL, SymbolToken(None, 10 + preceding_symbols)))
                yield _Parameter(IonType.SYMBOL.name + ' ' + native,
                                 IonPyText.from_value(IonType.SYMBOL, native), symbol_expected, True)
            yield _Parameter('%s %s' % (ion_type.name, native), native, native_expected, has_symbols)
            wrapper = _FROM_ION_TYPE[ion_type].from_value(ion_type, native)
            yield _Parameter(repr(wrapper), wrapper, expected, has_symbols)


def generate_containers_binary(container_map, preceding_symbols=0):
    for ion_type, container in six.iteritems(container_map):
        for test_tuple in container:
            obj = test_tuple[0]
            expecteds = test_tuple[1].binary
            has_symbols = False
            for elem in obj:
                if isinstance(elem, dict) and len(elem) > 0:
                    has_symbols = True
            if has_symbols and preceding_symbols:
                for expected in expecteds:
                    field_sid = expected[-2] & (~VARUINT_END_BYTE)
                    expected[-2] = VARUINT_END_BYTE | (preceding_symbols + field_sid)
            expected = bytearray()
            for e in expecteds:
                expected.extend(e)
            yield _Parameter(repr(obj), obj, expected, has_symbols, True)


def generate_annotated_values_binary(scalars_map, container_map):
    for value_p in chain(generate_scalars_binary(scalars_map, preceding_symbols=2),
                         generate_containers_binary(container_map, preceding_symbols=2)):
        obj = value_p.obj
        if not isinstance(obj, _IonNature):
            continue
        obj.ion_annotations = (_st(u'annot1'), _st(u'annot2'),)
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
        yield _Parameter(
            desc='ANNOTATED %s' % value_p.desc,
            obj=obj,
            expected=bytearray(wrapper) + value_p.expected,
            has_symbols=True,
            stream=value_p.stream
        )


@parametrize(
    *tuple(chain(
        generate_scalars_binary(SIMPLE_SCALARS_MAP_BINARY),
        generate_containers_binary(_SIMPLE_CONTAINER_MAP),
        generate_annotated_values_binary(SIMPLE_SCALARS_MAP_BINARY, _SIMPLE_CONTAINER_MAP),
    ))
)
def test_dump_load_binary(p):
    # test dump
    out = BytesIO()
    dump(p.obj, out, binary=True, sequence_as_stream=p.stream)
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


def generate_scalars_text(scalars_map):
    for ion_type, values in six.iteritems(scalars_map):
        for native, expected in values:
            native_expected = expected
            has_symbols = False
            if native is None:
                native_expected = b'null'
            elif ion_type is IonType.CLOB:
                # All six.binary_type are treated as BLOBs unless wrapped by an _IonNature
                native = _FROM_ION_TYPE[ion_type].from_value(ion_type, native)
            elif ion_type is IonType.SYMBOL and native is not None:
                has_symbols = True
                if not isinstance(native, SymbolToken):
                    native = _st(native)
            yield _Parameter('%s %s' % (ion_type.name, native), native, native_expected, has_symbols)
            if not (ion_type is IonType.CLOB):
                # Clobs were already wrapped.
                wrapper = _FROM_ION_TYPE[ion_type].from_value(ion_type, native)
                yield _Parameter(repr(wrapper), wrapper, expected, has_symbols)


def generate_containers_text(container_map):
    for ion_type, container in six.iteritems(container_map):
        for test_tuple in container:
            obj = test_tuple[0]
            expected = test_tuple[1].text[0]
            has_symbols = False
            for elem in obj:
                if isinstance(elem, dict) and len(elem) > 0:
                    has_symbols = True
            yield _Parameter(repr(obj), obj, expected, has_symbols, True)


def generate_annotated_values_text(scalars_map, container_map):
    for value_p in chain(generate_scalars_text(scalars_map),
                         generate_containers_text(container_map)):
        obj = value_p.obj
        if not isinstance(obj, _IonNature):
            continue
        obj.ion_annotations = (_st(u'annot1'), _st(u'annot2'),)
        yield _Parameter(
            desc='ANNOTATED %s' % value_p.desc,
            obj=obj,
            expected=b"'annot1'::'annot2'::" + value_p.expected,  # TODO text writer should emit unquoted symbol tokens.
            has_symbols=True,
            stream=value_p.stream
        )


@parametrize(
    *tuple(chain(
        generate_scalars_text(SIMPLE_SCALARS_MAP_TEXT),
        generate_containers_text(_SIMPLE_CONTAINER_MAP),
        generate_annotated_values_text(SIMPLE_SCALARS_MAP_TEXT, _SIMPLE_CONTAINER_MAP),
    ))
)
def test_dump_load_text(p):
    # test dump
    out = BytesIO()
    dump(p.obj, out, binary=False, sequence_as_stream=p.stream)
    res = out.getvalue()
    if not p.has_symbols:
        assert (b'$ion_1_0 ' + p.expected) == res
    else:
        # The payload contains a LST. The value comes last, so compare the end bytes.
        assert p.expected == res[len(res) - len(p.expected):]
    # test load
    out.seek(0)
    res = load(out, single_value=(not p.stream))
    if p.obj is None:
        assert isinstance(res, IonPyNull)
    else:
        def equals():
            if p.obj == res:
                return True
            if isinstance(p.obj, SymbolToken):
                if p.obj.text is None:
                    assert p.obj.sid is not None
                    # System symbol IDs are mapped correctly in the text format.
                    token = SYSTEM_SYMBOL_TABLE.get(p.obj.sid)
                    assert token is not None  # User symbols with unknown text won't be successfully read.
                    expected_token = token
                else:
                    # User symbols with text are not automatically mapped to SIDs in the text format.
                    expected_token = SymbolToken(p.obj.text, None)
                return expected_token == res
            else:
                try:
                    return isnan(p.obj) and isnan(res)
                except TypeError:
                    return False
        if not equals():
            assert p.obj == res  # Redundant, but provides better error message.


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
    u'a\u3000\x20c',
    _st(u''),
    _st(u'abc'),
    _st(u'abcdefghijklmno'),
    _st(u'a\U0001f4a9c'),
    _st(u'a\u3000\x20c'),
    b'abcd',
    IonPyBytes.from_value(IonType.CLOB, b'abcd'),
    [[[]]],
    [[], [], []],
    [{}, {}, {}],
    {u'foo': [], u'bar': [], u'baz': []},
    {u'foo': {u'foo': {}}},
    [{u'foo': [{u'foo': []}]}],
    {u'foo': [{u'foo': []}]},
    {
         u'foo': IonPyText.from_value(IonType.STRING, u'bar', annotations=(u'str',)),
         u'baz': 123,
         u'lst': IonPyList.from_value(IonType.LIST, [
             True, None, 1.23e4, IonPyText.from_value(IonType.SYMBOL, u'sym')
         ]),
         u'sxp': IonPyList.from_value(IonType.SEXP, [
             False, IonPyNull.from_value(IonType.STRUCT, None, (u'class',)), Decimal('5.678')
         ])
    },

]


def _generate_roundtrips(roundtrips):
    for is_binary in (True, False):

        def _adjust_sids(annotations=()):
            if is_binary and isinstance(obj, SymbolToken):
                return SymbolToken(obj.text, 10 + len(annotations))
            return obj

        def _to_obj(to_type=None, annotations=()):
            if to_type is None:
                to_type = ion_type
            obj_out = _adjust_sids(annotations)
            return _FROM_ION_TYPE[ion_type].from_value(to_type, obj_out, annotations=annotations), is_binary

        for obj in roundtrips:
            obj = _adjust_sids()
            yield obj, is_binary
            if not isinstance(obj, _IonNature):
                ion_type = _ion_type(obj)
                yield _to_obj()
            else:
                ion_type = obj.ion_type
            if isinstance(obj, IonPyNull):
                obj = None
            yield _to_obj(annotations=(u'annot1', u'annot2'))
            if isinstance(obj, list):
                yield _to_obj(IonType.SEXP)
                yield _to_obj(IonType.SEXP, annotations=(u'annot1', u'annot2'))


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
def test_roundtrip(p):
    obj, is_binary = p
    out = BytesIO()
    dump(obj, out, binary=is_binary)
    out.seek(0)
    res = load(out)
    _assert_roundtrip(obj, res)


@parametrize(True, False)
def test_single_value_with_stream_fails(is_binary):
    out = BytesIO()
    dump(['foo', 123], out, binary=is_binary, sequence_as_stream=True)
    out.seek(0)
    with raises(IonException):
        load(out, single_value=True)


@parametrize(True, False)
def test_unknown_object_type_fails(is_binary):
    class Dummy:
        pass
    out = BytesIO()
    with raises(TypeError):
        dump(Dummy(), out, binary=is_binary)
