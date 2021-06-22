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
import re
from pytest import raises

from amazon.ion.exceptions import IonException
from amazon.ion.symbols import SymbolToken, SYSTEM_SYMBOL_TABLE
from amazon.ion.writer_binary import _IVM
from amazon.ion.core import IonType, IonEvent, IonEventType, OffsetTZInfo, Multimap
from amazon.ion.simple_types import IonPyDict, IonPyText, IonPyList, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, \
    IonPyDecimal, IonPyTimestamp, IonPyBytes, IonPySymbol, _IonNature
from amazon.ion.equivalence import ion_equals
from amazon.ion.simpleion import dump, dumps, load, loads, _ion_type, _FROM_ION_TYPE, _FROM_TYPE_TUPLE_AS_SEXP, \
    _FROM_TYPE
from amazon.ion.util import record
from amazon.ion.writer_binary_raw import _serialize_symbol, _write_length
from tests.writer_util import VARUINT_END_BYTE, ION_ENCODED_INT_ZERO, SIMPLE_SCALARS_MAP_BINARY, SIMPLE_SCALARS_MAP_TEXT
from tests import parametrize
from amazon.ion.simpleion import c_ext

_st = partial(SymbolToken, sid=None, location=None)


class _Parameter(record('desc', 'obj', 'expected', 'has_symbols', ('stream', False), ('tuple_as_sexp', False))):
    def __str__(self):
        return self.desc


class _Expected(record('binary', 'text')):
    def __new__(cls, binary, text):
        return super(_Expected, cls).__new__(cls, (binary,), (text,))


def bytes_of(*args, **kwargs):
    return bytes(bytearray(*args, **kwargs))


_SIMPLE_CONTAINER_MAP = {
    IonType.LIST: (
        (
            [[], ],
            (_Expected(b'\xB0', b'[]'),)
        ),
        (
            [(), ],
            (_Expected(b'\xB0', b'[]'),)
        ),
        (
            [IonPyList.from_value(IonType.LIST, []), ],
            (_Expected(b'\xB0', b'[]'),)
        ),
        (
            [[0], ],
            (_Expected(
                bytes_of([
                    0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
                b'[0]'
            ),)
        ),
        (
            [(0,), ],
            (_Expected(
                bytes_of([
                    0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
                b'[0]'
            ),)
        ),
        (
            [IonPyList.from_value(IonType.LIST, [0]), ],
            (_Expected(
                bytes_of([
                    0xB0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
                b'[0]'
            ),)
        ),
    ),
    IonType.SEXP: (
        (
            [IonPyList.from_value(IonType.SEXP, []), ],
            (_Expected(b'\xC0', b'()'),)
        ),
        (
            [IonPyList.from_value(IonType.SEXP, [0]), ],
            (_Expected(
                bytes_of([
                    0xC0 | 0x01,  # Int value 0 fits in 1 byte.
                    ION_ENCODED_INT_ZERO
                ]),
                b'(0)'
            ),)
        ),
        (
            [(), ],  # NOTE: the generators will detect this and set 'tuple_as_sexp' to True for this case.
            (_Expected(b'\xC0', b'()'),)
        )
    ),
    IonType.STRUCT: (
        (
            [{}, ],
            (_Expected(b'\xD0', b'{}'),)
        ),
        (
            [IonPyDict.from_value(IonType.STRUCT, {}), ],
            (_Expected(b'\xD0', b'{}'),)
        ),
        (
            [{u'': u''}, ],
            (_Expected(
                bytes_of([
                    0xDE,  # The lower nibble may vary. It does not indicate actual length unless it's 0.
                    VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                    VARUINT_END_BYTE | 10,
                    0x80  # Empty string
                ]),
                b"{'':\"\"}"
            ),
             _Expected(
                 bytes_of([
                     0xD2,
                     VARUINT_END_BYTE | 10,
                     0x80  # Empty string
                 ]),
                 b"{'':\"\"}"
             ),
            )
        ),
        (
            [{u'foo': 0}, ],
            (_Expected(
                bytes_of([
                    0xDE,  # The lower nibble may vary. It does not indicate actual length unless it's 0.
                    VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                    VARUINT_END_BYTE | 10,
                    ION_ENCODED_INT_ZERO
                ]),
                b"{foo:0}"
            ),
             _Expected(
                 bytes_of([
                     0xD2,
                     VARUINT_END_BYTE | 10,
                     ION_ENCODED_INT_ZERO
                 ]),
                 b"{foo:0}"
             ),
            )
        ),
        (
            [IonPyDict.from_value(IonType.STRUCT, {u'foo': 0}), ],
            (_Expected(
                bytes_of([
                    0xDE,  # The lower nibble may vary. It does not indicate actual length unless it's 0.
                    VARUINT_END_BYTE | 2,  # Field name 10 and value 0 each fit in 1 byte.
                    VARUINT_END_BYTE | 10,
                    ION_ENCODED_INT_ZERO
                ]),
                b"{foo:0}"
            ),
             _Expected(
                 bytes_of([
                     0xD2,
                     VARUINT_END_BYTE | 10,
                     ION_ENCODED_INT_ZERO
                 ]),
                 b"{foo:0}"
             ),
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
                if c_ext:
                    symbol_expected = _serialize_symbol(
                        IonEvent(IonEventType.SCALAR, IonType.SYMBOL, SymbolToken(None, 10)))
                else:
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
            expecteds = test_tuple[1]
            final_expected = ()
            has_symbols = False
            tuple_as_sexp = False
            for one_expected in expecteds:
                one_expected = one_expected.binary
                for elem in obj:
                    if isinstance(elem, (dict, Multimap)) and len(elem) > 0:
                        has_symbols = True
                    elif ion_type is IonType.SEXP and isinstance(elem, tuple):
                        tuple_as_sexp = True
                if has_symbols and preceding_symbols:
                    # we need to make a distinct copy that will contain an altered encoding
                    one_expected = []
                    for expected in one_expected:
                        expected = bytearray(expected)
                        field_sid = expected[-2] & (~VARUINT_END_BYTE)
                        expected[-2] = VARUINT_END_BYTE | (preceding_symbols + field_sid)
                        one_expected.append(expected)
                expected = bytearray()
                for e in one_expected:
                    expected.extend(e)
                final_expected += (expected,)
            yield _Parameter(repr(obj), obj, final_expected, has_symbols, True, tuple_as_sexp=tuple_as_sexp)


def generate_annotated_values_binary(scalars_map, container_map):
    for value_p in chain(generate_scalars_binary(scalars_map, preceding_symbols=2),
                         generate_containers_binary(container_map, preceding_symbols=2)):
        obj = value_p.obj
        if not isinstance(obj, _IonNature):
            continue
        obj.ion_annotations = (_st(u'annot1'), _st(u'annot2'),)
        annot_length = 2  # 10 and 11 each fit in one VarUInt byte
        annot_length_length = 1  # 2 fits in one VarUInt byte

        final_expected = ()
        if isinstance(value_p.expected, (list, tuple)):
            expecteds = value_p.expected
        else:
            expecteds = (value_p.expected, )
        for one_expected in expecteds:
            value_length = len(one_expected)
            length_field = annot_length + annot_length_length + value_length
            wrapper = []
            _write_length(wrapper, length_field, 0xE0)

            if c_ext and obj.ion_type is IonType.SYMBOL and not isinstance(obj, IonPyNull) \
                    and not (hasattr(obj, 'sid') and (obj.sid < 10 or obj.sid is None)):
                wrapper.extend([
                    VARUINT_END_BYTE | annot_length,
                    VARUINT_END_BYTE | 11,
                    VARUINT_END_BYTE | 12
                ])
            else:
                wrapper.extend([
                    VARUINT_END_BYTE | annot_length,
                    VARUINT_END_BYTE | 10,
                    VARUINT_END_BYTE | 11
                ])

            exp = bytearray(wrapper) + one_expected
            final_expected += (exp, )

        yield _Parameter(
            desc='ANNOTATED %s' % value_p.desc,
            obj=obj,
            expected=final_expected,
            has_symbols=True,
            stream=value_p.stream
        )


def _assert_symbol_aware_ion_equals(assertion, output):
    if ion_equals(assertion, output):
        return True
    if isinstance(assertion, SymbolToken):
        expected_token = assertion
        if assertion.text is None:
            assert assertion.sid is not None
            # System symbol IDs are mapped correctly in the text format.
            token = SYSTEM_SYMBOL_TABLE.get(assertion.sid)
            assert token is not None  # User symbols with unknown text won't be successfully read.
            expected_token = token
        return expected_token == output
    else:
        try:
            return isnan(assertion) and isnan(output)
        except TypeError:
            return False


def _dump_load_run(p, dumps_func, loads_func, binary):
    # test dump
    res = dumps_func(p.obj, binary=binary, sequence_as_stream=p.stream, tuple_as_sexp=p.tuple_as_sexp,
                     omit_version_marker=True)
    if isinstance(p.expected, (tuple, list)):
        expecteds = p.expected
    else:
        expecteds = (p.expected,)
    write_success = False
    for expected in expecteds:
        if not p.has_symbols:
            if binary:
                write_success = (_IVM + expected) == res or expected == res
            else:
                write_success = (b'$ion_1_0 ' + expected) == res or expected == res
        else:
            # The payload contains a LST. The value comes last, so compare the end bytes.
            write_success = expected == res[len(res) - len(expected):]
        if write_success:
            break
    if not write_success:
        raise AssertionError('Expected: %s , found %s' % (expecteds, res))
    # test load
    res = loads_func(res, single_value=(not p.stream))
    _assert_symbol_aware_ion_equals(p.obj, res)


def _simple_dumps(obj, *args, **kw):
    buf = BytesIO()
    dump(obj, buf, *args, **kw)
    return buf.getvalue()


def _simple_loads(data, *args, **kw):
    buf = BytesIO()
    buf.write(data)
    buf.seek(0)
    return load(buf, *args, **kw)


@parametrize(
    *tuple(chain(
        generate_scalars_binary(SIMPLE_SCALARS_MAP_BINARY),
        generate_containers_binary(_SIMPLE_CONTAINER_MAP),
        generate_annotated_values_binary(SIMPLE_SCALARS_MAP_BINARY, _SIMPLE_CONTAINER_MAP),
    ))
)
def test_dump_load_binary(p):
    _dump_load_run(p, _simple_dumps, _simple_loads, binary=True)


@parametrize(
    *tuple(chain(
        generate_scalars_binary(SIMPLE_SCALARS_MAP_BINARY),
        generate_containers_binary(_SIMPLE_CONTAINER_MAP),
        generate_annotated_values_binary(SIMPLE_SCALARS_MAP_BINARY, _SIMPLE_CONTAINER_MAP),
    ))
)
def test_dumps_loads_binary(p):
    _dump_load_run(p, dumps, loads, binary=True)


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
            expected = test_tuple[1]
            final_expected = ()
            has_symbols = False
            tuple_as_sexp = False

            for one_expected in expected:
                one_expected = one_expected.text[0]
                for elem in obj:
                    if isinstance(elem, dict) and len(elem) > 0:
                        has_symbols = True
                    elif ion_type is IonType.SEXP and isinstance(elem, tuple):
                        tuple_as_sexp = True
                final_expected += (one_expected,)
            yield _Parameter(repr(obj), obj, final_expected, has_symbols, True, tuple_as_sexp=tuple_as_sexp)


def generate_annotated_values_text(scalars_map, container_map):
    for value_p in chain(generate_scalars_text(scalars_map),
                         generate_containers_text(container_map)):
        obj = value_p.obj
        if not isinstance(obj, _IonNature):
            continue
        obj.ion_annotations = (_st(u'annot1'), _st(u'annot2'),)

        annotated_expected = ()
        if isinstance(value_p.expected, (tuple, list)):
            for expected in value_p.expected:
                annotated_expected += (b"annot1::annot2::" + expected,)
        else:
            annotated_expected += (b"annot1::annot2::" + value_p.expected,)

        yield _Parameter(
            desc='ANNOTATED %s' % value_p.desc,
            obj=obj,
            expected=annotated_expected,
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
    _dump_load_run(p, _simple_dumps, _simple_loads, binary=False)


@parametrize(
    *tuple(chain(
        generate_scalars_text(SIMPLE_SCALARS_MAP_TEXT),
        generate_containers_text(_SIMPLE_CONTAINER_MAP),
        generate_annotated_values_text(SIMPLE_SCALARS_MAP_TEXT, _SIMPLE_CONTAINER_MAP),
    ))
)
def test_dumps_loads_text(p):
    def dump_func(*args, **kw):
        sval = dumps(*args, **kw)
        # encode to UTF-8 bytes for comparisons
        return sval.encode('UTF-8')

    _dump_load_run(p, dump_func, loads, binary=False)


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
    [(), (), ()],
    (((),),),
    ([], [], [],),
    {u'foo': [], u'bar': (), u'baz': []},
    {u'foo': {u'foo': {}}},
    [{u'foo': [{u'foo': []}]}],
    {u'foo': ({u'foo': []},)},
    {
         u'foo': IonPyText.from_value(IonType.STRING, u'bar', annotations=(u'str',)),
         u'baz': 123,
         u'lst': IonPyList.from_value(IonType.LIST, [
             True, None, 1.23e4, IonPyText.from_value(IonType.SYMBOL, u'sym')
         ]),
         u'sxp': IonPyList.from_value(IonType.SEXP, [
             False, IonPyNull.from_value(IonType.STRUCT, None, (u'class',)), Decimal('5.678'),
             IonPyText.from_value(IonType.SYMBOL, u'sym2'), IonPyText.from_value(IonType.SYMBOL, u'_a_s_d_f_')
         ]),
         u'lst_or_sxp': (123, u'abc')
    },

]


def _generate_roundtrips(roundtrips):
    for is_binary in (True, False):
        for indent in ('not used',) if is_binary else (None, '', ' ', '   ', '\t', '\n\t\n  '):
            def _adjust_sids(annotations=()):
                if is_binary and isinstance(obj, SymbolToken):
                    return SymbolToken(obj.text, 10 + len(annotations))
                return obj

            def _to_obj(to_type=None, annotations=(), tuple_as_sexp=False):
                if to_type is None:
                    to_type = ion_type
                obj_out = _adjust_sids(annotations)
                return _FROM_ION_TYPE[ion_type].from_value(to_type, obj_out, annotations=annotations), is_binary, indent, tuple_as_sexp

            for obj in roundtrips:
                obj = _adjust_sids()
                yield obj, is_binary, indent, False
                if not isinstance(obj, _IonNature):
                    ion_type = _ion_type(obj, _FROM_TYPE)
                    yield _to_obj()
                else:
                    ion_type = obj.ion_type
                if isinstance(obj, IonPyNull):
                    obj = None
                yield _to_obj(annotations=(u'annot1', u'annot2'))
                if isinstance(obj, list):
                    yield _to_obj(IonType.SEXP)
                    yield _to_obj(IonType.SEXP, annotations=(u'annot1', u'annot2'))
                if isinstance(obj, tuple) and not isinstance(obj, SymbolToken):
                    yield _to_obj(IonType.SEXP, tuple_as_sexp=True)
                    yield _to_obj(IonType.SEXP, annotations=(u'annot1', u'annot2'), tuple_as_sexp=True)


def _assert_roundtrip(before, after, tuple_as_sexp):
    # All loaded Ion values extend _IonNature, even if they were dumped from primitives. This recursively
    # wraps each input value in _IonNature for comparison against the output.
    def _to_ion_nature(obj):
        out = obj
        if not isinstance(out, _IonNature):
            from_type = _FROM_TYPE_TUPLE_AS_SEXP if tuple_as_sexp else _FROM_TYPE
            ion_type = _ion_type(out, from_type)
            out = _FROM_ION_TYPE[ion_type].from_value(ion_type, out)
        if isinstance(out, dict):
            update = {}
            for field, value in six.iteritems(out):
                update[field] = _to_ion_nature(value)
            update = IonPyDict.from_value(out.ion_type, update, out.ion_annotations)
            out = update
        elif isinstance(out, list):
            update = []
            for value in out:
                update.append(_to_ion_nature(value))
            update = IonPyList.from_value(out.ion_type, update, out.ion_annotations)
            out = update

        return out
    assert ion_equals(_to_ion_nature(before), after)


@parametrize(
    *tuple(_generate_roundtrips(_ROUNDTRIPS))
)
def test_roundtrip(p):
    obj, is_binary, indent, tuple_as_sexp = p
    out = BytesIO()
    dump(obj, out, binary=is_binary, indent=indent, tuple_as_sexp=tuple_as_sexp)
    out.seek(0)
    res = load(out)
    _assert_roundtrip(obj, res, tuple_as_sexp)


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

class PrettyPrintParams(record('ion_text', 'indent', ('exact_text', None), ('regexes', []))):
    pass

@parametrize(
        PrettyPrintParams(ion_text='a', indent='  ', exact_text="$ion_1_0\na"),
        PrettyPrintParams(ion_text='"a"', indent='  ', exact_text="$ion_1_0\n\"a\""),
        PrettyPrintParams(ion_text='\'$a__9\'', indent='  ', exact_text="$ion_1_0\n$a__9"),
        PrettyPrintParams(ion_text='\'$a_\\\'_9\'', indent='  ', exact_text="$ion_1_0\n\'$a_\\\'_9\'"),
        PrettyPrintParams(ion_text='[a, b, chair::2008-08-08T]', indent='  ',
            exact_text="$ion_1_0\n[\n  a,\n  b,\n  chair::2008-08-08T\n]"),
        PrettyPrintParams(ion_text='[a, b, chair::2008-08-08T]', indent=None, # not pretty print
            exact_text="$ion_1_0 [a,b,chair::2008-08-08T]"),
        PrettyPrintParams(ion_text='[apple, {roof: false}]', indent='\t',
            exact_text="$ion_1_0\n[\n\tapple,\n\t{\n\t\troof: false\n\t}\n]"),
        PrettyPrintParams(ion_text='[apple, "banana", {roof: false}]', indent='\t',
            exact_text="$ion_1_0\n[\n\tapple,\n\t\"banana\",\n\t{\n\t\troof: false\n\t}\n]"),
        PrettyPrintParams(ion_text='[apple, {roof: false, walls:4, door: wood::large::true}]', indent='\t',
            regexes=["\\A\\$ion_1_0\n\\[\n\tapple,\n\t\\{", "\n\t\tdoor: wood::large::true,?\n",
                "\n\t\troof: false,?\n", "\n\t\twalls: 4,?\n", "\n\t\\}\n\\]\\Z"])
        )
def test_pretty_print(p):
    if c_ext:
        # TODO support pretty print for C extension.
        return
    ion_text, indent, exact_text, regexes = p
    ion_value = loads(ion_text)
    actual_pretty_ion_text = dumps(ion_value, binary=False, indent=indent)
    if exact_text is not None:
        assert actual_pretty_ion_text == exact_text
    for regex_str in regexes:
        assert re.search(regex_str, actual_pretty_ion_text, re.M) is not None


# Regression test for issue #95
def test_struct_field():
    # pass a dict through simpleion to get a reconstituted dict of Ion values.
    struct_a = loads(dumps({u'dont_remember_my_name': 1}))

    # copy the value of the "dont_remember_my_name" field to a new struct, which is also passed through simpleion
    struct_b = {u'new_name': struct_a[u"dont_remember_my_name"]}
    struct_c = loads(dumps(struct_b))

    # The bug identified in ion-python#95 is that the name of the original field is somehow preserved.
    # verify this no longer happens
    assert u'dont_remember_my_name' not in struct_c
    assert u'new_name' in struct_c


def test_dumps_omit_version_marker():
    v = loads('5')
    assert dumps(v, binary=False) == '$ion_1_0 5'
    assert dumps(v, binary=False, omit_version_marker=True) == '5'

    # verify no impact on binary output
    assert dumps(v) == b'\xe0\x01\x00\xea\x21\x05'
    assert dumps(v, omit_version_marker=True) == b'\xe0\x01\x00\xea\x21\x05'

