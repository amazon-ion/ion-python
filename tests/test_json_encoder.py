# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

from amazon.ion.core import IonType
from amazon.ion.simple_types import IonPyList, IonPyDict, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, IonPyDecimal, \
    IonPyTimestamp, IonPyText, IonPyBytes, IonPySymbol
from amazon.ion.symbols import SymbolToken
from amazon.ion.simpleion import dumps, loads
from base64 import b64encode
import datetime
from decimal import Decimal
import json
import pytest
import sys

is_pypy = hasattr(sys, "pypy_version_info")

if is_pypy:
    # PyPy is not supported. Expect an ImportError.
    with pytest.raises(ImportError):
        from amazon.ion.json_encoder import IonToJSONEncoder
else:
    from amazon.ion.json_encoder import IonToJSONEncoder


def test_null():
    if is_pypy:
        return

    ion_types = [
        (IonPyNull, IonType.NULL),
        (IonPyBool, IonType.BOOL),
        (IonPyInt, IonType.INT),
        (IonPyFloat, IonType.FLOAT),
        (IonPyDecimal, IonType.DECIMAL),
        (IonPyTimestamp, IonType.TIMESTAMP),
        (IonPyText, IonType.STRING),
        (IonPySymbol, IonType.SYMBOL),
        (IonPyBytes, IonType.BLOB),
        (IonPyBytes, IonType.CLOB),
        (IonPyDict, IonType.STRUCT),
        (IonPyList, IonType.LIST),
        (IonPyList, IonType.SEXP)
    ]
    for ion_class, ion_type in ion_types:
        ion_value = ion_class.from_value(ion_type, None)
        json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
        assert json_string == 'null'


def test_bool():
    if is_pypy:
        return

    ion_value = loads(dumps(False))
    assert isinstance(ion_value, IonPyBool) and ion_value.ion_type == IonType.BOOL
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == 'false'


def test_int():
    if is_pypy:
        return

    ion_value = loads(dumps(-123))
    assert isinstance(ion_value, IonPyInt) and ion_value.ion_type == IonType.INT
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '-123'


def test_float():
    if is_pypy:
        return

    ion_value = loads(dumps(float(123.456)))
    assert isinstance(ion_value, IonPyFloat) and ion_value.ion_type == IonType.FLOAT
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '123.456'


def test_float_nan():
    if is_pypy:
        return

    ion_value = loads(dumps(float("NaN")))
    assert isinstance(ion_value, IonPyFloat) and ion_value.ion_type == IonType.FLOAT
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == 'null'


def test_float_inf():
    if is_pypy:
        return

    ion_value = loads(dumps(float("Inf")))
    assert isinstance(ion_value, IonPyFloat) and ion_value.ion_type == IonType.FLOAT
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == 'null'


def test_decimal():
    if is_pypy:
        return

    ion_value = loads(dumps(Decimal('123.456')))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '123.456'


def test_decimal_exp():
    if is_pypy:
        return

    ion_value = loads(dumps(Decimal('1.23456e2')))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '123.456'


def test_decimal_exp_negative():
    if is_pypy:
        return

    ion_value = loads(dumps(Decimal('12345.6e-2')))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '123.456'


def test_decimal_exp_large():
    if is_pypy:
        return

    ion_value = loads(dumps(Decimal('123.456e34')))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '1.23456e+36'


def test_decimal_exp_large_negative():
    if is_pypy:
        return

    ion_value = loads(dumps(Decimal('123.456e-34')))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '1.23456e-32'


def test_timestamp():
    if is_pypy:
        return

    ion_value = loads(dumps(datetime.datetime(2010, 6, 15, 3, 30, 45)))
    assert isinstance(ion_value, IonPyTimestamp) and ion_value.ion_type == IonType.TIMESTAMP
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '"2010-06-15 03:30:45"'


def test_symbol():
    if is_pypy:
        return

    ion_value = loads(dumps(SymbolToken(str("Symbol"), None)))
    assert isinstance(ion_value, IonPySymbol) and ion_value.ion_type == IonType.SYMBOL
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '"Symbol"'


def test_string():
    if is_pypy:
        return

    ion_value = loads(dumps(str("String")))
    assert isinstance(ion_value, IonPyText) and ion_value.ion_type == IonType.STRING
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '"String"'


def test_clob():
    if is_pypy:
        return

    ion_value = loads(dumps(IonPyBytes.from_value(IonType.CLOB, bytearray.fromhex("06 49 6f 6e 06"))))
    assert isinstance(ion_value, IonPyBytes) and ion_value.ion_type == IonType.CLOB
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '"\\u0006Ion\\u0006"'


def test_blob():
    if is_pypy:
        return

    ion_value = loads(dumps(bytes("Ion", "ASCII")))
    assert isinstance(ion_value, IonPyBytes) and ion_value.ion_type == IonType.BLOB
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '"SW9u"'


def test_list():
    if is_pypy:
        return

    ion_value = loads(dumps([str("Ion"), 123]))
    assert isinstance(ion_value, IonPyList) and ion_value.ion_type == IonType.LIST
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '["Ion", 123]'


def test_sexp():
    if is_pypy:
        return

    value = (str("Ion"), 123)
    ion_value = loads(dumps(loads(dumps(value, tuple_as_sexp=True))))
    assert isinstance(ion_value, IonPyList) and ion_value.ion_type == IonType.SEXP
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '["Ion", 123]'


def test_struct():
    if is_pypy:
        return

    value = {
        str("string_value"): str("Ion"),
        str("int_value"): 123,
        str("nested_struct"): {
            str("nested_value"): str("Nested Ion")
        }
    }
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyDict) and ion_value.ion_type == IonType.STRUCT
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    expected_string = '{"string_value": "Ion", "int_value": 123, "nested_struct": {"nested_value": "Nested Ion"}}'
    if not json_string == expected_string:
        # Assert as objects to handle different Python versions' JSON string key ordering
        assert json.loads(json_string) == json.loads(expected_string)


def test_annotation_suppression():
    if is_pypy:
        return

    ion_value = loads(dumps(IonPyInt.from_value(IonType.INT, 123, str("Annotation"))))
    assert isinstance(ion_value, IonPyInt) and ion_value.ion_type == IonType.INT
    json_string = json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json_string == '123'


@pytest.fixture(params=[
    (IonType.STRUCT, '{"value": %s}'),
    (IonType.LIST, '[%s]'),
    (IonType.SEXP, '[%s]'),
], ids=['struct', 'list', 'sexp'])
def ion_container(request):
    if is_pypy:
        pytest.skip("The JSON encoder is not supported on PyPy.")
    ion_type, expected_template = request.param

    def wrap(value):
        if ion_type == IonType.STRUCT:
            return IonPyDict.from_value(ion_type, {'value': value})
        return IonPyList.from_value(ion_type, [value])

    return wrap, expected_template


@pytest.mark.skipif(is_pypy, reason="The JSON encoder is not supported on PyPy.")
def test_native_value_added_to_loaded_struct():
    ion_value = loads('{hello: "world"}')
    ion_value['foo'] = 'bar'
    expected = '{"hello": "world", "foo": "bar"}'
    encoder = IonToJSONEncoder()
    assert json.dumps(ion_value, cls=IonToJSONEncoder) == expected
    assert encoder.encode(ion_value) == expected
    assert ''.join(encoder.iterencode(ion_value)) == expected
    assert isinstance(ion_value['hello'], IonPyText)
    assert type(ion_value['foo']) is str


@pytest.mark.parametrize('value, expected', [
    ('bar', '"bar"'),
    (42, '42'),
    (0, '0'),
    (-1, '-1'),
    (1.5, '1.5'),
    (True, 'true'),
    (False, 'false'),
    (None, 'null'),
    ([], '[]'),
    ({}, '{}'),
    ((), '[]'),
    (['x', 1], '["x", 1]'),
    ({'x': 1}, '{"x": 1}'),
    (('x', 1), '["x", 1]'),
])
def test_native_values_in_ion_containers(ion_container, value, expected):
    wrap, expected_template = ion_container
    assert json.dumps(wrap(value), cls=IonToJSONEncoder) == expected_template % expected


@pytest.mark.parametrize('ion_text, expected', [
    ('true', 'true'),
    ('false', 'false'),
    ('null.int', 'null'),
    ('1.25', '1.25'),
    ('1.5e0', '1.5'),
    ('2000-01-01T00:00:00Z', '"2000-01-01 00:00:00+00:00"'),
    ('symbol', '"symbol"'),
    ('{{SW9u}}', '"SW9u"'),
    ('{{"Ion"}}', '"Ion"'),
    ('nan', 'null'),
    ('+inf', 'null'),
    ('-inf', 'null'),
    ('annotation::1', '1'),
])
def test_ion_values_inside_native_containers(ion_container, ion_text, expected):
    wrap, expected_template = ion_container
    ion_value = loads(ion_text)
    native_value = ({'inner': ion_value},)
    result = json.dumps(wrap(native_value), cls=IonToJSONEncoder, allow_nan=False)
    assert result == expected_template % ('[{"inner": %s}]' % expected)
    assert native_value[0]['inner'] is ion_value


@pytest.mark.parametrize('value, expected', [
    (float('nan'), 'NaN'),
    (float('inf'), 'Infinity'),
    (float('-inf'), '-Infinity'),
])
def test_native_non_finite_floats_in_ion_containers(ion_container, value, expected):
    wrap, expected_template = ion_container
    ion_value = wrap(value)
    assert json.dumps(ion_value, cls=IonToJSONEncoder) == expected_template % expected
    with pytest.raises(ValueError):
        json.dumps(ion_value, cls=IonToJSONEncoder, allow_nan=False)


def test_native_dictionary_keys_in_ion_containers(ion_container):
    wrap, expected_template = ion_container
    value = {2: 'int', 2.5: 'float', False: 'bool', None: 'null'}
    expected = '{"2": "int", "2.5": "float", "false": "bool", "null": "null"}'
    assert json.dumps(wrap(value), cls=IonToJSONEncoder) == expected_template % expected


def test_skipkeys_in_ion_containers(ion_container):
    wrap, expected_template = ion_container
    ion_value = wrap({'keep': 1, (1, 2): 'skip'})
    with pytest.raises(TypeError):
        json.dumps(ion_value, cls=IonToJSONEncoder)
    assert json.dumps(ion_value, cls=IonToJSONEncoder, skipkeys=True) == (
        expected_template % '{"keep": 1}')


@pytest.mark.parametrize('value', [object(), {1}, b'bytes', Decimal('1.5')],
                         ids=['object', 'set', 'bytes', 'decimal'])
def test_unsupported_native_values_in_ion_containers(ion_container, value):
    wrap, _ = ion_container
    with pytest.raises(TypeError, match='is not JSON serializable'):
        json.dumps(wrap(value), cls=IonToJSONEncoder)


@pytest.mark.skipif(is_pypy, reason="The JSON encoder is not supported on PyPy.")
def test_duplicate_struct_fields_keep_latest_value():
    ion_value = loads('{value: 1, value: 2}')
    ion_value.add_item('value', 'latest')
    assert json.dumps(ion_value, cls=IonToJSONEncoder) == '{"value": "latest"}'
    assert ion_value.get_all_values('value') == [1, 2, 'latest']


def test_circular_ion_containers(ion_container):
    wrap, _ = ion_container
    ion_value = wrap(None)
    if isinstance(ion_value, IonPyDict):
        ion_value['value'] = ion_value
    else:
        ion_value[0] = ion_value
    with pytest.raises(ValueError, match='Circular reference detected'):
        json.dumps(ion_value, cls=IonToJSONEncoder)
