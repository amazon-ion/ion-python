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
from amazon.ion.encoder import IonEncoder
from amazon.ion.simple_types import IonPyList, IonPyDict, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, IonPyDecimal, \
    IonPyTimestamp, IonPyText, IonPyBytes, IonPySymbol
from amazon.ion.symbols import SymbolToken
from amazon.ion.simpleion import dumps, loads
from base64 import b64encode
import datetime
from decimal import Decimal
import json
import six


def test_null():
    value = None
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyNull)
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == 'null'


def test_bool():
    value = False
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyBool) and ion_value.ion_type == IonType.BOOL
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == 'false'


def test_int():
    value = -123
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyInt) and ion_value.ion_type == IonType.INT
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '-123'


def test_float():
    value = float(123.456)
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyFloat) and ion_value.ion_type == IonType.FLOAT
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '123.456'


def test_float_nan():
    value = float("NaN")
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyFloat) and ion_value.ion_type == IonType.FLOAT
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == 'null'


def test_float_inf():
    value = float("Inf")
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyFloat) and ion_value.ion_type == IonType.FLOAT
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == 'null'


def test_decimal():
    value = Decimal('123.456')
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '123.456'


def test_decimal_exp():
    value = Decimal('1.23456e2')
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '123.456'


def test_decimal_exp_negative():
    value = Decimal('12345.6e-2')
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '123.456'


def test_decimal_exp_large():
    value = Decimal('123.456e34')
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '1.23456e+36'


def test_decimal_exp_large_negative():
    value = Decimal('123.456e-34')
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyDecimal) and ion_value.ion_type == IonType.DECIMAL
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '1.23456e-32'


def test_timestamp():
    value = datetime.datetime(2010, 6, 15, 3, 30, 45)
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyTimestamp) and ion_value.ion_type == IonType.TIMESTAMP
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '"2010-06-15 03:30:45"'


def test_symbol():
    value = SymbolToken(six.text_type("Symbol"), None)
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPySymbol) and ion_value.ion_type == IonType.SYMBOL
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '"Symbol"'


def test_string():
    value = six.text_type("String")
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyText) and ion_value.ion_type == IonType.STRING
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '"String"'


def test_clob():
    ion_value = IonPyBytes.from_value(IonType.CLOB, b'Ion')
    assert isinstance(ion_value, IonPyBytes) and ion_value.ion_type == IonType.CLOB
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '"Ion"'


def test_blob():
    value = b64encode("Ion".encode("ASCII")) if six.PY2 else bytes("Ion", "ASCII")
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyBytes) and ion_value.ion_type == IonType.BLOB
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '"SW9u"'


def test_list():
    value = [six.text_type("Ion"), 123]
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyList) and ion_value.ion_type == IonType.LIST
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '["Ion", 123]'


def test_sexp():
    value = (six.text_type("Ion"), 123)
    ion_value = loads(dumps(value, tuple_as_sexp=True))
    assert isinstance(ion_value, IonPyList) and ion_value.ion_type == IonType.SEXP
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '["Ion", 123]'


def test_struct():
    value = {
        six.text_type("string_value"): six.text_type("Ion"),
        six.text_type("int_value"): 123,
        six.text_type("nested_struct"): {
            six.text_type("nested_value"): six.text_type("Nested Ion")
        }
    }
    ion_value = loads(dumps(value))
    assert isinstance(ion_value, IonPyDict) and ion_value.ion_type == IonType.STRUCT
    json_string = json.dumps(ion_value, cls=IonEncoder)
    expected_string = '{"string_value": "Ion", "int_value": 123, "nested_struct": {"nested_value": "Nested Ion"}}'
    if not json_string == expected_string:
        # Assert as objects to handle different Python versions' JSON string key ordering
        assert json.loads(json_string) == json.loads(expected_string)


def test_annotations():
    ion_value = IonPyInt.from_value(IonType.INT, 123, "Annotation")
    assert isinstance(ion_value, IonPyInt) and ion_value.ion_type == IonType.INT
    json_string = json.dumps(ion_value, cls=IonEncoder)
    assert json_string == '123'
