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

from datetime import datetime
from decimal import Decimal

from tests import parametrize, listify
from tests.event_aliases import *

from amazon.ion.core import Timestamp, TimestampPrecision
from amazon.ion.util import record
from amazon.ion.symbols import SymbolToken
from amazon.ion.simple_types import is_null, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, \
                                    IonPyDecimal, IonPyTimestamp, IonPyText, IonPyBytes, \
                                    IonPyList, IonPyDict, IonPySymbol
from amazon.ion.equivalence import ion_equals
from amazon.ion.simpleion import _ion_type, _FROM_TYPE

_TEST_FIELD_NAME = SymbolToken('foo', 10)
_TEST_ANNOTATIONS = (SymbolToken('bar', 11),)


class _P(record('desc', 'type', 'event')):
    def __str__(self):
        return self.desc


_EVENT_TYPES = [
    (IonPyNull, e_null()),
    (IonPyNull, e_int(None)),
    (IonPyBool, e_bool(True)),
    (IonPyInt, e_int(65000)),
    (IonPyInt, e_int(2 ** 64 + 1)),
    (IonPyFloat, e_float(1e0)),
    (IonPyDecimal, e_decimal(Decimal('1.1'))),
    (IonPyTimestamp, e_timestamp(datetime(2012, 1, 1))),
    (IonPyTimestamp, e_timestamp(Timestamp(2012, 1, 1, precision=TimestampPrecision.DAY))),
    (IonPySymbol, e_symbol(SymbolToken(u'Hola', None, None))),
    (IonPyText, e_string(u'Hello')),
    (IonPyBytes, e_clob(b'Goodbye')),

    # Technically this is not how we shape the event, but the container types don't care
    (IonPyList, e_start_sexp([1, 2, 3])),
    (IonPyDict, e_start_struct({u'hello': u'world'}))
]


@listify
def event_type_parameters():
    for cls, event in _EVENT_TYPES:
        yield _P(
            desc='EVENT TO %s - %s - %r' % (cls.__name__, event.ion_type.name, event.value),
            type=cls,
            event=event.derive_annotations(_TEST_ANNOTATIONS).derive_field_name(_TEST_FIELD_NAME)
        )


@parametrize(*event_type_parameters())
def test_event_types(p):
    value = p.event.value
    ion_type = p.event.ion_type

    event_output = p.type.from_event(p.event)
    value_output = p.type.from_value(ion_type, value, p.event.annotations)
    to_event_output = value_output.to_event(p.event.event_type, p.event.field_name, in_struct=True, depth=p.event.depth)
    if p.event.ion_type.is_container:
        # compensate for abuse of IonEvent.value, which is intended to be None for CONTAINER_START events,
        # but is populated and relied upon by the logic of this test code
        assert to_event_output.value is None
        to_event_output = to_event_output._replace(value=p.event.value)
    assert p.event == to_event_output

    if p.type is IonPyNull:
        # Null is a special case due to the non-extension of NoneType.
        assert not event_output
        assert not value_output
        assert is_null(event_output)
        assert is_null(value_output)
        assert ion_equals(event_output, value_output)
    else:
        # Make sure we don't change value equality symmetry.
        assert event_output == value
        assert value == event_output

        assert value_output == value
        assert value == value_output

        # Derive a new event from just the value because equality is stricter in some cases.
        value_event = e_scalar(ion_type, value)
        output_event = e_scalar(ion_type, event_output)
        assert value_event == output_event

    assert event_output.ion_type is ion_type
    assert p.event.annotations == event_output.ion_annotations

    assert value_output.ion_type is ion_type
    assert p.event.annotations == value_output.ion_annotations


def test_subclass_types():
    class Foo(dict):
        pass

    assert _ion_type(Foo(), _FROM_TYPE) is IonType.STRUCT
