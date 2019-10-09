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

import pytest

from tests.event_aliases import *

from amazon.ion.util import coroutine
from amazon.ion.symbols import SymbolToken
from amazon.ion.simple_types import IonPySymbol
from amazon.ion.simpleion import _dump, loads, _FROM_TYPE


def _to_event_parameters():
    return [
        [loads('5'), [IonEvent(IonEventType.SCALAR, IonType.INT, 5, None, (), depth=0)]],
        [loads('abc'), [IonEvent(IonEventType.SCALAR, IonType.SYMBOL, SymbolToken('abc', None), None, (), depth=0)]],
        [loads('{abc: 1}'), [
            IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT, depth=0),
            IonEvent(IonEventType.SCALAR, IonType.INT, 1, SymbolToken('abc', None), (), depth=1),
            IonEvent(IonEventType.CONTAINER_END),
        ]],
        [loads('$0'), [IonEvent(IonEventType.SCALAR, IonType.SYMBOL, SymbolToken(None, 0), None, (), depth=0)]],
        [loads('{$0: $0}'), [
            IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT, depth=0),
            IonEvent(IonEventType.SCALAR, IonType.SYMBOL, IonPySymbol(None, 0), SymbolToken(None, 0), (), depth=1),
            IonEvent(IonEventType.CONTAINER_END),
        ]],
        [loads('[1, 2, 3, [4, 5, 6], [7, 8, 9]]'), [
            IonEvent(IonEventType.CONTAINER_START, IonType.LIST, depth=0),
              IonEvent(IonEventType.SCALAR, IonType.INT, 1, depth=1),
              IonEvent(IonEventType.SCALAR, IonType.INT, 2, depth=1),
              IonEvent(IonEventType.SCALAR, IonType.INT, 3, depth=1),
              IonEvent(IonEventType.CONTAINER_START, IonType.LIST, depth=1),
                IonEvent(IonEventType.SCALAR, IonType.INT, 4, depth=2),
                IonEvent(IonEventType.SCALAR, IonType.INT, 5, depth=2),
                IonEvent(IonEventType.SCALAR, IonType.INT, 6, depth=2),
              IonEvent(IonEventType.CONTAINER_END),
              IonEvent(IonEventType.CONTAINER_START, IonType.LIST, depth=1),
                IonEvent(IonEventType.SCALAR, IonType.INT, 7, depth=2),
                IonEvent(IonEventType.SCALAR, IonType.INT, 8, depth=2),
                IonEvent(IonEventType.SCALAR, IonType.INT, 9, depth=2),
              IonEvent(IonEventType.CONTAINER_END),
            IonEvent(IonEventType.CONTAINER_END),
        ]],

        [5, [IonEvent(IonEventType.SCALAR, IonType.INT, 5, None, (), depth=0)]],
        [u'abc', [IonEvent(IonEventType.SCALAR, IonType.STRING, "abc", None, (), depth=0)]],
        [{'abc': 1}, [
            IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT, depth=0),
            IonEvent(IonEventType.SCALAR, IonType.INT, 1, "abc", (), depth=1),
            IonEvent(IonEventType.CONTAINER_END),
        ]],
    ]


def _to_event_test_name(params):
    return str(params[0])


@pytest.mark.parametrize("params", _to_event_parameters(), ids=_to_event_test_name)
def test_to_event(params):
    value, expected_events = params
    events = []

    @coroutine
    def event_receiver():
        event = yield
        while True:
            events.append(event)
            event = yield

    _dump(value, event_receiver(), _FROM_TYPE)

    assert events == expected_events

