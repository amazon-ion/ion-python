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

from io import BytesIO
from functools import partial

from tests import parametrize
from tests.trampoline_util import always_self, moves_to, yields_iter
from tests.trampoline_util import trampoline_scaffold, TrampolineParameters

from amazon.ion.core import DataEvent, Transition
from amazon.ion.core import ION_VERSION_MARKER_EVENT
from amazon.ion.writer import writer_trampoline, blocking_writer, partial_transition
from amazon.ion.writer import WriteEventType


# Trivial data for write events.
_TRIVIAL_DATA = b'DATA'


# Generates trivial write events.
def _trivial_event(write_event_type):
    return DataEvent(
        write_event_type,
        _TRIVIAL_DATA
    )

_PARTIAL_EVENT = _trivial_event(WriteEventType.HAS_PENDING)
_COMPLETE_EVENT = _trivial_event(WriteEventType.COMPLETE)
_NEEDS_INPUT_EVENT = _trivial_event(WriteEventType.NEEDS_INPUT)


def _trivial_transition(event_type, data, delegate):
    return Transition(DataEvent(event_type, data), delegate)


_partial_result = partial(partial_transition, _TRIVIAL_DATA)
_complete_result = partial(_trivial_transition, WriteEventType.COMPLETE, _TRIVIAL_DATA)
_needs_input_result = partial(_trivial_transition, WriteEventType.NEEDS_INPUT, _TRIVIAL_DATA)


_P = TrampolineParameters


@parametrize(
    _P(
        desc='START WITH NONE',
        coroutine=always_self(_partial_result),
        input=[None],
        expected=[TypeError],
    ),
    _P(
        desc='ALWAYS PARTIAL SELF - NORMAL',
        coroutine=always_self(_partial_result),
        input=[ION_VERSION_MARKER_EVENT] + [None] * 3,
        expected=[_PARTIAL_EVENT] * 4
    ),
    _P(
        desc='ALWAYS PARTIAL SELF - NON-NONE',
        coroutine=always_self(_partial_result),
        input=[ION_VERSION_MARKER_EVENT, ION_VERSION_MARKER_EVENT],
        expected=[_PARTIAL_EVENT, TypeError]
    ),
    _P(
        desc='ALWAYS COMPLETE SELF - NORMAL',
        coroutine=always_self(_complete_result),
        input=[ION_VERSION_MARKER_EVENT] * 10,
        expected=[_COMPLETE_EVENT] * 10,
    ),
    _P(
        desc='ALWAYS COMPLETE SELF - NONE',
        coroutine=always_self(_complete_result),
        input=[ION_VERSION_MARKER_EVENT, None],
        expected=[_COMPLETE_EVENT, TypeError],
    ),
    _P(
        desc='ALWAYS NEEDS INPUT SELF - NORMAL',
        coroutine=always_self(_needs_input_result),
        input=[ION_VERSION_MARKER_EVENT] * 10,
        expected=[_NEEDS_INPUT_EVENT] * 10,
    ),
    _P(
        desc='ALWAYS NEEDS INPUT SELF - NONE',
        coroutine=always_self(_needs_input_result),
        input=[ION_VERSION_MARKER_EVENT, None],
        expected=[_NEEDS_INPUT_EVENT, TypeError],
    ),
    _P(
        desc='MOVES TO',
        coroutine=moves_to(always_self(_complete_result), _needs_input_result),
        input=[ION_VERSION_MARKER_EVENT] * 4,
        expected=[_NEEDS_INPUT_EVENT] + [_COMPLETE_EVENT] * 3,
    ),
)
def test_trampoline(p):
    trampoline_scaffold(writer_trampoline, p)


@parametrize(
    _P(
        desc='SINGLE COMPLETE EVENT',
        coroutine=yields_iter(_COMPLETE_EVENT),
        input=1,
        expected=_TRIVIAL_DATA
    ),
    _P(
        desc='MULTIPLE NEEDS INPUT EVENT',
        coroutine=yields_iter(*([_NEEDS_INPUT_EVENT] * 4)),
        input=4,
        expected=_TRIVIAL_DATA * 4
    ),
    _P(
        desc='PARTIAL THEN COMPLETE EVENT',
        coroutine=yields_iter(_PARTIAL_EVENT, _COMPLETE_EVENT),
        input=1,
        expected=_TRIVIAL_DATA * 2
    ),
)
def test_blocking_writer(p):
    buf = BytesIO()
    writer = blocking_writer(p.coroutine, buf)
    for i in range(p.input):
        result_type = writer.send(None)
        assert isinstance(result_type, WriteEventType) and result_type is not WriteEventType.HAS_PENDING
    assert p.expected == buf.getvalue()
