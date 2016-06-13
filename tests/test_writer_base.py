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
from pytest import raises
from functools import partial

from tests import is_exception, parametrize

from amazon.ion.core import IonEvent, IonEventType
from amazon.ion.util import coroutine, record
from amazon.ion.writer import writer_trampoline, blocking_writer, partial_write_result
from amazon.ion.writer import WriteEvent, WriteEventType, WriteResult

# Trivial Ion event.
_IVM_EVENT = IonEvent(
    event_type=IonEventType.VERSION_MARKER
)

# Trivial data for write events.
_TRIVIAL_DATA = b'DATA'


# Generates trivial write events.
def _trivial_event(write_event_type):
    return WriteEvent(
        write_event_type,
        _TRIVIAL_DATA
    )

_PARTIAL_EVENT = _trivial_event(WriteEventType.HAS_PENDING)
_COMPLETE_EVENT = _trivial_event(WriteEventType.COMPLETE)
_NEEDS_INPUT_EVENT = _trivial_event(WriteEventType.NEEDS_INPUT)


# Generates trivial co-routine results
def _trivial_result(event_type, delegate):
    return WriteResult(WriteEvent(event_type, _TRIVIAL_DATA), delegate)

_partial_result = partial(partial_write_result, _TRIVIAL_DATA)
_complete_result = partial(_trivial_result, WriteEventType.COMPLETE)
_needs_input_result = partial(_trivial_result, WriteEventType.NEEDS_INPUT)


@coroutine
def _always_self(result_func):
    """A simple writer co-routine that yields a result and transitions to itself indefinitely."""
    ion_event, self = (yield)
    while True:
        yield result_func(self)


@coroutine
def _moves_to(target, result_func):
    """A simple writer co-routine that yields a result and transitions to another co-routine."""
    ion_event, self = (yield)
    yield result_func(target)


class _P(record('desc', 'coroutine', 'input', 'expected')):
    def __str__(self):
        return self.desc


@parametrize(
    _P(
        desc='START WITH NONE',
        coroutine=_always_self(_partial_result),
        input=[None],
        expected=[TypeError],
    ),
    _P(
        desc='ALWAYS PARTIAL SELF - NORMAL',
        coroutine=_always_self(_partial_result),
        input=[_IVM_EVENT] + [None] * 3,
        expected=[_PARTIAL_EVENT] * 4
    ),
    _P(
        desc='ALWAYS PARTIAL SELF - NON-NONE',
        coroutine=_always_self(_partial_result),
        input=[_IVM_EVENT, _IVM_EVENT],
        expected=[_PARTIAL_EVENT, TypeError]
    ),
    _P(
        desc='ALWAYS COMPLETE SELF - NORMAL',
        coroutine=_always_self(_complete_result),
        input=[_IVM_EVENT] * 10,
        expected=[_COMPLETE_EVENT] * 10,
    ),
    _P(
        desc='ALWAYS COMPLETE SELF - NONE',
        coroutine=_always_self(_complete_result),
        input=[_IVM_EVENT, None],
        expected=[_COMPLETE_EVENT, TypeError],
    ),
    _P(
        desc='ALWAYS NEEDS INPUT SELF - NORMAL',
        coroutine=_always_self(_needs_input_result),
        input=[_IVM_EVENT] * 10,
        expected=[_NEEDS_INPUT_EVENT] * 10,
    ),
    _P(
        desc='ALWAYS NEEDS INPUT SELF - NONE',
        coroutine=_always_self(_needs_input_result),
        input=[_IVM_EVENT, None],
        expected=[_NEEDS_INPUT_EVENT, TypeError],
    ),
    _P(
        desc='MOVES TO',
        coroutine=_moves_to(_always_self(_complete_result), _needs_input_result),
        input=[_IVM_EVENT] * 4,
        expected=[_NEEDS_INPUT_EVENT] + [_COMPLETE_EVENT] * 3,
    ),
)
def test_trampoline(p):
    trampoline = writer_trampoline(p.coroutine)
    assert len(p.input) == len(p.expected)
    for input, expected in zip(p.input, p.expected):
        if is_exception(expected):
            with raises(expected):
                trampoline.send(input)
        else:
            output = trampoline.send(input)
            assert expected == output


@coroutine
def _event_seq(*events):
    yield
    for event in events:
        yield event


@parametrize(
    _P(
        desc='SINGLE COMPLETE EVENT',
        coroutine=_event_seq(_COMPLETE_EVENT),
        input=1,
        expected=_TRIVIAL_DATA
    ),
    _P(
        desc='MULTIPLE NEEDS INPUT EVENT',
        coroutine=_event_seq(*([_NEEDS_INPUT_EVENT] * 4)),
        input=4,
        expected=_TRIVIAL_DATA * 4
    ),
    _P(
        desc='PARTIAL THEN COMPLETE EVENT',
        coroutine=_event_seq(_PARTIAL_EVENT, _COMPLETE_EVENT),
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
