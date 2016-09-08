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

from functools import partial
from io import BytesIO

from pytest import raises

from tests import is_exception, parametrize
from tests.event_aliases import e_int
from tests.trampoline_util import always_self
from tests.trampoline_util import trampoline_scaffold

from amazon.ion.core import Transition
from amazon.ion.core import ION_VERSION_MARKER_EVENT
from amazon.ion.core import ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT
from amazon.ion.reader import read_data_event, reader_trampoline, blocking_reader
from amazon.ion.reader import NEXT_EVENT, SKIP_EVENT
from amazon.ion.util import coroutine, record


_TRIVIAL_ION_EVENT = e_int(0)

_ivm_transition = partial(Transition, ION_VERSION_MARKER_EVENT)
_incomplete_transition = partial(Transition, ION_STREAM_INCOMPLETE_EVENT)
_end_transition = partial(Transition, ION_STREAM_END_EVENT)
_event_transition = partial(Transition, _TRIVIAL_ION_EVENT)


class ReaderTrampolineParameters(record('desc', 'coroutine', 'input', 'expected', ('allow_flush', False))):
    def __str__(self):
        return self.desc

_P = ReaderTrampolineParameters

_TRIVIAL_DATA_EVENT = read_data_event(b'DATA')
_EMPTY_DATA_EVENT = read_data_event(b'')


@parametrize(
    _P(
        desc='START WITH NONE',
        coroutine=always_self(_ivm_transition),
        input=[None],
        expected=[TypeError],
    ),
    _P(
        desc='START WITH SKIP',
        coroutine=always_self(_ivm_transition),
        input=[SKIP_EVENT],
        expected=[TypeError],
    ),
    _P(
        desc='ALWAYS IVM NEXT',
        coroutine=always_self(_ivm_transition),
        input=[NEXT_EVENT] * 4,
        expected=[ION_VERSION_MARKER_EVENT] * 4,
    ),
    _P(
        desc='ALWAYS IVM NEXT THEN SKIP',
        coroutine=always_self(_ivm_transition),
        input=[NEXT_EVENT, SKIP_EVENT],
        expected=[ION_VERSION_MARKER_EVENT, TypeError],
    ),
    _P(
        desc='ALWAYS INCOMPLETE NEXT EOF',
        coroutine=always_self(_incomplete_transition),
        input=[NEXT_EVENT, NEXT_EVENT],
        expected=[ION_STREAM_INCOMPLETE_EVENT, TypeError],
        allow_flush=False
    ),
    _P(
        desc='ALWAYS INCOMPLETE NEXT FLUSH',
        coroutine=always_self(_incomplete_transition),
        input=[NEXT_EVENT, NEXT_EVENT],
        expected=[ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_INCOMPLETE_EVENT],
        allow_flush=True
    ),
    _P(
        desc='ALWAYS INCOMPLETE SKIP',
        coroutine=always_self(_incomplete_transition),
        input=[NEXT_EVENT, SKIP_EVENT],
        expected=[ION_STREAM_INCOMPLETE_EVENT, TypeError],
    ),
    _P(
        desc='ALWAYS INCOMPLETE DATA',
        coroutine=always_self(_incomplete_transition),
        input=[NEXT_EVENT] + [_TRIVIAL_DATA_EVENT] * 4,
        expected=[ION_STREAM_INCOMPLETE_EVENT] * 5,
    ),
    _P(
        desc='ALWAYS END NEXT',
        coroutine=always_self(_end_transition),
        input=[NEXT_EVENT, NEXT_EVENT],
        expected=[ION_STREAM_END_EVENT, TypeError],
    ),
    _P(
        desc='ALWAYS END SKIP',
        coroutine=always_self(_end_transition),
        input=[NEXT_EVENT, SKIP_EVENT],
        expected=[ION_STREAM_END_EVENT, TypeError],
    ),
    _P(
        desc='ALWAYS END DATA',
        coroutine=always_self(_end_transition),
        input=[NEXT_EVENT] + [_TRIVIAL_DATA_EVENT] * 4,
        expected=[ION_STREAM_END_EVENT] * 5,
    ),
    _P(
        desc='ALWAYS END EMPTY DATA',
        coroutine=always_self(_end_transition),
        input=[NEXT_EVENT] + [_EMPTY_DATA_EVENT],
        expected=[ION_STREAM_END_EVENT, ValueError],
    ),
    _P(
        desc='ALWAYS EVENT DATA',
        coroutine=always_self(_event_transition),
        input=[NEXT_EVENT] + [_TRIVIAL_DATA_EVENT],
        expected=[_TRIVIAL_ION_EVENT, TypeError]
    )
)
def test_trampoline(p):
    trampoline_scaffold(reader_trampoline, p, p.allow_flush)


class _P(record('desc', 'coroutine', 'data', 'input', 'expected')):
    def __str__(self):
        return self.desc


@coroutine
def _asserts_events(expecteds, outputs, allow_flush=False):
    output = None
    for expected, next_output in zip(expecteds, outputs):
        actual = yield output
        assert expected == actual
        output = next_output
    yield output
    if not allow_flush:
        raise EOFError()
    yield ION_STREAM_END_EVENT


@parametrize(
    _P(
        desc='ALWAYS COMPLETE',
        coroutine=_asserts_events(
            [NEXT_EVENT, SKIP_EVENT],
            [ION_VERSION_MARKER_EVENT] * 2),
        data=b'',
        input=[NEXT_EVENT, SKIP_EVENT],
        expected=[ION_VERSION_MARKER_EVENT] * 2
    ),
    _P(
        desc='FIRST INCOMPLETE, THEN COMPLETE',
        coroutine=_asserts_events(
            [NEXT_EVENT, read_data_event(b'a'), SKIP_EVENT],
            [ION_STREAM_INCOMPLETE_EVENT] + ([ION_VERSION_MARKER_EVENT] * 2)
        ),
        data=b'a',
        input=[NEXT_EVENT, SKIP_EVENT],
        expected=[ION_VERSION_MARKER_EVENT] * 2
    ),
    _P(
        desc='FIRST STREAM END, THEN COMPLETE',
        coroutine=_asserts_events(
            [NEXT_EVENT, read_data_event(b'a'), SKIP_EVENT],
            [ION_STREAM_END_EVENT] + ([ION_VERSION_MARKER_EVENT] * 2)
        ),
        data=b'a',
        input=[NEXT_EVENT, SKIP_EVENT],
        expected=[ION_VERSION_MARKER_EVENT] * 2
    ),
    _P(
        desc='PREMATURE EOF',
        coroutine=_asserts_events(
            [NEXT_EVENT, read_data_event(b'a'), read_data_event(b'b')],
            [ION_STREAM_END_EVENT, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_INCOMPLETE_EVENT],
            allow_flush=False
        ),
        data=b'ab',
        input=[NEXT_EVENT],
        expected=[EOFError]
    ),
    _P(
        desc='FLUSH',
        coroutine=_asserts_events(
            [NEXT_EVENT, read_data_event(b'a'), read_data_event(b'b')],
            [ION_STREAM_END_EVENT, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_INCOMPLETE_EVENT],
            allow_flush=True
        ),
        data=b'ab',
        input=[NEXT_EVENT],
        expected=[ION_STREAM_END_EVENT]
    ),
    _P(
        desc='SINGLE EVENT, THEN NATURAL EOF',
        coroutine=_asserts_events(
            [NEXT_EVENT, read_data_event(b'a'), NEXT_EVENT],
            [ION_STREAM_END_EVENT, ION_VERSION_MARKER_EVENT, ION_STREAM_END_EVENT]
        ),
        data=b'a',
        input=[NEXT_EVENT, NEXT_EVENT],
        expected=[ION_VERSION_MARKER_EVENT, ION_STREAM_END_EVENT]
    ),
)
def test_blocking_reader(p):
    buf = BytesIO(p.data)
    reader = blocking_reader(p.coroutine, buf, buffer_size=1)
    for input, expected in zip(p.input, p.expected):
        if is_exception(expected):
            with raises(expected):
                reader.send(input)
        else:
            actual = reader.send(input)
            assert expected == actual
