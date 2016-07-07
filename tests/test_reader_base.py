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

from tests import is_exception, parametrize
from tests.trampoline_util import always_self, moves_to
from tests.trampoline_util import trampoline_scaffold, TrampolineParameters

from amazon.ion.core import DataEvent, Transition
from amazon.ion.core import ION_VERSION_MARKER_EVENT
from amazon.ion.core import ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT
from amazon.ion.reader import read_data_event, reader_trampoline, blocking_reader
from amazon.ion.reader import NEXT_EVENT, SKIP_EVENT


_ivm_transition = partial(Transition, ION_VERSION_MARKER_EVENT)
_incomplete_transition = partial(Transition, ION_STREAM_INCOMPLETE_EVENT)
_end_transition = partial(Transition, ION_STREAM_END_EVENT)

_P = TrampolineParameters

_TRIVIAL_DATA_EVENT = read_data_event(b'DATA')

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
        desc='ALWAYS INCOMPLETE NEXT',
        coroutine=always_self(_incomplete_transition),
        input=[NEXT_EVENT, NEXT_EVENT],
        expected=[ION_STREAM_INCOMPLETE_EVENT, TypeError],
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
)
def test_trampoline(p):
    trampoline_scaffold(reader_trampoline, p)
