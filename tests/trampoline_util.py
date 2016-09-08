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

from pytest import raises

from tests import is_exception

from amazon.ion.util import coroutine, record


@coroutine
def always_self(result_func):
    """A simple co-routine that yields a result and transitions to itself indefinitely."""
    _, self = (yield)
    while True:
        yield result_func(self)


@coroutine
def moves_to(target, result_func):
    """A simple co-routine that yields a result and transitions to another co-routine."""
    yield
    yield result_func(target)


@coroutine
def yields_iter(*seq):
    """A simple co-routine that yields over an iterable ignoring what is sent to it.

    Note that an iterator doesn't work because of the *priming* ``yield`` and ``listiterator``
    and the like don't support ``send()``.
    """
    yield
    for val in seq:
        yield val


class TrampolineParameters(record('desc', 'coroutine', 'input', 'expected')):
    def __str__(self):
        return self.desc


def trampoline_scaffold(trampoline_func, p, *args):
    """Testing structure for trampolines.

    Args:
        trampoline_func (Callable): The function to construct a trampoline.
        p (TrampolineParams): The parameters for the test.
    """
    trampoline = trampoline_func(p.coroutine, *args)
    assert len(p.input) == len(p.expected)
    for input, expected in zip(p.input, p.expected):
        if is_exception(expected):
            with raises(expected):
                trampoline.send(input)
        else:
            output = trampoline.send(input)
            assert expected == output
