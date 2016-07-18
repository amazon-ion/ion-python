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

"""Common test utilities for the ``tests`` package."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest

from contextlib import contextmanager


def listify(iter_func):
    """Takes an iterator function and returns a function that materializes it into a list."""
    def delegate(*args, **kwargs):
        return list(iter_func(*args, **kwargs))
    delegate.__name__ = iter_func.__name__
    return delegate


@contextmanager
def noop_manager():
    """A no-op context manager"""
    yield


def is_exception(val):
    """Returns whether a given value is an exception type (not an instance)."""
    return isinstance(val, type) and issubclass(val, Exception)


def parametrize(*values):
    """Idiomatic test parametrization.

    Parametrizes single parameter testing functions.
    The assumption is that the function decorated uses something
    like a ``namedtuple`` as its sole argument.

    Makes the ``id`` string be based on the parameter value's ``__str__``
    method.

    Examples:
        Usage of this decorator typically looks something like::

            class _P(namedtuple('Params', 'a, b, c')):
                def __str__(self):
                    return '{p.a} - {p.b} - {p.c}'.format(p=self)

            @tests.parametrize(
                _P(a='something', b=6, c=True),
                _P(s='some-other-thing', b=7, c=False),
                _P('look ma, positional', 9, True)
            def my_test(p):
                assert isinstance(p.a, str)
                assert p.b > 0
                assert isinstance(p.c, bool)

        :func:`amazon.ion.util.record` is also appropriate for the parameter
        class/tuple::

            class _P(amazon.ion.util.record('a', 'b', 'c')):
                def __str__(self):
                    return '{p.a} - {p.b} - {p.c}'.format(p=self)

    Args:
        values (Sequence[Any]): A sequence of values to pass to a single argument
            function.

    Returns:
        pytest.mark.parametrize: The decorator.
    """
    values = tuple((value,) for value in values)
    def decorator(func):
        if func.__code__.co_argcount != 1:
            raise ValueError('Expected a function with a single parameter.')
        argname = func.__code__.co_varnames[0]
        real_decorator = pytest.mark.parametrize(
            argnames=[argname],
            argvalues=values,
            ids=lambda x: str(x).replace('.', '_')
        )
        return real_decorator(func)

    return decorator
