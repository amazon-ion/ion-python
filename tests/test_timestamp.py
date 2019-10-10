# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from decimal import Decimal
from functools import partial

import pytest

from amazon.ion.core import Timestamp, TimestampPrecision, record, TIMESTAMP_MICROSECOND_FIELD
from amazon.ion.equivalence import ion_equals
from tests import parametrize, listify


class _FractionalCombination:

    def __init__(self, constructor_args,
                 expected_microsecond, expected_fractional_precision, expected_fractional_seconds):
        self.expected_microsecond = expected_microsecond
        self.expected_fractional_precision = expected_fractional_precision
        self.expected_fractional_seconds = expected_fractional_seconds
        self.constructor_args = constructor_args


class _FractionalCombinationParameter:

    def __init__(self, desc, timestamp, expected_microsecond,
                 expected_fractional_precision, expected_fractional_seconds):
        self.desc = desc
        self.timestamp = timestamp
        self.expected_microsecond = expected_microsecond
        self.expected_fractional_precision = expected_fractional_precision
        self.expected_fractional_seconds = expected_fractional_seconds

    def __str__(self):
        return self.desc


# Sometimes the 'microsecond' argument needs to be modified before being passed to the datetime constructor.
# It is therefore necessary to test that this works correctly regardless of whether 'microsecond' was specified
# as a positional or a keyword argument.
def _timestamp_from_kwargs(**kwargs):
    return str(kwargs),\
           Timestamp(2011, 1, 1, 0, 0, 0, precision=TimestampPrecision.SECOND, **kwargs)


def _timestamp_from_positional_microsecond(**kwargs):
    if TIMESTAMP_MICROSECOND_FIELD in kwargs:
        microsecond = kwargs[TIMESTAMP_MICROSECOND_FIELD]
        del kwargs[TIMESTAMP_MICROSECOND_FIELD]
        return '(%s, %s)' % (microsecond, str(kwargs)), \
               Timestamp(2011, 1, 1, 0, 0, 0, microsecond, precision=TimestampPrecision.SECOND, **kwargs)
    else:
        return _timestamp_from_kwargs(**kwargs)


def _constructor_args(**kwargs):
    return kwargs


_FRACTIONAL_COMBINATIONS = (
    _FractionalCombination(
        _constructor_args(
            microsecond=None,
            fractional_precision=None,
            fractional_seconds=Decimal('0')
        ),
        expected_microsecond=0,
        expected_fractional_precision=0,
        expected_fractional_seconds=Decimal(0)
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=None,
            fractional_precision=None,
            fractional_seconds=Decimal('0e4')
        ),
        expected_microsecond=0,
        expected_fractional_precision=0,
        expected_fractional_seconds=Decimal(0)
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=None,
            fractional_precision=None,
            fractional_seconds=Decimal('0.123456')
        ),
        expected_microsecond=123456,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.123456')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=None,
            fractional_precision=None,
            fractional_seconds=Decimal('0.123456789')
        ),
        expected_microsecond=123456,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.123456789')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=None,
            fractional_precision=None,
            fractional_seconds=Decimal('0.000123')
        ),
        expected_microsecond=123,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.000123')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=None,
            fractional_precision=None,
            fractional_seconds=Decimal('0.123000')
        ),
        expected_microsecond=123000,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.123000')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=None,
        ),
        expected_microsecond=0,
        expected_fractional_precision=0,
        expected_fractional_seconds=Decimal(0)
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=0,
            fractional_precision=6,
            fractional_seconds=None
        ),
        expected_microsecond=0,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.000000')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=123456,
            fractional_precision=6,
            fractional_seconds=None
        ),
        expected_microsecond=123456,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.123456')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=123,
            fractional_precision=6,
            fractional_seconds=None
        ),
        expected_microsecond=123,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.000123')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=123,
            fractional_precision=None,
            fractional_seconds=None
        ),
        expected_microsecond=123,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.000123')
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=123000,
            fractional_precision=6,
            fractional_seconds=None
        ),
        expected_microsecond=123000,
        expected_fractional_precision=6,
        expected_fractional_seconds=Decimal('0.123000')
    ),
    _FractionalCombination(
        _constructor_args(),
        expected_microsecond=0,
        expected_fractional_precision=0,
        expected_fractional_seconds=Decimal(0)
    ),
    _FractionalCombination(
        _constructor_args(
            microsecond=0,
            fractional_precision=0,
            fractional_seconds=None
        ),
        expected_microsecond=0,
        expected_fractional_precision=0,
        expected_fractional_seconds=Decimal(0)
    )
)


@listify
def generate_fractional_combination_parameters():
    for fractional_combination in _FRACTIONAL_COMBINATIONS:
        for constructor_variant in (_timestamp_from_kwargs, _timestamp_from_positional_microsecond):
            yield _FractionalCombinationParameter(
                *constructor_variant(**fractional_combination.constructor_args),
                expected_fractional_precision=fractional_combination.expected_fractional_precision,
                expected_fractional_seconds=fractional_combination.expected_fractional_seconds,
                expected_microsecond=fractional_combination.expected_microsecond
            )


@parametrize(*generate_fractional_combination_parameters())
def test_fractional_combinations(item):
    assert item.timestamp.microsecond == item.expected_microsecond
    assert item.timestamp.fractional_precision == item.expected_fractional_precision
    # Using ion_equals ensures that the Decimals are compared for Ion data model equivalence
    # (i.e. precision is significant).
    assert ion_equals(item.timestamp.fractional_seconds, item.expected_fractional_seconds)


class _InvalidArgumentsParameter:

    def __init__(self, desc, timestamp_constructor_thunk):
        self.desc = desc
        self.timestamp_constructor_thunk = timestamp_constructor_thunk

    def __str__(self):
        return self.desc


_INVALID_ARGUMENTS_PARAMETERS = (
    _InvalidArgumentsParameter(
        'fractional_precision without microsecond',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=6
        )
    ),
    _InvalidArgumentsParameter(
        'fractional_precision less than 0',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, 0,
            precision=TimestampPrecision.SECOND, fractional_precision=-1, fractional_seconds=None
        )
    ),
    _InvalidArgumentsParameter(
        'fractional_precision greater than 6',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, 0,
            precision=TimestampPrecision.SECOND, fractional_precision=7, fractional_seconds=None
        )
    ),
    _InvalidArgumentsParameter(
        'fractional_seconds less than 0',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=None, fractional_seconds=Decimal('-1')
        )
    ),
    _InvalidArgumentsParameter(
        'fractional_seconds is 1',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=None, fractional_seconds=Decimal('1')
        )
    ),
    _InvalidArgumentsParameter(
        'fractional_seconds and microseconds both specified',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, 1,
            precision=TimestampPrecision.SECOND, fractional_precision=None, fractional_seconds=Decimal('0.123456')
        )
    ),
    _InvalidArgumentsParameter(
        'fractional_seconds and fractional_precision both specified',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=Decimal('0.123456')
        )
    ),
    _InvalidArgumentsParameter(
        'microsecond is not 0 when fractional_precision is 0',
        lambda: Timestamp(
            2011, 1, 1,
            0, 0, 0, 123456,
            precision=TimestampPrecision.SECOND, fractional_precision=0, fractional_seconds=None
        )
    ),
    _InvalidArgumentsParameter(
        'microsecond requires more than fractional_precision',
        lambda: Timestamp(
            1, 1, 1, 1, 1, 1, 123456, precision=TimestampPrecision.SECOND, fractional_precision=3
        )
    )
)


@parametrize(*_INVALID_ARGUMENTS_PARAMETERS)
def test_invalid_constructor_arguments(p):
    with pytest.raises(ValueError):
        p.timestamp_constructor_thunk()
