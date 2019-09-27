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
from decimal import Decimal

import pytest

from amazon.ion.core import Timestamp, TimestampPrecision, record
from tests import parametrize


class _P(record('timestamps', 'expected_microseconds')):
    def __str__(self):
        return self.desc


MISSING_MICROSECOND = _P(
    timestamps=[Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=0
            ),
            Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=Decimal('0.123456')
            ),
            Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=Decimal('0.123456789')
            ),
            Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=Decimal('0.000123')
            ),
            Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=Decimal('0.123000')
            )],
    expected_microseconds=[0, 123456, 123456, 123, 123000]
)


@parametrize(
    MISSING_MICROSECOND
)
def test_missing_microsecond(item):
    for i in range(len(item.timestamps)):
        assert item.timestamps[i].microsecond == item.expected_microseconds[i]


class _P(record('timestamps', 'expected_fractional_precision')):
    def __str__(self):
        return self.desc


MISSING_FRACTIONAL_PRECISION = _P(
    timestamps=[Timestamp(
            2011, 1, 1,
            0, 0, 0, 0,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=None
            ),
            Timestamp(
            2011, 1, 1,
            0, 0, 0, 123456,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=None
            ),
            Timestamp(
            2011, 1, 1,
            0, 0, 0, 123,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=None
            ),
            Timestamp(
            2011, 1, 1,
            0, 0, 0, 123000,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=None
            )],
    expected_fractional_precision=[0, Decimal('0.123456'), Decimal('0.000123'), Decimal('0.123000')]
)


@parametrize(
    MISSING_FRACTIONAL_PRECISION
)
def test_missing_fractional_seconds(item):
    for i in range(len(item.timestamps)):
        assert item.timestamps[i].fractional_seconds == item.expected_fractional_precision[i]


def test_nonequivalent_microsecond_and_fractional_seconds():
    with pytest.raises(ValueError):
        Timestamp(
            2011, 1, 1,
            0, 0, 0, 123456,
            precision=TimestampPrecision.SECOND, fractional_precision=6, fractional_seconds=0
        )


def test_fractional_seconds_with_no_fractional_precision():
    with pytest.raises(ValueError):
        Timestamp(
            2011, 1, 1,
            0, 0, 0, None,
            precision=TimestampPrecision.SECOND, fractional_precision=None, fractional_seconds=Decimal('0.123')
        )


def test_fractional_precision_less_than_1():
    with pytest.raises(ValueError):
        Timestamp(
            2011, 1, 1,
            0, 0, 0, 0,
            precision=TimestampPrecision.SECOND, fractional_precision=0, fractional_seconds=0
        )


def test_fractional_seconds_greater_than_1():
    with pytest.raises(ValueError):
        Timestamp(
            2011, 1, 1,
            0, 0, 0, 0,
            precision=TimestampPrecision.SECOND, fractional_precision=1, fractional_seconds=2
        )
