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

from tests import parametrize

from amazon.ion.reader import BufferQueue
from amazon.ion.util import record


def read(expected):
    def action(queue):
        if isinstance(expected, int):
            read_len = 1
            actual = queue.read_byte()
        else:
            read_len = len(expected)
            actual = queue.read(read_len)
        assert actual == expected

        return -read_len, read_len

    return action


def read_byte(ch):
    return read(ord(ch))


def skip(amount, expected_rem=0):
    def action(queue):
        rem = queue.skip(amount)
        skip_amount = amount - rem
        assert expected_rem == rem
        return -skip_amount, skip_amount

    return action


def extend(data):
    def action(queue):
        queue.extend(data)

        return len(data), 0

    return action


def expect(exception, action):
    def raises_action(queue):
        with raises(exception):
            action(queue)
        return 0, 0

    return raises_action


class _P(record('desc', 'actions')):
    def __str__(self):
        return self.desc


@parametrize(
    _P(
        desc='EMPTY READ BYTE',
        actions=[
            expect(IndexError, read_byte(b'a')),
        ],
    ),
    _P(
        desc='EMPTY READ',
        actions=[
            expect(IndexError, read(b'a')),
        ],
    ),
    _P(
        desc='EMPTY SKIP',
        actions=[
            skip(1, expected_rem=1),
        ],
    ),
    _P(
        desc='SINGLE FULL',
        actions=[
            extend(b'abcd'),
            read(b'abcd'),
        ],
    ),
    _P(
        desc='SINGLE BYTE FULL',
        actions=[
            extend(b'a'),
            read_byte(b'a'),
        ],
    ),
    _P(
        desc='SINGLE SKIP FULL',
        actions=[
            extend(b'abcd'),
            skip(4, expected_rem=0),
            skip(4, expected_rem=4),
        ],
    ),
    _P(
        desc='SINGLE PART',
        actions=[
            extend(b'abcdefg'),
            read(b'ab'),
            read_byte(b'c'),
            skip(2),
            read(b'fg'),
        ],
    ),
    _P(
        desc='MULTI ALL',
        actions=[
            extend(b'abcd'),
            extend(b'efgh'),
            read(b'abcdefgh'),

            extend(b'ijkl'),
            extend(b'mnop'),
            read(b'ijklmnop'),

            extend(b'ab'),
            extend(b'cd'),
            extend(b'ef'),
            extend(b'gh'),
            read_byte(b'a'),
            skip(4),
            read_byte(b'f'),
            read_byte(b'g'),
            read_byte(b'h'),
        ],
    ),
    _P(
        desc='MULTI PART AND SPAN',
        actions=[
            extend(b'abcd'),
            extend(b'efgh'),
            read(b'ab'),
            read(b'cdef'),

            extend(b'ijkl'),
            read(b'ghij'),

            extend(b'mnop'),
            read(b'klmnop'),

            extend(b'abcd'),
            extend(b'efgh'),
            extend(b'ijkl'),
            extend(b'm'),
            read(b'abcdefghi'),
            read_byte(b'j'),
            read_byte(b'k'),
            read_byte(b'l'),
            read_byte(b'm'),
        ],
    ),
    _P(
        desc='MULTI MIDDLE',
        actions=[
            extend(b'abcdefg'),
            extend(b'hijklmn'),
            read(b'ab'),
            read(b'cdef'),
            read(b'ghij'),
            read(b'klm'),
            read(b'n')
        ],
    ),
    _P(
        desc='MULTI MIDDLE SKIP',
        actions=[
            extend(b'abcdefg'),
            extend(b'hijklmn'),
            read(b'ab'),
            skip(4),
            read(b'ghij'),
            skip(3),
            read(b'n')
        ],
    ),
)
def test_buffer_queue(p):
    queue = BufferQueue()
    expected_len = 0
    expected_pos = 0

    for action in p.actions:
        queue_len = len(queue)
        queue_pos = queue.position
        assert queue_len == expected_len
        assert queue_pos == expected_pos

        len_change, pos_change = action(queue)
        expected_len += len_change
        expected_pos += pos_change
