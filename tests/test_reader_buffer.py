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

from amazon.ion.reader import BufferQueue, CodePointArray
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


def unread(data):
    def action(queue):
        queue.unread(data)
        if isinstance(data, int):
            unread_len = 1
        else:
            unread_len = len(data)
        return unread_len, -unread_len

    return action


def unread_byte(ch):
    return unread(ord(ch))


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


def mark_eof():
    def action(queue):
        queue.mark_eof()
        return 1, 0

    return action


def expect_eof(is_eof):
    def action(queue):
        maybe_eof = queue.read_byte()
        assert is_eof is BufferQueue.is_eof(maybe_eof)
        return -1, 1

    return action


class _P(record('desc', 'actions', ('is_unicode', False))):
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
        desc='EMPTY UNREAD',
        actions=[
            expect(IndexError, unread_byte(b'a')),
        ],
    ),
    _P(
        desc='INCORRECT UNREAD',
        actions=[
            extend(b'ab'),
            read_byte(b'a'),
            expect(ValueError, unread_byte(b'c')),
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
        desc='SINGLE UNREAD',
        actions=[
            extend(b'a'),
            read_byte(b'a'),
            unread_byte(b'a'),
            read_byte(b'a')
        ]
    ),
    _P(
        desc='MULTI CODE UNIT UNREAD',
        actions=[
            extend(u'a\U0001f4a9c'),
            read(u'a\U0001f4a9'),
            unread(u'\U0001f4a9'),
            read(u'\U0001f4a9c')
        ],
        is_unicode=True
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
            read(b'abc'),
            unread_byte(b'c'),
            unread_byte(b'b'),
            read(b'bcd'),
            extend(b'EF'),
            extend(b'GH'),
            extend(b'ef'),
            extend(b'gh'),
            skip(4),
            read_byte(b'e'),
            read_byte(b'f'),
            read_byte(b'g'),
            read_byte(b'h'),
            unread_byte(b'h'),
            read_byte(b'h')
        ],
    ),
    _P(
        desc='MULTI ALL UNICODE',
        actions=[
            extend(u'abcd'),
            extend(u'efgh'),
            read(u'abcdefgh'),

            extend(u'ijkl'),
            extend(u'mnop'),
            read(u'ijklmnop'),

            extend(u'ab'),
            extend(u'cd'),
            read(u'abc'),
            unread_byte(u'c'),
            unread_byte(u'b'),
            read(u'bcd'),
            extend(u'EF'),
            extend(u'GH'),
            extend(u'ef'),
            extend(u'gh'),
            skip(4),
            read_byte(u'e'),
            read_byte(u'f'),
            read_byte(u'g'),
            read_byte(u'h'),
            unread_byte(u'h'),
            read_byte(u'h')
        ],
        is_unicode=True
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
    _P(
        desc='EOF',
        actions=[
            extend(b'ab'),
            mark_eof(),
            read(b'ab'),
            expect_eof(True)
        ]
    ),
    _P(
        desc='FAKE EOF',
        actions=[
            extend(b'ab\x04'),
            read(b'ab'),
            expect_eof(False)
        ]
    ),
    _P(
        desc='EOF UNICODE',
        actions=[
            extend(u'ab'),
            mark_eof(),
            read(u'ab'),
            expect_eof(True)
        ],
        is_unicode=True
    ),
    _P(
        desc='FAKE EOF UNICODE',
        actions=[
            extend(u'ab\x04'),
            read(u'ab'),
            expect_eof(False)
        ],
        is_unicode=True
    ),
    _P(
        desc='BYTES BUFFER GIVEN UNICODE',
        actions=[
            extend(b'abc'),
            expect(ValueError, extend(u'def'))
        ],
        is_unicode=False
    ),
    _P(
        desc='UNICODE BUFFER GIVEN BYTES',
        actions=[
            extend(u'abc'),
            expect(ValueError, extend(b'def'))
        ],
        is_unicode=True
    ),
    _P(
        desc='BYTES BUFFER UNREAD UNICODE',
        actions=[
            extend(b'a'),
            read_byte(b'a'),
            expect(ValueError, unread(u'a'))
        ],
        is_unicode=False
    ),
)
def test_buffer_queue(p):
    queue = BufferQueue(p.is_unicode)
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
