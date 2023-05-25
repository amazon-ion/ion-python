from typing import NamedTuple, Sequence, Callable

from _pytest.python_api import raises

from amazon.ion.sliceable_buffer import IncompleteReadError, SliceableBuffer
from tests import parametrize

"""
This test rips off the pattern from test_reader_buffer and the relevant
(for now) tests.

The SliceableBuffer is not intended as a drop-in replacement for the
BufferQueue so there are just enough differences to warrant copying and 
modifying vs re-using as-is.

The intention/hope is that eventually that buffer will be replaced by this
one and then this will be the only remaining test.
"""


def read(expected):
    def action(buffer):
        read_len = len(expected)
        (actual, buffer) = buffer.read_slice(read_len)
        assert actual == expected

        return -read_len, buffer

    return action


def expect(exception, action):
    def raises_action(buffer):
        with raises(exception):
            action(buffer)
        return 0, buffer

    return raises_action


def extend(data):
    def action(buffer):
        return len(data), buffer.extend(data)

    return action


def read_byte(expected):
    def action(buffer):
        (actual, buffer) = buffer.read_byte()
        assert expected[0] == actual

        return -1, buffer

    return action


def skip(n, expected_rem=0):
    def action(buffer):
        (skipped, buffer) = buffer.skip(n)
        assert expected_rem == n - skipped
        return -skipped, buffer

    return action


class _P(NamedTuple):
    desc: str
    actions: Sequence[Callable]
    is_unicode: bool = False

    def __str__(self):
        return self.desc


@parametrize(
    _P(
        desc='EMPTY READ BYTE',
        actions=[
            expect(IncompleteReadError, read_byte(b'a')),
        ],
    ),
    _P(
        desc='EMPTY READ SLICE',
        actions=[
            expect(IncompleteReadError, read(b'ignored')),
        ],
    ),
    _P(
        desc='INCOMPLETE READ SLICE',
        actions=[
            extend(b'abcd'),
            read_byte(b'a'),
            expect(IncompleteReadError, read(b'bcde')),
            read_byte(b'b')
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
        desc='CHOMP CHOMP DONE',
        actions=[
            extend(b'ab'),
            read_byte(b'a'),
            read_byte(b'b'),
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
        desc='PARTIAL SKIP INCOMPLETE',
        actions=[
            extend(b'ab'),
            extend(b'cd'),
            skip(5, expected_rem=1),
        ],
    ),
    _P(
        desc='SKIP TWO OF THREE',
        actions=[
            extend(b'ab'),
            extend(b'cd'),
            extend(b'ef'),
            skip(4, expected_rem=0),
            read(b'ef')
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
def test_buffer(p):
    buffer = SliceableBuffer.empty()
    expected_len = 0

    for action in p.actions:
        buffer_len = len(buffer)
        assert buffer_len == expected_len

        len_change, buffer = action(buffer)
        expected_len += len_change

    assert len(buffer) == expected_len
