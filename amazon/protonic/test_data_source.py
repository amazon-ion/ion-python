import pytest

from amazon.protonic.data_source import DataSource


def test_extend_and_advance():
    ds = DataSource()
    ds.extend(b'abcd')

    assert len(ds) == 4
    assert ds[1] == ord(b'b')

    ds.advance(2)

    assert len(ds) == 2
    assert ds[0] == ord(b'c')

    ds.extend(b'efg')

    assert len(ds) == 5

    ds.advance(3)

    assert len(ds) == 2
    assert ds[1] == ord(b'g')

    try:
        ds.advance(3)
    except ValueError as e:
        pass
    else:
        pytest.fail("Expected ValueError as we attempted to advance past buffer")

    ds.advance(2)

    try:
        ds[0]
    except IndexError as e:
        pass
    else:
        pytest.fail("Expected IndexError as we attempted to index outside of buffer")


def test_cross_buffer_slice():
    ds = DataSource()
    ds.extend(b'ab')
    ds.extend(b'cd')

    assert ds[0:2] == bytes(b'ab')
    assert ds[1:3] == bytes(b'bc')
    assert ds[2:4] == bytes(b'cd')

    ds.extend(b'ef')

    assert ds[1:5] == bytes(b'bcde')

    ds.advance(1)

    assert ds[0:4] == bytes(b'bcde')
    assert ds[1:5] == bytes(b'cdef')

    ds.advance(1)

    assert ds[0:2] == bytes(b'cd')

    # TODO add slice arg checks

def test_eof():
    ds = DataSource()
    ds.extend(b'a')

    assert not ds.is_complete()

    ds.eof()

    try:
        ds[0:2]
    except IndexError:
        pass
    else:
        pytest.fail("Should not be able to index across eof!")

    try:
        ds.extend(b'b')
    except ValueError as e:
        pass
    else:
        pytest.fail("Expected ValueError as we attempted to extend after eof!")

    try:
        ds.advance(2)
    except ValueError as e:
        pass
    else:
        pytest.fail("Expected ValueError as we attempted to advance over eof!")

    assert ds.is_complete()




