from collections import deque
from functools import reduce

EOF = b'\x04'


class DataSource:
    """
    Creates a buffer of bytes to provide zero-copy slices.

    If you need to talk in terms of unicode code points,
    you need to manage that above this. This only cares about raw bytes.
    """

    def __init__(self):
        # deque of memoryviews from left to right
        self._data = deque()
        # total length of _all_ the buffers in bytes
        self._size = 0
        # the current "0" index position within the left-most buffer
        self._cursor = 0

    def extend(self, data):
        """
        Extend the buffered data.
        """
        if self.is_complete():
            raise ValueError("Cannot extend past EOF!")

        self._data.append(memoryview(data))
        self._size += len(data)

    def eof(self):
        """
        cap the input
        """
        self._data.append(EOF)

    def clear(self):
        """
        clear the buffer
        """
        self._data = deque()
        self._size = 0
        self._cursor = 0

    def is_complete(self):
        """
        check that the last buffer _is_ EOF.
        """
        return self._data and self._data[-1] is EOF

    def advance(self, ct):
        """
        move the cursor forward by ct *bytes*.
        Any buffer segments which are now unreadable will be dropped.
        """
        cursor = self._cursor
        if ct > self._size:
            raise ValueError("Cannot advance past end of buffer!")

        droppable = cursor + ct
        while self._data and droppable:
            data = self._data[0]

            l = len(data)
            if droppable >= l:
                self._data.popleft()
                droppable -= l
            else:
                break

        self._cursor = droppable
        self._size -= ct

    def __len__(self):
        """
        length available to read, including an EOF marker if placed
        """
        return self._size

    def __getitem__(self, subscript):
        """
        provides get by position and slice.
        positions are relative to current cursor.
        all subscripts are bytes.
        if the entirety of a slice can be accessed from a memory view then it is provided without
        copy, otherwise a bytearray is built from the constituent slices.
        """
        if isinstance(subscript, slice):
            if subscript.step:
                raise ValueError("Steps are not supported!")

            if subscript.start > subscript.stop:
                raise IndexError("Slice start must be before slice stop")
            if subscript.stop > self._size:
                raise IndexError(f"Index {subscript} is beyond size {self._size}!")
            # TODO other invariant checks

            cursor = self._cursor
            start = subscript.start + cursor
            stop = subscript.stop + cursor
            slices = []
            for buffer in self._data:
                length = len(buffer)
                # if start is beyond end then skip
                if start >= length:
                    start -= length
                    stop -= length
                    continue

                # if end is captured by current then capture current and break
                if stop <= length:
                    slices.append(buffer[start:stop])
                    break

                # otherwise capture what's needed and iterate
                slices.append(buffer[start:])
                start = 0
                stop -= length

            if len(slices) == 1:
                return slices[0]
            else:
                return reduce(lambda acc, b: acc + b, slices, bytearray())


        elif isinstance(subscript, int):
            if subscript < 0:
                raise IndexError("Negative indexes not yet supported!")
            if subscript >= self._size:
                raise IndexError(f"Index {subscript} is beyond size {self._size}!")

            remaining = subscript + self._cursor
            for buffer in self._data:
                length = len(buffer)
                if remaining < length:
                    return buffer[remaining]
                remaining -= length

        else:
            raise TypeError(f"Can slice or index {subscript} is neither!")
