from typing import NamedTuple, List


class SliceableBuffer:
    """
    A theoretically infinite immutable buffer from which readers may read bytes
    or slices by position.

    Reads return the data and a new buffer starting at the end of the read.
    As the reader advances past chunks (either through reads or skips), whole
    chunks are dropped from the buffer.

    Built with the assumption that chunks will be reasonably large and that
    relatively few (single digit) chunks will be buffered at once.
    """

    @staticmethod
    def empty():
        """
        Create a new buffer with no data.
        """
        return SliceableBuffer([])

    def __init__(self, chunks, offset=0, size=0):
        """
        *Class internal usage only.*

        Users should use `empty()` to get a new buffer.
        """
        self._chunks: List[_ChunkPair] = chunks
        # the offset adds complexity but enables keeping and dropping of whole
        # chunks which is more efficient than slicing and copying the chunks
        # on each read or skip.
        self._offset = offset
        self.size = size

    def extend(self, chunk):
        """
        Return a new buffer with the chunk appended.
        """
        if not chunk:
            raise ValueError("Chunk must be not None and non-empty!")

        mem_chunk = memoryview(chunk)
        pair = _ChunkPair(mem_chunk, len(mem_chunk))
        return SliceableBuffer(
            self._chunks + [pair],
            self._offset,
            self.size + pair.length)

    def read_byte(self):
        """
        Read the next byte from the buffer, return (byte, new buffer).

        Raise IncompleteReadError if the buffer is empty.
        """
        size = self.size
        chunks = self._chunks
        offset = self._offset

        try:
            # assume that we have data, and that chunks are non-empty
            (chunk, length) = chunks[0]
        except IndexError:
            raise IncompleteReadError("Buffer is empty!")

        if length == offset + 1:
            return chunk[offset], SliceableBuffer(chunks[1:], 0, size - 1)
        else:
            return chunk[offset], SliceableBuffer(chunks, offset + 1, size - 1),

    def read_slice(self, n):
        """
        Read a slice of the buffer, return (slice, new buffer).

        Raise IncompleteReadError if the slice cannot be fully read.

        Chunks which are no longer readable are dropped from the new buffer.
        Bytes are only copied if the read requires bridging chunks.
        """
        size = self.size
        chunks = self._chunks
        offset = self._offset
        if n < 1:
            raise ValueError("n must be >= 1")

        endpos = offset + n

        if size < n:
            raise IncompleteReadError(f'Buffer has size {size}, but {n} bytes were requested!')

        # short-circuit when we can serve full read from first chunk
        # optimizes for common case and simplifies accumulation loop
        (chunk, length) = chunks[0]
        if endpos < length:
            return chunk[offset:endpos], SliceableBuffer(chunks, offset + n, size - n)
        elif endpos == length:
            return chunk[offset:], SliceableBuffer(chunks[1:], 0, size - n)

        slices = [_ChunkPair(chunk[offset:], length - offset)]

        # remaining and i are used to init the new buffer after the loop
        remaining = endpos - length
        i = 1
        for (i, pair) in enumerate(chunks[1:], start=1):
            (chunk, length) = pair
            if remaining < length:
                slices.append(_ChunkPair(chunk[:remaining], remaining))
                break

            slices.append(pair)
            remaining -= length
            if remaining == 0:
                # move i forward to drop the chunk in the new buffer
                i += 1
                break

        combined = bytearray(n)
        cursor = 0
        for (chunk, length) in slices:
            combined[cursor:cursor + length] = chunk
            cursor += length

        return memoryview(combined), SliceableBuffer(chunks[i:], remaining, size - n)

    def skip(self, n):
        """
        Skip max(n, size) bytes, return (skipped, new buffer).

        Chunks which are no longer readable are dropped from the new buffer.

        Unlike the read methods, skip allows partial skipping, which is more
        memory efficient when skipping large tokens that span chunks.
        """
        size = self.size
        chunks = self._chunks
        offset = self._offset
        endpos = offset + n

        if size <= n:
            return size, SliceableBuffer([])

        remaining = endpos
        i = 0
        for (i, (_, length)) in enumerate(chunks):
            if remaining < length:
                break

            remaining -= length
            if remaining == 0:
                # move i forward to drop the chunk in the new buffer
                i += 1
                break

        return n, SliceableBuffer(chunks[i:], remaining, size - n)

    def __len__(self):
        """
        Length of data in bytes remaining in buffer.
        """
        return self.size


class IncompleteReadError(IndexError):
    pass


class _ChunkPair(NamedTuple):
    chunk: memoryview
    length: int
