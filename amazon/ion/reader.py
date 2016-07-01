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

"""Provides common functionality for Ion binary and text readers."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

from collections import deque

from .core import DataEvent, IonEventType, Transition
from .core import ION_STREAM_END_EVENT
from .util import coroutine, Enum


class BufferQueue(object):
    """A simple circular buffer of buffers."""
    def __init__(self):
        self.__segments = deque()
        self.__offset = 0
        self.__size = 0
        self.position = 0

    def extend(self, data):
        # TODO Determine if there are any other accumulation strategies that make sense.
        # TODO Determine if we should use memoryview to avoid copying.
        self.__segments.append(data)
        self.__size += len(data)

    def read(self, length, skip=False):
        """Consumes the first ``length`` bytes from the accumulator."""
        if length > self.__size:
            raise IndexError(
                'Cannot pop %d bytes, %d bytes in buffer queue' % (length, self.__size))
        self.position += length
        self.__size -= length
        segments = self.__segments
        offset = self.__offset

        data = bytearray()
        while length > 0:
            segment = segments[0]
            segment_off = offset
            segment_len = len(segment)
            segment_rem = segment_len - segment_off
            segment_read_len = min(segment_rem, length)

            if segment_off == 0 and segment_read_len == segment_rem:
                # consume an entire segment
                if skip:
                    segment_slice = b''
                else:
                    segment_slice = segment
            else:
                # Consume a part of the segment.
                if skip:
                    segment_slice = b''
                else:
                    segment_slice = segment[segment_off:segment_off + segment_read_len]
                offset = 0
            segment_off += segment_read_len
            if segment_off == segment_len:
                segments.popleft()
                self.__offset = 0
            else:
                self.__offset = segment_off

            if length <= segment_rem and len(data) == 0:
                return segment_slice
            data.extend(segment_slice)
            length -= segment_read_len
        return data

    def read_byte(self):
        if self.__size < 1:
            raise IndexError('Buffer queue is empty')
        segments = self.__segments
        segment = segments[0]
        segment_len = len(segment)
        offset = self.__offset
        octet = six.indexbytes(segment, offset)
        offset += 1
        if offset == segment_len:
            offset = 0
            segments.popleft()
        self.__offset = offset
        self.__size -= 1
        self.position += 1
        return octet

    def skip(self, length):
        """Removes ``length`` bytes and returns the number length still required to skip"""
        if length >= self.__size:
            skip_amount = self.__size
            rem = length - skip_amount
            self.__segments.clear()
            self.__offset = 0
            self.__size = 0
            self.position += skip_amount
        else:
            rem = 0
            self.read(length, skip=True)
        return rem

    def __len__(self):
        return self.__size


class ReadEventType(Enum):
    """Events that are pushed into an Ion reader co-routine.

    Attributes:
        DATA: Indicates more data for the reader.  The expected type is :class:`bytes`.
        NEXT: Indicates that the reader should yield the next event.
        SKIP: Indicates that the reader should proceed to the end of the current container.
            This type is not meaningful at the top-level.
    """
    DATA = 0
    NEXT = 1
    SKIP = 2

NEXT_EVENT = DataEvent(ReadEventType.NEXT, None)
SKIP_EVENT = DataEvent(ReadEventType.SKIP, None)


def read_data_event(data):
    """Simple wrapper over the :class:`DataEvent` constructor to wrap a :class:`bytes` like
    with the ``DATA`` :class:`ReadEventType`.

    Args:
        data (bytes): The data for the event.
    """
    return DataEvent(ReadEventType.DATA, data)


@coroutine
def reader_trampoline(start):
    """Provides the co-routine trampoline for a reader state machine.

    The given co-routine is a state machine that yields :class:`Transition` and takes
    a Transition of :class:`amazon.ion.core.DataEvent` and the co-routine itself.

    A reader must start with a ``ReadEventType.NEXT`` event to prime the parser.  In many cases
    this will lead to an ``IonEventType.INCOMPLETE`` being yielded, but not always
    (consider a reader over an in-memory data structure).

    Notes:
        A reader delimits its incomplete parse points with ``IonEventType.INCOMPLETE``.
        Readers also delimit complete parse points with ``IonEventType.STREAM_END``;
        this is similar to the ``INCOMPLETE`` case except that it denotes that a logical
        termination of data is *allowed*. When these event are received, the only valid
        input event type is a ``ReadEventType.DATA``.

        Generally, ``ReadEventType.NEXT`` is used to get the next parse event, but
        ``ReadEventType.SKIP`` can be used to skip over the current container.

        An internal state machine co-routine can delimit a state change without yielding
        to the caller by yielding ``None`` event, this will cause the trampoline to invoke
        the transition delegate, immediately.
    Args:
        start: The reader co-routine to initially delegate to.

    Yields:
        amazon.ion.core.IonEvent: the result of parsing.

        Receives :class:`DataEvent` to parse into :class:`amazon.ion.core.IonEvent`.
    """
    data_event = yield
    if data_event is None or data_event.type is not ReadEventType.NEXT:
        raise TypeError('Reader must be started with NEXT')
    trans = Transition(None, start)
    while True:
        trans = trans.delegate.send(Transition(data_event, trans.delegate))
        data_event = None
        if trans.event is not None:
            # Only yield if there is an event.
            data_event = (yield trans.event)
            if trans.event.event_type.is_stream_signal:
                if data_event.type is not ReadEventType.DATA:
                    raise TypeError('Reader expected data: %r' % (data_event,))
            else:
                if data_event.type is ReadEventType.DATA:
                    raise TypeError('Reader did not expect data')
            if data_event.type is ReadEventType.DATA and len(data_event.data) == 0:
                raise ValueError('Empty data not allowed')
            if trans.event.depth == 0 \
                    and trans.event.event_type is not IonEventType.CONTAINER_START \
                    and data_event.type is ReadEventType.SKIP:
                raise TypeError('Cannot skip at the top-level')


_DEFAULT_BUFFER_SIZE = 8196


@coroutine
def blocking_reader(reader, input, buffer_size=_DEFAULT_BUFFER_SIZE):
    """Provides an implementation of using the reader co-routine with a file-like object.

    Args:
        reader(Coroutine): A reader co-routine.
        input(BaseIO): The file-like object to read from.
        buffer_size(Optional[int]): The optional buffer size to use.
    """
    ion_event = None
    while True:
        read_event = (yield ion_event)
        ion_event = reader.send(read_event)
        while ion_event is not None and ion_event.event_type.is_stream_signal:
            data = input.read(buffer_size)
            if len(data) == 0:
                # End of file.
                if ion_event.event_type is not IonEventType.STREAM_END:
                    raise EOFError('Premature EOF while parsing')
                yield ION_STREAM_END_EVENT
                return
            ion_event = reader.send(read_data_event(data))
