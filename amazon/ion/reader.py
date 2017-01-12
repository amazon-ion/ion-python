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

import sys

from amazon.ion.symbols import SymbolToken
from .core import DataEvent, IonEventType, Transition
from .core import ION_STREAM_END_EVENT
from .util import coroutine, Enum


class CodePoint(int):
    """Evaluates as a code point ordinal, while also containing the unicode character representation and
    indicating whether the code point was escaped.
    """
    def __init__(self, *args, **kwargs):
        self.char = None
        self.is_escaped = False


def _narrow_unichr(code_point):
    """Retrieves the unicode character representing any given code point, in a way that won't break on narrow builds.

    This is necessary because the built-in unichr function will fail for ordinals above 0xFFFF on narrow builds (UCS2);
    ordinals above 0xFFFF would require recalculating and combining surrogate pairs. This avoids that by retrieving the
    unicode character that was initially read.

    Args:
        code_point (int|CodePoint): An int or a subclass of int that contains the unicode character representing its
            code point in an attribute named 'char'.
    """
    try:
        if len(code_point.char) > 1:
            return code_point.char
    except AttributeError:
        pass
    return six.unichr(code_point)


_NARROW_BUILD = sys.maxunicode < 0x10ffff
_WIDE_BUILD = not _NARROW_BUILD

safe_unichr = six.unichr if (six.PY3 or _WIDE_BUILD) else _narrow_unichr


class CodePointArray:
    """A mutable sequence of code points. Used in place of bytearray() for text values."""
    def __init__(self, initial_bytes=None):
        self.__text = u''
        if initial_bytes is not None:
            for b in initial_bytes:
                self.append(b)

    def append(self, value):
        self.__text += safe_unichr(value)

    def extend(self, values):
        if isinstance(values, six.text_type):
            self.__text += values
        else:
            assert isinstance(values, six.binary_type)
            for b in six.iterbytes(values):
                self.append(b)

    def as_symbol(self):
        return SymbolToken(self.__text, sid=None, location=None)

    def as_text(self):
        return self.__text

    def __len__(self):
        return len(self.__text)

    def __repr__(self):
        return 'CodePointArray(text=%s)' % (self.__text,)

    __str__ = __repr__

    def insert(self, index, value):
        raise ValueError('Attempted to add code point in middle of sequence.')

    def __setitem__(self, index, value):
        raise ValueError('Attempted to set code point in middle of sequence.')

    def __getitem__(self, index):
        return self.__text[index]

    def __delitem__(self, index):
        raise ValueError('Attempted to delete from code point sequence.')


_EOF = b'\x04'  # End of transmission character.


class BufferQueue(object):
    """A simple circular buffer of buffers."""
    def __init__(self, is_unicode=False):
        self.__segments = deque()
        self.__offset = 0
        self.__size = 0
        self.__data_cls = CodePointArray if is_unicode else bytearray
        if is_unicode:
            self.__chr = safe_unichr
            self.__element_type = six.text_type
        else:
            self.__chr = chr if six.PY2 else lambda x: x
            self.__element_type = six.binary_type
        self.__ord = ord if (six.PY3 and is_unicode) else lambda x: x
        self.position = 0
        self.is_unicode = is_unicode

    @staticmethod
    def is_eof(c):
        return c is _EOF  # Note reference equality, ensuring that the EOF literal is still illegal as part of the data.

    @staticmethod
    def _incompatible_types(element_type, data):
        raise ValueError('Incompatible input data types. Expected %r, got %r.' % (element_type, type(data)))

    def extend(self, data):
        # TODO Determine if there are any other accumulation strategies that make sense.
        # TODO Determine if we should use memoryview to avoid copying.
        if not isinstance(data, self.__element_type):
            BufferQueue._incompatible_types(self.__element_type, data)
        self.__segments.append(data)
        self.__size += len(data)

    def mark_eof(self):
        self.__segments.append(_EOF)
        self.__size += 1

    def read(self, length, skip=False):
        """Consumes the first ``length`` bytes from the accumulator."""
        if length > self.__size:
            raise IndexError(
                'Cannot pop %d bytes, %d bytes in buffer queue' % (length, self.__size))
        self.position += length
        self.__size -= length
        segments = self.__segments
        offset = self.__offset

        data = self.__data_cls()
        while length > 0:
            segment = segments[0]
            segment_off = offset
            segment_len = len(segment)
            segment_rem = segment_len - segment_off
            segment_read_len = min(segment_rem, length)

            if segment_off == 0 and segment_read_len == segment_rem:
                # consume an entire segment
                if skip:
                    segment_slice = self.__element_type()
                else:
                    segment_slice = segment
            else:
                # Consume a part of the segment.
                if skip:
                    segment_slice = self.__element_type()
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
        if self.is_unicode:
            return data.as_text()
        else:
            return data

    def read_byte(self):
        if self.__size < 1:
            raise IndexError('Buffer queue is empty')
        segments = self.__segments
        segment = segments[0]
        segment_len = len(segment)
        offset = self.__offset
        if BufferQueue.is_eof(segment):
            octet = _EOF
        else:
            octet = self.__ord(six.indexbytes(segment, offset))
        offset += 1
        if offset == segment_len:
            offset = 0
            segments.popleft()
        self.__offset = offset
        self.__size -= 1
        self.position += 1
        return octet

    def unread(self, c):
        """Unread the given character, byte, or code point.

        If this is a unicode buffer and the input is an int or byte, it will be interpreted as an ordinal representing
        a unicode code point.

        If this is a binary buffer, the input must be a byte or int; a unicode character will raise an error.
        """
        if self.position < 1:
            raise IndexError('Cannot unread an empty buffer queue.')
        if isinstance(c, six.text_type):
            if not self.is_unicode:
                BufferQueue._incompatible_types(self.is_unicode, c)
        else:
            c = self.__chr(c)
        num_code_units = self.is_unicode and len(c) or 1
        if self.__offset == 0:
            if num_code_units == 1 and six.PY3:
                if self.is_unicode:
                    segment = c
                else:
                    segment = six.int2byte(c)
            else:
                segment = c
            self.__segments.appendleft(segment)
        else:
            self.__offset -= num_code_units

            def verify(ch, idx):
                existing = self.__segments[0][self.__offset + idx]
                if existing != ch:
                    raise ValueError('Attempted to unread %s when %s was expected.' % (ch, existing))
            if num_code_units == 1:
                verify(c, 0)
            else:
                for i in range(num_code_units):
                    verify(c[i], i)
        self.__size += num_code_units
        self.position -= num_code_units

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

    def __iter__(self):
        while self.__size > 0:
            yield self.read_byte()

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
        data (bytes|unicode): The data for the event. Bytes are accepted by both binary and text readers, while unicode
            is accepted by text readers with is_unicode=True.
    """
    return DataEvent(ReadEventType.DATA, data)


@coroutine
def reader_trampoline(start, allow_flush=False):
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
        allow_flush(Optional[bool]): True if this reader supports receiving ``NEXT`` after
            yielding ``INCOMPLETE`` to trigger an attempt to flush pending parse events,
            otherwise False.

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
                    if not allow_flush or not (trans.event.event_type is IonEventType.INCOMPLETE and
                                               data_event.type is ReadEventType.NEXT):
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
                if ion_event.event_type is IonEventType.INCOMPLETE:
                    ion_event = reader.send(NEXT_EVENT)
                    continue
                else:
                    yield ION_STREAM_END_EVENT
                    return
            ion_event = reader.send(read_data_event(data))
