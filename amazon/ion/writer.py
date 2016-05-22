# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

"""Provides common functionality for Ion binary and text writers."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .core import IonEventType
from .util import coroutine
from .util import record
from .util import Enum


class WriteEventType(Enum):
    """Events that can come from an Ion writer co-routine.

    Attributes:
        HAS_PENDING: Indicates that the writer has more pending events to yield,
            and that it should be sent ``None`` as the event to *flush* them out.
            Generally, this is signalled when an internal buffer has been filled by an input event and needs
            to be flushed in order to make progress.
        NEEDS_INPUT: Indicates that the writer has no pending events to yield
            and that it should be sent a :class:`amazon.ion.core.IonEvent`.
        COMPLETE: Indicates that the writer has flushed out complete Ion values at the top-level.
            This is similar to ``NEEDS_INPUT`` except that it signifies point that all data emitted by the writer
            is in sync with the events given to it.
    """
    HAS_PENDING = 1
    NEEDS_INPUT = 2
    COMPLETE = 3


class WriteEvent(record('type', 'data')):
    """IonEvent generated as a result of serialization.

    Args:
        type (WriteEventType): The type of event.
        data (bytes):  The serialized data returned.  If no data is to be serialized,
            this should be the empty byte string.
    """


class WriteResult(record('write_event', 'delegate')):
    """The result of the write co-routine state machine.

    Args:
        event (WriteEvent): The event from the writer.
        delegate (Coroutine): The coroutine to delegate serialization to. May be ``None`` indicating that
        the current coroutine is to be delegated back to.
    """


def partial_write_result(data, delegate):
    """Generates a :class:`WriteResult` that has an event indicating ``HAS_PENDING``."""
    return WriteResult(WriteEvent(WriteEventType.HAS_PENDING, data), delegate)


@coroutine
def writer_trampoline(start):
    """Provides the co-routine trampoline for a writer state machine.

    The given co-routine is a state machine that yields :class:`WriteResult` and takes
    a pair of :class:`amazon.ion.core.IonEvent` and the co-routine itself.

    Notes:
        A writer delimits its logical flush points with ``WriteEventType.COMPLETE``, depending
        on the configuration, a user may need to send an ``IonEventType.STREAM_END`` to
        force this to occur.

    Args:
        start: The writer co-routine to initially delegate to.

    Yields:
        WriteEvent: the result of serialization.

        Receives :class:`amazon.ion.core.IonEvent` to serialize into :class:`WriteEvent`.
    """
    current = WriteResult(None, start)
    while True:
        ion_event = (yield current.write_event)
        if current.write_event is None:
            if ion_event is None:
                raise TypeError('Cannot start Writer with no event')
        else:
            if current.write_event.type is WriteEventType.HAS_PENDING and ion_event is not None:
                raise TypeError('Writer expected to receive no event: %r' % (ion_event,))
            if current.write_event.type is not WriteEventType.HAS_PENDING and ion_event is None:
                raise TypeError('Writer expected to receive event')
            if ion_event is not None and ion_event.event_type is IonEventType.INCOMPLETE:
                raise TypeError('Writer cannot receive INCOMPLETE event')
        current = current.delegate.send((ion_event, current.delegate))


@coroutine
def blocking_writer(writer, output):
    """Provides an implementation of using the writer co-routine with a file-like object.

    Args:
        writer (Coroutine): A writer co-routine.
        output (BaseIO): The file-like object to pipe events to.

    Yields:
        WriteEventType: Yields when no events are pending.

        Receives :class:`amazon.ion.core.IonEvent` to write to the ``output``.
    """
    result_type = None
    while True:
        ion_event = (yield result_type)
        result_event = WriteEvent(WriteEventType.HAS_PENDING, None)
        while result_event.type is WriteEventType.HAS_PENDING:
            result_event = writer.send(ion_event)
            result_type = result_event.type
            ion_event = None
            output.write(result_event.data)
