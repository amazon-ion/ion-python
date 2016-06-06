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

"""Provides common functionality for Ion binary and text writers."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .core import DataEvent, IonEventType, Transition
from .util import coroutine, record, Enum


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


def partial_transition(data, delegate):
    """Generates a :class:`Transition` that has an event indicating ``HAS_PENDING``."""
    return Transition(DataEvent(WriteEventType.HAS_PENDING, data), delegate)


@coroutine
def writer_trampoline(start):
    """Provides the co-routine trampoline for a writer state machine.

    The given co-routine is a state machine that yields :class:`Transition` and takes
    a :class:`Transition` with a :class:`amazon.ion.core.IonEvent` and the co-routine itself.

    Notes:
        A writer delimits its logical flush points with ``WriteEventType.COMPLETE``, depending
        on the configuration, a user may need to send an ``IonEventType.STREAM_END`` to
        force this to occur.

    Args:
        start: The writer co-routine to initially delegate to.

    Yields:
        DataEvent: the result of serialization.

        Receives :class:`amazon.ion.core.IonEvent` to serialize into :class:`DataEvent`.
    """
    trans = Transition(None, start)
    while True:
        ion_event = (yield trans.event)
        if trans.event is None:
            if ion_event is None:
                raise TypeError('Cannot start Writer with no event')
        else:
            if trans.event.type is WriteEventType.HAS_PENDING and ion_event is not None:
                raise TypeError('Writer expected to receive no event: %r' % (ion_event,))
            if trans.event.type is not WriteEventType.HAS_PENDING and ion_event is None:
                raise TypeError('Writer expected to receive event')
            if ion_event is not None and ion_event.event_type is IonEventType.INCOMPLETE:
                raise TypeError('Writer cannot receive INCOMPLETE event')
        trans = trans.delegate.send(Transition(ion_event, trans.delegate))


_WRITE_EVENT_HAS_PENDING_EMPTY = DataEvent(WriteEventType.HAS_PENDING, None)


def _drain(writer, ion_event):
    """Drain the writer of its pending write events.

    Args:
        writer (Coroutine): A writer co-routine.
        ion_event (amazon.ion.core.IonEvent): The first event to apply to the writer.

    Yields:
        DataEvent: Yields each pending data event.
    """
    result_event = _WRITE_EVENT_HAS_PENDING_EMPTY
    while result_event.type is WriteEventType.HAS_PENDING:
        result_event = writer.send(ion_event)
        ion_event = None
        yield result_event


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
        for result_event in _drain(writer, ion_event):
            output.write(result_event.data)
        result_type = result_event.type
