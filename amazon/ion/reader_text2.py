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

from collections import deque
from typing import Optional, NamedTuple, Callable, Tuple

from amazon.ion.core import IonType, IonEvent, \
    IonEventType
from amazon.ion.reader import ReadEventType
from amazon.ion.sliceable_buffer import SliceableBuffer
from amazon.ion.util import coroutine


def _whitespace(byte):
    return byte in bytearray(b" \n\t\r\f")


def _tlv_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    pass


def _list_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    pass


def _struct_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    pass

def _sexp_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    raise NotImplementedError("todo")


_container_parsers = [
    _list_parser,
    _sexp_parser,
    _struct_parser,
]

class _ContextFrame(NamedTuple):
    parser: Callable[[SliceableBuffer], Tuple[IonEvent, SliceableBuffer]]
    ion_type: Optional[IonType]
    depth: int


@coroutine
def stream_handler():
    """
    Handler for an Ion Text value-stream.
    """
    buffer: SliceableBuffer = SliceableBuffer.empty()
    context_stack = deque([_ContextFrame(_tlv_parser, None, 0)])
    ion_event = None
    skip_or_next = ReadEventType.NEXT
    expect_data = False

    while True:
        read_event = yield ion_event
        assert read_event is not None

        # part 1: handle user's read event
        if expect_data:
            if read_event.type is not ReadEventType.DATA:
                raise TypeError("Data expected")
            buffer = buffer.extend(read_event.data)
        else:
            if read_event.type is ReadEventType.DATA:
                raise TypeError("Next or Skip expected")
            skip_or_next = read_event.type

        if skip_or_next is ReadEventType.SKIP:
            raise NotImplementedError("Skip is not supported")

        # part 2: do some lexxing
        (parser, ctx_type, depth) = context_stack[-1]

        (ion_event, buffer) = parser(buffer)
        event_type = ion_event.type
        ion_type = ion_event.ion_type

        if event_type is IonEventType.STREAM_END:
            expect_data = True
        elif event_type is IonEventType.INCOMPLETE:
            # todo: flushable/commit or something something
            raise NotImplementedError("Incomplete is not supported")
        elif event_type is IonEventType.CONTAINER_START:
            parser = _container_parsers[ion_type - IonType.LIST]
            context_stack.append(_ContextFrame(parser, ion_type, depth + 1))
        elif event_type is IonEventType.CONTAINER_END:
            assert ion_type is ctx_type
            assert depth > 0
            context_stack.pop()
