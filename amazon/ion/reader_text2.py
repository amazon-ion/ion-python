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
    IonEventType, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT
from amazon.ion.protons import *
from amazon.ion.reader import ReadEventType
from amazon.ion.sliceable_buffer import SliceableBuffer
from amazon.ion.util import coroutine


def _whitespace(byte):
    return byte in bytearray(b" \n\t\r\f")


_stop = peek(one_of(b" \n\t\r\f{}[](),"))


def tag_stop(tag_value):
    return terminated(tag(tag_value), _stop)


def is_empty(buffer: SliceableBuffer):
    if buffer.size:
        return ParseResult(ResultType.FAILURE, buffer, None)
    else:
        return ParseResult(ResultType.SUCCESS, buffer, None)


_tlv_parsec = preceded(
    take_while(_whitespace),
    alt(
        # constant(is_empty, ION_STREAM_END_EVENT),
        constant(tag_stop(b"nan"), IonEvent(IonEventType.SCALAR, IonType.FLOAT, "Nan")),
        constant(tag_stop(b"null"), IonEvent(IonEventType.SCALAR, IonType.NULL, None)),
        constant(tag_stop(b"true"), IonEvent(IonEventType.SCALAR, IonType.BOOL, True)),
        constant(tag_stop(b"false"), IonEvent(IonEventType.SCALAR, IonType.BOOL, False)),
        constant(tag(b"{"), IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT)),
        constant(tag(b"["), IonEvent(IonEventType.CONTAINER_START, IonType.LIST))))

_stream_end = preceded(take_while(_whitespace), is_empty)

def _tlv_parser(buffer: SliceableBuffer) -> Tuple[IonEvent, SliceableBuffer]:
    result = _tlv_parsec(buffer)
    if result.type is ResultType.SUCCESS:
        return result.value.derive_depth(0), result.buffer
    if result.type is ResultType.INCOMPLETE:
        stream_end = _stream_end(buffer)
        if stream_end.type is ResultType.SUCCESS:
            return ION_STREAM_END_EVENT, stream_end.buffer

        return ION_STREAM_INCOMPLETE_EVENT, buffer

    raise ValueError("parse failed on _____")

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
def text_stream_handler():
    """
    Handler for an Ion Text value-stream.
    """
    buffer: SliceableBuffer = SliceableBuffer.empty()
    context_stack = deque([_ContextFrame(_tlv_parser, None, 0)])
    ion_event = None
    skip_or_next = ReadEventType.NEXT
    expect_data = False
    incomplete = False

    while True:
        read_event = yield ion_event
        assert read_event is not None

        # part 1: handle user's read event
        if expect_data:
            if read_event.type is not ReadEventType.DATA:
                # flush it
                if incomplete:
                    buffer = buffer.extend(b"\t")
                else:
                    raise TypeError("Data expected")
            else:
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

        event_type = ion_event.event_type
        ion_type = ion_event.ion_type
        expect_data = False
        incomplete = False

        if event_type is IonEventType.STREAM_END:
            expect_data = True
        elif event_type is IonEventType.INCOMPLETE:
            expect_data = True
            incomplete = True
        elif event_type is IonEventType.CONTAINER_START:
            parser = _container_parsers[ion_type - IonType.LIST]
            context_stack.append(_ContextFrame(parser, ion_type, depth + 1))
        elif event_type is IonEventType.CONTAINER_END:
            assert ion_type is ctx_type
            assert depth > 0
            context_stack.pop()
