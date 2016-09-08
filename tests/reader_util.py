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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from pytest import raises

from tests import is_exception, listify

from amazon.ion.core import IonEventType
from amazon.ion.util import record
from tests.event_aliases import END
from tests.event_aliases import NEXT


def add_depths(events):
    """Adds the appropriate depths to an iterable of :class:`IonEvent`."""
    depth = 0
    for event in events:
        if is_exception(event):
            # Special case for expectation of exception.
            yield event
        else:
            if event.event_type == IonEventType.CONTAINER_END:
                depth -= 1

            if event.event_type.is_stream_signal:
                yield event
            else:
                yield event.derive_depth(depth)

            if event.event_type == IonEventType.CONTAINER_START:
                depth += 1


class ReaderParameter(record('desc', 'event_pairs', ('is_unicode', False))):
    def __str__(self):
        return self.desc


def reader_scaffold(reader, event_pairs):
    input_events = (e for e, _ in event_pairs)
    output_events = add_depths(e for _, e in event_pairs)
    for read_event, expected in zip(input_events, output_events):
        if is_exception(expected):
            with raises(expected):
                reader.send(read_event).value  # Forces evaluation of all value thunks.
        else:
            actual = reader.send(read_event)
            assert expected == actual


def value_iter(event_func, values, *args):
    """Generates input/output event pairs from a sequence whose first element is the raw data and the following
    elements are the expected output events.
    """
    for seq in values:
        data = seq[0]
        event_pairs = list(event_func(data, seq[1:], *args))
        yield data, event_pairs


def all_top_level_as_one_stream_params(iterator, *args):
    @listify
    def generate_event_pairs():
        yield (NEXT, END)
        for data, event_pairs in iterator(*args):
            for event_pair in event_pairs:
                yield event_pair
            yield (NEXT, END)

    yield ReaderParameter(
        desc='TOP LEVEL ALL',
        event_pairs=generate_event_pairs()
    )
