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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
from pytest import raises

from amazon.ion.core import ION_STREAM_END_EVENT, IonEventType, IonEvent
from amazon.ion.util import record
from amazon.ion.writer import WriteEventType
from tests import is_exception
from tests import noop_manager

_STREAM_END_EVENT = (ION_STREAM_END_EVENT,)


class P(record('desc', 'events', 'expected')):
    def __str__(self):
        return self.desc


def _scalar_p(ion_type, value, expected, force_stream_end):
    events = (IonEvent(IonEventType.SCALAR, ion_type, value),)
    if force_stream_end:
        events += _STREAM_END_EVENT
    return P(
        desc='SCALAR %s - %s' % (ion_type.name, expected),
        events=events,
        expected=expected,
    )


def generate_scalars(scalars_map, force_stream_end=False):
    for ion_type, values in six.iteritems(scalars_map):
        for native, expected in values:
            yield _scalar_p(ion_type, native, expected, force_stream_end)


def generate_containers(containers_map, force_stream_end=False):
    for ion_type, container in six.iteritems(containers_map):
        for container_value_events, expected in container:
            start_event = IonEvent(IonEventType.CONTAINER_START, ion_type)
            end_event = IonEvent(IonEventType.CONTAINER_END, ion_type)
            events = (start_event,) + container_value_events + (end_event,)
            if force_stream_end:
                events += _STREAM_END_EVENT
            yield P(
                desc='EMPTY %s' % ion_type.name,
                events=events,
                expected=expected,
            )


def assert_writer_events(p, new_writer, conversion=lambda x: x):
    buf, buf_writer = new_writer()

    ctx = noop_manager()
    if is_exception(p.expected):
        ctx = raises(p.expected)

    result_type = None
    with ctx:
        for event in p.events:
            result_type = buf_writer.send(event)

    if not is_exception(p.expected):
        assert result_type is WriteEventType.COMPLETE
        assert conversion(p.expected) == buf.getvalue()

