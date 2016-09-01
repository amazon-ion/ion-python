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

from datetime import datetime, timedelta

import six
from decimal import Decimal
from pytest import raises

from amazon.ion.core import ION_STREAM_END_EVENT, IonEventType, IonEvent, IonType, timestamp, OffsetTZInfo, \
    TimestampPrecision
from amazon.ion.symbols import SYMBOL_ZERO_TOKEN, SymbolToken
from amazon.ion.util import record
from amazon.ion.writer import WriteEventType
from tests import is_exception
from tests import noop_manager

_STREAM_END_EVENT = (ION_STREAM_END_EVENT,)

ION_ENCODED_INT_ZERO = 0x20
VARUINT_END_BYTE = 0x80

class WriterParameter(record('desc', 'events', 'expected')):
    def __str__(self):
        return self.desc


def _scalar_p(ion_type, value, expected, force_stream_end):
    events = (IonEvent(IonEventType.SCALAR, ion_type, value),)
    if force_stream_end:
        events += _STREAM_END_EVENT
    return WriterParameter(
        desc='SCALAR %s - %r - %r' % (ion_type.name, value, expected),
        events=events,
        expected=expected,
    )

_D = Decimal
_DT = datetime
_IT = IonType

SIMPLE_SCALARS_MAP = {
    _IT.NULL: (
        (None, b'\x0F'),
    ),
    _IT.BOOL: (
        (None, b'\x1F'),
        (False, b'\x10'),
        (True, b'\x11')
    ),
    _IT.INT: (
        (None, b'\x2F'),
        (0, b'\x20'),
        (1, b'\x21\x01'),
        (-1, b'\x31\x01'),
        (0xFFFFFFFF, b'\x24\xFF\xFF\xFF\xFF'),
        (-0xFFFFFFFF, b'\x34\xFF\xFF\xFF\xFF'),
        (0xFFFFFFFFFFFFFFFF, b'\x28\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
        (-0xFFFFFFFFFFFFFFFF, b'\x38\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
        (0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
         b'\x2E\x90\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
        (-0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
         b'\x3E\x90\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
    ),
    _IT.FLOAT: (
        (None, b'\x4F'),
        (0e0, b'\x40'),
        (1e0, b'\x48\x3F\xF0\x00\x00\x00\x00\x00\x00'),
        (-1e0, b'\x48\xBF\xF0\x00\x00\x00\x00\x00\x00'),
        (1e1, b'\x48\x40\x24\x00\x00\x00\x00\x00\x00'),
        (-1e1, b'\x48\xC0\x24\x00\x00\x00\x00\x00\x00'),
        (float('inf'), b'\x48\x7F\xF0\x00\x00\x00\x00\x00\x00'),
        (float('-inf'), b'\x48\xFF\xF0\x00\x00\x00\x00\x00\x00'),
    ),
    _IT.DECIMAL: (
        (None, b'\x5F'),
        (_D(0), b'\x50'),
        (_D(0).copy_negate(), b'\x52\x80\x80'),
        (_D("1e1"), b'\x52\x81\x01'),
        (_D("1e0"), b'\x52\x80\x01'),
        (_D("1e-1"), b'\x52\xC1\x01'),
        (_D("0e-1"), b'\x51\xC1'),
        (_D("0e1"), b'\x51\x81'),
        (_D("-1e1"), b'\x52\x81\x81'),
        (_D("-1e0"), b'\x52\x80\x81'),
        (_D("-1e-1"), b'\x52\xC1\x81'),
        (_D("-0e-1"), b'\x52\xC1\x80'),
        (_D("-0e1"), b'\x52\x81\x80'),
    ),
    _IT.TIMESTAMP: (
        (None, b'\x6F'),
        # TODO Clarify whether there's a valid zero-length Timestamp representation.
        (_DT(year=1, month=1, day=1), b'\x67\xC0\x81\x81\x81\x80\x80\x80'),
        (_DT(year=1, month=1, day=1, tzinfo=OffsetTZInfo(timedelta(minutes=-1))),
         b'\x67\xC1\x81\x81\x81\x80\x81\x80'),
        (_DT(year=1, month=1, day=1, hour=0, minute=0, second=0, microsecond=1),
         b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC6\x01'),
        (timestamp(year=1, month=1, day=1, precision=TimestampPrecision.DAY), b'\x64\xC0\x81\x81\x81'),
        (timestamp(year=1, month=1, day=1, off_minutes=-1, precision=TimestampPrecision.SECOND),
         b'\x67\xC1\x81\x81\x81\x80\x81\x80'),
        (timestamp(year=1, month=1, day=1, hour=0, minute=0, second=0, microsecond=1, precision=TimestampPrecision.SECOND),
         b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC6\x01'),
        (timestamp(2016, precision=TimestampPrecision.YEAR), b'\x63\xC0\x0F\xE0'),  # -00:00
        (timestamp(2016, off_hours=0, precision=TimestampPrecision.YEAR), b'\x63\x80\x0F\xE0'),
        (
            timestamp(2016, 2, 1, 0, 1, off_minutes=1, precision=TimestampPrecision.MONTH),
            b'\x64\x81\x0F\xE0\x82'
        ),
        (
            timestamp(2016, 2, 1, 23, 0, off_hours=-1, precision=TimestampPrecision.DAY),
            b'\x65\xFC\x0F\xE0\x82\x82'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, off_hours=-7, precision=TimestampPrecision.MINUTE),
            b'\x68\x43\xA4\x0F\xE0\x82\x82\x87\x80'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, off_hours=-7, precision=TimestampPrecision.SECOND),
            b'\x69\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, 1000, off_hours=-7, precision=TimestampPrecision.SECOND),
            b'\x6B\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC3\x01'
        ),
    ),
    _IT.SYMBOL: (
        (None, b'\x7F'),
        (SYMBOL_ZERO_TOKEN, b'\x70'),
        (SymbolToken(u'$ion', 1), b'\x71\x01'),
    ),
    _IT.STRING: (
        (None, b'\x8F'),
        (u'', b'\x80'),
        (u'abc', b'\x83abc'),
        (u'abcdefghijklmno', b'\x8E\x8Fabcdefghijklmno'),
        (u'a\U0001f4a9c', b'\x86' + bytearray([b for b in u'a\U0001f4a9c'.encode('utf-8')])),
        (u'a\u0009\x0a\x0dc', b'\x85' + bytearray([b for b in 'a\t\n\rc'.encode('utf-8')])),
    ),
    _IT.CLOB: (
        (None, b'\x9F'),
        (b'', b'\x90'),
        (b'abc', b'\x93' + b'abc'),
        (b'abcdefghijklmno', b'\x9E\x8Fabcdefghijklmno'),
    ),
    _IT.BLOB: (
        (None, b'\xAF'),
        (b'', b'\xA0'),
        (b'abc', b'\xA3' + b'abc'),
        (b'abcdefghijklmno', b'\xAE\x8Fabcdefghijklmno'),
    ),
    _IT.LIST: (
        (None, b'\xBF'),
    ),
    _IT.SEXP: (
        (None, b'\xCF'),
    ),
    _IT.STRUCT: (
        (None, b'\xDF'),
    ),
}

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
            yield WriterParameter(
                desc='CONTAINER %s - %r' % (ion_type.name, expected),
                events=events,
                expected=expected,
            )


def assert_writer_events(p, new_writer):
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
        assert p.expected == buf.getvalue()

