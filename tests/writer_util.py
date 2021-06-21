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

from base64 import b64encode
from datetime import datetime, timedelta

import six
from decimal import Decimal

import sys
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

_D = Decimal
_DT = datetime
_IT = IonType


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


def _convert_symbol_pairs_to_string_pairs(symbol_pairs):
    for value, literals in symbol_pairs:
        final_literals = ()
        if not isinstance(literals, (tuple, list)):
            literals = (literals,)
        for literal in literals:
            if literal.decode('utf-8')[0] == "'":
                final_literals += (literal.replace(b"'", b'"'),)
            else:
                # Add quotes to unquoted symbols
                final_literals += ((b'"' + literal + b'"'),)
        yield value, final_literals


def _convert_clob_pairs(clob_pairs):
    for value, literal in clob_pairs:
        yield (value, b'{{' + b64encode(value) + b'}}')


_SIMPLE_SYMBOLS_TEXT=(
    (u'', br"''"),
    (u'\u0000', (br"'\x00'", br"'\0'")),
    (u'4hello', br"'4hello'"),
    (u'hello', br"hello"),
    (u'_hello_world', br"_hello_world"),
    (u'null', br"'null'"),
    (u'hello world', br"'hello world'"),
    (u'hello\u0009\x0a\x0dworld', br"'hello\t\n\rworld'"),
    (u'hello\aworld', (br"'hello\x07world'", br"'hello\aworld'")),
    (u'hello\u3000world', (br"'hello\u3000world'", b"'hello\xe3\x80\x80world'")),  # A full width space.
    (u'hello\U0001f4a9world', (br"'hello\U0001f4a9world'", b"'hello\xf0\x9f\x92\xa9world'")),  # A 'pile of poo' emoji code point.
)
_SIMPLE_STRINGS_TEXT=tuple(_convert_symbol_pairs_to_string_pairs(_SIMPLE_SYMBOLS_TEXT))

_SIMPLE_CLOBS_TEXT=(
    (b'', br'{{""}}'),
    (b'\x00', (br'{{"\x00"}}', br'{{"\0"}}')),
    (b'hello', br'{{"hello"}}'),
    (b'hello\x09\x0a\x0dworld', br'{{"hello\t\n\rworld"}}'),
    (b'hello\x7Eworld', br'{{"hello~world"}}'),
    (b'hello\xFFworld', (br'{{"hello\xffworld"}}', br'{{"hello\xFFworld"}}')),
)
_SIMPLE_BLOBS_TEXT=tuple(_convert_clob_pairs(_SIMPLE_CLOBS_TEXT))

if sys.version_info < (2, 7):
    # Python < 2.7 was not good at some float irrationals.
    _FLOAT_1_1_ENC = b'1.1000000000000001e0'
    _FLOAT_2_E_NEG_15_ENC = b'2.0000000000000002e-15'
else:
    _FLOAT_1_1_ENC = b'1.1e0'
    _FLOAT_2_E_NEG_15_ENC = b'2e-15'

SIMPLE_SCALARS_MAP_TEXT = {
    _IT.NULL: (
        (None, b'null'),
    ),
    _IT.BOOL: (
        (None, b'null.bool'),
        (True, b'true'),
        (False, b'false'),
    ),
    _IT.INT: (
        (None, b'null.int'),
        (-1, b'-1'),
        (0, b'0'),
        (1, b'1'),
        (0xFFFFFFFF, b'4294967295'),
        (-0xFFFFFFFF, b'-4294967295'),
        (0xFFFFFFFFFFFFFFFF, b'18446744073709551615'),
        (-0xFFFFFFFFFFFFFFFF, b'-18446744073709551615'),
    ),
    _IT.FLOAT: (
        (None, b'null.float'),
        (float('NaN'), b'nan'),
        (float('+Inf'), b'+inf'),
        (float('-Inf'), b'-inf'),
        (-0.0, (b'-0.0e0', b'-0e0')),
        (0.0, (b'0.0e0', b'0e0')),
        (1.0, (b'1.0e0', b'1e+0')),
        (-9007199254740991.0, (b'-9007199254740991.0e0', b'-9007199254740991e+0')),
        (2.0e-15, (_FLOAT_2_E_NEG_15_ENC, b'2.0000000000000001554e-15')),
        (1.1, (_FLOAT_1_1_ENC, b'1.1000000000000000888e+0')),
        (1.1999999999999999555910790149937383830547332763671875e0, (b'1.2e0', b'1.1999999999999999556e+0')),
    ),
    _IT.DECIMAL: (
        (None, b'null.decimal'),
        (_D('-0.0'), b'-0.0'),
        (_D('0'), b'0d0'),
        (_D('0e100'), b'0d+100'),
        (_D('0e-15'), b'0d-15'),
        (_D('-1e1000'), b'-1d+1000'),
        (_D('-4.412111311414141e1000'), b'-4.412111311414141d+1000'),
        # (_D('1.1999999999999999555910790149937383830547332763671875e0'),
        #     b'1.1999999999999999555910790149937383830547332763671875'),
    ),
    _IT.TIMESTAMP: (
        (None, b'null.timestamp'),
        (_DT(2016, 1, 1), b'2016-01-01T00:00:00.000000-00:00'),
        (_DT(2016, 1, 1, 12), b'2016-01-01T12:00:00.000000-00:00'),
        (_DT(2016, 1, 1, 12, 34, 12), b'2016-01-01T12:34:12.000000-00:00'),
        (_DT(2016, 1, 1, 12, 34, 12, 555000), b'2016-01-01T12:34:12.555000-00:00'),
        (_DT(2016, 1, 1, 12, 34, 12, tzinfo=OffsetTZInfo()), b'2016-01-01T12:34:12.000000Z'),
        (_DT(2016, 1, 1, 12, 34, 12, tzinfo=OffsetTZInfo(timedelta(hours=-7))),
            b'2016-01-01T12:34:12.000000-07:00'),
        (timestamp(year=1, month=1, day=1, precision=TimestampPrecision.DAY), (b'0001-01-01T',  b'0001-01-01')),
        (timestamp(year=1, month=1, day=1, off_minutes=-1, precision=TimestampPrecision.SECOND),
         b'0001-01-01T00:00:00-00:01'),
        (
            timestamp(year=1, month=1, day=1, hour=0, minute=0, second=0,
                      microsecond=1, precision=TimestampPrecision.SECOND),
            b'0001-01-01T00:00:00.000001-00:00'
        ),
        (
            timestamp(year=1, month=1, day=1, hour=0, minute=0, second=0,
                      microsecond=100000, precision=TimestampPrecision.SECOND, fractional_precision=1),
            b'0001-01-01T00:00:00.1-00:00'
        ),
        (timestamp(2016, precision=TimestampPrecision.YEAR), b'2016T'),
        (timestamp(2016, off_hours=0, precision=TimestampPrecision.YEAR), b'2016T'),
        (
            timestamp(2016, 2, 1, 0, 1, off_minutes=1, precision=TimestampPrecision.MONTH),
            b'2016-02T'
        ),
        (
            timestamp(2016, 2, 1, 23, 0, off_hours=-1, precision=TimestampPrecision.DAY),
            (b'2016-02-01T', b'2016-02-01')
        ),
        (
            timestamp(2016, 2, 2, 0, 0, off_hours=-7, precision=TimestampPrecision.MINUTE),
            b'2016-02-02T00:00-07:00'
        ),
        (
           timestamp(2016, 2, 2, 0, 0, 30, off_hours=-7, precision=TimestampPrecision.SECOND),
           b'2016-02-02T00:00:30-07:00'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, 1000, off_hours=-7,
                      precision=TimestampPrecision.SECOND),
            # When fractional_precision not specified, defaults to 6 (same as regular datetime).
            b'2016-02-02T00:00:30.001000-07:00'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, 1000, off_hours=-7,
                      precision=TimestampPrecision.SECOND, fractional_precision=3),
            b'2016-02-02T00:00:30.001-07:00'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, 100000, off_hours=-7,
                      precision=TimestampPrecision.SECOND, fractional_precision=1),
            b'2016-02-02T00:00:30.1-07:00'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, precision=TimestampPrecision.SECOND,
                      fractional_seconds=Decimal('0.00001000')),
            (b'2016-02-02T00:00:30.00001000-00:00', b'2016-02-02T00:00:30.000010-00:00')
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, precision=TimestampPrecision.SECOND,
                      fractional_seconds=Decimal('0.7e-500')),
            (b'2016-02-02T00:00:30.' + b'0' * 500 + b'7-00:00', b'2016-02-02T00:00:30.000000-00:00')
        )
    ),
    _IT.SYMBOL: (
        (None, b'null.symbol'),
        (SymbolToken(None, 4), (b'$4', b'name')),  # System symbol 'name'.
        (SymbolToken(u'a token', 400), b"'a token'"),
    ) + _SIMPLE_SYMBOLS_TEXT,
    _IT.STRING: (
        (None, b'null.string'),
    ) + _SIMPLE_STRINGS_TEXT,
    _IT.CLOB: (
        (None, b'null.clob'),
    ) + _SIMPLE_CLOBS_TEXT,
    _IT.BLOB: (
        (None, b'null.blob'),
    ) + _SIMPLE_BLOBS_TEXT,
    _IT.LIST: (
        (None, b'null.list'),
    ),
    _IT.SEXP: (
        (None, b'null.sexp'),
    ),
    _IT.STRUCT: (
        (None, b'null.struct'),
    ),
}

SIMPLE_SCALARS_MAP_BINARY = {
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
        (_DT(year=1, month=1, day=1), b'\x68\xC0\x81\x81\x81\x80\x80\x80\xc6'),
        (_DT(year=1, month=1, day=1, tzinfo=OffsetTZInfo(timedelta(minutes=-1))),
         b'\x68\xC1\x81\x81\x81\x80\x81\x80\xc6'),
        (_DT(year=1, month=1, day=1, hour=0, minute=0, second=0, microsecond=1),
         b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC6\x01'),
        (timestamp(year=1, month=1, day=1, precision=TimestampPrecision.DAY), b'\x64\xC0\x81\x81\x81'),
        (timestamp(year=1, month=1, day=1, off_minutes=-1, precision=TimestampPrecision.SECOND),
         b'\x67\xC1\x81\x81\x81\x80\x81\x80'),
        (
            timestamp(year=1, month=1, day=1, hour=0, minute=0, second=0,
                      microsecond=1, precision=TimestampPrecision.SECOND),
            b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC6\x01'
        ),
        (
            timestamp(year=1, month=1, day=1, hour=0, minute=0, second=0,
                      microsecond=100000, precision=TimestampPrecision.SECOND, fractional_precision=1),
            b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC1\x01'
        ),
        (timestamp(2016, precision=TimestampPrecision.YEAR), b'\x63\xC0\x0F\xE0'),  # -00:00
        (timestamp(2016, off_hours=0, precision=TimestampPrecision.YEAR),
            (b'\x63\x80\x0F\xE0', b'\x63\xC0\x0F\xE0')),
        (
            timestamp(2016, 2, 1, 0, 1, off_minutes=1, precision=TimestampPrecision.MONTH),
            (b'\x64\x81\x0F\xE0\x82', b'\x64\xC0\x0F\xE0\x82')
        ),
        (
            timestamp(2016, 2, 1, 23, 0, off_hours=-1, precision=TimestampPrecision.DAY),
            (b'\x65\xFC\x0F\xE0\x82\x82', b'\x65\xC0\x0F\xE0\x82\x81')
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
            timestamp(2016, 2, 2, 0, 0, 30, 1000, off_hours=-7,
                      precision=TimestampPrecision.SECOND),
            # When fractional_precision not specified, defaults to 6 (same as regular datetime).
            b'\x6C\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC6\x03\xE8'  # The last three octets represent 1000d-6
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, 1000, off_hours=-7,
                      precision=TimestampPrecision.SECOND, fractional_precision=3),
            b'\x6B\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC3\x01'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, 100000, off_hours=-7,
                      precision=TimestampPrecision.SECOND, fractional_precision=1),
            b'\x6B\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC1\x01'
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, precision=TimestampPrecision.SECOND,
                      fractional_seconds=Decimal('0.000010000')),
            (b'\x6B\xC0\x0F\xE0\x82\x82\x80\x80\x9E\xC9\x27\x10',
             b'\x6A\xC0\x0F\xE0\x82\x82\x80\x80\x9E\xC6\x0A')
        ),
        (
            timestamp(2016, 2, 2, 0, 0, 30, precision=TimestampPrecision.SECOND,
                      fractional_seconds=Decimal('0.7e-500')),
            (b'\x6B\xC0\x0F\xE0\x82\x82\x80\x80\x9E\x43\xF5\x07',
             b'\x69\xC0\x0F\xE0\x82\x82\x80\x80\x9E\xC6')
        )
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

        if isinstance(p.expected, (tuple, list)):
            expecteds = p.expected
        else:
            expecteds = (p.expected,)
        assert_res = False
        for expected in expecteds:
            if expected == buf.getvalue():
                assert_res = True
                break
        assert assert_res
