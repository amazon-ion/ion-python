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

from datetime import timedelta
from decimal import Decimal
from itertools import chain
from random import Random
from six import int2byte

from tests import parametrize, listify
from tests.reader_util import reader_scaffold, ReaderParameter, all_top_level_as_one_stream_params, value_iter
from tests.event_aliases import *

from amazon.ion.core import IonType, timestamp, TimestampPrecision, OffsetTZInfo
from amazon.ion.exceptions import IonException
from amazon.ion.reader import read_data_event, ReadEventType
from amazon.ion.reader_binary import raw_reader, _TypeID, _CONTAINER_TIDS, _TID_VALUE_TYPE_TABLE
from amazon.ion.symbols import SYMBOL_ZERO_TOKEN, SymbolToken

_PREC_YEAR = TimestampPrecision.YEAR
_PREC_MONTH = TimestampPrecision.MONTH
_PREC_DAY = TimestampPrecision.DAY
_PREC_MINUTE = TimestampPrecision.MINUTE
_PREC_SECOND = TimestampPrecision.SECOND

_ts = timestamp

_P = ReaderParameter

_IVM_PAIRS =[
    (NEXT, END),
    (e_read(b'\xE0\x01\x00\xEA'), IVM)
]


@listify
def _prepend_ivm(params):
    for p in params:
        new_p = _P(
            desc=p.desc,
            event_pairs=_IVM_PAIRS + p.event_pairs
        )
        yield new_p

_BASIC_PARAMS = (
    _P(
        desc='EMPTY',
        event_pairs=[(NEXT, END)]
    ),
    _P(
        desc='IVM',
        event_pairs=_IVM_PAIRS,
    ),
    _P(
        desc='IVM PARTS',
        event_pairs=[
            (NEXT, END),
            (e_read(b'\xE0\x01'), INC),
            (e_read(b'\x00\xEA'), IVM),
            (NEXT, END),
        ],
    ),
    _P(
        desc='IVM NOP IVM',
        event_pairs=[
            (NEXT, END),
            (e_read(b'\xE0\x01\x00\xEA\x02\x00'), IVM),
            (NEXT, INC),
            (e_read(b'\xFF\xE0\x01\x00\xEA'), IVM),
            (NEXT, END),
        ],
    ),
    _P(
        desc='NO START IVM',
        event_pairs=[
            (NEXT, END),
            (e_read(b'\x0F'), IonException),
        ],
    ),
)

_BAD_VALUES = (
    # The annotation wrapper declares 6 octets, but the wrapped value (a symbol value) ends after only 4.
    (b'\xe6\x81\x84\x71\x04\x71\x04', 'ANNOT LENGTH TOO LONG - SCALAR'),
    # The annotation wrapper declares 6 octets, but the wrapped value (a list) ends after only 4.
    (b'\xe6\x81\x84\xb1\x20\x71\x04', 'ANNOT LENGTH TOO LONG - CONTAINER'),
    # The annotation wrapper declares 4 octets, and the SIDs take up all four.
    (b'\xe4\x83\x84\x85\x86', 'ANNOT LENGTH TOO SHORT - NO VALUE'),
    # The annotation wrapper declares 4 octets, but the subfields (including a list) take up five.
    (b'\xe4\x81\x84\xb2\x21\x01', 'ANNOT LENGTH TOO SHORT - CONTAINER'),
    # The annotation wrapper declares 3 octets, but the subfields (including an int) take up four.
    (b'\xe3\x81\x84\x21\x01', 'ANNOT LENGTH TOO SHORT - SCALAR'),
)


def _bad_params():
    for pair in _BAD_VALUES:
        yield _P(
            desc=pair[1],
            event_pairs=_IVM_PAIRS + [
                (NEXT, END),
                (e_read(pair[0]), IonException)
            ]
        )

# This is an encoding of a single top-level value and the expected events with ``NEXT``.
_TOP_LEVEL_VALUES = (
    (b'\x0F', e_null()),

    (b'\x10', e_bool(False)),
    (b'\x11', e_bool(True)),
    (b'\x1F', e_bool()),

    (b'\x2F', e_int()),
    (b'\x20', e_int(0)),
    (b'\x21\xFE', e_int(0xFE)),
    (b'\x22\x00\x01', e_int(1)),
    (b'\x24\x01\x2F\xEF\xCC', e_int(0x12FEFCC)),
    (b'\x29\x12\x34\x56\x78\x90\x12\x34\x56\x78', e_int(0x123456789012345678)),
    (b'\x2E\x81\x05', e_int(5)), # Over padded length.

    (b'\x3F', e_int()),  # null.int has two equivalent representations.
    (b'\x31\x01', e_int(-1)),
    (b'\x32\xC1\xC2', e_int(-0xC1C2)),
    (b'\x36\xC1\xC2\x00\x00\x10\xFF', e_int(-0xC1C2000010FF)),
    (b'\x39\x12\x34\x56\x78\x90\x12\x34\x56\x78', e_int(-0x123456789012345678)),
    (b'\x3E\x82\x00\xA0', e_int(-160)), # Over padded length + overpadded integer.

    (b'\x4F', e_float()),
    (b'\x40', e_float(0.0)),
    (b'\x44\x3F\x80\x00\x00', e_float(1.0)),
    (b'\x44\x7F\x80\x00\x00', e_float(float('+Inf'))),
    (b'\x48\x42\x02\xA0\x5F\x20\x00\x00\x00', e_float(1e10)),
    (b'\x48\x7F\xF8\x00\x00\x00\x00\x00\x00', e_float(float('NaN'))),

    (b'\x5F', e_decimal()),
    (b'\x50', e_decimal(Decimal())),
    (b'\x52\x47\xE8', e_decimal(Decimal('0e-1000'))),
    (b'\x54\x07\xE8\x00\x00', e_decimal(Decimal('0e1000'))),
    (b'\x52\x81\x01', e_decimal(Decimal('1e1'))),
    (b'\x53\xD4\x04\xD2', e_decimal(Decimal('1234e-20'))),
    (b'\x52\x80\x80', e_decimal(Decimal(0).copy_negate())),
    (b'\x52\x80\x01', e_decimal(Decimal('1e0'))),
    (b'\x52\xC1\x01', e_decimal(Decimal('1e-1'))),
    (b'\x51\xC1', e_decimal(Decimal('0e-1'))),
    (b'\x51\x81', e_decimal(Decimal('0e1'))),
    (b'\x52\x81\x81', e_decimal(Decimal('-1e1'))),
    (b'\x52\x80\x81', e_decimal(Decimal('-1e0'))),
    (b'\x52\xC1\x81', e_decimal(Decimal('-1e-1'))),
    (b'\x52\xC1\x80', e_decimal(Decimal('-0e-1'))),
    (b'\x52\x81\x80', e_decimal(Decimal('-0e1'))),

    (b'\x6F', e_timestamp()),
    (b'\x63\xC0\x0F\xE0', e_timestamp(_ts(2016, precision=_PREC_YEAR))), # -00:00
    (b'\x63\x80\x0F\xE0', e_timestamp(_ts(2016, off_hours=0, precision=_PREC_YEAR))),
    (
        b'\x64\x81\x0F\xE0\x82',
        e_timestamp(_ts(2016, 2, 1, 0, 1, off_minutes=1, precision=_PREC_MONTH))
    ),
    (
        b'\x65\xFC\x0F\xE0\x82\x82',
        e_timestamp(_ts(2016, 2, 1, 23, 0, off_hours=-1, precision=_PREC_DAY))
    ),
    (
        b'\x68\x43\xA4\x0F\xE0\x82\x82\x87\x80',
        e_timestamp(_ts(2016, 2, 2, 0, 0, off_hours=-7, precision=_PREC_MINUTE))
    ),
    (
        b'\x69\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E',
        e_timestamp(_ts(2016, 2, 2, 0, 0, 30, off_hours=-7, precision=_PREC_SECOND))
    ),
    (
        b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC7\x01',
        e_timestamp(_ts(1, 1, 1, 0, 0, 0, None, precision=_PREC_SECOND, fractional_precision=None,
                        fractional_seconds=Decimal('1e-7')))
    ),
    (
        b'\x6B\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC3\x01',
        e_timestamp(_ts(
            2016, 2, 2, 0, 0, 30, 1000, off_hours=-7, precision=_PREC_SECOND, fractional_precision=3
        ))
    ),
    (
        b'\x67\xC0\x81\x81\x81\x80\x80\x80',
        e_timestamp(_ts(year=1, month=1, day=1, precision=_PREC_SECOND))
    ),
    (
        b'\x67\xC1\x81\x81\x81\x80\x81\x80',
        e_timestamp(_ts(year=1, month=1, day=1, off_minutes=-1, precision=_PREC_SECOND))
    ),
    (
        b'\x69\xC1\x81\x81\x81\x80\x81\x80\x80\x00',  # Fractions with coefficients of 0 and exponents > -1 are ignored.
        e_timestamp(_ts(year=1, month=1, day=1, off_minutes=-1, precision=_PREC_SECOND))
    ),
    (
        b'\x69\xC0\x81\x81\x81\x80\x80\x80\xC6\x01',
        e_timestamp(_ts(
            year=1, month=1, day=1, hour=0, minute=0, second=0, microsecond=1,
            precision=_PREC_SECOND, fractional_precision=6
        ))
    ),
    (
        b'\x6C\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC6\x03\xE8',  # The last three octets represent 1000d-6
        e_timestamp(_ts(
            2016, 2, 2, 0, 0, 30, 1000, off_hours=-7, precision=TimestampPrecision.SECOND
        )),
    ),
    (
        b'\x6C\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC6\x03\xE8',  # The last three octets represent 1000d-6
        e_timestamp(_ts(
            2016, 2, 2, 0, 0, 30, 1000, off_hours=-7, precision=TimestampPrecision.SECOND, fractional_precision=6
        )),
    ),

    (b'\x7F', e_symbol()),
    (b'\x70', e_symbol(SYMBOL_ZERO_TOKEN)),
    (b'\x71\x02', e_symbol(SymbolToken(None, 2))),
    (b'\x7A' + b'\xFF' * 10, e_symbol(SymbolToken(None, 0xFFFFFFFFFFFFFFFFFFFF))),

    (b'\x8F', e_string()),
    (b'\x80', e_string(u'')),
    (b'\x84\xf0\x9f\x92\xa9', e_string(u'\U0001F4A9')),
    (b'\x88$ion_1_0', e_string(u'$ion_1_0')),

    (b'\x9F', e_clob()),
    (b'\x90', e_clob(b'')),
    (b'\x94\xf0\x9f\x92\xa9', e_clob(b'\xf0\x9f\x92\xa9')),

    (b'\xAF', e_blob()),
    (b'\xA0', e_blob(b'')),
    (b'\xA4\xf0\x9f\x92\xa9', e_blob(b'\xf0\x9f\x92\xa9')),

    (b'\xBF', e_null_list()),
    (b'\xB0', e_start_list(), e_end_list()),

    (b'\xCF', e_null_sexp()),
    (b'\xC0', e_start_sexp(), e_end_sexp()),

    (b'\xDF', e_null_struct()),
    (b'\xD0', e_start_struct(), e_end_struct()),
    (b'\xD1\x82\x84\x20', e_start_struct(), e_int(0, field_name=SymbolToken(None, 4)), e_end_struct())
)


def _top_level_event_pairs(data, events):
    first = True
    for event in events:
        input_event = NEXT
        if first:
            input_event = e_read(data)
            first = False
        yield input_event, event

_top_level_iter = partial(value_iter, _top_level_event_pairs, _TOP_LEVEL_VALUES)


def _gen_type_len(tid, length):
    """Very primitive type length encoder."""
    type_code = tid << 4
    if length < 0xE:
        return int2byte(type_code | length)
    else:
        type_code |= 0xE
        if length <= 0x7F:
            return int2byte(type_code) + int2byte(0x80 | length)

    raise ValueError('No support for long lengths in reader test')

_TEST_ANNOTATION_DATA = b'\x82\x84\x87'
_TEST_ANNOTATION_LEN = len(_TEST_ANNOTATION_DATA)
_TEST_ANNOTATION_SIDS = (SymbolToken(None, 4), SymbolToken(None, 7))


def _top_level_value_params():
    """Converts the top-level tuple list into parameters with appropriate ``NEXT`` inputs.

    The expectation is starting from an end of stream top-level context.
    """
    for data, event_pairs in _top_level_iter():
        _, first = event_pairs[0]
        yield _P(
            desc='TL %s - %s - %r' % \
                 (first.event_type.name, first.ion_type.name, first.value),
            event_pairs=[(NEXT, END)] + event_pairs + [(NEXT, END)],
        )


def _annotate_params(params):
    """Adds annotation wrappers for a given iterator of parameters,

    The requirement is that the given parameters completely encapsulate a single value.
    """
    for param in params:
        @listify
        def annotated():
            for input_event, output_event in param.event_pairs:
                if input_event.type is ReadEventType.DATA:
                    data_len = _TEST_ANNOTATION_LEN + len(input_event.data)
                    data = _gen_type_len(_TypeID.ANNOTATION, data_len) \
                           + _TEST_ANNOTATION_DATA \
                           + input_event.data
                    input_event = read_data_event(data)
                    output_event = output_event.derive_annotations(_TEST_ANNOTATION_SIDS)
                yield input_event, output_event

        yield _P(
            desc='ANN %s' % param.desc,
            event_pairs=annotated(),
        )


def _data_event_len(event_pairs):
    length = 0
    for read_event, _ in event_pairs:
        if read_event.type is ReadEventType.DATA:
            length += len(read_event.data)
    return length


def _containerize_params(params, with_skip=True):
    """Adds container wrappers for a given iteration of parameters.

    The requirement is that each parameter is a self-contained single value.
    """
    rnd = Random()
    rnd.seed(0xC0FFEE)
    params = list(params)
    for param in params:
        data_len = _data_event_len(param.event_pairs)
        for tid in _CONTAINER_TIDS:
            ion_type = _TID_VALUE_TYPE_TABLE[tid]

            field_data = b''
            field_tok = None
            field_desc = ''
            if ion_type is IonType.STRUCT:
                field_sid = rnd.randint(0, 0x7F)
                field_data = int2byte(field_sid | 0x80)
                field_tok = SymbolToken(None, field_sid)
                field_desc = ' (f:0x%02X)' % field_sid

            @listify
            def add_field_names(event_pairs):
                first = True
                for read_event, ion_event in event_pairs:
                    if first and not ion_event.event_type.is_stream_signal:
                        ion_event = ion_event.derive_field_name(field_tok)
                        first = False
                    yield read_event, ion_event

            type_header = _gen_type_len(tid, data_len + len(field_data)) + field_data

            start = [
                (NEXT, END),
                (e_read(type_header), e_start(ion_type)),
                (NEXT, INC)
            ]
            mid = add_field_names(param.event_pairs[1:-1])
            end = [(NEXT, e_end(ion_type)), (NEXT, END)]

            desc = 'SINGLETON %s%s - %s' % (ion_type.name, field_desc, param.desc)
            yield _P(
                desc=desc,
                event_pairs=start + mid + end,
            )

            # Version with SKIP
            if with_skip:
                @listify
                def only_data_inc(event_pairs):
                    for read_event, ion_event in event_pairs:
                        if read_event.type is ReadEventType.DATA:
                            yield read_event, INC

                start = start[:-1] + [(SKIP, INC)]
                end = only_data_inc(param.event_pairs)
                end = end[:-1] + [(end[-1][0], e_end(ion_type)), (NEXT, END)]
                
                yield _P(
                    desc='SKIP %s' % desc,
                    event_pairs=start + end,
                )

# TODO Add NOP pad fuzz.
# TODO Add data incomplete fuzz.


@parametrize(*chain(
    _BASIC_PARAMS,
    _bad_params(),
    _prepend_ivm(_top_level_value_params()),
    _prepend_ivm(_annotate_params(_top_level_value_params())),
    _prepend_ivm(_containerize_params(_top_level_value_params())),
    _prepend_ivm(
        _containerize_params(
            _containerize_params(_top_level_value_params(), with_skip=False)
        )
    ),
    _prepend_ivm(all_top_level_as_one_stream_params(_top_level_iter)),
))
def test_raw_reader(p):
    reader_scaffold(raw_reader(), p.event_pairs)
