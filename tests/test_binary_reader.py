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

from decimal import Decimal
from functools import partial
from itertools import chain
from random import Random
from six import int2byte

from pytest import raises

from tests import parametrize, is_exception

from amazon.ion.util import record
from amazon.ion.core import ION_STREAM_END_EVENT, ION_STREAM_INCOMPLETE_EVENT, \
                            ION_VERSION_MARKER_EVENT, \
                            IonEvent, IonEventType, IonType, \
                            timestamp, TimestampPrecision
from amazon.ion.exceptions import IonException
from amazon.ion.reader import NEXT_EVENT, SKIP_EVENT, read_data_event, ReadEventType
from amazon.ion.reader_binary import raw_reader, _TypeID, _CONTAINER_TIDS, _TID_VALUE_TYPE_TABLE
from amazon.ion.symbols import SYMBOL_ZERO_TOKEN, SymbolToken

_PREC_YEAR = TimestampPrecision.YEAR
_PREC_MONTH = TimestampPrecision.MONTH
_PREC_DAY = TimestampPrecision.DAY
_PREC_MINUTE = TimestampPrecision.MINUTE
_PREC_SECOND = TimestampPrecision.SECOND

_e_scalar = partial(IonEvent, IonEventType.SCALAR)
_e_null = partial(_e_scalar, IonType.NULL)
_e_bool = partial(_e_scalar, IonType.BOOL)
_e_int = partial(_e_scalar, IonType.INT)
_e_float = partial(_e_scalar, IonType.FLOAT)
_e_decimal = partial(_e_scalar, IonType.DECIMAL)
_e_timestamp = partial(_e_scalar, IonType.TIMESTAMP)
_e_symbol = partial(_e_scalar, IonType.SYMBOL)
_e_string = partial(_e_scalar, IonType.STRING)
_e_clob = partial(_e_scalar, IonType.CLOB)
_e_blob = partial(_e_scalar, IonType.BLOB)

_e_null_list = partial(_e_scalar, IonType.LIST, None)
_e_null_sexp = partial(_e_scalar, IonType.SEXP, None)
_e_null_struct = partial(_e_scalar, IonType.STRUCT, None)

_e_start = partial(IonEvent, IonEventType.CONTAINER_START)
_e_start_list = partial(_e_start, IonType.LIST)
_e_start_sexp = partial(_e_start, IonType.SEXP)
_e_start_struct = partial(_e_start, IonType.STRUCT)

_e_end = partial(IonEvent, IonEventType.CONTAINER_END)
_e_end_list = partial(_e_end, IonType.LIST)
_e_end_sexp = partial(_e_end, IonType.SEXP)
_e_end_struct = partial(_e_end, IonType.STRUCT)

_ts = timestamp

_NEXT = NEXT_EVENT
_SKIP = SKIP_EVENT
_D = read_data_event

_INC = ION_STREAM_INCOMPLETE_EVENT
_END = ION_STREAM_END_EVENT
_IVM = ION_VERSION_MARKER_EVENT


def _add_depths(events):
    depth = 0
    for event in events:
        if event.event_type == IonEventType.CONTAINER_END:
            depth -= 1

        if event.event_type.is_stream_signal:
            yield event
        else:
            yield event.derive_depth(depth)

        if event.event_type == IonEventType.CONTAINER_START:
            depth += 1


class _P(record('desc', 'inputs', 'outputs')):
    def __str__(self):
        return self.desc


_IVM_IN = [_NEXT, _D(b'\xE0\x01\x00\xEA')]
_IVM_OUT = [_END, _IVM]


def _prepend_ivm(params):
    out = []
    for p in params:
        new_p = _P(
            desc=p.desc,
            inputs=_IVM_IN + p.inputs,
            outputs=_IVM_OUT + p.outputs
        )
        out.append(new_p)
    return out

_BASIC_PARAMS = (
    _P(
        desc='EMPTY',
        inputs=[_NEXT],
        outputs=[_END],
    ),
    _P(
        desc='IVM',
        inputs=_IVM_IN,
        outputs=_IVM_OUT,
    ),
    _P(
        desc='IVM PARTS',
        inputs=[_NEXT, _D(b'\xE0\x01'), _D(b'\x00\xEA'), _NEXT],
        outputs=[_END, _INC, _IVM, _END],
    ),
    _P(
        desc='IVM NOP IVM',
        inputs=[_NEXT, _D(b'\xE0\x01\x00\xEA\x02\x00'), _NEXT, _D(b'\xFF\xE0\x01\x00\xEA'), _NEXT],
        outputs=[_END, _IVM, _INC, _IVM, _END],
    ),
    _P(
        desc='NO START IVM',
        inputs=[_NEXT, _D(b'\x0F')],
        outputs=[_END, IonException],
    ),
)


# This is an encoding of a single top-level value and the expected events with ``NEXT``.
_TOP_LEVEL_VALUES = (
    (b'\x0F', _e_null()),

    (b'\x10', _e_bool(False)),
    (b'\x11', _e_bool(True)),
    (b'\x1F', _e_bool()),

    (b'\x2F', _e_int()),
    (b'\x20', _e_int(0)),
    (b'\x21\xFE', _e_int(0xFE)),
    (b'\x22\x00\x01', _e_int(1)),
    (b'\x24\x01\x2F\xEF\xCC', _e_int(0x12FEFCC)),
    (b'\x29\x12\x34\x56\x78\x90\x12\x34\x56\x78', _e_int(0x123456789012345678)),
    (b'\x2E\x81\x05', _e_int(5)), # Overpaded length.

    (b'\x31\x01', _e_int(-1)),
    (b'\x32\xC1\xC2', _e_int(-0xC1C2)),
    (b'\x36\xC1\xC2\x00\x00\x10\xFF', _e_int(-0xC1C2000010FF)),
    (b'\x39\x12\x34\x56\x78\x90\x12\x34\x56\x78', _e_int(-0x123456789012345678)),
    (b'\x3E\x82\x00\xA0', _e_int(-160)), # Overpadded length + overpadded integer.

    (b'\x4F', _e_float()),
    (b'\x40', _e_float(0.0)),
    (b'\x44\x3F\x80\x00\x00', _e_float(1.0)),
    (b'\x44\x7F\x80\x00\x00', _e_float(float('+Inf'))),
    (b'\x48\x42\x02\xA0\x5F\x20\x00\x00\x00', _e_float(1e10)),
    (b'\x48\x7F\xF8\x00\x00\x00\x00\x00\x00', _e_float(float('NaN'))),

    (b'\x5F', _e_decimal()),
    (b'\x50', _e_decimal(Decimal())),
    (b'\x52\x47\xE8', _e_decimal(Decimal('0e-1000'))),
    (b'\x54\x07\xE8\x00\x00', _e_decimal(Decimal('0e1000'))),
    (b'\x52\x81\x01', _e_decimal(Decimal('1e1'))),
    (b'\x53\xD4\x04\xD2', _e_decimal(Decimal('1234e-20'))),

    (b'\x6F', _e_timestamp()),
    (b'\x63\xC0\x0F\xE0', _e_timestamp(_ts(2016, precision=_PREC_YEAR))), # -00:00
    (b'\x63\x80\x0F\xE0', _e_timestamp(_ts(2016, off_hours=0, precision=_PREC_YEAR))),
    (
        b'\x64\x81\x0F\xE0\x82',
        _e_timestamp(_ts(2016, 2, 1, 0, 1, off_minutes=1, precision=_PREC_MONTH))
    ),
    (
        b'\x65\xFC\x0F\xE0\x82\x82',
        _e_timestamp(_ts(2016, 2, 1, 23, 0, off_hours=-1, precision=_PREC_DAY))
    ),
    (
        b'\x68\x43\xA4\x0F\xE0\x82\x82\x87\x80',
        _e_timestamp(_ts(2016, 2, 2, 0, 0, off_hours=-7, precision=_PREC_MINUTE))
    ),
    (
        b'\x69\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E',
        _e_timestamp(_ts(2016, 2, 2, 0, 0, 30, off_hours=-7, precision=_PREC_SECOND))
    ),
    (
        b'\x6B\x43\xA4\x0F\xE0\x82\x82\x87\x80\x9E\xC3\x81',
        _e_timestamp(_ts(2016, 2, 2, 0, 0, 30, 1000, off_hours=-7, precision=_PREC_SECOND))
    ),

    (b'\x7F', _e_symbol()),
    (b'\x70', _e_symbol(SYMBOL_ZERO_TOKEN)),
    (b'\x71\x02', _e_symbol(SymbolToken(None, 2))),
    (b'\x7A' + b'\xFF' * 10, _e_symbol(SymbolToken(None, 0xFFFFFFFFFFFFFFFFFFFF))),

    (b'\x8F', _e_string()),
    (b'\x80', _e_string(u'')),
    (b'\x84\xf0\x9f\x92\xa9', _e_string(u'\U0001F4A9')),
    (b'\x88$ion_1_0', _e_string(u'$ion_1_0')),

    (b'\x9F', _e_clob()),
    (b'\x90', _e_clob(b'')),
    (b'\x94\xf0\x9f\x92\xa9', _e_clob(b'\xf0\x9f\x92\xa9')),

    (b'\xAF', _e_blob()),
    (b'\xA0', _e_blob(b'')),
    (b'\xA4\xf0\x9f\x92\xa9', _e_blob(b'\xf0\x9f\x92\xa9')),

    (b'\xBF', _e_null_list()),
    (b'\xB0', _e_start_list(), _e_end_list()),
    
    (b'\xCF', _e_null_sexp()),
    (b'\xC0', _e_start_sexp(), _e_end_sexp()),
    
    (b'\xDF', _e_null_struct()),
    (b'\xD0', _e_start_struct(), _e_end_struct()),
)


def _top_level_iter():
    for seq in _TOP_LEVEL_VALUES:
        data = seq[0]
        events = list(_add_depths(seq[1:]))
        yield data, events


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
    for data, events in _top_level_iter():
        yield _P(
            desc='TL %s - %s - %r' % \
                 (events[0].event_type.name, events[0].ion_type.name, events[0].value),
            inputs=[_NEXT, _D(data)] + [_NEXT] * len(events),
            outputs=[_END] + events + [_END],
        )


def _annotate_params(params):
    """Adds annotation wrappers for a given iterator of parameters,

    The requirement is that the given parameters completely encapsulate a single value.
    """
    for param in params:
        def annotated_inputs():
            for event in param.inputs:
                if event.type == ReadEventType.DATA:
                    data_len = _TEST_ANNOTATION_LEN + len(event.data)
                    data = _gen_type_len(_TypeID.ANNOTATION, data_len) \
                           + _TEST_ANNOTATION_DATA \
                           + event.data
                    event = read_data_event(data)
                yield event

        def annotated_outputs():
            first = True
            for event in param.outputs:
                if first and not event.event_type.is_stream_signal:
                    event = event.derive_annotations(_TEST_ANNOTATION_SIDS)
                    first = False
                yield event

        yield _P(
            desc='ANN %s' % param.desc,
            inputs=list(annotated_inputs()),
            outputs=list(annotated_outputs()),
        )


def _data_event_len(events):
    length = 0
    for event in events:
        if event.type is ReadEventType.DATA:
            length += len(event.data)
    return length


def _containerize_params(params, with_skip=True):
    """Adds container wrappers for a given iteration of parameters.

    The requirement is that each parameter is a self-contained single value.
    """
    rnd = Random()
    rnd.seed(0xC0FFEE)
    params = list(params)
    for param in params:
        data_len = _data_event_len(param.inputs)
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

            def field_name_outputs(events):
                first = True
                for event in events:
                    if first and not event.event_type.is_stream_signal:
                        event = event.derive_field_name(field_tok)
                        first = False
                    yield event

            type_header = _gen_type_len(tid, data_len + len(field_data)) + field_data
            inputs = [_NEXT, _D(type_header)] + param.inputs + [_NEXT]

            out_start = [_END, _e_start(ion_type), _INC]
            out_mid = list(field_name_outputs(param.outputs[1:-1]))
            out_end = [_e_end(ion_type), _END]
            outputs = list(_add_depths(chain(out_start, out_mid, out_end)))

            desc = 'SINGLETON %s%s - %s' % (ion_type.name, field_desc, param.desc)
            yield _P(
                desc=desc,
                inputs=inputs,
                outputs=outputs,
            )

            # Version with SKIP
            if with_skip:
                def only_data(events):
                    for event in events:
                        if event.type is ReadEventType.DATA:
                            yield event

                data_events = list(only_data(param.inputs))
                inputs = [_NEXT, _D(type_header), _SKIP] + data_events + [_NEXT]
                out_start = out_start[:-1] + [_INC] * len(data_events)
                outputs = list(_add_depths(chain(out_start, out_end)))
                yield _P(
                    desc='SKIP %s' % desc,
                    inputs=inputs,
                    outputs=outputs
                )


def _all_top_level_as_one_stream_params():
    inputs = [_NEXT]
    outputs = [_END]
    for data, events in _top_level_iter():
        inputs.extend([_D(data)] + [_NEXT] * len(events))
        outputs.extend(events + [_END])
    yield _P(
        desc='TOP LEVEL ALL',
        inputs=inputs,
        outputs=outputs,
    )

# TODO Add NOP pad fuzz.
# TODO Add data incomplete fuzz.


@parametrize(*chain(
    _BASIC_PARAMS,
    _prepend_ivm(_top_level_value_params()),
    _prepend_ivm(_annotate_params(_top_level_value_params())),
    _prepend_ivm(_containerize_params(_top_level_value_params())),
    _prepend_ivm(_containerize_params(_containerize_params(_top_level_value_params(), with_skip=False))),
    _prepend_ivm(_all_top_level_as_one_stream_params()),
))
def test_raw_reader(p):
    reader = raw_reader()
    for read_event, expected in zip(p.inputs, p.outputs):
        if is_exception(expected):
            with raises(expected):
                reader.send(read_event)
        else:
            actual = reader.send(read_event)
            assert expected == actual
