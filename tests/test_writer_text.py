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

import six

from base64 import b64encode
from datetime import datetime, timedelta
from decimal import Decimal
from io import BytesIO
from itertools import chain
from pytest import raises

from tests import is_exception, noop_manager, parametrize

from amazon.ion.core import IonType, IonEvent, IonEventType, OffsetTZInfo
from amazon.ion.symbols import SymbolToken
from amazon.ion.util import record
from amazon.ion.writer import blocking_writer, WriteEventType
from amazon.ion.writer_text import raw_writer


_D = Decimal
_DT = datetime

_E = IonEvent
_IT = IonType
_ET = IonEventType


class _P(record('desc', 'events', 'expected')):
    def __str__(self):
        return self.desc


def _scalar_p(ion_type, value, expected):
    return _P(
        desc='SCALAR %s - %s' % (ion_type.name, expected),
        events=(_E(_ET.SCALAR, ion_type, value),),
        expected=expected,
    )


def _convert_symbol_pairs(symbol_pairs):
    for value, literal in symbol_pairs:
        yield (value, literal.replace(b"'", b'"'))


def _convert_clob_pairs(clob_pairs):
    for value, literal in clob_pairs:
        yield (value, b'{{%s}}' % b64encode(value))


_SIMPLE_SYMBOLS=(
    (u'', br"''"),
    (u'\u0000', br"'\x00'"),
    (u'4hello', br"'4hello'"),
    (u'hello', br"'hello'"),
    (u'hello world', br"'hello world'"),
    (u'hello\u0009\x0a\x0dworld', br"'hello\t\n\rworld'"),
    (u'hello\aworld', br"'hello\x07world'"),
    (u'hello\u3000world', br"'hello\u3000world'"), # A full width space.
    (u'hello\U0001f4a9world', br"'hello\U0001f4a9world'"), # A 'pile of poo' emoji code point.
)
_SIMPLE_STRINGS=tuple(_convert_symbol_pairs(_SIMPLE_SYMBOLS))

_SIMPLE_CLOBS=(
    (b'', br'{{""}}'),
    (b'\x00', br'{{"\x00"}}'),
    (b'hello', br'{{"hello"}}'),
    (b'hello\x09\x0a\x0dworld', br'{{"hello\t\n\rworld"}}'),
    (b'hello\xFFworld', br'{{"hello\xffworld"}}'),
)
_SIMPLE_BLOBS=tuple(_convert_clob_pairs(_SIMPLE_CLOBS))

_SIMPLE_SCALARS_MAP = {
    _IT.NULL:(
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
        (-0.0, b'-0.0e0'),
        (0.0, b'0.0e0'),
        (1.0, b'1.0e0'),
        (-9007199254740991.0, b'-9007199254740991.0e0'),
        (2.0e-15, b'2e-15'),
        # Python is good at shortening the representation for these irrationals
        (1.1, b'1.1e0'),
        (1.1999999999999999555910790149937383830547332763671875e0, b'1.2e0'),
    ),
    _IT.DECIMAL: (
        (None, b'null.decimal'),
        (_D('-0.0'), b'-0.0'),
        (_D('0'), b'0d0'),
        (_D('0e100'), b'0d+100'),
        (_D('0e-15'), b'0d-15'),
        (_D('-1e1000'), b'-1d+1000'),
        (_D('-4.412111311414141e1000'), b'-4.412111311414141d+1000'),
        (_D('1.1999999999999999555910790149937383830547332763671875e0'),
            b'1.1999999999999999555910790149937383830547332763671875'),
    ),
    _IT.TIMESTAMP: (
        (None, b'null.timestamp'),
        (_DT(2016, 1, 1), b'2016-01-01T00:00:00-00:00'),
        (_DT(2016, 1, 1, 12), b'2016-01-01T12:00:00-00:00'),
        (_DT(2016, 1, 1, 12, 34, 12), b'2016-01-01T12:34:12-00:00'),
        (_DT(2016, 1, 1, 12, 34, 12, 555000), b'2016-01-01T12:34:12.555000-00:00'),
        (_DT(2016, 1, 1, 12, 34, 12, tzinfo=OffsetTZInfo()), b'2016-01-01T12:34:12+00:00'),
        (_DT(2016, 1, 1, 12, 34, 12, tzinfo=OffsetTZInfo(timedelta(hours=-7))),
            b'2016-01-01T12:34:12-07:00'),
    ),
    _IT.SYMBOL: (
        (None, b'null.symbol'),
        (SymbolToken(None, 4), b'$4'), # System symbol 'name'.
        (SymbolToken(u'a token', 400), b"'a token'"),
    ) + _SIMPLE_SYMBOLS,
    _IT.STRING: (
        (None, b'null.string'),
    ) + _SIMPLE_STRINGS,
    _IT.CLOB: (
        (None, b'null.clob'),
    ) + _SIMPLE_CLOBS,
    _IT.BLOB: (
        (None, b'null.blob'),
    ) + _SIMPLE_BLOBS,
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


def _generate_simple_scalars():
    for ion_type, values in six.iteritems(_SIMPLE_SCALARS_MAP):
        for native, expected in values:
            yield _scalar_p(ion_type, native, expected)

_EMPTY_CONTAINER_MAP = {
    _IT.LIST: b'[]',
    _IT.SEXP: b'()',
    _IT.STRUCT: b'{}',
}


def _generate_empty_containers():
    for ion_type, expected in six.iteritems(_EMPTY_CONTAINER_MAP):
        start_event = _E(_ET.CONTAINER_START, ion_type)
        end_event = _E(_ET.CONTAINER_END, ion_type)
        events = (start_event, end_event)
        yield _P(
            desc='EMPTY %s' % ion_type.name,
            events=events,
            expected=expected,
        )

_SIMPLE_ANNOTATIONS = (
    SymbolToken(None, 4), # System symbol 'name'.
    u'\x00',
    u'\uff4e', # An full-width latin 'n' code point.
    u'\U0001f4a9', # A 'pile of poo' emoji code point.
)
_SIMPLE_ANNOTATIONS_ENCODED = br"$4::'\x00'::'\uff4e'::'\U0001f4a9'::"


def _generate_annotated_values():
    for value_p in chain(_generate_simple_scalars(), _generate_empty_containers()):
        events = (value_p.events[0].derive_annotations(_SIMPLE_ANNOTATIONS),) + value_p.events[1:]
        yield _P(
            desc='ANN %s' % value_p.desc,
            events=events,
            expected=_SIMPLE_ANNOTATIONS_ENCODED + value_p.expected,
        )


_SIMPLE_FIELD_NAME = u'field'
_TOKEN_FIELD_NAME = SymbolToken(None, 4) # System symbol 'name'.


def _generate_containers(*generators, **opts):
    """Composes the empty tests with the simple scalars to make singletons."""
    repeat = opts.get('repeat', 1)
    targets = tuple(chain(*generators))
    for empty_p in _generate_empty_containers():
        start_event, end_event = empty_p.events
        delim = b','
        if start_event.ion_type is _IT.SEXP:
            delim = b' '
        for value_p in targets:
            field_names = (None,)
            if start_event.ion_type is _IT.STRUCT:
                field_names = (_SIMPLE_FIELD_NAME, _TOKEN_FIELD_NAME)
            for field_name in field_names:
                b_start = empty_p.expected[0:1]
                b_end = empty_p.expected[-1:]

                value_event = value_p.events[0]
                value_expected = value_p.expected
                if field_name is not None:
                    value_event = value_event.derive_field_name(field_name)
                    if isinstance(field_name, SymbolToken):
                        value_expected = b'$%d:%s' % (field_name.sid, value_expected)
                    else:
                        value_expected = b"'%s':%s" % (field_name.encode(), value_expected)
                # Make sure to compose the rest of the value (if not scalar, i.e. empty containers).
                value_events = (value_event,) + value_p.events[1:]
                events = (start_event,) + (value_events * repeat) + (end_event,)

                expected = b_start + delim.join([value_expected] * repeat) + b_end

                yield _P(
                    desc='SINGLETON %s %r' % (start_event.ion_type.name, expected),
                    events=events,
                    expected=expected
                )


_P_TOP_LEVEL = [
    _P(
        desc='TOP-LEVEL IVM',
        events=[
            _E(_ET.VERSION_MARKER),
        ],
        expected=b'$ion_1_0',
    ),
    _P(
        desc='TOP-LEVEL IVM x2',
        events=[
            _E(_ET.VERSION_MARKER),
            _E(_ET.VERSION_MARKER),
            _E(_ET.STREAM_END),
        ],
        expected=b'$ion_1_0 $ion_1_0',
    ),
    _P(
        desc='TOP-LEVEL STREAM END',
        events=[
            _E(_ET.STREAM_END),
        ],
        expected=b'',
    ),
    _P(
        desc='TOP-LEVEL INCOMPLETE',
        events=[
            _E(_ET.INCOMPLETE),
        ],
        expected=TypeError,
    ),
    _P(
        desc='TOP-LEVEL CONTAINER_END',
        events=[
            _E(_ET.CONTAINER_END, _IT.LIST),
        ],
        expected=TypeError,
    ),
]


@parametrize(
    *tuple(chain(
        _P_TOP_LEVEL,
        _generate_simple_scalars(),
        _generate_empty_containers(),
        _generate_annotated_values(),
        _generate_containers(
            _generate_simple_scalars(),
            _generate_empty_containers(),
            _generate_annotated_values(),
            # Add an extra level of depth.
            _generate_containers(
                _generate_empty_containers(),
            )
        ),
        _generate_containers(
            _generate_annotated_values(),
            repeat=4
        ),
    ))
)
def test_raw_writer(p):
    writer = raw_writer()
    buf = BytesIO()
    buf_writer = blocking_writer(writer, buf)

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
