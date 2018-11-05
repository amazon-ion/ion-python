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

from functools import partial

import six
import sys

from base64 import b64encode
from datetime import timedelta, datetime
from io import BytesIO
from itertools import chain

from decimal import Decimal

from amazon.ion.core import OffsetTZInfo, IonEvent, IonType, IonEventType
from amazon.ion.core import record
from amazon.ion.exceptions import IonException

from tests import parametrize

from amazon.ion.simpleion import loads, dumps
from amazon.ion.symbols import SymbolToken
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_text import raw_writer
from tests.writer_util import assert_writer_events, WriterParameter, generate_scalars, generate_containers, \
    SIMPLE_SCALARS_MAP_TEXT

_D = Decimal
_DT = datetime

_E = IonEvent
_IT = IonType
_ET = IonEventType
_P = WriterParameter

_EMPTY_CONTAINER_MAP = {
    _IT.LIST: (
        (
            (),
            b'[]',
        ),
    ),
    _IT.SEXP: (
        (
            (),
            b'()',
        ),
    ),
    _IT.STRUCT: (
        (
            (),
            b'{}',
        ),
    ),
}

_generate_simple_scalars = partial(generate_scalars, SIMPLE_SCALARS_MAP_TEXT)
_generate_empty_containers = partial(generate_containers, _EMPTY_CONTAINER_MAP)

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


def _generate_simple_containers(*generators, **opts):
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
                        value_expected = \
                            (u'$%d:'% field_name.sid).encode() + value_expected
                    else:
                        value_expected = \
                            field_name.encode() + b":" + value_expected
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
    _P(
        desc="NULL WITH VALUE",
        events=[
            _E(_ET.SCALAR, _IT.NULL, u'foo')
        ],
        expected=TypeError
    ),
]


def new_writer():
    buf = BytesIO()
    return buf, blocking_writer(raw_writer(), buf)


@parametrize(
    *tuple(chain(
        _P_TOP_LEVEL,
        _generate_simple_scalars(),
        _generate_empty_containers(),
        _generate_annotated_values(),
        _generate_simple_containers(
            _generate_simple_scalars(),
            _generate_empty_containers(),
            _generate_annotated_values(),
            # Add an extra level of depth.
            _generate_simple_containers(
                _generate_empty_containers(),
            )
        ),
        _generate_simple_containers(
            _generate_annotated_values(),
            repeat=4
        ),
    ))
)
def test_raw_writer(p):
    assert_writer_events(p, new_writer)

@parametrize(
        (None, True),
        ('', True),
        (' ', True),
        (' ', True),
        ('\t', True),
        ('\n', True),
        ('       ', True),
        ('  \t     \n', True),
        ('  \t  x  \n', False),
        ('\"', False),
        ('apple', False),
        ('1', False))
def test_valid_indent_strs(p):
    indent, is_valid = p
    try:
        raw_writer(indent)
        assert is_valid
    except ValueError as e:
        assert not is_valid
        assert 'only whitespace' in str(e)
    if is_valid:
        # we only allow indent strings to be used to be used if they produce valid ion.
        ion_val = loads('[a, {x:2, y: height::17}]')
        assert ion_val == loads(dumps(ion_val, binary=False, indent=indent))

class _P_Q(record('symbol', 'needs_quotes', ('backslash_required', False))):
    pass

@parametrize(
        _P_Q('a', False),
        _P_Q('a0', False),
        _P_Q('_a_0_', False),
        _P_Q('int', False),
        _P_Q('int32', False),
        _P_Q('FALSE', False),
        _P_Q('False', False),
        _P_Q('bool', False),
        _P_Q('NaN', False),
        _P_Q('T', False),
        _P_Q('type', False),
        _P_Q('inf', False),
        # begin needs quotes
        _P_Q('a.0', True),
        _P_Q('1', True),
        _P_Q('{', True),
        _P_Q('\'', True, backslash_required=True),
        _P_Q('"', True),
        _P_Q('\\', True, backslash_required=True),
        # begin reserved words
        _P_Q('nan', True),
        _P_Q('null', True),
        _P_Q('null.int', True),
        _P_Q('+inf', True),
        _P_Q('-inf', True),
        _P_Q('false', True),
        _P_Q('true', True),
        )
def test_quote_symbols(p):
    symbol_text, needs_quotes, backslash_required = p

    try:
        loads(symbol_text + '::4')
        threw_without_quotes = False
    except IonException as e:
        threw_without_quotes = True

    quoted_symbol_text = "'" + ('\\' if backslash_required else '') + symbol_text + "'"
    ion_value = loads(quoted_symbol_text + "::4")
    quoted = "'" in dumps(ion_value, binary=False)

    assert needs_quotes == threw_without_quotes
    assert needs_quotes == quoted
    assert len(ion_value.ion_annotations) == 1
    assert ion_value.ion_annotations[0].text == symbol_text
