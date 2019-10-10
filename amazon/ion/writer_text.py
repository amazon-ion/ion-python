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

"""Implementations of Ion Text writers."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import math
import re
from functools import partial

import six

from datetime import datetime
from decimal import Decimal
from io import BytesIO

from amazon.ion.symbols import SymbolToken
from . import symbols

from .util import coroutine, unicode_iter
from .core import DataEvent, Transition, IonEventType, IonType, TIMESTAMP_PRECISION_FIELD, TimestampPrecision, \
    _ZERO_DELTA, TIMESTAMP_FRACTION_PRECISION_FIELD, MICROSECOND_PRECISION, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, \
    Timestamp, DECIMAL_ZERO
from .writer import partial_transition, writer_trampoline, serialize_scalar, validate_scalar_value, \
    illegal_state_null, NOOP_WRITER_EVENT
from .writer import WriteEventType

_IVM_WRITER_EVENT = DataEvent(WriteEventType.COMPLETE, symbols.TEXT_ION_1_0.encode())

_NULL_TYPE_NAMES = [
    b'null',
    b'null.bool',
    b'null.int',
    b'null.float',
    b'null.decimal',
    b'null.timestamp',
    b'null.symbol',
    b'null.string',
    b'null.clob',
    b'null.blob',
    b'null.list',
    b'null.sexp',
    b'null.struct',
]


def _serialize_bool(ion_event):
    if ion_event.value:
        return b'true'
    else:
        return b'false'


def _serialize_scalar_from_string_representation_factory(type_name, types, str_func=str):
    """Builds functions that leverage Python ``str()`` or similar functionality.

    Args:
        type_name (str): The name of the Ion type.
        types (Union[Sequence[type],type]): The Python types to validate for.
        str_func (Optional[Callable]): The function to convert the value with, defaults to ``str``.

    Returns:
        function: The function for serializing scalars of a given type to Ion text bytes.
    """
    def serialize(ion_event):
        value = ion_event.value
        validate_scalar_value(value, types)
        return six.b(str_func(value))
    serialize.__name__ = '_serialize_' + type_name
    return serialize


_serialize_int = _serialize_scalar_from_string_representation_factory(
    'int', six.integer_types
)


_EXPONENT_PAT = re.compile(r'[eE]')


# TODO Make this cleaner.
def _float_str(val):
    if math.isnan(val):
        return 'nan'
    if math.isinf(val):
        if val > 0:
            return '+inf'
        else:
            return '-inf'
    text = repr(val)
    if _EXPONENT_PAT.search(text) is None:
        text += 'e0'
    return text

_serialize_float = _serialize_scalar_from_string_representation_factory(
    'float', float,
    str_func=_float_str
)


# TODO Make this cleaner.
def _decimal_str(val):
    text = str(val)
    new_text = _EXPONENT_PAT.sub('d', text)
    if text == new_text and text.find('.') == -1:
        new_text += 'd0'
    return new_text


_serialize_decimal = _serialize_scalar_from_string_representation_factory(
    'decimal', Decimal,
    str_func=_decimal_str
)


def _bytes_utc_offset(dt):
    offset = dt.utcoffset()
    if offset is None:
        return '-00:00'
    elif offset == _ZERO_DELTA:
        return 'Z'
    offset_str = dt.strftime('%z')
    offset_str = offset_str[:3] + ':' + offset_str[3:]
    return offset_str


def _bytes_datetime(dt):
    original_dt = dt
    precision = getattr(original_dt, TIMESTAMP_PRECISION_FIELD, TimestampPrecision.SECOND)
    if dt.year < 1900:
        # In some Python interpreter versions, strftime inexplicably does not support pre-1900 years.
        # This unfortunate ugliness compensates for that.
        year = str(dt.year)
        year = ('0' * (4 - len(year))) + year
        dt = dt.replace(year=2008)  # Note: this fake year must be a leap year.
    else:
        year = dt.strftime('%Y')
    tz_string = year

    if precision.includes_month:
        tz_string += dt.strftime('-%m')
    else:
        return tz_string + 'T'

    if precision.includes_day:
        tz_string += dt.strftime('-%dT')
    else:
        return tz_string + 'T'

    if precision.includes_minute:
        tz_string += dt.strftime('%H:%M')
    else:
        return tz_string

    if precision.includes_second:
        tz_string += dt.strftime(':%S')
    else:
        return tz_string + _bytes_utc_offset(dt)

    if isinstance(original_dt, Timestamp):
        fractional_seconds = getattr(original_dt, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, None)
        if fractional_seconds is not None:
            _, digits, exponent = fractional_seconds.as_tuple()
            if not (fractional_seconds == DECIMAL_ZERO and exponent >= 0):
                leading_zeroes = -exponent - len(digits)
                tz_string += '.'
                if leading_zeroes > 0:
                    tz_string += '0' * leading_zeroes
                tz_string += ''.join(str(x) for x in digits)
    else:
        # This must be a normal datetime, which always has a range-validated microsecond value.
        tz_string += '.' + dt.strftime('%f')
    return tz_string + _bytes_utc_offset(dt)


_serialize_timestamp = _serialize_scalar_from_string_representation_factory(
    'timestamp',
    datetime,
    str_func=_bytes_datetime
)


_PRINTABLE_ASCII_START = 0x20
_PRINTABLE_ASCII_END = 0x7E


def _is_printable_ascii(code_point):
    return code_point >= _PRINTABLE_ASCII_START and code_point <= _PRINTABLE_ASCII_END


_SERIALIZE_COMMON_ESCAPE_MAP = {
    six.byte2int(b'\n'): br'\n',
    six.byte2int(b'\r'): br'\r',
    six.byte2int(b'\t'): br'\t',
}
_2B_ESCAPE_MAX = 0xFF
_4B_ESCAPE_MAX = 0xFFFF


def _escape(code_point):
    escape = _SERIALIZE_COMMON_ESCAPE_MAP.get(code_point, None)
    if escape is not None:
        return escape
    if code_point <= _2B_ESCAPE_MAX:
        return (u'\\x%02x' % code_point).encode()
    if code_point <= _4B_ESCAPE_MAX:
        return (u'\\u%04x' % code_point).encode()
    return (u'\\U%08x' % code_point).encode()


def _bytes_text(code_point_iter, quote, prefix=b'', suffix=b''):
    quote_code_point = None if len(quote) == 0 else six.byte2int(quote)
    buf = BytesIO()
    buf.write(prefix)
    buf.write(quote)
    for code_point in code_point_iter:
        if code_point == quote_code_point:
            buf.write(b'\\' + quote)
        elif code_point == six.byte2int(b'\\'):
            buf.write(b'\\\\')
        elif _is_printable_ascii(code_point):
            buf.write(six.int2byte(code_point))
        else:
            buf.write(_escape(code_point))
    buf.write(quote)
    buf.write(suffix)
    return buf.getvalue()


_SINGLE_QUOTE = b"'"
_DOUBLE_QUOTE = b'"'
# all typed nulls (such as null.int) and the +inf, and -inf keywords are covered by this regex
_UNQUOTED_SYMBOL_REGEX = re.compile(r'\A[a-zA-Z$_][a-zA-Z0-9$_]*\Z')
_ADDITIONAL_SYMBOLS_REQUIRING_QUOTES = set(['nan', 'null', 'false', 'true'])

def _symbol_needs_quotes(text):
    return text in _ADDITIONAL_SYMBOLS_REQUIRING_QUOTES or _UNQUOTED_SYMBOL_REGEX.search(text) is None

def _serialize_symbol_value(value, suffix=b''):
    # TODO Support not quoting operators in s-expressions: http://amzn.github.io/ion-docs/docs/symbols.html
    try:
        text = value.text
        if text is None:
            return (u'$%d' % value.sid).encode() + suffix
    except AttributeError:
        text = value
    validate_scalar_value(text, (six.text_type, type(SymbolToken)))
    quote = _SINGLE_QUOTE if _symbol_needs_quotes(text) else b''
    return _bytes_text(unicode_iter(text), quote, suffix=suffix)


def _serialize_symbol(ion_event):
    return _serialize_symbol_value(ion_event.value)


def _serialize_string(ion_event):
    # TODO Support multi-line strings.
    value = ion_event.value
    validate_scalar_value(value, six.text_type)
    return _bytes_text(unicode_iter(value), _DOUBLE_QUOTE)


_LOB_START = b'{{'
_LOB_END = b'}}'


def _serialize_clob(ion_event):
    value = ion_event.value
    return _bytes_text(six.iterbytes(value), _DOUBLE_QUOTE, prefix=_LOB_START, suffix=_LOB_END)


def _serialize_blob(ion_event):
    value = ion_event.value
    return _LOB_START + base64.b64encode(value) + _LOB_END


_SERIALIZE_SCALAR_JUMP_TABLE = {
    IonType.NULL: illegal_state_null,
    IonType.BOOL: _serialize_bool,
    IonType.INT: _serialize_int,
    IonType.FLOAT: _serialize_float,
    IonType.DECIMAL: _serialize_decimal,
    IonType.TIMESTAMP: _serialize_timestamp,
    IonType.SYMBOL: _serialize_symbol,
    IonType.STRING: _serialize_string,
    IonType.CLOB: _serialize_clob,
    IonType.BLOB: _serialize_blob,
}


_serialize_scalar = partial(serialize_scalar, jump_table=_SERIALIZE_SCALAR_JUMP_TABLE, null_table=_NULL_TYPE_NAMES)


_FIELD_NAME_DELIMITER = b':'
_ANNOTATION_DELIMITER = b'::'


def _serialize_field_name(ion_event):
    return _serialize_symbol_value(ion_event.field_name, suffix=_FIELD_NAME_DELIMITER)


def _serialize_annotation_value(annotation):
    return _serialize_symbol_value(annotation, suffix=_ANNOTATION_DELIMITER)


def _serialize_container_factory(suffix, container_map):
    """Returns a function that serializes container start/end.

    Args:
        suffix (str): The suffix to name the function with.
        container_map (Dictionary[core.IonType, bytes]): The

    Returns:
        function: The closure for serialization.
    """
    def serialize(ion_event):
        if not ion_event.ion_type.is_container:
            raise TypeError('Expected container type')
        return container_map[ion_event.ion_type]
    serialize.__name__ = '_serialize_container_' + suffix
    return serialize

_CONTAINER_START_MAP = {
    IonType.STRUCT: b'{',
    IonType.LIST: b'[',
    IonType.SEXP: b'(',
}
_CONTAINER_END_MAP = {
    IonType.STRUCT: b'}',
    IonType.LIST: b']',
    IonType.SEXP: b')',
}
_CONTAINER_DELIMITER_MAP_NORMAL = {
    IonType.STRUCT: b',',
    IonType.LIST: b',',
    IonType.SEXP: b' ',
}
_CONTAINER_DELIMITER_MAP_PRETTY = {
    IonType.STRUCT: b',',
    IonType.LIST: b',',
    IonType.SEXP: b'', # we use newlines when pretty printing
}

_serialize_container_start = _serialize_container_factory('start', _CONTAINER_START_MAP)
_serialize_container_end = _serialize_container_factory('end', _CONTAINER_END_MAP)
_serialize_container_delimiter_normal = _serialize_container_factory('delimiter', _CONTAINER_DELIMITER_MAP_NORMAL)
_serialize_container_delimiter_pretty = _serialize_container_factory('delimiter', _CONTAINER_DELIMITER_MAP_PRETTY)


@coroutine
def _raw_writer_coroutine(depth=0, container_event=None, whence=None, indent=None):
    pretty = indent is not None
    serialize_container_delimiter = \
            _serialize_container_delimiter_pretty if pretty else _serialize_container_delimiter_normal
    has_written_values = False
    transition = None
    while True:
        ion_event, self = (yield transition)
        delegate = self

        if has_written_values and not ion_event.event_type.ends_container:
            # TODO This will always emit a delimiter for containers--should make it not do that.
            # Write the delimiter for the next value.
            if depth == 0:
                # if we are pretty printing, we'll insert a newline between top-level containers
                delimiter = b'' if pretty else b' '
            else:
                delimiter = serialize_container_delimiter(container_event)
            if len(delimiter) > 0:
                yield partial_transition(delimiter, self)

        if pretty and (has_written_values or container_event is not None) and not ion_event.event_type is IonEventType.STREAM_END:
            yield partial_transition(b'\n', self)
            indent_depth = depth - (1 if ion_event.event_type is IonEventType.CONTAINER_END else 0)
            if indent_depth > 0:
                yield partial_transition(indent * indent_depth, self)

        if depth > 0 \
                and container_event.ion_type is IonType.STRUCT \
                and ion_event.event_type.begins_value:
            # Write the field name.
            yield partial_transition(_serialize_field_name(ion_event), self)
            if pretty:
                # separate the field name and the field value
                yield partial_transition(b' ', self)

        if ion_event.event_type.begins_value:
            # Write the annotations.
            for annotation in ion_event.annotations:
                yield partial_transition(_serialize_annotation_value(annotation), self)

        if ion_event.event_type is IonEventType.CONTAINER_START:
            writer_event = DataEvent(WriteEventType.NEEDS_INPUT, _serialize_container_start(ion_event))
            delegate = _raw_writer_coroutine(depth + 1, ion_event, self, indent=indent)
        elif depth == 0:
            # Serialize at the top-level.
            if ion_event.event_type is IonEventType.STREAM_END:
                writer_event = NOOP_WRITER_EVENT
            elif ion_event.event_type is IonEventType.VERSION_MARKER:
                writer_event = _IVM_WRITER_EVENT
            elif ion_event.event_type is IonEventType.SCALAR:
                writer_event = DataEvent(WriteEventType.COMPLETE, _serialize_scalar(ion_event))
            else:
                raise TypeError('Invalid event: %s' % ion_event)
        else:
            # Serialize within a container.
            if ion_event.event_type is IonEventType.SCALAR:
                writer_event = DataEvent(WriteEventType.NEEDS_INPUT, _serialize_scalar(ion_event))
            elif ion_event.event_type is IonEventType.CONTAINER_END:
                write_type = WriteEventType.COMPLETE if depth == 1 else WriteEventType.NEEDS_INPUT
                writer_event = DataEvent(write_type, _serialize_container_end(container_event))
                delegate = whence
            else:
                raise TypeError('Invalid event: %s' % ion_event)

        has_written_values = True
        transition = Transition(writer_event, delegate)


# TODO Add options for text formatting.
def raw_writer(indent=None):
    """Returns a raw text writer co-routine.

    Yields:
        DataEvent: serialization events to write out

        Receives :class:`amazon.ion.core.IonEvent` or ``None`` when the co-routine yields
        ``HAS_PENDING`` :class:`WriteEventType` events.
    """

    is_whitespace_str = isinstance(indent, str) and re.search(r'\A\s*\Z', indent, re.M) is not None
    if not (indent is None or is_whitespace_str):
        raise ValueError('The indent parameter must either be None or a string containing only whitespace')

    indent_bytes = six.b(indent) if isinstance(indent, str) else indent

    return writer_trampoline(_raw_writer_coroutine(indent=indent_bytes))

# TODO Determine if we need to do anything special for non-raw writer.  Validation?
text_writer = raw_writer
