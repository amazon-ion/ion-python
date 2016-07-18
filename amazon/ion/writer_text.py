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
from .core import DataEvent, Transition, IonEventType, IonType
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


# TODO Support precision better.
def _bytes_datetime(dt):
    tz_string = dt.isoformat()
    if dt.tzinfo is None:
        # Add unknown offset.
        tz_string += '-00:00'
    return tz_string


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
    quote_code_point = six.byte2int(quote)
    buf = BytesIO()
    buf.write(prefix)
    buf.write(quote)
    for code_point in code_point_iter:
        if code_point == quote_code_point:
            buf.write(b'\\' + quote)
        if _is_printable_ascii(code_point):
            buf.write(six.int2byte(code_point))
        else:
            buf.write(_escape(code_point))
    buf.write(quote)
    buf.write(suffix)
    return buf.getvalue()


_SINGLE_QUOTE = b"'"
_DOUBLE_QUOTE = b'"'


def _serialize_symbol_value(value, suffix=b''):
    # TODO Be more aggressive about not quoting.
    # TODO Support not quoting operators in s-expressions.
    try:
        text = value.text
        if text is None:
            return (u'$%d' % value.sid).encode() + suffix
    except AttributeError:
        text = value
    validate_scalar_value(text, (six.text_type, type(SymbolToken)))
    return _bytes_text(unicode_iter(text), _SINGLE_QUOTE, suffix=suffix)


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


_TOP_LEVEL_DELIMITER = b' '
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
_CONTAINER_DELIMITER_MAP = {
    IonType.STRUCT: b',',
    IonType.LIST: b',',
    IonType.SEXP: b' ',
}

_serialize_container_start = _serialize_container_factory('start', _CONTAINER_START_MAP)
_serialize_container_end = _serialize_container_factory('end', _CONTAINER_END_MAP)
_serialize_container_delimiter = _serialize_container_factory('delimiter', _CONTAINER_DELIMITER_MAP)


@coroutine
def _raw_writer_coroutine(depth=0, container_event=None, whence=None):
    has_written_values = False
    transition = None
    while True:
        ion_event, self = (yield transition)
        delegate = self

        if has_written_values and not ion_event.event_type.ends_container:
            # TODO This will always emit a delimiter for containers--should make it not do that.
            # Write the delimiter for the next value.
            if depth == 0:
                yield partial_transition(_TOP_LEVEL_DELIMITER, self)
            else:
                yield partial_transition(_serialize_container_delimiter(container_event), self)

        if depth > 0 \
                and container_event.ion_type is IonType.STRUCT \
                and ion_event.event_type.begins_value:
            # Write the field name.
            yield partial_transition(_serialize_field_name(ion_event), self)

        if ion_event.event_type.begins_value:
            # Write the annotations.
            for annotation in ion_event.annotations:
                yield partial_transition(_serialize_annotation_value(annotation), self)

        if depth == 0:
            # Serialize at the top-level.
            if ion_event.event_type is IonEventType.STREAM_END:
                writer_event = NOOP_WRITER_EVENT
            elif ion_event.event_type is IonEventType.VERSION_MARKER:
                writer_event = _IVM_WRITER_EVENT
            elif ion_event.event_type is IonEventType.SCALAR:
                writer_event = DataEvent(WriteEventType.COMPLETE, _serialize_scalar(ion_event))
            elif ion_event.event_type is IonEventType.CONTAINER_START:
                writer_event = DataEvent(WriteEventType.NEEDS_INPUT, _serialize_container_start(ion_event))
                delegate = _raw_writer_coroutine(1, ion_event, self)
            else:
                raise TypeError('Invalid event: %s' % ion_event)
        else:
            # Serialize within a container.
            if ion_event.event_type is IonEventType.SCALAR:
                writer_event = DataEvent(WriteEventType.NEEDS_INPUT, _serialize_scalar(ion_event))
            elif ion_event.event_type is IonEventType.CONTAINER_START:
                writer_event = DataEvent(WriteEventType.NEEDS_INPUT, _serialize_container_start(ion_event))
                delegate = _raw_writer_coroutine(depth + 1, ion_event, self)
            elif ion_event.event_type is IonEventType.CONTAINER_END:
                if depth == 1:
                    write_type = WriteEventType.COMPLETE
                else:
                    write_type = WriteEventType.NEEDS_INPUT
                writer_event = DataEvent(write_type, _serialize_container_end(container_event))
                delegate = whence
            else:
                raise TypeError('Invalid event: %s' % ion_event)

        has_written_values = True
        transition = Transition(writer_event, delegate)


# TODO Add options for text formatting.
def raw_writer():
    """Returns a raw text writer co-routine.

    Yields:
        DataEvent: serialization events to write out

        Receives :class:`amazon.ion.core.IonEvent` or ``None`` when the co-routine yields
        ``HAS_PENDING`` :class:`WriteEventType` events.
    """
    return writer_trampoline(_raw_writer_coroutine())


# TODO Determine if we need to do anything special for non-raw writer.  Validation?
writer = raw_writer
