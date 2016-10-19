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

"""Provides a ``simplejson``-like API for dumping and loading Ion data."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
from decimal import Decimal
from itertools import chain

import six

from .core import IonEvent, IonEventType, IonType, ION_STREAM_END_EVENT, Timestamp
from .exceptions import IonException
from .reader import blocking_reader, NEXT_EVENT
from .reader_binary import raw_reader
from .reader_managed import managed_reader
from .simple_types import IonPyList, IonPyDict, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, IonPyDecimal, \
    IonPyTimestamp, IonPyText, IonPyBytes, IonPySymbol, is_null
from .symbols import SymbolToken
from .writer import blocking_writer
from .writer_binary import binary_writer


_ION_CONTAINER_END_EVENT = IonEvent(IonEventType.CONTAINER_END)


def dump(obj, fp, imports=None, sequence_as_stream=False, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, cls=None, indent=None,
         separators=None, encoding='utf-8', default=None, use_decimal=True, namedtuple_as_object=True,
         tuple_as_array=True, bigint_as_string=False, sort_keys=False, item_sort_key=None, for_json=None,
         ignore_nan=False, int_as_string_bitcount=None, iterable_as_array=False, **kw):
    """Serialize ``obj`` as an Ion-formatted stream to ``fp`` (a file-like object), using the following conversion
    table::
        +-------------------+-------------------+
        |  Python           |       Ion         |
        |-------------------+-------------------|
        | None              |    null.null      |
        |-------------------+-------------------|
        | IonPyNull(<type>) |    null.<type>    |
        |-------------------+-------------------|
        | True, False,      |                   |
        | IonPyInt(BOOL),   |     bool          |
        | IonPyBool,        |                   |
        |-------------------+-------------------|
        | int (Python 2, 3) |                   |
        | long (Python 2),  |      int          |
        | IonPyInt(INT)     |                   |
        |-------------------+-------------------|
        | float, IonPyFloat |     float         |
        |-------------------+-------------------|
        | Decimal,          |                   |
        | IonPyDecimal      |     decimal       |
        |-------------------+-------------------|
        | datetime,         |                   |
        | Timestamp,        |    timestamp      |
        | IonPyTimestamp    |                   |
        |-------------------+-------------------|
        | SymbolToken,      |                   |
        | IonPySymbol,      |     symbol        |
        | IonPyText(SYMBOL) |                   |
        |-------------------+-------------------|
        | str (Python 3),   |                   |
        | unicode (Python2),|     string        |
        | IonPyText(STRING) |                   |
        |-------------------+-------------------|
        | IonPyBytes(CLOB)  |     clob          |
        |-------------------+-------------------|
        | str (Python 2),   |                   |
        | bytes (Python 3)  |     blob          |
        | IonPyBytes(BLOB)  |                   |
        |-------------------+-------------------|
        | list, tuple,      |                   |
        | IonPyList(LIST)   |     list          |
        |-------------------+-------------------|
        | IonPyList(SEXP)   |     sexp          |
        |-------------------+-------------------|
        | dict, namedtuple, |                   |
        | IonPyDict         |     struct        |
        +-------------------+-------------------+

    Args:
        obj (Any): A python object to serialize according to the above table. Any Python object which is neither an
            instance of or inherits from one of the types in the above table will raise TypeError.
        fp (BaseIO): A file-like object.
        imports (Optional[Sequence[SymbolTable]]): A sequence of shared symbol tables to be used by by the writer.
        sequence_as_stream (Optional[True|False]): When True, if ``obj`` is a sequence, it will be treated as a stream
            of top-level Ion values (i.e. the resulting Ion data will begin with ``obj``'s first element).
            Default: False.
        skipkeys: NOT IMPLEMENTED
        ensure_ascii: NOT IMPLEMENTED
        check_circular: NOT IMPLEMENTED
        allow_nan: NOT IMPLEMENTED
        cls: NOT IMPLEMENTED
        indent: NOT IMPLEMENTED
        separators: NOT IMPLEMENTED
        encoding: NOT IMPLEMENTED
        default: NOT IMPLEMENTED
        use_decimal: NOT IMPLEMENTED
        namedtuple_as_object: NOT IMPLEMENTED
        tuple_as_array: NOT IMPLEMENTED
        bigint_as_string: NOT IMPLEMENTED
        sort_keys: NOT IMPLEMENTED
        item_sort_key: NOT IMPLEMENTED
        for_json: NOT IMPLEMENTED
        ignore_nan: NOT IMPLEMENTED
        int_as_string_bitcount: NOT IMPLEMENTED
        iterable_as_array: NOT IMPLEMENTED
        **kw: NOT IMPLEMENTED

    """
    writer = blocking_writer(binary_writer(imports), fp)
    if sequence_as_stream and isinstance(obj, (list, tuple)):
        # Treat this top-level sequence as a stream; serialize its elements as top-level values, but don't serialize the
        # sequence itself.
        for top_level in obj:
            _dump(top_level, writer)
    else:
        _dump(obj, writer)
    writer.send(ION_STREAM_END_EVENT)

_FROM_TYPE = dict(chain(
    six.iteritems({
        type(None): IonType.NULL,
        type(True): IonType.BOOL,
        type(False): IonType.BOOL,
        float: IonType.FLOAT,
        six.text_type: IonType.STRING,
        Decimal: IonType.DECIMAL,
        datetime: IonType.TIMESTAMP,
        Timestamp: IonType.TIMESTAMP,
        six.binary_type: IonType.BLOB,
        SymbolToken: IonType.SYMBOL,
        list: IonType.LIST,
        dict: IonType.STRUCT
    }),
    six.iteritems(
        dict(zip(six.integer_types, [IonType.INT] * len(six.integer_types)))
    ),
))


def _ion_type(obj):
    types = [type(obj)]
    while types:
        current_type = types.pop()
        if current_type in _FROM_TYPE:
            return _FROM_TYPE[current_type]
        types.extend(current_type.__bases__)

    raise TypeError('Unknown scalar type %r' % (type(obj),))

def _dump(obj, writer, field=None):
    null = is_null(obj)
    try:
        ion_type = obj.ion_type
        ion_nature = True
    except AttributeError:
        ion_type = _ion_type(obj)
        ion_nature = False
    if not null and ion_type.is_container:
        if ion_nature:
            event = obj.to_event(IonEventType.CONTAINER_START, field_name=field)
        else:
            event = IonEvent(IonEventType.CONTAINER_START, ion_type, field_name=field)
        writer.send(event)
        if ion_type is IonType.STRUCT:
            for field, val in six.iteritems(obj):
                _dump(val, writer, field)
        else:
            for elem in obj:
                _dump(elem, writer)
        event = _ION_CONTAINER_END_EVENT
    else:
        # obj is a scalar value
        if ion_nature:
            event = obj.to_event(IonEventType.SCALAR, field_name=field)
        else:
            event = IonEvent(IonEventType.SCALAR, ion_type, obj, field_name=field)
    writer.send(event)


def dumps(obj, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, cls=None, indent=None,
          separators=None, encoding='utf-8', default=None, use_decimal=True, namedtuple_as_object=True,
          tuple_as_array=True, bigint_as_string=False, sort_keys=False, item_sort_key=None, for_json=None,
          ignore_nan=False, int_as_string_bitcount=None, iterable_as_array=False, **kw):
    """Not yet implemented"""
    raise IonException("Not yet implemented")


def load(fp, catalog=None, single_value=True, encoding='utf-8', cls=None, object_hook=None, parse_float=None,
         parse_int=None, parse_constant=None, object_pairs_hook=None, use_decimal=None, **kw):
    """Deserialize ``fp`` (a file-like object), which contains an Ion stream, to a Python object using the following
    conversion table::
        +-------------------+-------------------+
        |  Ion              |     Python        |
        |-------------------+-------------------|
        | null.<type>       | IonPyNull(<type>) |
        |-------------------+-------------------|
        | bool              |    IonPyBool      |
        |-------------------+-------------------|
        | int               |    IonPyInt       |
        |-------------------+-------------------|
        | float             |    IonPyFloat     |
        |-------------------+-------------------|
        | decimal           |   IonPyDecimal    |
        |-------------------+-------------------|
        | timestamp         |  IonPyTimestamp   |
        |-------------------+-------------------|
        | symbol            |   IonPySymbol     |
        |-------------------+-------------------|
        | string            | IonPyText(STRING) |
        |-------------------+-------------------|
        | clob              |  IonPyBytes(CLOB) |
        |-------------------+-------------------|
        | blob              |  IonPyBytes(BLOB) |
        |-------------------+-------------------|
        | list              |   IonPyList(LIST) |
        |-------------------+-------------------|
        | sexp              |   IonPyList(SEXP) |
        |-------------------+-------------------|
        | struct            |     IonPyDict     |
        +-------------------+-------------------+

    Args:
        fp (BaseIO): A file-like object containing Ion data.
        catalog (Optional[SymbolTableCatalog]): The catalog to use for resolving symbol table imports.
        single_value (Optional[True|False]): When True, the data in ``obj`` is interpreted as a single Ion value, and
            will be returned without an enclosing container. If True and there are multiple top-level values in the Ion
            stream, IonException will be raised. NOTE: this means that when data is dumped using
            ``sequence_as_stream=True``, it must be loaded using ``single_value=False``. Default: True.
        encoding: NOT IMPLEMENTED
        cls: NOT IMPLEMENTED
        object_hook: NOT IMPLEMENTED
        parse_float: NOT IMPLEMENTED
        parse_int: NOT IMPLEMENTED
        parse_constant: NOT IMPLEMENTED
        object_pairs_hook: NOT IMPLEMENTED
        use_decimal: NOT IMPLEMENTED
        **kw: NOT IMPLEMENTED

    Returns (Any):
        if single_value is True:
            A Python object representing a single Ion value.
        else:
            A sequence of Python objects representing a stream of Ion values.
    """
    reader = blocking_reader(managed_reader(raw_reader(), catalog), fp)
    out = []  # top-level
    _load(out, reader)
    if single_value:
        if len(out) != 1:
            raise IonException('Stream contained more than a single value')
        return out[0]
    return out


_FROM_ION_TYPE = [
    IonPyNull,
    IonPyBool,
    IonPyInt,
    IonPyFloat,
    IonPyDecimal,
    IonPyTimestamp,
    IonPySymbol,
    IonPyText,
    IonPyBytes,
    IonPyBytes,
    IonPyList,
    IonPyList,
    IonPyDict
]


def _load(out, reader, end_type=IonEventType.STREAM_END, in_struct=False):

    def add(obj):
        if in_struct:
            # TODO what about duplicate field names?
            out[event.field_name.text] = obj
        else:
            out.append(obj)

    event = reader.send(NEXT_EVENT)
    while event.event_type is not end_type:
        ion_type = event.ion_type
        if event.event_type is IonEventType.CONTAINER_START:
            container = _FROM_ION_TYPE[ion_type].from_event(event)
            _load(container, reader, IonEventType.CONTAINER_END, ion_type is IonType.STRUCT)
            add(container)
        elif event.event_type is IonEventType.SCALAR:
            if event.value is None or ion_type is IonType.NULL or event.ion_type.is_container:
                scalar = IonPyNull.from_event(event)
            else:
                scalar = _FROM_ION_TYPE[ion_type].from_event(event)
            add(scalar)
        event = reader.send(NEXT_EVENT)


def loads(fp, encoding='utf-8', cls=None, object_hook=None, parse_float=None, parse_int=None, parse_constant=None,
          object_pairs_hook=None, use_decimal=None, **kw):
    """Not yet implemented"""
    raise IonException("Not yet implemented")
