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

import six

from .core import IonEvent, IonEventType, IonType, ION_STREAM_END_EVENT
from .exceptions import IonException
from .reader import blocking_reader, NEXT_EVENT
from .reader_binary import raw_reader
from .reader_managed import managed_reader
from .simple_types import IonPyList, IonPyDict, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, IonPyDecimal, \
    IonPyTimestamp, IonPyText, IonPyBytes, _IonNature, IonPySymbol
from .symbols import SymbolToken
from .writer import blocking_writer
from .writer_binary import binary_writer


_ION_CONTAINER_END_EVENT = IonEvent(IonEventType.CONTAINER_END)


def dump(obj, fp, imports=None, sequence_as_stream=False, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, cls=None, indent=None,
         separators=None, encoding='utf-8', default=None, use_decimal=True, namedtuple_as_object=True,
         tuple_as_array=True, bigint_as_string=False, sort_keys=False, item_sort_key=None, for_json=None,
         ignore_nan=False, int_as_string_bitcount=None, iterable_as_array=False, **kw):
    writer = blocking_writer(binary_writer(imports), fp)
    if sequence_as_stream and isinstance(obj, (list, tuple)):
        # Treat this top-level sequence as a stream; serialize its elements as top-level values, but don't serialize the
        # sequence itself.
        for top_level in obj:
            _dump(top_level, writer)
    else:
        _dump(obj, writer)
    writer.send(ION_STREAM_END_EVENT)


def _ion_type(obj):
    if obj is None:
        ion_type = IonType.NULL
    elif obj is True or obj is False:
        ion_type = IonType.BOOL
    elif isinstance(obj, six.integer_types):
        ion_type = IonType.INT
    elif isinstance(obj, float):
        ion_type = IonType.FLOAT
    elif isinstance(obj, six.text_type):
        ion_type = IonType.STRING
    elif isinstance(obj, Decimal):
        ion_type = IonType.DECIMAL
    elif isinstance(obj, datetime):  # TODO accept 'Timestamp' too?
        ion_type = IonType.TIMESTAMP
    elif isinstance(obj, six.binary_type):
        ion_type = IonType.BLOB
    elif isinstance(obj, SymbolToken):
        ion_type = IonType.SYMBOL
    elif isinstance(obj, list):
        ion_type = IonType.LIST
    elif isinstance(obj, dict):
        ion_type = IonType.STRUCT
    else:
        raise ValueError('Unknown scalar type %r' % (type(obj),))
    return ion_type


def _dump(obj, writer, field=None):
    if obj is None:
        event = IonEvent(IonEventType.SCALAR, IonType.NULL, field_name=field)
    elif obj is True:
        event = IonEvent(IonEventType.SCALAR, IonType.BOOL, True, field_name=field)
    elif obj is False:
        event = IonEvent(IonEventType.SCALAR, IonType.BOOL, False, field_name=field)
    elif isinstance(obj, dict):
        if isinstance(obj, _IonNature):
            event = obj.to_event(IonEventType.CONTAINER_START, field_name=field)
        else:
            event = IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT, field_name=field)
        writer.send(event)
        for field, val in six.iteritems(obj):
            _dump(val, writer, field)
        event = _ION_CONTAINER_END_EVENT
    elif isinstance(obj, list):
        if isinstance(obj, _IonNature):
            event = obj.to_event(IonEventType.CONTAINER_START, field_name=field)
        else:
            event = IonEvent(IonEventType.CONTAINER_START, IonType.LIST, field_name=field)
        writer.send(event)
        for elem in obj:
            _dump(elem, writer)
        event = _ION_CONTAINER_END_EVENT
    else:
        # obj is a scalar value
        if isinstance(obj, _IonNature):
            event = obj.to_event(IonEventType.SCALAR, field_name=field)
        else:
            event = IonEvent(IonEventType.SCALAR, _ion_type(obj), obj, field_name=field)
    writer.send(event)


def dumps(obj, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, cls=None, indent=None,
          separators=None, encoding='utf-8', default=None, use_decimal=True, namedtuple_as_object=True,
          tuple_as_array=True, bigint_as_string=False, sort_keys=False, item_sort_key=None, for_json=None,
          ignore_nan=False, int_as_string_bitcount=None, iterable_as_array=False, **kw):
    raise IonException("Not yet implemented")


def load(fp, catalog=None, single_value=True, encoding='utf-8', cls=None, object_hook=None, parse_float=None,
         parse_int=None, parse_constant=None, object_pairs_hook=None, use_decimal=None, **kw):
    reader = blocking_reader(managed_reader(raw_reader(), catalog), fp)
    out = []  # top-level
    _load(out, reader)
    if single_value:
        if len(out) != 1:
            raise IonException('Stream contained more than a single value')
        return out[0]
    return out


def _load(out, reader, end_type=IonEventType.STREAM_END, in_struct=False):

    def add(obj):
        if in_struct:
            # TODO what about duplicate field names?
            out[event.field_name.text] = obj
        else:
            out.append(obj)

    event = reader.send(NEXT_EVENT)
    while event.event_type is not end_type:
        if event.event_type is IonEventType.CONTAINER_START:
            # TODO any difference between list and sexp?
            if event.ion_type is IonType.LIST or event.ion_type is IonType.SEXP:
                lst = IonPyList.from_event(event)
                _load(lst, reader, IonEventType.CONTAINER_END)
                add(lst)
            elif event.ion_type is IonType.STRUCT:
                dct = IonPyDict.from_event(event)
                _load(dct, reader, IonEventType.CONTAINER_END, True)
                add(dct)
            else:
                raise ValueError("Illegal IonType for %r: %r " % (event.event_type, event.ion_type))
        elif event.event_type is IonEventType.SCALAR:
            if event.value is None or event.ion_type is IonType.NULL or event.ion_type.is_container:
                add(IonPyNull.from_event(event))
            elif event.ion_type is IonType.BOOL:
                add(IonPyBool.from_event(event))
            elif event.ion_type is IonType.INT:
                add(IonPyInt.from_event(event))
            elif event.ion_type is IonType.FLOAT:
                add(IonPyFloat.from_event(event))
            elif event.ion_type is IonType.DECIMAL:
                add(IonPyDecimal.from_event(event))
            elif event.ion_type is IonType.TIMESTAMP:
                add(IonPyTimestamp.from_event(event))
            elif event.ion_type is IonType.STRING:
                add(IonPyText.from_event(event))
            elif event.ion_type is IonType.SYMBOL:
                add(IonPySymbol.from_event(event))
            elif event.ion_type is IonType.BLOB or event.ion_type is IonType.CLOB:
                add(IonPyBytes.from_event(event))
            else:
                raise ValueError("Illegal IonType for %r: %r " % (event.event_type, event.ion_type))
        event = reader.send(NEXT_EVENT)


def loads(fp, encoding='utf-8', cls=None, object_hook=None, parse_float=None, parse_int=None, parse_constant=None,
          object_pairs_hook=None, use_decimal=None, **kw):
    raise IonException("Not yet implemented")
