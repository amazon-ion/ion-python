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
from io import BytesIO, TextIOBase
from itertools import chain

import six

from amazon.ion.reader_text import text_reader
from amazon.ion.writer_text import text_writer
from .core import IonEvent, IonEventType, IonType, ION_STREAM_END_EVENT, Timestamp, ION_VERSION_MARKER_EVENT
from .exceptions import IonException
from .reader import blocking_reader, NEXT_EVENT
from .reader_binary import binary_reader
from .reader_managed import managed_reader
from .simple_types import IonPyList, IonPyDict, IonPyNull, IonPyBool, IonPyInt, IonPyFloat, IonPyDecimal, \
    IonPyTimestamp, IonPyText, IonPyBytes, IonPySymbol, is_null
from .symbols import SymbolToken
from .writer import blocking_writer
from .writer_binary import binary_writer

# Using C extension as default, and original python implementation if C extension doesn't exist.
c_ext = True
try:
    import amazon.ion.ionc as ionc
except ModuleNotFoundError:
    c_ext = False


_ION_CONTAINER_END_EVENT = IonEvent(IonEventType.CONTAINER_END)
_IVM = b'\xe0\x01\x00\xea'
_TEXT_TYPES = (TextIOBase, six.StringIO)


def dump_original(obj, fp, imports=None, binary=True, sequence_as_stream=False, skipkeys=False, ensure_ascii=True,
         check_circular=True, allow_nan=True, cls=None, indent=None, separators=None, encoding='utf-8', default=None,
         use_decimal=True, namedtuple_as_object=True, tuple_as_array=True, bigint_as_string=False, sort_keys=False,
         item_sort_key=None, for_json=None, ignore_nan=False, int_as_string_bitcount=None, iterable_as_array=False,
         tuple_as_sexp=False, omit_version_marker=False, **kw):
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
        | list,             |                   |
        | tuple (when       |                   |
        |  tuple_as_sexp=   |     list          |
        |  False)           |                   |
        | IonPyList(LIST)   |                   |
        |-------------------+-------------------|
        | tuple (when       |                   |
        |  tuple_as_sexp=   |     sexp          |
        |  True),           |                   |
        | IonPyList(SEXP)   |                   |
        |-------------------+-------------------|
        | dict, namedtuple, |                   |
        | IonPyDict         |     struct        |
        +-------------------+-------------------+

    Args:
        obj (Any): A python object to serialize according to the above table. Any Python object which is neither an
            instance of nor inherits from one of the types in the above table will raise TypeError.
        fp (BaseIO): A file-like object.
        imports (Optional[Sequence[SymbolTable]]): A sequence of shared symbol tables to be used by by the writer.
        binary (Optional[True|False]): When True, outputs binary Ion. When false, outputs text Ion.
        sequence_as_stream (Optional[True|False]): When True, if ``obj`` is a sequence, it will be treated as a stream
            of top-level Ion values (i.e. the resulting Ion data will begin with ``obj``'s first element).
            Default: False.
        skipkeys: NOT IMPLEMENTED
        ensure_ascii: NOT IMPLEMENTED
        check_circular: NOT IMPLEMENTED
        allow_nan: NOT IMPLEMENTED
        cls: NOT IMPLEMENTED
        indent (Str): If binary is False and indent is a string, then members of containers will be pretty-printed with
            a newline followed by that string repeated for each level of nesting. None (the default) selects the most
            compact representation without any newlines. Example: to indent with four spaces per level of nesting,
            use ``'    '``.
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
        tuple_as_sexp (Optional[True|False]): When True, all tuple values will be written as Ion s-expressions.
            When False, all tuple values will be written as Ion lists. Default: False.
        omit_version_marker (Optional|True|False): If binary is False and omit_version_marker is True, omits the
            Ion Version Marker ($ion_1_0) from the output.  Default: False.
        **kw: NOT IMPLEMENTED

    """
    raw_writer = binary_writer(imports) if binary else text_writer(indent=indent)
    writer = blocking_writer(raw_writer, fp)
    from_type = _FROM_TYPE_TUPLE_AS_SEXP if tuple_as_sexp else _FROM_TYPE
    if binary or not omit_version_marker:
        writer.send(ION_VERSION_MARKER_EVENT)  # The IVM is emitted automatically in binary; it's optional in text.
    if sequence_as_stream and isinstance(obj, (list, tuple)):
        # Treat this top-level sequence as a stream; serialize its elements as top-level values, but don't serialize the
        # sequence itself.
        for top_level in obj:
            _dump(top_level, writer, from_type)
    else:
        _dump(obj, writer, from_type)
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
        tuple: IonType.LIST,
        dict: IonType.STRUCT
    }),
    six.iteritems(
        dict(zip(six.integer_types, [IonType.INT] * len(six.integer_types)))
    ),
))

_FROM_TYPE_TUPLE_AS_SEXP = dict(_FROM_TYPE)
_FROM_TYPE_TUPLE_AS_SEXP.update({
    tuple: IonType.SEXP
})


def _ion_type(obj, from_type):
    types = [type(obj)]
    while types:
        current_type = types.pop()
        if current_type in from_type:
            if current_type is SymbolToken:
                # SymbolToken is a tuple. Since tuple also has a mapping, SymbolToken has to be special-cased
                # to avoid relying on how the dict is ordered.
                return IonType.SYMBOL
            return from_type[current_type]
        types.extend(current_type.__bases__)

    raise TypeError('Unknown scalar type %r' % (type(obj),))


def _dump(obj, writer, from_type, field=None, in_struct=False, depth=0):
    null = is_null(obj)
    try:
        ion_type = obj.ion_type
        ion_nature = True
    except AttributeError:
        ion_type = _ion_type(obj, from_type)
        ion_nature = False
    if ion_type is None:
        raise IonException('Value must have a non-None ion_type: %s, depth: %d, field: %s' % (repr(obj), depth, field))
    if not null and ion_type.is_container:
        if ion_nature:
            event = obj.to_event(IonEventType.CONTAINER_START, field_name=field, in_struct=in_struct, depth=depth)
        else:
            event = IonEvent(IonEventType.CONTAINER_START, ion_type, field_name=field, depth=depth)
        writer.send(event)
        if ion_type is IonType.STRUCT:
            for field, val in six.iteritems(obj):
                _dump(val, writer, from_type, field, in_struct=True, depth=depth+1)
        else:
            for elem in obj:
                _dump(elem, writer, from_type, depth=depth+1)
        event = _ION_CONTAINER_END_EVENT
    else:
        # obj is a scalar value
        if ion_nature:
            event = obj.to_event(IonEventType.SCALAR, field_name=field, in_struct=in_struct, depth=depth)
        else:
            event = IonEvent(IonEventType.SCALAR, ion_type, obj, field_name=field, depth=depth)
    writer.send(event)


def dumps(obj, imports=None, binary=True, sequence_as_stream=False, skipkeys=False, ensure_ascii=True, check_circular=True,
          allow_nan=True, cls=None, indent=None, separators=None, encoding='utf-8', default=None, use_decimal=True,
          namedtuple_as_object=True, tuple_as_array=True, bigint_as_string=False, sort_keys=False, item_sort_key=None,
          for_json=None, ignore_nan=False, int_as_string_bitcount=None, iterable_as_array=False, tuple_as_sexp=False,
          omit_version_marker=False, **kw):
    """Serialize ``obj`` as Python ``string`` or ``bytes`` object, using the conversion table used by ``dump`` (above).

    Args:
        obj (Any): A python object to serialize according to the above table. Any Python object which is neither an
            instance of nor inherits from one of the types in the above table will raise TypeError.
        imports (Optional[Sequence[SymbolTable]]): A sequence of shared symbol tables to be used by by the writer.
        binary (Optional[True|False]): When True, outputs binary Ion. When false, outputs text Ion.
        sequence_as_stream (Optional[True|False]): When True, if ``obj`` is a sequence, it will be treated as a stream
            of top-level Ion values (i.e. the resulting Ion data will begin with ``obj``'s first element).
            Default: False.
        skipkeys: NOT IMPLEMENTED
        ensure_ascii: NOT IMPLEMENTED
        check_circular: NOT IMPLEMENTED
        allow_nan: NOT IMPLEMENTED
        cls: NOT IMPLEMENTED
        indent (Str): If binary is False and indent is a string, then members of containers will be pretty-printed with
            a newline followed by that string repeated for each level of nesting. None (the default) selects the most
            compact representation without any newlines. Example: to indent with four spaces per level of nesting,
            use ``'    '``.
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
        tuple_as_sexp (Optional[True|False]): When True, all tuple values will be written as Ion s-expressions.
            When False, all tuple values will be written as Ion lists. Default: False.
        omit_version_marker (Optional|True|False): If binary is False and omit_version_marker is True, omits the
            Ion Version Marker ($ion_1_0) from the output.  Default: False.
        **kw: NOT IMPLEMENTED

    Returns:
        Union[str|bytes]: The string or binary representation of the data.  if ``binary=True``, this will be a
            ``bytes`` object, otherwise this will be a ``str`` object (or ``unicode`` in the case of Python 2.x)
    """
    ion_buffer = six.BytesIO()

    dump(obj, ion_buffer, imports=imports, sequence_as_stream=sequence_as_stream, binary=binary, skipkeys=skipkeys,
         ensure_ascii=ensure_ascii, check_circular=check_circular,
         allow_nan=allow_nan, cls=cls, indent=indent, separators=separators, encoding=encoding, default=default,
         use_decimal=use_decimal, namedtuple_as_object=namedtuple_as_object, tuple_as_array=tuple_as_array,
         bigint_as_string=bigint_as_string, sort_keys=sort_keys, item_sort_key=item_sort_key, for_json=for_json,
         ignore_nan=ignore_nan, int_as_string_bitcount=int_as_string_bitcount, iterable_as_array=iterable_as_array,
         tuple_as_sexp=tuple_as_sexp, omit_version_marker=omit_version_marker, **kw)

    ret_val = ion_buffer.getvalue()
    ion_buffer.close()
    if not binary:
        ret_val = ret_val.decode('utf-8')
    return ret_val


def load_original(fp, catalog=None, single_value=True, encoding='utf-8', cls=None, object_hook=None, parse_float=None,
         parse_int=None, parse_constant=None, object_pairs_hook=None, use_decimal=None, **kw):
    """Deserialize ``fp`` (a file-like object), which contains a text or binary Ion stream, to a Python object using the
    following conversion table::
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
    if isinstance(fp, _TEXT_TYPES):
        raw_reader = text_reader(is_unicode=True)
    else:
        maybe_ivm = fp.read(4)
        fp.seek(0)
        if maybe_ivm == _IVM:
            raw_reader = binary_reader()
        else:
            raw_reader = text_reader()
    reader = blocking_reader(managed_reader(raw_reader, catalog), fp)
    out = []  # top-level
    _load(out, reader)
    if single_value:
        if len(out) != 1:
            raise IonException('Stream contained %d values; expected a single value.' % (len(out),))
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
            out.add_item(event.field_name.text, obj)
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


def loads(ion_str, catalog=None, single_value=True, encoding='utf-8', cls=None, object_hook=None, parse_float=None,
          parse_int=None, parse_constant=None, object_pairs_hook=None, use_decimal=None, **kw):
    """Deserialize ``ion_str``, which is a string representation of an Ion object, to a Python object using the
    conversion table used by load (above).

    Args:
        fp (str): A string representation of Ion data.
        catalog (Optional[SymbolTableCatalog]): The catalog to use for resolving symbol table imports.
        single_value (Optional[True|False]): When True, the data in ``ion_str`` is interpreted as a single Ion value,
            and will be returned without an enclosing container. If True and there are multiple top-level values in
            the Ion stream, IonException will be raised. NOTE: this means that when data is dumped using
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

    if isinstance(ion_str, six.binary_type):
        ion_buffer = BytesIO(ion_str)
    elif isinstance(ion_str, six.text_type):
        ion_buffer = six.StringIO(ion_str)
    else:
        raise TypeError('Unsupported text: %r' % ion_str)

    return load(ion_buffer, catalog=catalog, single_value=single_value, encoding=encoding, cls=cls,
                object_hook=object_hook, parse_float=parse_float, parse_int=parse_int, parse_constant=parse_constant,
                object_pairs_hook=object_pairs_hook, use_decimal=use_decimal)


def dump_extension(obj, fp, binary=True, sequence_as_stream=False, tuple_as_sexp=False, omit_version_marker=False):
    res = ionc.ionc_write(obj, binary, sequence_as_stream, tuple_as_sexp)

    # TODO support "omit_version_marker" rather than hacking.
    if not binary and not omit_version_marker:
        res = b'$ion_1_0 ' + res
    fp.write(res)


def load_extension(fp, single_value=True, encoding='utf-8'):
    data = fp.read()
    data = data if isinstance(data, bytes) else bytes(data, encoding)
    return ionc.ionc_read(data, single_value, False)


def dump(obj, fp, imports=None, binary=True, sequence_as_stream=False, skipkeys=False, ensure_ascii=True,
         check_circular=True, allow_nan=True, cls=None, indent=None, separators=None, encoding='utf-8', default=None,
         use_decimal=True, namedtuple_as_object=True, tuple_as_array=True, bigint_as_string=False, sort_keys=False,
         item_sort_key=None, for_json=None, ignore_nan=False, int_as_string_bitcount=None, iterable_as_array=False,
         tuple_as_sexp=False, omit_version_marker=False, **kw):
    if c_ext and imports is None:
        try:
            return dump_extension(obj, fp, binary=binary, sequence_as_stream=sequence_as_stream,
                              tuple_as_sexp=tuple_as_sexp, omit_version_marker=omit_version_marker)
        except:
            return dump_original(obj, fp, imports=imports, binary=binary, sequence_as_stream=sequence_as_stream,
                                 skipkeys=skipkeys, ensure_ascii=ensure_ascii, check_circular=check_circular,
                                 allow_nan=allow_nan, cls=cls, indent=indent, separators=separators, encoding=encoding,
                                 default=default, use_decimal=use_decimal, namedtuple_as_object=namedtuple_as_object,
                                 tuple_as_array=tuple_as_array, bigint_as_string=bigint_as_string, sort_keys=sort_keys,
                                 item_sort_key=item_sort_key, for_json=for_json, ignore_nan=ignore_nan,
                                 int_as_string_bitcount=int_as_string_bitcount, iterable_as_array=iterable_as_array,
                                 tuple_as_sexp=tuple_as_sexp, omit_version_marker=omit_version_marker, **kw)
    else:
        return dump_original(obj, fp, imports=imports, binary=binary, sequence_as_stream=sequence_as_stream,
                             skipkeys=skipkeys, ensure_ascii=ensure_ascii,check_circular=check_circular,
                             allow_nan=allow_nan, cls=cls, indent=indent, separators=separators, encoding=encoding,
                             default=default, use_decimal=use_decimal, namedtuple_as_object=namedtuple_as_object,
                             tuple_as_array=tuple_as_array, bigint_as_string=bigint_as_string, sort_keys=sort_keys,
                             item_sort_key=item_sort_key, for_json=for_json, ignore_nan=ignore_nan,
                             int_as_string_bitcount=int_as_string_bitcount, iterable_as_array=iterable_as_array,
                             tuple_as_sexp=tuple_as_sexp, omit_version_marker=omit_version_marker, **kw)


def load(fp, catalog=None, single_value=True, encoding='utf-8', cls=None, object_hook=None, parse_float=None,
         parse_int=None, parse_constant=None, object_pairs_hook=None, use_decimal=None, **kw):
    if c_ext and catalog is None:
        try:
            return load_extension(fp, single_value=single_value, encoding=encoding)
        except:
            return load_original(fp, catalog=catalog, single_value=single_value, encoding=encoding, cls=cls,
                                 object_hook=object_hook, parse_float=parse_float, parse_int=parse_int,
                                 parse_constant=parse_constant, object_pairs_hook=object_pairs_hook,
                                 use_decimal=use_decimal, **kw)
    else:
        return load_original(fp, catalog=catalog, single_value=single_value, encoding=encoding, cls=cls,
                             object_hook=object_hook, parse_float=parse_float, parse_int=parse_int,
                             parse_constant=parse_constant, object_pairs_hook=object_pairs_hook,
                             use_decimal=use_decimal, **kw)
