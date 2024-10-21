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

"""Provides a ``simplejson``-like API for dumping and loading Ion data.

The below table describes how types from the Ion data model map to the IonPy types
in simple_types.py as well as what other Python types are supported on dump.

TODO: add "bare" mapping to table.

        +-------------------+-------------------+-----------------------------------+
        | Ion Data Type     | IonPy Type        | Other Dump Mappings               |
        |-------------------+-------------------|-----------------------------------|
        | null.<type>       | IonPyNull(<type>) | None                              |
        |-------------------+-------------------|-----------------------------------|
        | bool              |    IonPyBool      | bool                              |
        |-------------------+-------------------|-----------------------------------|
        | int               |    IonPyInt       | int                               |
        |-------------------+-------------------|-----------------------------------|
        | float             |    IonPyFloat     | float                             |
        |-------------------+-------------------|-----------------------------------|
        | decimal           |   IonPyDecimal    | decimal.Decimal                   |
        |-------------------+-------------------|-----------------------------------|
        | timestamp         |  IonPyTimestamp   | Timestamp, datetime               |
        |-------------------+-------------------|-----------------------------------|
        | symbol            |   IonPySymbol     | IonPyText(SYMBOL), SymbolToken    |
        |-------------------+-------------------|-----------------------------------|
        | string            | IonPyText(STRING) | str, unicode                      |
        |-------------------+-------------------|-----------------------------------|
        | clob              |  IonPyBytes(CLOB) |                                   |
        |-------------------+-------------------|-----------------------------------|
        | blob              |  IonPyBytes(BLOB) | bytes                             |
        |-------------------+-------------------|-----------------------------------|
        | list              |   IonPyList(LIST) | list, tuple (tuple_as_sexp=False) |
        |-------------------+-------------------|-----------------------------------|
        | sexp              |   IonPyList(SEXP) | tuple (tuple_as_sexp=True)        |
        |-------------------+-------------------|-----------------------------------|
        | struct            |     IonPyDict     | dict, namedtuple                  |
        +-------------------+-------------------+-----------------------------------+

A C-extension is used when available for greater performance. That is enabled by default but may be
disabled as below:
    ``simpleion.c_ext = False``

"""
import io
import warnings
from datetime import datetime
from decimal import Decimal
from enum import IntFlag
from io import BytesIO, TextIOBase
from types import GeneratorType
from typing import Union

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

try:
    import amazon.ion.ionc as ionc
    __IS_C_EXTENSION_SUPPORTED = True
except ModuleNotFoundError:
    __IS_C_EXTENSION_SUPPORTED = False
except ImportError as e:
    __IS_C_EXTENSION_SUPPORTED = False
    warnings.warn(
        f"Failed to load c-extension module: {e.msg} falling back to pure python implementation",
        ImportWarning)

# TODO: when we release a new major version, come up with a better way to encapsulate these two variables.
# __IS_C_EXTENSION_SUPPORTED is a private flag indicating whether the c extension was loaded/is supported.
# c_ext is a user-facing flag to check whether the c extension is available and/or disable the c extension.
# However, if you mutate it, then it can no longer be used to see if the c extension is available.
c_ext = __IS_C_EXTENSION_SUPPORTED


def dump(obj, fp, imports=None, binary=True, sequence_as_stream=False, indent=None,
         tuple_as_sexp=False, omit_version_marker=False, trailing_commas=False):
    """Serialize ``obj`` as an Ion formatted stream and write it to fp.

    The python object hierarchy is mapped to the Ion data model as described in the module pydoc.

    Common examples are below, please refer to the
    [Ion Cookbook](https://amazon-ion.github.io/ion-docs/guides/cookbook.html) for detailed information.

    Write an object as Ion Text to the file handle:
        ``simpleion.dump(ion_object, file_handle, binary=False)``

    Write an object as Ion Binary to the file handle:
        ``simpleion.dump(ion_object, binary=True)``

    Args:
        obj (Any): A python object to serialize according to the above table. Any Python object which is neither an
            instance of nor inherits from one of the types in the above table will raise TypeError.
        fp: Object that implements the buffer protocol to write data to.
        imports (Optional[Sequence[SymbolTable]]): A sequence of shared symbol tables to be used by by the writer.
        binary (Optional[True|False]): When True, outputs binary Ion. When false, outputs text Ion.
        sequence_as_stream (Optional[True|False]): When True, if ``obj`` is a sequence, it will be treated as a stream
            of top-level Ion values (i.e. the resulting Ion data will begin with ``obj``'s first element).
            Default: False.
        indent (Str): If binary is False and indent is a string, then members of containers will be pretty-printed with
            a newline followed by that string repeated for each level of nesting. None (the default) selects the most
            compact representation without any newlines. Example: to indent with four spaces per level of nesting,
            use ``'    '``. Supported only in the pure Python implementation (because pretty printing is not yet
            supported in the C implementation).
        tuple_as_sexp (Optional[True|False]): When True, all tuple values will be written as Ion s-expressions.
            When False, all tuple values will be written as Ion lists. Default: False.
        omit_version_marker (Optional[True|False]): If binary is False and omit_version_marker is True, omits the
            Ion Version Marker ($ion_1_0) from the output.  Default: False.
        trailing_commas (Optional[True|False]): If binary is False and pretty printing (indent is not None), includes
            trailing commas in containers. Default: False. Supported only in the pure Python implementation (because
            pretty printing is not yet supported in the C implementation).

    Returns None.
    """
    if c_ext and __IS_C_EXTENSION_SUPPORTED and (imports is None and indent is None):
        return dump_extension(obj, fp, binary=binary, sequence_as_stream=sequence_as_stream,
                              tuple_as_sexp=tuple_as_sexp, omit_version_marker=omit_version_marker)
    else:
        return dump_python(obj, fp, imports=imports, binary=binary, sequence_as_stream=sequence_as_stream,
                           indent=indent,
                           tuple_as_sexp=tuple_as_sexp, omit_version_marker=omit_version_marker,
                           trailing_commas=trailing_commas)


def dumps(obj, imports=None, binary=True, sequence_as_stream=False,
          indent=None, tuple_as_sexp=False, omit_version_marker=False, trailing_commas=False):
    """Serialize obj as described by dump, return the serialized data as bytes or unicode.

    Returns:
         Union[str|bytes]: The string or binary representation of the data.  if ``binary=True``, this will be a
             ``bytes`` object, otherwise this will be a ``str`` object
    """
    ion_buffer = io.BytesIO()

    dump(obj, ion_buffer, imports=imports, sequence_as_stream=sequence_as_stream, binary=binary,
         indent=indent, tuple_as_sexp=tuple_as_sexp, omit_version_marker=omit_version_marker,
         trailing_commas=trailing_commas)

    ret_val = ion_buffer.getvalue()
    ion_buffer.close()
    if not binary:
        ret_val = ret_val.decode('utf-8')
    return ret_val


class IonPyValueModel(IntFlag):
    """Flags to control the types of values that are emitted from load(s).

    The flags may be mixed so that users can intentionally demote certain Ion
    constructs to standard Python types. In general this will increase read
    performance and improve interoperability with other Python code at the
    expense of data fidelity.

    For example:
        model = IonPyValueModel.MAY_BE_BARE | IonPyValueModel.SYMBOL_AS_TEXT

    would mean that any Symbols without annotations would be emitted as str,
    any Symbols with annotations would be emitted as IonPyText(IonType.SYMBOL).

    todo: add/extend this as desired. some possible additions:
        CLOB_AS_BYTES
        SEXP_AS______ (list, tuple, either?)
        TIMESTAMP_AS_DATETIME
        IGNORE_ANNOTATIONS
        IGNORE_NULL_TYPING
        ALWAYS_BARE (union of all flags)
    """

    ION_PY = 0
    """All values will be instances of the IonPy classes."""

    MAY_BE_BARE = 1
    """Values will be of the IonPy classes when needed to ensure data fidelity,
    otherwise they will be standard Python types or "core" Ion types, such as
    SymbolToken or Timestamp.
    
    If a value has an annotation or the IonType would be ambiguous without the
    IonPy wrapper, it will not be emitted as a bare value. The other flags can
    be used to broaden which Ion values may be emitted as bare values.
    
    NOTE: Structs do not currently have a "bare" type.
    """

    SYMBOL_AS_TEXT = 2
    """Symbol values will be IonPyText(IonType.SYMBOL) or str if bare.
    
    Symbol Ids and import locations are always lost if present. When bare values
    are emitted, the type information (Symbol vs String) is also lost.
    If the text is unknown, which may happen when the Symbol Id is within the
    range of an imported table, but the text undefined, an exception is raised.
    """

    STRUCT_AS_STD_DICT = 4
    """"Struct values will be __todo__ or standard Python dicts if bare.

    Like json, this will only preserve the final mapping for a given field.
    For example, given a struct:
    { foo: 17, foo: 31 }

    The value for field 'foo' will be 31.

    As noted in the pydoc for the class, there is no "bare" value for Structs:
    the IonPyDict is both the IonPy wrapper and the multi-map.
    """


def load(fp, catalog=None, single_value=True, parse_eagerly=True,
         text_buffer_size_limit=None, value_model=IonPyValueModel.ION_PY):
    """Deserialize Ion values from ``fp``, a file-handle to an Ion stream, as Python object(s) using the
    conversion table described in the pydoc. Common examples are below, please refer to the
    [Ion Cookbook](https://amazon-ion.github.io/ion-docs/guides/cookbook.html) for detailed information.

    Read an Ion value from file_handle:
        ``simpleion.load(file_handle)``

    Read Ion values using an iterator:
        ``
        it = simpleion.load(file_handle, parse_eagerly=False)
        # iterate through top-level Ion objects
        next(it)
        next(it)
        ``

    Read an Ion value with 50k text_buffer_size_limit:
        ``simpleion.load(file_handle, text_buffer_size_limit=50000)``

    Args:
        fp: a file handle or other object that implements the buffer protocol.
        catalog (Optional[SymbolTableCatalog]): The catalog to use for resolving symbol table imports.
            NOTE: when present the pure python load is used, which will impact performance.
        single_value (Optional[True|False]): When True, the data in the ``fp`` is interpreted as a single Ion value,
            and will be returned without an enclosing container. If True and there are multiple top-level values in
            the Ion stream, IonException will be raised. NOTE: this means that when data is dumped using
            ``sequence_as_stream=True``, it must be loaded using ``single_value=False``. Default: True.
        parse_eagerly (Optional[True|False]): Used in conjunction with ``single_value=False`` to return the result as
            list or an iterator. Lazy parsing is significantly more efficient for many-valued streams.
        text_buffer_size_limit (int): The maximum byte size allowed for text values when the C extension is enabled
            (default: 4096 bytes). This option only has an effect when the C extension is enabled (and it is enabled by
            default). When the C extension is disabled, there is no limit on the size of text values.
        value_model (IonPyValueModel): Controls the types of values that are emitted from load(s).
            Default: IonPyValueModel.ION_PY. See the IonPyValueModel class for more information.
            NOTE: this option only has an effect when the C extension is enabled (which is the default).
    Returns (Any):
        if single_value is True:
            A Python object representing a single Ion value.
        else:
            A sequence of Python objects representing a stream of Ion values, may be a list or an iterator.
    """
    if c_ext and __IS_C_EXTENSION_SUPPORTED and catalog is None:
        return load_extension(fp, parse_eagerly=parse_eagerly, single_value=single_value,
                              text_buffer_size_limit=text_buffer_size_limit, value_model=value_model)
    else:
        return load_python(fp, catalog=catalog, single_value=single_value, parse_eagerly=parse_eagerly)


def loads(ion_str: Union[bytes, str], catalog=None, single_value=True, parse_eagerly=True,
          text_buffer_size_limit=None, value_model=IonPyValueModel.ION_PY):
    """Deserialize Ion value(s) from the bytes or str object. Behavior is as described by load."""

    if isinstance(ion_str, bytes):
        ion_buffer = BytesIO(ion_str)
    elif isinstance(ion_str, str):
        ion_buffer = io.StringIO(ion_str)
    else:
        raise TypeError('Unsupported text: %r' % ion_str)

    return load(ion_buffer, catalog=catalog, single_value=single_value,
                parse_eagerly=parse_eagerly, text_buffer_size_limit=text_buffer_size_limit, value_model=value_model)


# ... implementation from here down ...


_ION_CONTAINER_END_EVENT = IonEvent(IonEventType.CONTAINER_END)
_IVM = b'\xe0\x01\x00\xea'
_TEXT_TYPES = (TextIOBase, io.StringIO)


def dump_python(obj, fp, imports=None, binary=True, sequence_as_stream=False,
                indent=None, tuple_as_sexp=False, omit_version_marker=False,
                trailing_commas=False):
    """'pure' Python implementation. Users should prefer to call ``dump``."""
    raw_writer = binary_writer(imports) if binary else text_writer(indent=indent, trailing_commas=trailing_commas)
    writer = blocking_writer(raw_writer, fp)
    from_type = _FROM_TYPE_TUPLE_AS_SEXP if tuple_as_sexp else _FROM_TYPE
    if binary or not omit_version_marker:
        writer.send(ION_VERSION_MARKER_EVENT)  # The IVM is emitted automatically in binary; it's optional in text.
    if sequence_as_stream and isinstance(obj, (list, tuple)) or isinstance(obj, GeneratorType):
        # Treat this top-level sequence as a stream; serialize its elements as top-level values, but don't serialize the
        # sequence itself.
        for top_level in obj:
            _dump(top_level, writer, from_type)
    else:
        _dump(obj, writer, from_type)
    writer.send(ION_STREAM_END_EVENT)


_FROM_TYPE = {
    IonPyNull: IonType.NULL,
    type(None): IonType.NULL,
    type(True): IonType.BOOL,
    type(False): IonType.BOOL,
    float: IonType.FLOAT,
    str: IonType.STRING,
    Decimal: IonType.DECIMAL,
    datetime: IonType.TIMESTAMP,
    Timestamp: IonType.TIMESTAMP,
    bytes: IonType.BLOB,
    SymbolToken: IonType.SYMBOL,
    list: IonType.LIST,
    tuple: IonType.LIST,
    dict: IonType.STRUCT,
    int: IonType.INT
}

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
            for field, val in iter(obj.items()):
                _dump(val, writer, from_type, field, in_struct=True, depth=depth + 1)
        else:
            for elem in obj:
                _dump(elem, writer, from_type, depth=depth + 1)
        event = _ION_CONTAINER_END_EVENT
    else:
        # obj is a scalar value
        if ion_nature:
            event = obj.to_event(IonEventType.SCALAR, field_name=field, in_struct=in_struct, depth=depth)
        else:
            event = IonEvent(IonEventType.SCALAR, ion_type, obj, field_name=field, depth=depth)
    writer.send(event)


def load_python(fp, catalog=None, single_value=True, parse_eagerly=True):
    """'pure' Python implementation. Users should prefer to call ``load``."""
    if isinstance(fp, _TEXT_TYPES):
        raw_reader = text_reader(is_unicode=True)
    else:
        pos = fp.tell()
        maybe_ivm = fp.read(4)
        fp.seek(pos)
        if maybe_ivm == _IVM:
            raw_reader = binary_reader()
        else:
            raw_reader = text_reader()
    reader = blocking_reader(managed_reader(raw_reader, catalog), fp)
    if parse_eagerly:
        out = []  # top-level
        _load(out, reader)
        if single_value:
            if len(out) != 1:
                raise IonException('Stream contained %d values; expected a single value.' % (len(out),))
            return out[0]
        return out
    else:
        out = _load_iteratively(reader)
        if single_value:
            result = next(out)
            try:
                next(out)
                raise IonException('Stream contained more than 1 values; expected a single value.')
            except StopIteration:
                return result
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


def _load_iteratively(reader, end_type=IonEventType.STREAM_END):
    event = reader.send(NEXT_EVENT)
    while event.event_type is not end_type:
        ion_type = event.ion_type
        if event.event_type is IonEventType.CONTAINER_START:
            container = _FROM_ION_TYPE[ion_type].from_event(event)
            _load(container, reader, IonEventType.CONTAINER_END, ion_type is IonType.STRUCT)
            yield container
        elif event.event_type is IonEventType.SCALAR:
            if event.value is None or ion_type is IonType.NULL or ion_type.is_container:
                scalar = IonPyNull.from_event(event)
            else:
                scalar = _FROM_ION_TYPE[ion_type].from_event(event)
            yield scalar
        event = reader.send(NEXT_EVENT)


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


def dump_extension(obj, fp, binary=True, sequence_as_stream=False, tuple_as_sexp=False, omit_version_marker=False):
    """C-extension implementation. Users should prefer to call ``dump``."""

    res = ionc.ionc_write(obj, binary, sequence_as_stream, tuple_as_sexp)

    # TODO: support "omit_version_marker" rather than hacking.
    # TODO: support "trailing_commas" (support is not included in the C code).
    if not binary and not omit_version_marker:
        res = b'$ion_1_0 ' + res
    fp.write(res)


def load_extension(fp, single_value=True, parse_eagerly=True,
                   text_buffer_size_limit=None, value_model=IonPyValueModel.ION_PY):
    """C-extension implementation. Users should prefer to call ``load``."""
    iterator = ionc.ionc_read(fp, value_model=value_model.value, text_buffer_size_limit=text_buffer_size_limit)
    if single_value:
        try:
            value = next(iterator)
        except StopIteration:
            return None
        try:
            next(iterator)
            raise IonException('Stream contained more than 1 values; expected a single value.')
        except StopIteration:
            pass
        return value
    if parse_eagerly:
        return list(iterator)
    return iterator
