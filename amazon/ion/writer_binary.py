# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

"""Binary Ion writer with symbol table management."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .exceptions import IonException
from .symbols import SID_ION_SYMBOL_TABLE, SID_IMPORTS, SHARED_TABLE_TYPE, \
    SID_NAME, SID_VERSION, SID_MAX_ID, SID_SYMBOLS, SymbolTable, LOCAL_TABLE_TYPE
from .writer_binary_raw import _raw_binary_writer, _WRITER_EVENT_NEEDS_INPUT_EMPTY,\
    _WRITER_EVENT_COMPLETE_EMPTY
from .writer_buffer import BufferTree
from .core import IonEventType, IonType, IonEvent
from .util import coroutine, Enum, record
from .writer import writer_trampoline, partial_write_result, _drain
from .writer import WriteEvent, WriteEventType, WriteResult


_IVM = bytearray([0xE0, 0x01, 0x00, 0xEA])


class _SymbolEventType(Enum):
    """Enumeration of events that impact a symbol table.

    Attributes:
        START_LST: Indicates that a local symbol table should be started.
        SYMBOL: Indicates that a symbol should be interned into the local symbol, and the resulting symbol ID returned.
        FINISH: Indicates that the local symbol table (if any) should be ended.
    """
    START_LST = 0
    SYMBOL = 1
    FINISH = 2


class _SymbolEvent(record('event_type', ('symbol_text', None))):
    """Symbol event used by the managed writer coroutine to trigger an action in the symbol table coroutine.

    Only events of type SYMBOL should have symbol text associated with them.

    Args:
        event_type (_SymbolEventType): The type of symbol event.
        symbol_text (Optional[unicode]): The symbol text associated with the event.
    """


_SYMBOL_EVENT_START_LST = _SymbolEvent(_SymbolEventType.START_LST)
_SYMBOL_EVENT_FINISH = _SymbolEvent(_SymbolEventType.FINISH)

_ION_EVENT_STRUCT_START = IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT)
_ION_EVENT_STREAM_END = IonEvent(IonEventType.STREAM_END)
_ION_EVENT_CONTAINER_END = IonEvent(IonEventType.CONTAINER_END)
_ION_EVENT_RAW_LST_STRUCT_START = IonEvent(
    IonEventType.CONTAINER_START, IonType.STRUCT, annotations=[SID_ION_SYMBOL_TABLE])
_ION_EVENT_RAW_IMPORTS_LIST_START = IonEvent(IonEventType.CONTAINER_START, IonType.LIST, field_name=SID_IMPORTS)
_ION_EVENT_RAW_SYMBOLS_LIST_START = IonEvent(IonEventType.CONTAINER_START, IonType.LIST, field_name=SID_SYMBOLS)


@coroutine
def _symbol_table_coroutine(writer_buffer, imports):

    def start_lst():
        write(_ION_EVENT_RAW_LST_STRUCT_START)
        if imports:
            write(_ION_EVENT_RAW_IMPORTS_LIST_START)
            for imported in imports:
                # TODO The system table could be allowed as the first import.
                if imported.table_type is not SHARED_TABLE_TYPE:
                    # TODO This should probably fail at creation of the managed writer coroutine, but that currently
                    # requires two imports iterations.
                    raise IonException('Only shared tables may be imported.')
                write(_ION_EVENT_STRUCT_START)
                write(IonEvent(IonEventType.SCALAR, IonType.STRING, imported.name, field_name=SID_NAME))
                write(IonEvent(IonEventType.SCALAR, IonType.INT, imported.version, field_name=SID_VERSION))
                write(IonEvent(IonEventType.SCALAR, IonType.INT, imported.max_id, field_name=SID_MAX_ID))
                write(_ION_EVENT_CONTAINER_END)
            write(_ION_EVENT_CONTAINER_END)
        return _WRITER_EVENT_NEEDS_INPUT_EMPTY

    def write_symbol(symbol_text):
        if symbol_text is None:
            raise IonException('Illegal state: local symbol event with None symbol.')
        token = local_symbols.get(symbol_text)
        if token is None:
            token = local_symbols.intern(symbol_text)  # This duplicates the 'get' call...
            write(IonEvent(IonEventType.SCALAR, IonType.STRING, token.text))
        return WriteEvent(WriteEventType.NEEDS_INPUT, token.sid)

    # TODO support extending the current LST (by importing it)
    # TODO symtab locking?
    local_symbols = None
    symbol_writer = _raw_binary_writer(writer_buffer)
    write = symbol_writer.send
    has_local_symbols = False
    write_result = None
    while True:
        symbol_event, self = (yield write_result)
        if symbol_event.event_type is _SymbolEventType.START_LST:
            write_event = start_lst()
            local_symbols = SymbolTable(LOCAL_TABLE_TYPE, [], imports=imports)  # Initialize the map.
        elif symbol_event.event_type is _SymbolEventType.SYMBOL:
            if local_symbols is None:
                raise IonException('Illegal state: local symbol table not started.')
            if not has_local_symbols:
                write(_ION_EVENT_RAW_SYMBOLS_LIST_START)
                has_local_symbols = True
            write_event = write_symbol(symbol_event.symbol_text)
        elif symbol_event.event_type is _SymbolEventType.FINISH:
            # If there are no local symbols or imports, there is no need for an explicit LST.
            if has_local_symbols or imports:
                if has_local_symbols:
                    write(_ION_EVENT_CONTAINER_END)  # End the symbols list.
                write(_ION_EVENT_CONTAINER_END)  # End the symbol table struct.
                for partial in _drain(symbol_writer, _ION_EVENT_STREAM_END):
                    yield partial_write_result(partial.data, self)
            write_event = _WRITER_EVENT_COMPLETE_EMPTY
        else:
            raise TypeError('Invalid event: %s' % symbol_event)
        write_result = WriteResult(write_event, self)


@coroutine
def _managed_binary_writer_coroutine(imports):

    def init():
        _value_writer = _raw_binary_writer(BufferTree())
        _symbol_writer = _raw_symbol_writer(BufferTree(), imports)
        return _value_writer, _symbol_writer

    def intern_symbol(sym):
        return symbol_writer.send(_SymbolEvent(_SymbolEventType.SYMBOL, sym)).data

    def intern_symbols(event):
        field_name = event.field_name
        annotations = event.annotations
        if field_name:
            event = event.derive_field_name(intern_symbol(field_name))
        if annotations:
            event = event.derive_annotations([intern_symbol(annotation) for annotation in annotations])
        if event.ion_type is IonType.SYMBOL and event.value is not None:
            event = event.derive_value(intern_symbol(ion_event.value))
        return event

    value_writer, symbol_writer = init()
    write_result = None
    has_written_values = False
    ivm_needed = True
    while True:
        ion_event, self = (yield write_result)
        if ion_event.event_type is IonEventType.VERSION_MARKER:
            if has_written_values:
                # TODO This could be handled by flushing first.
                raise IonException('Unable to write IVM before STREAM_END')
            else:
                if ivm_needed:
                    yield partial_write_result(_IVM, self)
                ivm_needed = False
            write_event = _WRITER_EVENT_NEEDS_INPUT_EMPTY
        elif ion_event.event_type is IonEventType.STREAM_END:
            if has_written_values:
                for partial in _drain(symbol_writer, _SYMBOL_EVENT_FINISH):
                    yield partial_write_result(partial.data, self)
                for partial in _drain(value_writer, _ION_EVENT_STREAM_END):
                    yield partial_write_result(partial.data, self)
                value_writer, symbol_writer = init()
                has_written_values = False
            write_event = _WRITER_EVENT_COMPLETE_EMPTY
            ivm_needed = True
        else:  # Intern any symbols and delegate to the raw writer.
            if not has_written_values:
                if ivm_needed:
                    yield partial_write_result(_IVM, self)
                ivm_needed = False
                symbol_writer.send(_SYMBOL_EVENT_START_LST)
                has_written_values = True
            ion_event = intern_symbols(ion_event)
            write_event = value_writer.send(ion_event)
        write_result = WriteResult(write_event, self)


def _raw_symbol_writer(writer_buffer, imports):
    """Returns a raw binary symbol table writer co-routine.

    Keyword Args:
        writer_buffer (amazon.ion.writer_buffer.BufferTree): The buffer in which this writer's values will be
                                                             stored.
        imports Optional[Sequence[amazon.ion.symbols.SymbolTable]]: A list of shared symbol tables to be used by this
                                                                    writer.

    Yields:
        WriteEvent: serialization events to write out

        Receives :class:`amazon.ion.core.IonEvent`.
    """
    return writer_trampoline(_symbol_table_coroutine(writer_buffer, imports))


def binary_writer(imports=None):
    """Returns a binary writer co-routine.

    Keyword Args:
        imports Optional[Sequence[amazon.ion.symbols.SymbolTable]]: A list of shared symbol tables to be used by this
                                                                    writer.

    Yields:
        WriteEvent: serialization events to write out

        Receives :class:`amazon.ion.core.IonEvent`.
    """
    return writer_trampoline(_managed_binary_writer_coroutine(imports))

