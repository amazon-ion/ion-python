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

"""Binary Ion writer with symbol table management."""
from enum import IntEnum

from .core import ION_STREAM_END_EVENT, IonEventType, IonType, IonEvent, DataEvent, Transition
from .exceptions import IonException
from .symbols import SID_ION_SYMBOL_TABLE, SID_IMPORTS, SHARED_TABLE_TYPE, \
                     SID_NAME, SID_VERSION, SID_MAX_ID, SID_SYMBOLS, LOCAL_TABLE_TYPE, \
                     SymbolTable, _SYSTEM_SYMBOL_TOKENS
from .util import coroutine, record
from .writer import NOOP_WRITER_EVENT, writer_trampoline, partial_transition, WriteEventType, \
                    _drain
from .writer_binary_raw import _WRITER_EVENT_NEEDS_INPUT_EMPTY, _raw_binary_writer
from .writer_buffer import BufferTree

_IVM = b'\xE0\x01\x00\xEA'


class _SymbolEventType(IntEnum):
    """Enumeration of events that impact a symbol table.

    Attributes:
        START_LST: Indicates that a local symbol table should be started.
        SYMBOL: Indicates that a symbol should be interned into the local symbol,
            and the resulting symbol ID returned.
        FINISH: Indicates that the local symbol table (if any) should be ended.
    """
    START_LST = 0
    SYMBOL = 1
    FINISH = 2


class _SymbolEvent(record('event_type', ('symbol', None))):
    """Symbol event used by the managed writer coroutine to trigger an action in the symbol
    table coroutine.

    Only events of type SYMBOL should have symbol text associated with them.

    Args:
        event_type (_SymbolEventType): The type of symbol event.
        symbol (Optional[SymbolToken | unicode]): The symbol token or text associated with the event.
    """


def _system_token(sid):
    return _SYSTEM_SYMBOL_TOKENS[sid - 1]

_SYMBOL_EVENT_START_LST = _SymbolEvent(_SymbolEventType.START_LST)
_SYMBOL_EVENT_FINISH = _SymbolEvent(_SymbolEventType.FINISH)

_ION_EVENT_STRUCT_START = IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT)
_ION_EVENT_STREAM_END = ION_STREAM_END_EVENT
_ION_EVENT_CONTAINER_END = IonEvent(IonEventType.CONTAINER_END)
_ION_EVENT_RAW_LST_STRUCT_START = IonEvent(
    IonEventType.CONTAINER_START, IonType.STRUCT, annotations=[_system_token(SID_ION_SYMBOL_TABLE)])
_ION_EVENT_RAW_IMPORTS_LIST_START = IonEvent(
    IonEventType.CONTAINER_START, IonType.LIST, field_name=_system_token(SID_IMPORTS))
_ION_EVENT_RAW_SYMBOLS_LIST_START = IonEvent(
    IonEventType.CONTAINER_START, IonType.LIST, field_name=_system_token(SID_SYMBOLS))


@coroutine
def _symbol_table_coroutine(writer_buffer, imports):

    def start_lst():
        write(_ION_EVENT_RAW_LST_STRUCT_START)
        if imports:
            write(_ION_EVENT_RAW_IMPORTS_LIST_START)
            for imported in imports:
                # TODO The system table could be allowed as the first import.
                if imported.table_type is not SHARED_TABLE_TYPE:
                    # TODO This should probably fail at creation of the managed writer coroutine,
                    # but that currently requires two imports iterations.
                    raise IonException('Only shared tables may be imported.')
                write(_ION_EVENT_STRUCT_START)
                write(IonEvent(
                    IonEventType.SCALAR, IonType.STRING, imported.name, field_name=_system_token(SID_NAME)))
                write(IonEvent(
                    IonEventType.SCALAR, IonType.INT, imported.version, field_name=_system_token(SID_VERSION)))
                write(IonEvent(
                    IonEventType.SCALAR, IonType.INT, imported.max_id, field_name=_system_token(SID_MAX_ID)))
                write(_ION_EVENT_CONTAINER_END)
            write(_ION_EVENT_CONTAINER_END)
        return _WRITER_EVENT_NEEDS_INPUT_EMPTY

    def write_symbol(symbol):
        if symbol is None:
            raise IonException('Illegal state: local symbol event with None symbol.')
        try:
            key = symbol.sid
            symbol_text = symbol.text
            if symbol_text is not None:
                key = symbol_text
        except AttributeError:
            assert isinstance(symbol, str)
            key = symbol
            symbol_text = symbol
        token = local_symbols.get(key)
        if token is None:
            assert symbol_text is not None
            token = local_symbols.intern(symbol_text)  # This duplicates the 'get' call...
            write(IonEvent(IonEventType.SCALAR, IonType.STRING, token.text))
        return DataEvent(WriteEventType.NEEDS_INPUT, token)

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
            local_symbols = SymbolTable(LOCAL_TABLE_TYPE, [], imports=imports)
        elif symbol_event.event_type is _SymbolEventType.SYMBOL:
            if local_symbols is None:
                raise IonException('Illegal state: local symbol table not started.')
            if not has_local_symbols:
                write(_ION_EVENT_RAW_SYMBOLS_LIST_START)
                has_local_symbols = True
            write_event = write_symbol(symbol_event.symbol)
        elif symbol_event.event_type is _SymbolEventType.FINISH:
            # If there are no local symbols or imports, there is no need for an explicit LST.
            if has_local_symbols or imports:
                if has_local_symbols:
                    write(_ION_EVENT_CONTAINER_END)  # End the symbols list.
                write(_ION_EVENT_CONTAINER_END)  # End the symbol table struct.
                for partial in _drain(symbol_writer, _ION_EVENT_STREAM_END):
                    yield partial_transition(partial.data, self)
            write_event = NOOP_WRITER_EVENT
        else:
            raise TypeError('Invalid event: %s' % symbol_event)
        write_result = Transition(write_event, self)


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
        if field_name or field_name == '':
            event = event.derive_field_name(intern_symbol(field_name))
        if annotations:
            event = event.derive_annotations(
                [intern_symbol(annotation) for annotation in annotations])
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
                    yield partial_transition(_IVM, self)
                ivm_needed = False
            write_event = _WRITER_EVENT_NEEDS_INPUT_EMPTY
        elif ion_event.event_type is IonEventType.STREAM_END:
            if has_written_values:
                for partial in _drain(symbol_writer, _SYMBOL_EVENT_FINISH):
                    yield partial_transition(partial.data, self)
                for partial in _drain(value_writer, _ION_EVENT_STREAM_END):
                    yield partial_transition(partial.data, self)
                value_writer, symbol_writer = init()
                has_written_values = False
            write_event = NOOP_WRITER_EVENT
            ivm_needed = True
        else:
            # Intern any symbols and delegate to the raw writer.
            if not has_written_values:
                if ivm_needed:
                    yield partial_transition(_IVM, self)
                ivm_needed = False
                symbol_writer.send(_SYMBOL_EVENT_START_LST)
                has_written_values = True
            ion_event = intern_symbols(ion_event)
            write_event = value_writer.send(ion_event)
        write_result = Transition(write_event, self)


def _raw_symbol_writer(writer_buffer, imports):
    """Returns a raw binary symbol table writer co-routine.

    Keyword Args:
        writer_buffer (BufferTree): The buffer in which this writer's values will be stored.
        imports (Optional[Sequence[SymbolTable]]): A list of shared symbol tables to
            be used by this writer.

    Yields:
        DataEvent: serialization events to write out

        Receives :class:`amazon.ion.core.IonEvent`.
    """
    return writer_trampoline(_symbol_table_coroutine(writer_buffer, imports))


def binary_writer(imports=None):
    """Returns a binary writer co-routine.

    Keyword Args:
        imports (Optional[Sequence[SymbolTable]]): A list of shared symbol tables
            to be used by this writer.

    Yields:
        DataEvent: serialization events to write out

        Receives :class:`amazon.ion.core.IonEvent`.
    """
    return writer_trampoline(_managed_binary_writer_coroutine(imports))

