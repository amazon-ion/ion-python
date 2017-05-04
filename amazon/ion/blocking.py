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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

from amazon.ion.core import IonType, IonEventType, ION_STREAM_END_EVENT, ION_VERSION_MARKER_EVENT, IonEvent
from amazon.ion.exceptions import IonException
from amazon.ion.reader import BlockingBuffer
from amazon.ion.reader_binary import _HandlerContext, _start_type_handler_direct, _field_name_handler_direct
from amazon.ion.reader_managed import _ManagedContext, _managed_thunk_event, _ImportDesc
from amazon.ion.symbols import SID_ION_SYMBOL_TABLE, SID_IMPORTS, SHARED_TABLE_TYPE, SID_NAME, SID_VERSION, \
    SID_MAX_ID, SymbolTable, LOCAL_TABLE_TYPE, SID_SYMBOLS, SymbolTableCatalog, TEXT_ION_1_0, TEXT_SYMBOLS, \
    TEXT_IMPORTS, TEXT_ION_SYMBOL_TABLE, TEXT_ION, TEXT_NAME, TEXT_VERSION, TEXT_MAX_ID
from amazon.ion.writer_binary import _system_token, _IVM
from amazon.ion.writer_binary_raw import _serialize_container, _serialize_annotation_wrapper, _serialize_scalar_direct
from amazon.ion.writer_binary_raw_fields import _write_varuint
from amazon.ion.writer_buffer import BufferTree


class _BinaryWriterRaw:
    def __init__(self):
        self.__buffer = BufferTree()
        self.__containers = []

    @property
    def __current_container(self):
        return self.__containers[-1]

    @property
    def __depth(self):
        return len(self.__containers)

    def __start_value(self, field_name, annotations):
        if self.__depth > 0 and self.__current_container[0] is IonType.STRUCT:
            # A field name symbol ID is required at this position.
            if field_name is None:
                raise ValueError('Field name required within struct.')
            sid_buffer = bytearray()
            _write_varuint(sid_buffer, field_name.sid)  # Write the field name's symbol ID.
            self.__buffer.add_scalar_value(sid_buffer)
        if annotations:
            self.__buffer.start_container()

    def __end_value(self, annotations):
        if annotations:
            _serialize_annotation_wrapper(self.__buffer, annotations)

    def start_container(self, ion_type, annotations=None, field_name=None):
        self.__start_value(field_name, annotations)
        self.__buffer.start_container()
        self.__containers.append((ion_type, annotations))

    def end_container(self):
        if self.__depth < 1:
            raise ValueError('Unable to end container at depth 0.')
        ion_type, annotations = self.__containers.pop()
        _serialize_container(self.__buffer, ion_type)
        self.__end_value(annotations)

    def write_scalar(self, ion_type, value, annotations=None, field_name=None):
        self.__start_value(field_name, annotations)
        scalar_buffer = _serialize_scalar_direct(value, ion_type)
        self.__buffer.add_scalar_value(scalar_buffer)
        self.__end_value(annotations)

    def flush(self, fp):
        if self.__depth != 0:
            raise ValueError('Unable to flush below the top level.')
        for chunk in self.__buffer.drain():
            fp.write(chunk)


class BinaryWriter:
    def __init__(self, imports):
        self.imports = imports
        self.__symbol_table_writer = _BinaryWriterRaw()
        self.__value_writer = _BinaryWriterRaw()
        self.__has_written_values = False
        self.__local_symbols = None

    def __start_lst(self):
        self.__symbol_table_writer.start_container(IonType.STRUCT, annotations=(_system_token(SID_ION_SYMBOL_TABLE),))
        if self.imports:
            self.__symbol_table_writer.start_container(IonType.LIST, field_name=_system_token(SID_IMPORTS))
            for imported in self.imports:
                # TODO The system table could be allowed as the first import.
                if imported.table_type is not SHARED_TABLE_TYPE:
                    # TODO This should probably fail at creation of the managed writer,
                    # but that currently requires two imports iterations.
                    raise IonException('Only shared tables may be imported.')
                self.__symbol_table_writer.start_container(IonType.STRUCT)
                self.__symbol_table_writer.write_scalar(
                    IonType.STRING, imported.name, field_name=_system_token(SID_NAME))
                self.__symbol_table_writer.write_scalar(
                    IonType.INT, imported.version, field_name=_system_token(SID_VERSION))
                self.__symbol_table_writer.write_scalar(
                    IonType.INT, imported.max_id, field_name=_system_token(SID_MAX_ID))
                self.__symbol_table_writer.end_container()
            self.__symbol_table_writer.end_container()

    def __intern_symbol(self, symbol):
        if self.__local_symbols is None:
            self.__start_lst()
            self.__local_symbols = SymbolTable(LOCAL_TABLE_TYPE, [], imports=self.imports)
            self.__symbol_table_writer.start_container(IonType.LIST, field_name=_system_token(SID_SYMBOLS))
        if symbol is None:
            raise IonException('Illegal state: local symbol event with None symbol.')
        try:
            key = symbol.sid
            symbol_text = symbol.text
            if symbol_text is not None:
                key = symbol_text
        except AttributeError:
            # assert isinstance(symbol, six.text_type)
            if not isinstance(symbol, six.text_type):  # TODO this is a quick hack to support data loaded by simplejson for benchmarks. On Py2, its keys are str (binary type).
                assert isinstance(symbol, six.binary_type)
                symbol = six.text_type(symbol)
            key = symbol
            symbol_text = symbol
        token = self.__local_symbols.get(key)
        if token is None:
            assert symbol_text is not None
            token = self.__local_symbols.intern(symbol_text)  # This duplicates the 'get' call...
            self.__symbol_table_writer.write_scalar(IonType.STRING, token.text)
        return token

    def __end_lst(self):
        if self.imports or self.__local_symbols is not None:
            if self.__local_symbols is not None:
                self.__symbol_table_writer.end_container()  # End the symbols list.
            self.__symbol_table_writer.end_container()  # End the symbol table struct.

    def __intern_symbols(self, ion_type, value=None, annotations=None, field_name=None):
        if not self.__has_written_values:
            if self.imports:
                self.__start_lst()
            self.__has_written_values = True
        if field_name is not None:
            field_name = self.__intern_symbol(field_name)
        if annotations:
            annotations = [self.__intern_symbol(annotation) for annotation in annotations]
        if ion_type is IonType.SYMBOL and value is not None:
            # TODO validate type is text or symbol token
            value = self.__intern_symbol(value)
        return value, annotations, field_name

    def start_container(self, ion_type, annotations=None, field_name=None):
        _, annotations, field_name = self.__intern_symbols(ion_type, annotations=annotations, field_name=field_name)
        self.__value_writer.start_container(ion_type, annotations, field_name)

    def end_container(self):
        self.__value_writer.end_container()

    def write_scalar(self, ion_type, value, annotations=None, field_name=None):
        value, annotations, field_name = self.__intern_symbols(ion_type, value, annotations, field_name)
        self.__value_writer.write_scalar(ion_type, value, annotations, field_name)

    def finish(self, fp):
        fp.write(_IVM)
        self.__end_lst()
        if self.__has_written_values:
            self.__symbol_table_writer.flush(fp)
            self.__value_writer.flush(fp)
            self.__has_written_values = False
        self.__local_symbols = None


class _ReaderBinaryRaw:
    def __init__(self, fp):
        self.__context = _HandlerContext(
            position=0,
            limit=None,
            queue=BlockingBuffer(fp),
            field_name=None,
            annotations=None,
            depth=0,
            whence=None
        )
        self.__expect_ivm = True
        self.__container_stack = [None]

    # TODO add a way to skip
    def __iter__(self):
        while True:
            at_top = self.__context.depth == 0
            event, child_ctx = _start_type_handler_direct(_field_name_handler_direct, self.__context, self.__expect_ivm,
                                                          at_top=at_top, container_type=self.__container_stack[-1])
            if event is None:
                # This is NOP padding.
                continue
            if event.event_type is IonEventType.STREAM_END:
                break
            if event.event_type is IonEventType.CONTAINER_START:
                self.__container_stack.append(event.ion_type)
                self.__context = child_ctx
            elif event.event_type is IonEventType.CONTAINER_END:
                self.__container_stack.pop()
                self.__context = self.__context.whence

            self.__expect_ivm = False
            yield event
        yield ION_STREAM_END_EVENT


class _ReaderManaged:
    def __init__(self, reader, catalog=None):
        self.__reader_iter = iter(reader)
        self.__catalog = SymbolTableCatalog() if catalog is None else catalog
        self.__context = _ManagedContext(catalog)

    def __skip_container(self):
        # TODO make use of better skipping API once implemented
        event_type = IonEventType.CONTAINER_START
        while event_type is not IonEventType.CONTAINER_END:
            event_type = next(self.__reader_iter).event_type

    def __read_symbols_list(self, symbols):
        ion_event = None
        while True:
            if ion_event is not None:
                event_type = ion_event.event_type
                ion_type = ion_event.ion_type

                if event_type is IonEventType.CONTAINER_END:
                    break

                if event_type is IonEventType.CONTAINER_START:
                    # We need to skip past this container (ignoring the end container event).
                    self.__skip_container()
                if ion_type is IonType.STRING:
                    # Add the potentially null text.
                    symbols.append(ion_event.value)

            ion_event = next(self.__reader_iter)

    def __read_import_desc(self, imports):
        ion_event = None
        desc = _ImportDesc()
        while True:
            event_type = ion_event.event_type
            ion_type = ion_event.ion_type

            if event_type is IonEventType.CONTAINER_END:
                if desc.name is not None or desc.name == TEXT_ION:
                    table = self.__context.catalog.resolve(desc.name, desc.version, desc.max_id)
                    imports.append(table)

                break

            if event_type is IonEventType.CONTAINER_START:
                # We need to skip past this container (ignoring the end container event).
                self.__skip_container()
            else:
                field_name = self.__context.resolve(ion_event.field_name).text
                if field_name == TEXT_NAME and ion_type is IonType.STRING:
                    desc.name = ion_event.value
                elif field_name == TEXT_VERSION and ion_type is IonType.INT:
                    desc.version = ion_event.value
                elif field_name == TEXT_MAX_ID and ion_type is IonType.INT:
                    desc.max_id = ion_event.value

            ion_event = next(self.__reader_iter)

    def __read_imports(self, imports):
        ion_event = None
        while True:
            if ion_event is not None:
                event_type = ion_event.event_type
                ion_type = ion_event.ion_type

                if event_type is IonEventType.CONTAINER_END:
                    break

                if event_type is IonEventType.CONTAINER_START:
                    if ion_type is IonType.STRUCT:
                        self.__read_import_desc(imports)
                    else:
                        # We need to skip past this container (ignoring the end container event).
                        self.__skip_container()

            ion_event = next(self.__reader_iter)

    def __read_local_symbol_table(self):
        symbols = []
        imports = None

        ion_event = None
        while True:
            if ion_event is not None:
                if ion_event.event_type is IonEventType.CONTAINER_END:
                    break

                event_type = ion_event.event_type
                ion_type = ion_event.ion_type
                if ion_event.field_name is None:
                    field_name = None
                else:
                    field_name = self.__context.resolve(ion_event.field_name).text

                if event_type is IonEventType.CONTAINER_START:
                    if field_name == TEXT_SYMBOLS and ion_type is IonType.LIST:
                        # Yield to handling local symbol list.
                        self.__read_symbols_list(symbols)

                    elif field_name == TEXT_IMPORTS and ion_type is IonType.LIST:
                        # Yield to handling imports.
                        imports = []
                        self.__read_imports(imports)

                    else:
                        # We need to skip past this container (ignoring the end container event).
                        self.__skip_container()
                elif ion_type == IonType.SYMBOL \
                        and field_name == TEXT_IMPORTS \
                        and self.__context.resolve(ion_event.value).text == TEXT_ION_SYMBOL_TABLE:
                    if self.__context.symbol_table.table_type.is_system:
                        # Force the imports to nothing (system tables import implicitly).
                        imports = None
                    else:
                        # Set the imports to the previous local symbol table.
                        imports = [self.__context.symbol_table]

            ion_event = next(self.__reader_iter)

        # Construct the resulting context and terminate the processing.
        symbol_table = SymbolTable(LOCAL_TABLE_TYPE, symbols, imports=imports)
        self.__context = _ManagedContext(self.__context.catalog, symbol_table)

    def __resolve_symbols(self, ion_event):
        field_name = ion_event.field_name
        if field_name is not None:
            field_name = self.__context.resolve(field_name)

        annotations = ion_event.annotations
        if annotations:
            annotations = tuple(self.__context.resolve(annotation) for annotation in annotations)

        value = ion_event.value
        if ion_event.ion_type is IonType.SYMBOL and value is not None:
            value = self.__context.resolve(value)

        return IonEvent(
            ion_event.event_type,
            ion_event.ion_type,
            value,
            field_name,
            annotations,
            ion_event.depth
        )

    def __iter__(self):
        while True:
            ion_event = next(self.__reader_iter)
            if ion_event is not None:
                event_type = ion_event.event_type
                ion_type = ion_event.ion_type
                depth = ion_event.depth

                # System values only happen at the top-level
                if depth == 0:
                    if event_type is IonEventType.VERSION_MARKER:
                        if ion_event != ION_VERSION_MARKER_EVENT:
                            raise IonException('Invalid IVM: %s' % (ion_event,))
                        # Reset and swallow IVM
                        self.__context = _ManagedContext(self.__context.catalog)
                        continue

                    elif ion_type is IonType.SYMBOL \
                            and len(ion_event.annotations) == 0 \
                            and ion_event.value is not None \
                            and self.__context.resolve(ion_event.value).text == TEXT_ION_1_0:
                        # A faux IVM is a NOP
                        continue

                    elif event_type is IonEventType.CONTAINER_START \
                            and ion_type is IonType.STRUCT \
                            and self.__context.has_symbol_table_annotation(ion_event.annotations):
                        # Activate a new symbol processor.
                        self.__read_local_symbol_table()
                        continue

                # No system processing or we have to get data, yield control.
                if ion_event is not None:
                    if ion_event.event_type is IonEventType.STREAM_END:
                        break
                    ion_event = self.__resolve_symbols(ion_event)
                yield ion_event

        yield ION_STREAM_END_EVENT


class ReaderBinary:
    def __init__(self, fp, catalog=None):
        self.__reader = _ReaderManaged(_ReaderBinaryRaw(fp), catalog=catalog)

    def __iter__(self):
        reader_iter = iter(self.__reader)
        for ion_event in reader_iter:
            yield ion_event

