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

"""Provides symbol table managed processing for Ion readers."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .core import IonEventType, IonType, IonThunkEvent, MemoizingThunk, Transition, \
                  ION_VERSION_MARKER_EVENT
from .exceptions import IonException
from .reader import NEXT_EVENT, SKIP_EVENT
from .symbols import SymbolTable, SymbolTableCatalog, \
                     LOCAL_TABLE_TYPE, SYSTEM_SYMBOL_TABLE, \
                     TEXT_ION, TEXT_ION_1_0, TEXT_ION_SYMBOL_TABLE, TEXT_SYMBOLS, TEXT_IMPORTS, \
                     TEXT_NAME, TEXT_VERSION, TEXT_MAX_ID
from .util import coroutine, record, Enum


class _ManagedContext(record('catalog', ('symbol_table', SYSTEM_SYMBOL_TABLE))):
    """Context for the managed reader.

    Args:
        catalog (SymbolTableCatalog): The catalog for this context.
        symbol_table (SymbolTable): The symbol table.
    """
    def resolve(self, token):
        """Attempts to resolve the :class:`SymbolToken` against the current table.

        If the ``text`` is not None, the token is returned, otherwise, a token
        in the table is attempted to be retrieved.  If not token is found, then
        this method will raise.
        """
        if token.text is not None:
            return token
        resolved_token = self.symbol_table.get(token.sid, None)
        if resolved_token is None:
            raise IonException('Out of range SID: %d' % token.sid)
        return resolved_token

    def has_symbol_table_annotation(self, annotations):
        if len(annotations) == 0:
            return False

        first = self.resolve(annotations[0])
        return first.text == TEXT_ION_SYMBOL_TABLE


class _IonManagedThunkEvent(IonThunkEvent):
    """An :class:`IonEvent` whose ``value`` field is a thunk."""
    def __new__(cls, event_type, ion_type, value, field_name, annotations, depth):
        return super(_IonManagedThunkEvent, cls).__new__(
            cls, event_type, ion_type,
            value, MemoizingThunk(field_name), MemoizingThunk(annotations),
            depth
        )

    @property
    def field_name(self):
        # We're masking the field_name attribute, this gets around that.
        return self[3]()

    @property
    def annotations(self):
        # We're masking the annotations attribute, this gets around that.
        return self[4]()

    def derive_annotations(self, annotations):
        annotations_thunk = annotations
        if not callable(annotations):
            def annotations_thunk():
                return annotations

        return IonThunkEvent.derive_annotations(self, annotations_thunk)

    def derive_field_name(self, field_name):
        field_name_thunk = field_name
        if not callable(field_name):
            def field_name_thunk():
                return field_name

        return IonThunkEvent.derive_field_name(self, field_name_thunk)


def _managed_thunk_event(ctx, ion_event):
    event_type = ion_event.event_type
    ion_type = ion_event.ion_type

    def field_name_thunk():
        field_name = ion_event.field_name
        if field_name is not None:
            field_name = ctx.resolve(field_name)
        return field_name

    def annotations_thunk():
        return tuple(ctx.resolve(annotation) for annotation in ion_event.annotations)

    def value_thunk():
        value = ion_event.value
        if ion_type is IonType.SYMBOL and value is not None:
            value = ctx.resolve(value)
        return value

    return _IonManagedThunkEvent(
        event_type,
        ion_type,
        value_thunk,
        field_name_thunk,
        annotations_thunk,
        ion_event.depth
    )


@coroutine
def _symbols_handler(symbols, whence):
    ion_event, self = yield
    while True:
        event_type = ion_event.event_type
        ion_type = ion_event.ion_type

        if event_type is IonEventType.CONTAINER_END:
            yield Transition(NEXT_EVENT, whence)

        if event_type is IonEventType.CONTAINER_START:
            # We need to skip past this container (ignoring the end container event).
            ion_event, _ = yield Transition(SKIP_EVENT, self)

        if ion_type is IonType.STRING:
            # Add the potentially null text.
            symbols.append(ion_event.value)

        ion_event, _ = yield Transition(NEXT_EVENT, self)


class _ImportDesc(object):
    __slots__ = [TEXT_NAME, TEXT_VERSION, TEXT_MAX_ID]

    def __init__(self, name=None, version=1, max_id=None):
        self.name = name
        self.version = version
        self.max_id = max_id

    def __str__(self):
        return '_ImportDesc(%s, %s, %s)' % (self.name, self.version, self.max_id)


@coroutine
def _import_desc_handler(ctx, imports, whence):
    ion_event, self = yield

    desc = _ImportDesc()
    while True:
        event_type = ion_event.event_type
        ion_type = ion_event.ion_type

        if event_type is IonEventType.CONTAINER_END:
            if desc.name is not None or desc.name == TEXT_ION:
                table = ctx.catalog.resolve(desc.name, desc.version, desc.max_id)
                imports.append(table)

            yield Transition(NEXT_EVENT, whence)

        if event_type is IonEventType.CONTAINER_START:
            # We need to skip past this container (ignoring the end container event).
            ion_event, _ = yield Transition(SKIP_EVENT, self)
        else:
            field_name = ctx.resolve(ion_event.field_name).text
            if field_name == TEXT_NAME and ion_type is IonType.STRING:
                desc.name = ion_event.value
            elif field_name == TEXT_VERSION and ion_type is IonType.INT:
                desc.version = ion_event.value
            elif field_name == TEXT_MAX_ID and ion_type is IonType.INT:
                desc.max_id = ion_event.value

        ion_event, _ = yield Transition(NEXT_EVENT, self)


@coroutine
def _imports_handler(ctx, imports, whence):
    ion_event, self = yield
    while True:
        event_type = ion_event.event_type
        ion_type = ion_event.ion_type

        if event_type is IonEventType.CONTAINER_END:
            yield Transition(NEXT_EVENT, whence)

        trans = Transition(NEXT_EVENT, self)
        if event_type is IonEventType.CONTAINER_START:
            if ion_type is IonType.STRUCT:
                trans = Transition(NEXT_EVENT, _import_desc_handler(ctx, imports, self))
            else:
                # We need to skip past this container (ignoring the end container event).
                ion_event, _ = yield Transition(SKIP_EVENT, self)

        ion_event, _ = yield trans


@coroutine
def _local_symbol_table_handler(ctx):
    symbols = []
    imports = None

    ion_event, self = yield
    while True:
        if ion_event.event_type is IonEventType.CONTAINER_END:
            break

        event_type = ion_event.event_type
        ion_type = ion_event.ion_type
        if ion_event.field_name is None:
            field_name = None
        else:
            field_name = ctx.resolve(ion_event.field_name).text

        trans = Transition(NEXT_EVENT, self)
        if event_type is IonEventType.CONTAINER_START:
            if field_name == TEXT_SYMBOLS and ion_type is IonType.LIST:
                # Yield to handling local symbol list.
                trans = Transition(NEXT_EVENT, _symbols_handler(symbols, self))

            elif field_name == TEXT_IMPORTS and ion_type is IonType.LIST:
                # Yield to handling imports.
                imports = []
                trans = Transition(NEXT_EVENT, _imports_handler(ctx, imports, self))

            else:
                # We need to skip past this container (ignoring the end container event).
                ion_event, _ = yield Transition(SKIP_EVENT, self)
        elif ion_type == IonType.SYMBOL \
                and field_name == TEXT_IMPORTS \
                and ctx.resolve(ion_event.value).text == TEXT_ION_SYMBOL_TABLE:
            if ctx.symbol_table.table_type.is_system:
                # Force the imports to nothing (system tables import implicitly).
                imports = None
            else:
                # Set the imports to the previous local symbol table.
                imports = [ctx.symbol_table]

        ion_event, _ = yield trans

    # Construct the resulting context and terminate the processing.
    symbol_table = SymbolTable(LOCAL_TABLE_TYPE, symbols, imports=imports)
    yield Transition(_ManagedContext(ctx.catalog, symbol_table), None)


@coroutine
def managed_reader(reader, catalog=None):
    """Managed reader wrapping another reader.

    Args:
        reader (Coroutine): The underlying non-blocking reader co-routine.
        catalog (Optional[SymbolTableCatalog]): The catalog to use for resolving imports.

    Yields:
        Events from the underlying reader delegating to symbol table processing as needed.
        The user will never see things like version markers or local symbol tables.
    """
    if catalog is None:
        catalog = SymbolTableCatalog()

    ctx = _ManagedContext(catalog)
    symbol_trans = Transition(None, None)
    ion_event = None
    while True:
        if symbol_trans.delegate is not None \
                and ion_event is not None \
                and not ion_event.event_type.is_stream_signal:
            # We have a symbol processor active, do not yield to user.
            delegate = symbol_trans.delegate
            symbol_trans = delegate.send(Transition(ion_event, delegate))
            if symbol_trans.delegate is None:
                # When the symbol processor terminates, the event is the context
                # and there is no delegate.
                ctx = symbol_trans.event
                data_event = NEXT_EVENT
            else:
                data_event = symbol_trans.event
        else:
            data_event = None

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
                        ctx = _ManagedContext(ctx.catalog)
                        data_event = NEXT_EVENT

                    elif ion_type is IonType.SYMBOL \
                            and len(ion_event.annotations) == 0 \
                            and ion_event.value is not None \
                            and ctx.resolve(ion_event.value).text == TEXT_ION_1_0:
                        assert symbol_trans.delegate is None

                        # A faux IVM is a NOP
                        data_event = NEXT_EVENT

                    elif event_type is IonEventType.CONTAINER_START \
                            and ion_type is IonType.STRUCT \
                            and ctx.has_symbol_table_annotation(ion_event.annotations):
                        assert symbol_trans.delegate is None

                        # Activate a new symbol processor.
                        delegate = _local_symbol_table_handler(ctx)
                        symbol_trans = Transition(None, delegate)
                        data_event = NEXT_EVENT

            if data_event is None:
                # No system processing or we have to get data, yield control.
                if ion_event is not None:
                    ion_event = _managed_thunk_event(ctx, ion_event)
                data_event = yield ion_event

        ion_event = reader.send(data_event)


