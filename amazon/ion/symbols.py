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

"""Provides support for Ion symbol tokens."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

from itertools import chain
from itertools import islice
from itertools import repeat

from .exceptions import CannotSubstituteTable
from .util import record

TEXT_ION = u'$ion'
TEXT_ION_1_0 = u'$ion_1_0'
TEXT_ION_SYMBOL_TABLE = u'$ion_symbol_table'
TEXT_NAME = u'name'
TEXT_VERSION = u'version'
TEXT_IMPORTS = u'imports'
TEXT_SYMBOLS = u'symbols'
TEXT_MAX_ID = u'max_id'
TEXT_ION_SHARED_SYMBOL_TABLE = u'$ion_shared_symbol_table'

SID_ION = 1
SID_ION_1_0 = 2
SID_ION_SYMBOL_TABLE = 3
SID_NAME = 4
SID_VERSION = 5
SID_IMPORTS = 6
SID_SYMBOLS = 7
SID_MAX_ID = 8
SID_ION_SHARED_SYMBOL_TABLE = 9


class ImportLocation(record('name', 'position')):
    """Represents the import location of a symbol token.

    An import location can be thought of as a position independent address of an imported symbol.
    This metadata, if defined, can indicate identity of a symbol that came from a shared symbol
    token that the application does not have access to.

    Note:
        Version is nor part of an import location because a position in a shared table by name
        uniquely identifies that slot irrespective of version.

    Args:
        name (unicode): The name of the shared symbol table.
        position (int): The position in the shared symbol table.
    """


class SymbolToken(record('text', 'sid', 'location')):
    """Representation of a *symbolic token*.

    A symbolic token may be a *part* of an Ion value in several contexts:

    * The field name of a value in a ``struct``
    * An annotation of a value.
    * A scalar of type ``symbol`` in the Ion data model.

    Args:
        text (Optional[unicode]): The text image of the token.
        sid  (Optional[int]): The local symbol ID of the token.
        location (Optional[ImportTableLocation]): The import source of the token.

    Note:
        At least one of ``text`` or ``sid`` should be non-``None``
    """
    def __new__(cls, text, sid, location=None):
        if text is None and sid is None:
            raise ValueError('SymbolToken must specify at least one of text or sid')
        return super(SymbolToken, cls).__new__(cls, text, sid, location)


def _system_symbol_token(text, sid):
    """Defines an Ion 1.0 system symbol token."""
    return SymbolToken(text, sid, ImportLocation(TEXT_ION, sid))


SYMBOL_ZERO_TOKEN = SymbolToken(None, 0)

_SYSTEM_SYMBOL_TOKENS = (
    _system_symbol_token(TEXT_ION, SID_ION),
    _system_symbol_token(TEXT_ION_1_0, SID_ION_1_0),
    _system_symbol_token(TEXT_ION_SYMBOL_TABLE, SID_ION_SYMBOL_TABLE),
    _system_symbol_token(TEXT_NAME, SID_NAME),
    _system_symbol_token(TEXT_VERSION, SID_VERSION),
    _system_symbol_token(TEXT_IMPORTS, SID_IMPORTS),
    _system_symbol_token(TEXT_SYMBOLS, SID_SYMBOLS),
    _system_symbol_token(TEXT_MAX_ID, SID_MAX_ID),
    _system_symbol_token(TEXT_ION_SHARED_SYMBOL_TABLE, SID_ION_SHARED_SYMBOL_TABLE),
)


class _SymbolTableType(record('is_system', 'is_shared', 'is_local')):
    """A set of flags indicating attributes of a symbol table.

    Args:
        is_system (bool): Whether or not the symbol table is a system table.
        is_shared (bool): Whether or not the symbol table is a is_shared table.
        is_local (bool): Whether or not the symbol table is is_local.
    """

SYSTEM_TABLE_TYPE = _SymbolTableType(is_system=True, is_shared=True, is_local=False)
SHARED_TABLE_TYPE = _SymbolTableType(is_system=False, is_shared=True, is_local=False)
LOCAL_TABLE_TYPE = _SymbolTableType(is_system=False, is_shared=False, is_local=True)


class SymbolTable(object):
    """A collection of symbols that is ordered.

    Symbol tables are basically an Unicode string to integer interning table.

    A few things to consider about symbol tables:

    * System symbol tables never have imports and are shared symbol tables themselves.
    * Shared symbol tables never import the system symbol table.
    * Local symbol tables implicitly import the system symbol table.

    Shared symbol tables have tokens that always have a ``location`` attribute referring to themselves.
    Local symbol tables have tokens whose ``location`` attribute refers to **either** the shared symbol
    that it was imported from or ``None`` if the symbol was defined locally.

    Note:
        Shared symbol tables (which include system symbol tables) are immutable.
        Local symbol tables support interning as a mutable operation.
        The implementation doesn't enforce making properties read-only to enforce this invariant.

    Args:
        table_type (_SymbolTableType): The type of symbol table.
        symbols (Iterable[unicode]): The symbols text associated *locally* to this table.
        imports (Optional[Iterable[SymbolTable]]): The imports of the table.
        name (Optional[unicode]): The name of this table.  Required for shared symbol tables.
        version (Optional[int]): The version of this table.  Required for shared symbol tables.
        is_substitute (Optional[bool]): Whether or not this table is substituted.  A substituted symbol
                table is one that is not resolvable and has placeholder entries.
    """
    def __init__(self, table_type, symbols, name=None, version=None, imports=None, is_substitute=False):
        if table_type.is_system and imports is not None:
            raise ValueError('System tables cannot have imports')
        if table_type.is_shared and (name is None or version is None or version <= 0):
            raise ValueError('Shared symbol tables must have a name and >= 1 version')
        if table_type.is_local and (name is not None or version is not None):
            raise ValueError('Local symbol tables cannot have a name or version')
        if table_type.is_system and (name != TEXT_ION):
            raise ValueError('System symbol tables must be named "%s"' % TEXT_ION)
        if name is not None and not isinstance(name, six.text_type):
            raise TypeError('Shared symbol tables must have a unicode name: %r' % name)

        self.table_type = table_type
        self.name = name
        self.version = version
        self.imports = imports
        self.is_substitute = is_substitute
        self.max_id = 0
        self.__symbols = []
        self.__mapping = {}

        if table_type.is_local or table_type.is_system:
            for token in _SYSTEM_SYMBOL_TOKENS:
                self.__add(token)
            self.max_id = len(_SYSTEM_SYMBOL_TOKENS)

        if imports is not None:
            for table in imports:
                for token in table:
                    if table_type.is_shared:
                        self.__add_shared(token)
                    else:
                        if not table.table_type.is_local \
                                or token.location is None \
                                or token.location.name != TEXT_ION:
                            # TODO Determine if this code should handle LST as import.
                            # If the import is a local symbol table, we need to ignore system
                            # imports.  This supports LST append.
                            self.__add_import(token)

        # System symbols are bootstrapped
        if not table_type.is_system:
            for symbol in symbols:
                self.__add_text(symbol)

    def __import_location(self, sid):
        """Returns a location for this table's SID.

        Only meaningful for shared tables.
        """
        return ImportLocation(self.name, sid)

    def __new_sid(self):
        """Allocates a new local SID."""
        self.max_id += 1
        sid = self.max_id
        return sid

    def __add(self, token):
        """Unconditionally adds a token to the table."""
        self.__symbols.append(token)
        text = token.text
        if text is not None and text not in self.__mapping:
            self.__mapping[text] = token

    def __add_shared(self, original_token):
        """Adds a token, normalizing the SID and import reference to this table."""
        sid = self.__new_sid()
        token = SymbolToken(original_token.text, sid, self.__import_location(sid))
        self.__add(token)
        return token

    def __add_import(self, original_token):
        """Adds a token, normalizing only the SID"""
        sid = self.__new_sid()
        token = SymbolToken(original_token.text, sid, original_token.location)
        self.__add(token)
        return token

    def __add_text(self, text):
        """Adds the given Unicode text as a locally defined symbol."""
        if text is not None and not isinstance(text, six.text_type):
            raise TypeError('Local symbol definition must be a Unicode sequence or None: %r' % text)
        sid = self.__new_sid()
        location = None
        if self.table_type.is_shared:
            location = self.__import_location(sid)
        token = SymbolToken(text, sid, location)
        self.__add(token)
        return token

    def intern(self, text):
        """Interns the given Unicode sequence into the symbol table.

        Note:
            This operation is only valid on local symbol tables.

        Args:
            text (unicode): The target to intern.

        Returns:
            SymbolToken: The mapped symbol token which may already exist in the table.
        """
        if self.table_type.is_shared:
            raise TypeError('Cannot intern on shared symbol table')
        if not isinstance(text, six.text_type):
            raise TypeError('Cannot intern non-Unicode sequence into symbol table: %r' % text)

        token = self.get(text)
        if token is None:
            token = self.__add_text(text)
        return token

    def get(self, key, default=None):
        """Returns a token by text or local ID, with a default.

        A given text image may be associated with more than one symbol ID.  This will return the first definition.

        Note:
            User defined symbol IDs are always one-based.  Symbol zero is a special symbol that
            always has no text.

        Args:
            key (unicode | int):  The key to lookup.
            default(Optional[SymbolToken]): The default to return if the key is not found

        Returns:
            SymbolToken: The token associated with the key or the default if it doesn't exist.
        """
        if isinstance(key, six.text_type):
            return self.__mapping.get(key, None)
        if not isinstance(key, int):
            raise TypeError('Key must be int or Unicode sequence.')

        # TODO determine if $0 should be returned for all symbol tables.
        if key == 0:
            return SYMBOL_ZERO_TOKEN

        # Translate one-based SID to zero-based intern table
        index = key - 1
        if index < 0 or key > len(self):
            return default
        return self.__symbols[index]

    def __getitem__(self, key):
        """Returns a token by text or local ID.

        Args:
            key (unicode | int): The text or ID to lookup.

        Returns:
            SymbolToken: The token associated with the key.

        Raises:
            KeyError: If the key is not in the table.

        See Also:
            :meth:`get()`
        """
        token = self.get(key)
        if token is None:
            raise KeyError('No symbol for: %s' % key)
        return token

    def __len__(self):
        """Returns the number of symbols within the table."""
        return len(self.__symbols)

    def __iter__(self):
        """Iterator over the table's tokens.

        Returns:
            Iterable[SymbolToken]: The tokens in this table in defined order.
        """
        return iter(self.__symbols)

    def __eq__(self, other):
        """Compares two symbol tables together.

        Two symbol tables are considered equal if the underlying tokens are the same and the
        ``table_type``, ``name``, ``version``, and ``is_substitute`` attributes are defined and are equal.

        Note:
            This is implemented using ``getattr`` to allow duck-typed table implementations to compare.
            Any custom symbol table-like implementation should implement this method accordingly.

            Things that are not compared:

             * ``imports`` are never compared as they are denormalized into the tokens of the table.
             * ``max_id`` is never compared as its an invariant that it matches the iteration.
        """
        if self is other:
            return True

        # Compare relevant attributes
        if self.table_type != getattr(other, 'table_type', None):
            return False
        if self.name != getattr(other, 'name', None):
            return False
        if self.version != getattr(other, 'version', None):
            return False
        if self.is_substitute != getattr(other, 'is_substitute', None):
            return False

        # Compare tokens.
        other_iter = getattr(other, '__iter__')
        if not callable(other_iter):
            return False
        for token, other_token in six.moves.zip_longest(self, other):
            if token != other_token:
                return False

        return True

    def __ne__(self, other):
        return not self == other

SYSTEM_SYMBOL_TABLE = SymbolTable(
    table_type=SYSTEM_TABLE_TYPE,
    symbols=_SYSTEM_SYMBOL_TOKENS,
    name=TEXT_ION,
    version=1
)


def local_symbol_table(imports=None, symbols=()):
    """Constructs a local symbol table.

    Args:
        imports (Optional[SymbolTable]): Shared symbol tables to import.
        symbols (Optional[Iterable[Unicode]]): Initial local symbols to add.

    Returns:
        SymbolTable: A mutable local symbol table with the seeded local symbols.
    """
    return SymbolTable(
        table_type=LOCAL_TABLE_TYPE,
        symbols=symbols,
        imports=imports
    )


def shared_symbol_table(name, version, symbols, imports=None):
    """Constructs a shared symbol table.

    Args:
        name (unicode): The name of the shared symbol table.
        version (int): The version of the shared symbol table.
        symbols (Iterable[unicode]): The symbols to associate with the table.
        imports (Optional[Iterable[SymbolTable]): The shared symbol tables to inject into this one.

    Returns:
        SymbolTable: The constructed table.
    """
    return SymbolTable(
        table_type=SHARED_TABLE_TYPE,
        symbols=symbols,
        name=name,
        version=version,
        imports=imports
    )


def placeholder_symbol_table(name, version, max_id):
    """Constructs a shared symbol table that consists symbols that all have no known text.

    This is generally used for cases where a shared symbol table is not available by the
    application.

    Args:
        name (unicode): The name of the shared symbol table.
        version (int): The version of the shared symbol table.
        max_id (int): The maximum ID allocated by this symbol table, must be ``>= 0``

    Returns:
        SymbolTable: The synthesized table.
    """
    if version <= 0:
        raise ValueError('Version must be grater than or equal to 1: %s' % version)
    if max_id < 0:
        raise ValueError('Max ID must be zero or positive: %s' % max_id)

    return SymbolTable(
        table_type=SHARED_TABLE_TYPE,
        symbols=repeat(None, max_id),
        name=name,
        version=version,
        is_substitute=True
    )


def substitute_symbol_table(table, version, max_id):
    """Substitutes a given shared symbol table for another version.

    * If the given table has **more** symbols than the requested substitute, then the generated
      symbol table will be a subset of the given table.
    * If the given table has **less** symbols than the requested substitute, then the generated
      symbol table will have symbols with unknown text generated for the difference.

    Args:
        table (SymbolTable): The shared table to derive from.
        version (int): The version to target.
        max_id (int): The maximum ID allocated by the substitute, must be ``>= 0``.

    Returns:
        SymbolTable: The synthesized table.
    """
    if not table.table_type.is_shared:
        raise ValueError('Symbol table to substitute from must be a shared table')
    if version <= 0:
        raise ValueError('Version must be grater than or equal to 1: %s' % version)
    if max_id < 0:
        raise ValueError('Max ID must be zero or positive: %s' % max_id)

    # TODO Recycle the symbol tokens from the source table into the substitute.
    if max_id <= table.max_id:
        symbols = (token.text for token in islice(table, max_id))
    else:
        symbols = chain(
            (token.text for token in table),
            repeat(None, max_id - table.max_id)
        )

    return SymbolTable(
        table_type=SHARED_TABLE_TYPE,
        symbols=symbols,
        name=table.name,
        version=version,
        is_substitute=True
    )


class SymbolTableCatalog(object):
    """A collection of symbol tables that can be used to resolve imports.

    Note:
        The catalog will return a placeholder symbol table when resolving a table
        that doesn't exist. For tables that don't exist with any version, this placeholder
        will be completely devoid of text mappings.  For tables that exist with a non-exact version,
        a derived substitute will be generated.
    """
    def __init__(self):
        self.__tables = {}

    def register(self, table):
        """Adds a shared table to the catalog.

        Args:
            table (SymbolTable): A non-system, shared symbol table.
        """
        if table.table_type.is_system:
            raise ValueError('Cannot add system table to catalog')
        if not table.table_type.is_shared:
            raise ValueError('Cannot add local table to catalog')
        if table.is_substitute:
            raise ValueError('Cannot add substitute table to catalog')

        versions = self.__tables.get(table.name)
        if versions is None:
            versions = {}
            self.__tables[table.name] = versions
        versions[table.version] = table

    def resolve(self, name, version, max_id):
        """Resolves the table for a given name and version.

        Args:
            name (unicode): The name of the table to resolve.
            version (int): The version of the table to resolve.
            max_id (Optional[int]): The maximum ID of the table requested.
                May be ``None`` in which case an exact match on ``name`` and ``version``
                is required.

        Returns:
            SymbolTable: The *closest* matching symbol table.  This is either an exact match,
            a placeholder, or a derived substitute depending on what tables are registered.
        """
        if not isinstance(name, six.text_type):
            raise TypeError('Name must be a Unicode sequence: %r' % name)
        if not isinstance(version, int):
            raise TypeError('Version must be an int: %r' % version)
        if version <= 0:
            raise ValueError('Version must be positive: %s' % version)
        if max_id is not None and max_id < 0:
            raise ValueError('Max ID must be zero or positive: %s' % max_id)

        versions = self.__tables.get(name)
        if versions is None:
            if max_id is None:
                raise CannotSubstituteTable(
                    'Found no table for %s, but no max_id' % name
                )
            return placeholder_symbol_table(name, version, max_id)

        table = versions.get(version)
        if table is None:
            # TODO Replace the keys map with a search tree based dictionary.
            keys = list(versions)
            keys.sort()
            table = versions[keys[-1]]

        if table.version == version and (max_id is None or table.max_id == max_id):
            return table

        if max_id is None:
            raise CannotSubstituteTable(
                'Found match for %s, but not version %d, and no max_id' % (name, version)
            )

        return substitute_symbol_table(table, version, max_id)
