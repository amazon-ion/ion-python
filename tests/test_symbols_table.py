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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import itertools
import pytest

import tests

import amazon.ion.symbols as symbols

from amazon.ion.util import record


def test_symbol_token_no_text_or_sid():
    with pytest.raises(ValueError):
        symbols.SymbolToken(None, None)


def test_system_symbols():
    sys_table = symbols.SYSTEM_SYMBOL_TABLE

    assert 9 == len(sys_table)
    assert u'$ion' == sys_table.name
    assert 1 == sys_table.version

    with pytest.raises(TypeError):
        sys_table.intern(u'moo')

    with pytest.raises(ValueError):
        symbols.SymbolTable(
            table_type=symbols.SYSTEM_TABLE_TYPE,
            symbols=(),
            name=u'my_ion',
            version=1
        )

    with pytest.raises(ValueError):
        symbols.SymbolTable(
            table_type=symbols.SYSTEM_TABLE_TYPE,
            symbols=(),
            name=u'$ion',
            version=2,
            imports=sys_table
        )


class _P(record('name', 'version', 'symbols', ('exc', ValueError))):
    def __str__(self):
        return  '{p.name!r}, {p.version}, {p.symbols!r}'.format(p = self)


@tests.parametrize(
    _P(name=None, version=None, symbols=[]),
    _P(name=None, version=1, symbols=[]),
    _P(name=u'my_shared', version=None, symbols=[]),
    _P(name=u'my_shared', version=0, symbols=[]),
    _P(name=u'my_shared', version=-1, symbols=[]),
    _P(name=b'my_shared', version=1, symbols=[], exc=TypeError),
    _P(name=u'my_shared', version=1, symbols=[b'a'], exc=TypeError),
)
def test_shared_symbls_malformed(p):
    with pytest.raises(p.exc):
        symbols.shared_symbol_table(p.name, p.version, p.symbols)


FOO_TEXTS = (u'a', u'b', u'c')
FOO_TABLE = symbols.shared_symbol_table(
    name=u'foo',
    version=1,
    symbols=FOO_TEXTS
)

BAR_NEW_TEXTS = (u'c', u'd', u'e')
BAR_TEXTS = FOO_TEXTS + BAR_NEW_TEXTS
BAR_TABLE = symbols.shared_symbol_table(
    name=u'bar',
    version=2,
    symbols=BAR_NEW_TEXTS,
    imports=(FOO_TABLE,)
)

PLACEHOLDER = symbols.placeholder_symbol_table(u'placeholder', 1, 10)
PLACEHOLDER_TEXTS = tuple(itertools.repeat(None, 10))

SUB_SOURCE_LESS_SYMBOLS = symbols.substitute_symbol_table(BAR_TABLE, 3, 10)
SUB_SOURCE_LESS_TEXTS = tuple(itertools.chain(BAR_TEXTS, itertools.repeat(None, 4)))

SUB_SOURCE_MORE_SYMBOLS = symbols.substitute_symbol_table(BAR_TABLE, 1, 4)
SUB_SOURCE_MORE_TEXTS = BAR_TEXTS[:4]

SUB_SOURCE_EQUAL_SYMBOLS = symbols.substitute_symbol_table(BAR_TABLE, 2, 6)
SUB_SOURCE_EQUAL_TEXTS = BAR_TEXTS


class _P(record('desc', 'table', 'name', 'version', 'is_substitute', 'symbol_texts')):
    def __str__(self):
        return  self.desc


@tests.parametrize(
    _P(
        desc='FOO_TABLE',
        table=FOO_TABLE,
        name=u'foo',
        version=1,
        is_substitute=False,
        symbol_texts=FOO_TEXTS
    ),
    _P(
        desc='BAR_TABLE',
        table=BAR_TABLE,
        name=u'bar',
        version=2,
        is_substitute=False,
        symbol_texts=BAR_TEXTS
    ),
    _P(
        desc='PLACEHOLDER',
        table=PLACEHOLDER,
        name=u'placeholder',
        version=1,
        is_substitute=True,
        symbol_texts=PLACEHOLDER_TEXTS
    ),
    _P(
        desc='SUB_SOURCE_LESS',
        table=SUB_SOURCE_LESS_SYMBOLS,
        name=u'bar',
        version=3,
        is_substitute=True,
        symbol_texts=SUB_SOURCE_LESS_TEXTS
    ),
    _P(
        desc='SUB_SOURCE_MORE',
        table=SUB_SOURCE_MORE_SYMBOLS,
        name=u'bar',
        version=1,
        is_substitute=True,
        symbol_texts=SUB_SOURCE_MORE_TEXTS
    ),
    _P(
        desc='SUB_SOURCE_EQUALS',
        table=SUB_SOURCE_EQUAL_SYMBOLS,
        name=u'bar',
        version=2,
        is_substitute=True,
        symbol_texts=SUB_SOURCE_EQUAL_TEXTS),
)
def test_shared_symbols(p):
    assert p.name == p.table.name
    assert p.version == p.table.version
    assert len(p.symbol_texts) == len(p.table)
    assert p.is_substitute == p.table.is_substitute

    text_iter = iter(p.symbol_texts)
    curr_sid = 1
    for token in p.table:
        text = next(text_iter)
        assert text == token.text
        assert curr_sid == token.sid
        assert symbols.ImportLocation(p.name, curr_sid) == token.location
        curr_sid += 1

    assert None == p.table.get(u'z')
    with pytest.raises(KeyError):
        p.table[u'z']
    assert None == p.table.get(1024)
    with pytest.raises(KeyError):
        p.table[1024]

    seen = set()
    curr_id = 1
    for text in p.symbol_texts:
        token = p.table[curr_id]
        assert text == token.text
        if text is not None:
            if text not in seen:
                seen.add(text)
                assert token == p.table[text]
            else:
                mapped_token = p.table[text]
                assert token != mapped_token
                assert token.sid > mapped_token.sid
        curr_id += 1

    assert symbols.SYMBOL_ZERO_TOKEN == p.table[0]


def test_shared_symbols_intern():
    with pytest.raises(TypeError):
        FOO_TABLE.intern(u'hello')


class _P(record('name', 'version')):
    def __str__(self):
        return '{p.name!r} {p.version!r}'.format(p=self)


@tests.parametrize(
    _P(u'local_name', None),
    _P(None, 1),
    _P(u'local_name', 1)
)
def test_local_symbols_malformed(p):
    with pytest.raises(ValueError):
        symbols.SymbolTable(
            table_type=symbols.LOCAL_TABLE_TYPE,
            symbols=[u'something'],
            name=p.name,
            version=p.version
        )


COLLIDE_TABLE = symbols.shared_symbol_table(
    name=u'collide',
    version=3,
    symbols=tuple(x.text for x in symbols._SYSTEM_SYMBOL_TOKENS)
)

_T = symbols.SymbolToken
_L = functools.partial(symbols.ImportLocation, FOO_TABLE.name)
_SID_START = len(symbols.SYSTEM_SYMBOL_TABLE) + 1
_COLLIDE_SID_START = len(symbols.SYSTEM_SYMBOL_TABLE) + len(COLLIDE_TABLE) + 1


class _P(record('desc', 'symbol_texts', 'tokens', ('imports', ()), ('symbol_count_override', None))):
    def __str__(self):
        return self.desc


@tests.parametrize(
    _P(
        desc='DISTINCT',
        symbol_texts=[u'a', u'b', u'c'],
        tokens=[
            _T(u'a', _SID_START + 0),
            _T(u'b', _SID_START + 1),
            _T(u'c', _SID_START + 2),
        ],
    ),
    _P(
        desc='DUPLICATE',
        symbol_texts=[u'a', u'a', u'c'],
        tokens=[
            _T(u'a', _SID_START + 0),
            _T(u'c', _SID_START + 1),
        ],
    ),
    _P(
        desc='DUPLICATE SYSTEM',
        symbol_texts=[u'name', u'version', u'c'],
        tokens=[
            _T(u'c', _SID_START + 0),
        ],
    ),
    _P(
        desc='IMPORT DISTINCT',
        symbol_texts=[u'x', u'y', u'z'],
        imports=[
            FOO_TABLE,
        ],
        tokens=[
            _T(u'a', _SID_START + 0, _L(1)),
            _T(u'b', _SID_START + 1, _L(2)),
            _T(u'c', _SID_START + 2, _L(3)),
            _T(u'x', _SID_START + 3),
            _T(u'y', _SID_START + 4),
            _T(u'z', _SID_START + 5),
        ],
    ),
    _P(
        desc='IMPORT DUP',
        symbol_texts=[u'c', u'y', u'z'],
        imports=[
            FOO_TABLE,
        ],
        tokens=[
            _T(u'a', _SID_START + 0, _L(1)),
            _T(u'b', _SID_START + 1, _L(2)),
            _T(u'c', _SID_START + 2, _L(3)),
            _T(u'y', _SID_START + 3),
            _T(u'z', _SID_START + 4),
        ],
    ),
    _P(
        desc='IMPORT DUP SYSTEM',
        symbol_texts=[u'c', u'imports', u'z'],
        imports=[
            COLLIDE_TABLE,
            FOO_TABLE,
        ],
        tokens=[
            _T(u'a', _COLLIDE_SID_START + 0, _L(1)),
            _T(u'b', _COLLIDE_SID_START + 1, _L(2)),
            _T(u'c', _COLLIDE_SID_START + 2, _L(3)),
            _T(u'z', _COLLIDE_SID_START + 3),
        ],
        symbol_count_override=len(COLLIDE_TABLE) + len(FOO_TABLE) + 1
    ),
)
def test_local_symbols(p):
    table = symbols.local_symbol_table(p.imports)
    for text in p.symbol_texts:
        table.intern(text)

    sys_table = symbols.SYSTEM_SYMBOL_TABLE
    if p.symbol_count_override is not None:
        user_symbol_count = p.symbol_count_override
    else:
        user_symbol_count = len(p.tokens)
    assert (user_symbol_count + len(sys_table)) == len(table)

    assert symbols.SYMBOL_ZERO_TOKEN

    for token in itertools.chain(sys_table, p.tokens):
        assert token == table[token.text]
        assert token == table[token.sid]

    with pytest.raises(KeyError):
        table[-1]

    with pytest.raises(KeyError):
        table[u'no_such_symbol']

    with pytest.raises(KeyError):
        table[1000]

