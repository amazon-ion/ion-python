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

from pytest import raises

from tests import parametrize, is_exception

import amazon.ion.symbols as symbols

from amazon.ion.exceptions import CannotSubstituteTable
from amazon.ion.util import record


FOO_1_TEXTS = (u'aa', u'bb', u'cc')
FOO_1_TABLE = symbols.shared_symbol_table(
    name=u'foo',
    version=1,
    symbols=FOO_1_TEXTS
)

# Note the one symbol symbol overlap.
FOO_2_TEXTS = (u'cc', u'dd', u'ee')
FOO_2_TABLE = symbols.shared_symbol_table(
    name=u'foo',
    version=2,
    imports=[FOO_1_TABLE],
    symbols=FOO_2_TEXTS
)

# Note the gap in version.
FOO_4_TEXTS = (u'ff', u'gg', u'hh')
FOO_4_TABLE = symbols.shared_symbol_table(
    name=u'foo',
    version=4,
    imports=[FOO_1_TABLE, FOO_2_TABLE],
    symbols=FOO_4_TEXTS
)

REGISTER_TABLES = (
    FOO_1_TABLE, FOO_2_TABLE, FOO_4_TABLE
)


class _P(record('desc', 'name', 'version', 'max_id', 'expected')):
    def __str__(self):
        return '{p.desc} - {p.name}, {p.version}, {p.max_id}'.format(p=self)


@parametrize(
    _P(
        desc='EXACT MATCH',
        name=u'foo',
        version=2,
        max_id=FOO_2_TABLE.max_id,
        expected=FOO_2_TABLE,
    ),
    _P(
        desc='NO MATCH',
        name=u'bar',
        version=1,
        max_id=10,
        expected=symbols.placeholder_symbol_table(name=u'bar', version=1, max_id=10),
    ),
    _P(
        desc='NAME MATCH (HIGHER VERSION)',
        name=u'foo',
        version=5,
        max_id=12,
        expected=symbols.substitute_symbol_table(FOO_4_TABLE, version=5, max_id=12),
    ),
    _P(
        desc='NAME MATCH (LOWER VERSION)',
        name=u'foo',
        version=3,
        max_id=7,
        expected=symbols.substitute_symbol_table(FOO_4_TABLE, version=3, max_id=7),
    ),
    _P(
        # TODO Determine if this is the correct behavior.
        desc='NAME/VERSION MATCH (HIGHER MAX_ID)',
        name=u'foo',
        version=2,
        max_id=15,
        expected=symbols.substitute_symbol_table(FOO_2_TABLE, version=2, max_id=15),
    ),
    _P(
        desc='NAME/VERSION MATCH (LOWER MAX_ID)',
        name=u'foo',
        version=2,
        max_id=4,
        expected=symbols.substitute_symbol_table(FOO_2_TABLE, version=2, max_id=4),
    ),
    _P(
        desc='EXACT MATCH, NO MAX ID',
        name=u'foo',
        version=2,
        max_id=None,
        expected=FOO_2_TABLE,
    ),
    _P(
        desc='NO MATCH, NO MAX ID',
        name=u'bar',
        version=1,
        max_id=None,
        expected=CannotSubstituteTable,
    ),
    _P(
        desc='NAME MATCH, NO MAX ID',
        name=u'foo',
        version=3,
        max_id=None,
        expected=CannotSubstituteTable,
    ),
)
def test_catalog(p):
    catalog = symbols.SymbolTableCatalog()
    for table in REGISTER_TABLES:
        catalog.register(table)

    if is_exception(p.expected):
        with raises(p.expected):
            catalog.resolve(p.name, p.version, p.max_id)
    else:
        resolved = catalog.resolve(p.name, p.version, p.max_id)
        assert p.expected == resolved


class _P(record('desc', 'table', ('expected', ValueError))):
    def __str__(self):
        return self.desc


@parametrize(
    _P(
        desc='SYSTEM',
        table=symbols.SYSTEM_SYMBOL_TABLE
    ),
    _P(
        desc='LOCAL',
        table=symbols.local_symbol_table()),
    _P(
        desc='PLACEHOLDER',
        table=symbols.placeholder_symbol_table(name=u'placeholder', version=1, max_id=10),
    ),
    _P(
        desc='SUBSTITUTE',
        table=symbols.substitute_symbol_table(FOO_4_TABLE, version=10, max_id=200),
    )
)
def test_catalog_bad_register(p):
    catalog = symbols.SymbolTableCatalog()
    with raises(p.expected):
        catalog.register(p.table)
