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

from itertools import chain

from tests import parametrize, listify
from tests.reader_util import reader_scaffold, add_depths
from tests.event_aliases import *

from amazon.ion.exceptions import IonException, CannotSubstituteTable
from amazon.ion.reader_managed import managed_reader, _ImportDesc, _IonManagedThunkEvent
from amazon.ion.symbols import shared_symbol_table, local_symbol_table, \
                               SymbolToken, ImportLocation, \
                               SymbolTableCatalog, \
                               SYSTEM_SYMBOL_TABLE, \
                               TEXT_ION, TEXT_ION_1_0, TEXT_ION_SYMBOL_TABLE, \
                               TEXT_NAME, TEXT_VERSION, TEXT_MAX_ID, \
                               TEXT_IMPORTS, TEXT_SYMBOLS
from amazon.ion.util import coroutine, record

_DATA = e_read(b'DUMMY')

_SYS = SYSTEM_SYMBOL_TABLE

_sid = partial(SymbolToken, None)
_tok = SymbolToken
_loc = ImportLocation
_desc = _ImportDesc


def _nextify(iter_func):
    """Creates a decorator that generates event pairs of ``(NEXT_EVENT, event)``
    where ``event`` are instances returned by the decorated function.
    """
    def delegate(*args, **kw_args):
        for ion_event in iter_func(*args, **kw_args):
            yield NEXT_EVENT, ion_event
    delegate.__name__ = iter_func.__name__
    return delegate


_APPEND = object()


def _system_sid_token(text):
    return _sid(SYSTEM_SYMBOL_TABLE[text].sid)


def _create_local_sid_token(table):
    sids = {}
    for token in table:
        if token.location is None or token.location.name != TEXT_ION:
            sids[token.text] = token.sid

    def token(text):
        return _sid(sids[text])

    return token


def _text_token(text):
    return _tok(text, sid=None)


@listify
@_nextify
def _lst(imports=None, symbols=None, token=_system_sid_token):
    """Generates a list of event pairs for a local symbol table.

    Args:
        sys (SymbolTable): The symbol table to resolve SIDs from.
        imports (Optional[Iterable[_ImportDesc]]): The symbol tables to import.
        symbols (Optional[Iterable[Unicode]]): The local symbols to declare.
        token (Optional[Callable]): Function to construct the token directly from text,
            by default it uses the system symbol table.
    Returns:
        List[Tuple[ReadEvent,IonEvent]]
    """

    yield e_start_struct(annotations=(token(TEXT_ION_SYMBOL_TABLE),))

    if imports is not None:
        if imports is _APPEND:
            yield e_symbol(token(TEXT_ION_SYMBOL_TABLE), field_name=(token(TEXT_IMPORTS)))
        else:
            yield e_start_list(field_name=(token(TEXT_IMPORTS)))
            for desc in imports:
                yield e_start_struct()
                if desc.name is not None:
                    yield e_string(desc.name, field_name=token(TEXT_NAME))
                if desc.version is not None:
                    yield e_int(desc.version, field_name=token(TEXT_VERSION))
                if desc.max_id is not None:
                    yield e_int(desc.max_id, field_name=token(TEXT_MAX_ID))
                yield e_end_struct()
            yield e_end_list()

    if symbols is not None:
        yield e_start_list(field_name=(token(TEXT_SYMBOLS)))
        for symbol_text in symbols:
            yield e_string(symbol_text)
        yield e_end_list()

    yield e_end_struct()


_SHADOW_ION_TEXTS = [
    TEXT_ION_1_0,
    TEXT_ION_SYMBOL_TABLE,
    TEXT_NAME,
    TEXT_VERSION,
    TEXT_MAX_ID,
    TEXT_IMPORTS,
    TEXT_SYMBOLS
]
_SHADOW_ION_TABLE = shared_symbol_table(
    u'shadow_ion',
    1,
    _SHADOW_ION_TEXTS
)
_SHADOW_ION_DESC = _desc(
    _SHADOW_ION_TABLE.name, _SHADOW_ION_TABLE.version, _SHADOW_ION_TABLE.max_id
)


def _test_catalog():
    catalog = SymbolTableCatalog()
    foo_1 = shared_symbol_table(u'foo', 1, [u'a', u'b'])
    foo_3 = shared_symbol_table(u'foo', 3, [u'c', u'd', u'e', u'f'], imports=[foo_1])
    bar_1 = shared_symbol_table(u'bar', 1, [u'x', u'y', u'z'])
    zoo_4 = shared_symbol_table(u'zoo', 4, [u'm', u'n', u'o'])

    for table in [foo_1, foo_3, bar_1, zoo_4, _SHADOW_ION_TABLE]:
        catalog.register(table)

    return catalog


@coroutine
def _predefined_reader(event_pairs):
    expected_iter = (e for e, _ in event_pairs)
    output_iter = iter(add_depths(e for _, e in event_pairs))
    output = None
    while True:
        actual = yield output
        expected = next(expected_iter)
        assert expected == actual
        output = next(output_iter)


class _P(record('desc', 'outer', 'inner', ('catalog', None))):
    def __str__(self):
        return self.desc


def _create_lst_params(
        prefix_desc='SYSTEM SIDS',
        prefix_pairs=None,
        token=_system_sid_token,
        append_start=SYSTEM_SYMBOL_TABLE.max_id):
    """

    Args:
        prefix_desc (Optional[String]): The prefix for the test parameter description.
        prefix_pairs (Optional[Iterable[Tuple[DataEvent, IonEvent]]]): The prefix of event pairs to
            put into the inner stream, should only be system values.
        token (Optional[Callable]): The token encoder.
        append_start (Optional[int]): The start of the LST for direct append test cases.
    """
    if prefix_pairs is None:
        prefix_pairs = [(NEXT, IVM)]
    params = [
        _P(
            desc='LOCAL ONLY',
            inner=_lst(symbols=[u'a', u'b', u'c'], token=token) + [
                (NEXT, e_symbol(_sid(10))),
                (NEXT, e_symbol(_sid(11))),
                (NEXT, e_symbol(_sid(12))),
                (NEXT, e_symbol(None)),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'a', 10))),
                (NEXT, e_symbol(_tok(u'b', 11))),
                (NEXT, e_symbol(_tok(u'c', 12))),
                (NEXT, e_symbol(None)),
            ],
        ),
        _P(
            desc='LOCAL ONLY RESET',
            inner=_lst(symbols=[u'a'], token=token) + [
                (NEXT, e_symbol(_sid(10))),
            ] + _lst(symbols=[u'b'], token=_system_sid_token) + [
                (NEXT, e_symbol(_sid(10))),
                (NEXT, IVM),
                (NEXT, e_symbol(_sid(10))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'a', 10))),
                (NEXT, e_symbol(_tok(u'b', 10))),
                (NEXT, IonException),
            ],
        ),
        _P(
            desc='$ion_1_0 USED AS NOP',
            inner=_lst(symbols=[u'$ion_1_0', u'a'], token=token) + [
                (NEXT, e_symbol(_sid(11))),
                (NEXT, e_symbol(_sid(10))),
                (NEXT, e_symbol(_sid(11))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'a', 11))),
                (NEXT, e_symbol(_tok(u'a', 11))),
            ],
        ),
        _P(
            desc='IMPORTS ONLY - PLACEHOLDER',
            inner=_lst(imports=[_desc(u'unknown', 1, 10), _desc(u'other', 1, 5)], token=token) + [
                (NEXT, e_symbol(_sid(10))),
                (NEXT, e_symbol(_sid(22))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(None, 10, _loc(u'unknown', 1)))),
                (NEXT, e_symbol(_tok(None, 22, _loc(u'other', 3)))),
            ],
        ),
        _P(
            desc='IMPORTS ONLY - EXACT MATCH, NO MAX ID',
            inner=_lst(imports=[_desc(u'foo', 3), _desc(u'bar', 1)], token=token) + [
                (NEXT, e_symbol(_sid(15))),
                (NEXT, e_symbol(_sid(16))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'f', 15, _loc(u'foo', 6)))),
                (NEXT, e_symbol(_tok(u'x', 16, _loc(u'bar', 1)))),
            ],
        ),
        _P(
            desc='IMPORTS ONLY - EXACT MATCH, NO VERSION, NO MAX ID',
            inner=_lst(imports=[_desc(u'foo'), _desc(u'bar')], token=token) + [
                (NEXT, e_symbol(_sid(11))),
                (NEXT, e_symbol(_sid(13))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'b', 11, _loc(u'foo', 2)))),
                (NEXT, e_symbol(_tok(u'y', 13, _loc(u'bar', 2)))),
            ],
        ),
        _P(
            desc='IMPORTS ONLY - NO MATCH, NO VERSION, NO MAX ID',
            inner=_lst(imports=[_desc(u'zoo')], token=token),
            outer=[
                (NEXT, CannotSubstituteTable),
            ],
        ),
        _P(
            desc='IMPORTS ONLY - INEXACT MATCH, NO VERSION',
            inner=_lst(imports=[_desc(u'bar', max_id=2)], token=token) + [
                (NEXT, e_symbol(_sid(11))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'y', 11, _loc(u'bar', 2)))),
            ],
        ),
        _P(
            desc='IMPORTS AND LOCALS',
            inner=_lst(
                imports=[_desc(u'foo', 3, 4)],
                symbols=[u'aa', u'bb', u'cc', u'dd'],
                token=token
            ) + [
                (NEXT, e_symbol(_sid(13))),
                (NEXT, e_symbol(_sid(15))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'd', 13, _loc(u'foo', 4)))),
                (NEXT, e_symbol(_tok(u'bb', 15))),
            ],
        ),
        _P(
            desc='APPEND SYSTEM SYMBOLS',
            inner=_lst(imports=_APPEND, symbols=[u'a', u'b', u'c'], token=token) + [
                (NEXT, e_symbol(_sid(append_start + 1))),
                (NEXT, e_symbol(_sid(append_start + 3))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'a', append_start + 1))),
                (NEXT, e_symbol(_tok(u'c', append_start + 3))),
            ],
        ),
        _P(
            desc='APPEND TO PREVIOUS LST',
            inner=_lst(symbols=[u'a', u'b', u'c'], token=token) + [
                (NEXT, e_symbol(_sid(10))),
                (NEXT, e_symbol(_sid(12))),
            ] + _lst(imports=_APPEND, symbols=[u'd', u'e', u'f'], token=_system_sid_token) + [
                (NEXT, e_symbol(_sid(10))),
                (NEXT, e_symbol(_sid(13))),
                (NEXT, e_symbol(_sid(15))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'a', 10))),
                (NEXT, e_symbol(_tok(u'c', 12))),
                (NEXT, e_symbol(_tok(u'a', 10))),
                (NEXT, e_symbol(_tok(u'd', 13))),
                (NEXT, e_symbol(_tok(u'f', 15))),
            ],
        ),
        _P(
            desc='APPEND TO PREVIOUS LST WITH IMPORTS',
            inner=_lst(
                imports=[_desc(u'foo', 3, 4)],
                symbols=[u'aa', u'bb', u'cc'],
                token=token
            ) + [
                (NEXT, e_symbol(_sid(10))),
                (NEXT, e_symbol(_sid(14))),
            ] + _lst(imports=_APPEND, symbols=[u'dd', u'ee', u'ff'], token=_system_sid_token) + [
                (NEXT, e_symbol(_sid(11))),
                (NEXT, e_symbol(_sid(15))),
                (NEXT, e_symbol(_sid(18))),
            ],
            outer=[
                (NEXT, e_symbol(_tok(u'a', 10, _loc(u'foo', 1)))),
                (NEXT, e_symbol(_tok(u'aa', 14))),
                (NEXT, e_symbol(_tok(u'b', 11, _loc(u'foo', 2)))),
                (NEXT, e_symbol(_tok(u'bb', 15))),
                (NEXT, e_symbol(_tok(u'ee', 18))),
            ],
        ),
    ]
    for param in params:
        yield _P(
            desc='LST - ' + prefix_desc + ' - ' + param.desc,
            catalog=_test_catalog(),
            inner= prefix_pairs + param.inner,
            outer=param.outer
        )


_BASIC_PARAMS = [
    _P(
        desc='IVM ONLY',
        catalog=None,
        inner=[
            (NEXT, IVM),
            (NEXT, IVM),
            (NEXT, END),
        ],
        outer=[
            (NEXT, END),
        ],
    ),
    _P(
        desc='SYSTEM SYMBOL',
        catalog=None,
        inner=[
            (NEXT, IVM),
            (NEXT, e_symbol(_sid(4))),
        ],
        outer=[
            (NEXT, e_symbol(_SYS[4])),
        ],
    ),
    _P(
        desc='SYSTEM ANNOTATION',
        catalog=None,
        inner=[
            (NEXT, IVM),
            (NEXT, e_int(100, annotations=(_sid(5),))),
        ],
        outer=[
            (NEXT, e_int(100, annotations=(_SYS[5],))),
        ],
    ),
    _P(
        desc='SYSTEM FIELD_NAME',
        catalog=None,
        inner=[
            (NEXT, IVM),
            # We can cheat because we don't have a real underlying reader.
            (NEXT, e_string(u'hi', field_name=_sid(6))),
        ],
        outer=[
            (NEXT, e_string(u'hi', field_name=_SYS[6])),
        ],
    ),
]


@parametrize(*chain(
    _BASIC_PARAMS,
    _create_lst_params(),
    _create_lst_params(
        prefix_desc='SHADOW IMPORT',
        prefix_pairs=_lst(imports=[_SHADOW_ION_DESC]),
        token=_create_local_sid_token(local_symbol_table([_SHADOW_ION_TABLE])),
        append_start=SYSTEM_SYMBOL_TABLE.max_id + _SHADOW_ION_TABLE.max_id
    ),
    _create_lst_params(
        prefix_desc='SHADOW LOCAL',
        prefix_pairs=_lst(symbols=_SHADOW_ION_TEXTS),
        token=_create_local_sid_token(local_symbol_table(symbols=_SHADOW_ION_TEXTS)),
        append_start=SYSTEM_SYMBOL_TABLE.max_id + len(_SHADOW_ION_TEXTS)
    ),
    _create_lst_params(
        prefix_desc='TEXT TOKENS',
        token=_text_token,
    ),
))
def test_managed_reader(p):
    reader_scaffold(managed_reader(_predefined_reader(p.inner), p.catalog), p.outer)


def test_managed_thunk_event():
    event = IonEvent(
        IonEventType.SCALAR, IonType.INT, 5, _tok(u'foo', None), ()
    )
    thunk_event = _IonManagedThunkEvent(
        IonEventType.SCALAR, IonType.INT, lambda: 5, lambda: _tok(u'foo', None), lambda: (), None
    )

    assert thunk_event == event

    assert thunk_event.derive_annotations((_tok(u'bar', None),)) \
        == event.derive_annotations((_tok(u'bar', None),))

    assert thunk_event.derive_annotations(lambda: (_tok(u'bar', None),)) \
        == event.derive_annotations((_tok(u'bar', None),))

    assert thunk_event.derive_field_name(_tok(u'bar', None)) \
        == event.derive_field_name(_tok(u'bar', None))

    assert thunk_event.derive_field_name(lambda: _tok(u'bar', None)) \
        == event.derive_field_name(_tok(u'bar', None))
