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

"""Tests the `equivalence` module."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from decimal import Decimal
from functools import partial
from itertools import chain

from datetime import datetime, timedelta

from amazon.ion.core import IonType, timestamp, TimestampPrecision, OffsetTZInfo
from amazon.ion.equivalence import ion_equals
from amazon.ion.simple_types import IonPyNull, IonPyInt, IonPyBool, IonPyFloat, IonPyDecimal, IonPyText, IonPyBytes, \
    IonPyList, IonPyTimestamp, IonPySymbol, IonPyDict, _IonNature
from amazon.ion.symbols import SymbolToken, ImportLocation
from tests import parametrize


def _ion_nature(_ion_nature_cls, ion_type, value=None, annotations=()):
    return _ion_nature_cls.from_value(ion_type, value, annotations)


_IT = IonType
_D = Decimal
_dt = datetime
_ts = timestamp
_TP = TimestampPrecision
_ST = SymbolToken

_null = partial(_ion_nature, IonPyNull)
_bool = partial(_ion_nature, IonPyInt, _IT.BOOL)
_int = partial(_ion_nature, IonPyBool, _IT.INT)
_float = partial(_ion_nature, IonPyFloat, _IT.FLOAT)
_decimal = partial(_ion_nature, IonPyDecimal, _IT.DECIMAL)
_timestamp = partial(_ion_nature, IonPyTimestamp, _IT.TIMESTAMP)
_string = partial(_ion_nature, IonPyText, _IT.STRING)
_symbol_text = partial(_ion_nature, IonPyText, _IT.SYMBOL)
_symbol_token = partial(_ion_nature, IonPySymbol, _IT.SYMBOL)
_clob = partial(_ion_nature, IonPyBytes, _IT.CLOB)
_blob = partial(_ion_nature, IonPyBytes, _IT.BLOB)
_list = partial(_ion_nature, IonPyList, _IT.LIST)
_sexp = partial(_ion_nature, IonPyList, _IT.SEXP)
_struct = partial(_ion_nature, IonPyDict, _IT.STRUCT)


_EQUIVS = (
    # For each tuple, all elements in that tuple are equivalent under the Ion data model.
    (None, _null(_IT.NULL), _null(_IT.NULL)),
    (True, _bool(True), _bool(1)),
    (False, _bool(0), _bool(False)),
    (0, _int(0), _int(0)),
    (0, -0, _int(0), _int(-0)),
    (1, _int(1), _int(1)),
    (-1, _int(-1), _int(-1)),
    (0.0, 0e0, 0., 0., 0e1, 0e-1),
    (1.0, 1.0),
    (float('nan'), float('nan')),
    (float('+inf'), float('+inf')),
    (float('-inf'), float('-inf')),
    (_D(0), _D('0'), _decimal(_D(0)), _decimal(_D('0')), _D('0.'), _decimal(_D('0.'))),
    (_D('0.0'), _decimal(_D('0.0')), _D('0e-1'), _decimal(_D('0e-1'))),
    (_D('1.'), _decimal(_D('1e0')), _D('1e-0')),
    (_D('1e2'), _decimal(_D('1e2'))),
    (_D('-0.'), _D('-0'), _decimal(_D('-0.')), _decimal(_D('-0'))),
    (
        # Regular datetimes are always seconds precision with 6 digits of fractional precision.
        _dt(1, 1, 1),
        _dt(1, 1, 1, 0, 0, 0),
        _timestamp(_dt(1, 1, 1, 0, 0, 0, 0, tzinfo=None)),
        _ts(1, microsecond=0, precision=_TP.SECOND, fractional_precision=6),
        _timestamp(_ts(1, microsecond=0, precision=_TP.SECOND)),
        _ts(1, precision=_TP.SECOND, fractional_seconds=Decimal('0.000000')),
    ),
    (
        u'abc',
        _string(u'abc')
    ),
    (
        # Having the same text makes all these equivalent, regardless of symbol ID or import location.
        u'abc',
        _symbol_text(u'abc'),
        _symbol_token(_ST(u'abc', None)),
        _symbol_token(_ST(u'abc', 10)),
        _ST(u'abc', 10),
        _ST(u'abc', None),
        _ST(u'abc', 10, ImportLocation(u'foo', 1)),
        _symbol_token(_ST(u'abc', 10, ImportLocation(u'bar', 2)))
    ),
    (
        # Symbols with unknown text from a local symbol table context are equivalent to each other.
        _symbol_token(_ST(None, 10)),
        _symbol_token(_ST(None, 11)),
        _ST(None, 12),
        _ST(None, 13),
    ),
    (
        _symbol_token(_ST(None, 0)),
        _ST(None, 0),
    ),
    (
        b'abc',
        _blob(b'abc'),
    ),
    (
        b'abc',
        _clob(b'abc'),
    ),
    (
        [],
        _list([]),
    ),
    (
        [],
        _sexp([]),
    ),
    (
        {},
        _struct({}),
    ),
    (
        {u'abc': 123, 'def': 456},
        _struct({u'def': 456, u'abc': 123}),
    )
)

_EQUIVS_INSTANTS = (
    (
        # These all represent the same instant.
        _dt(2000, 1, 1),
        _dt(2000, 1, 1, tzinfo=(OffsetTZInfo())),
        _dt(2000, 1, 1, 1, tzinfo=OffsetTZInfo(timedelta(hours=1))),
        _ts(1999, 12, 31, 23, 59, off_hours=0, off_minutes=-1, precision=_TP.SECOND),
        _timestamp(_ts(2000, 1, 1, 1, off_hours=1, off_minutes=0, precision=_TP.SECOND)),
        _timestamp(_ts(2000, 1, 1, 1, off_hours=1, off_minutes=0, precision=_TP.SECOND,
                       fractional_seconds=Decimal('0.000000'))),
    ),
)

_NONEQUIVS = (
    # For each tuple, each element is not equivalent to any other element equivalent under the Ion data model.
    (None, 0, _null(_IT.BOOL)),
    (True, False),
    (True, _bool(False)),
    (_bool(True), False),
    (_bool(True), _bool(False)),
    (1, -1, 1.),
    (1, _int(-1), _float(1)),
    (_int(1), _int(-1)),
    (1, _int(-1)),
    (1., 1),
    (-0., 0.),
    (_float(-0.), _float(0.), _int(0)),
    (-0., _float(0.), 0, None),
    (_float(0e1), _float(-0e1)),
    (float('+inf'), float('-inf')),
    (_float(float('+inf')), _float(float('-inf'))),
    (float('+inf'), _float(float('-inf'))),
    (_float(float('nan')), _float(float('+inf'))),
    (float('nan'), _float(float('+inf')), None),
    (_D('-0.'), _D(0), None),
    (_D('-0'), _decimal(_D(0))),
    (_decimal(_D('-0')), _decimal(_D('0.'))),
    (_D('0.'), _D('0e1'), _decimal(_D('0e-1'))),
    (_D('1.'), _D('1.0'), _decimal(_D('1.00'))),
    (
        # Timestamps with different precisions are not equivalent.
        _dt(1, 1, 1),
        _timestamp(_ts(1, 1, 1, precision=_TP.DAY)),
        _ts(1, microsecond=0, precision=_TP.SECOND, fractional_precision=3),
        _dt(1, 1, 1, tzinfo=OffsetTZInfo()),
        _timestamp(_ts(1, microsecond=0, precision=_TP.SECOND, fractional_precision=6, off_hours=-1, off_minutes=0)),
        None
    ),
    (
        # Timestamps that represent the same instant with different offsets are not equivalent.
        _dt(2000, 1, 1),
        _dt(2000, 1, 1, tzinfo=OffsetTZInfo()),
        _timestamp(_ts(2000, 1, 1, 1, off_hours=1, off_minutes=0, precision=_TP.SECOND)),
        _ts(1999, 12, 31, 23, 59, off_hours=0, off_minutes=-1, precision=_TP.SECOND),
        None,
    ),
    (
        # Timestamps with different fractional seconds are not equivalent.
        _timestamp(_ts(2000, 1, 1, 1, precision=_TP.SECOND, fractional_seconds=Decimal('0.123456789'))),
        _timestamp(_ts(2000, 1, 1, 1, precision=_TP.SECOND, fractional_seconds=Decimal('0.9999999'))),
        _timestamp(_ts(2000, 1, 1, 1, precision=_TP.SECOND, fractional_seconds=Decimal('0.000000000'))),
        None,
    ),
    (
        _string(u'abc'),
        u'abcd',
        _symbol_text(u'abc'),
        _symbol_text(u'abcde'),
        _symbol_token(_ST(None, 10)),
        _symbol_token(_ST(u'abcdef', 10)),
        _ST(u'abcdefg', 10),
        None,
    ),
    (
        _ST(None, 10, ImportLocation(u'foo', 1)),
        _ST(None, 10, ImportLocation(u'foo', 2)),
        _symbol_token(_ST(None, 10, ImportLocation(u'bar', 1))),
        _symbol_token(_ST(None, 10, ImportLocation(u'bar', 2))),
        _symbol_token(_ST(None, 10)),
        _ST(None, 0),  # Symbol 0 is only equivalent to itself.
        None,
    ),
    (
        _blob(b'abc'),
        _clob(b'abc'),
        None,
    ),
    (
        _list([]),
        _sexp([]),
        {},
        None
    ),
    (
        _list([]),
        [u'abc'],
        [u'def', u'abc'],
        _list([u'abc', u'def'])
    ),
    (
        _sexp([]),
        [u'abc'],
        [u'def', u'abc'],
        _sexp([u'abc', u'def'])
    ),
    (
        _struct({}),
        {u'abc': 123}
    ),
    (
        {u'abc': 456, u'def': 123},
        {u'abc': 123, u'def': 456},
        _struct({u'abc': 123, u'def': 123}),
        _struct({u'abc': 456, u'def': 456}),
    )
)


class _Parameter:
    def __init__(self, desc, assertion):
        self.desc = desc
        self.assertion = assertion

    def __str__(self):
        return self.desc

_P = _Parameter


def _desc(a, b, operator):
    def _str(val):
        if isinstance(val, _IonNature):
            return '%s(%s, ion_type=%s, ion_annotations=%s)' % (type(val), val, val.ion_type, val.ion_annotations)
        return '%s(%s)' % (type(val), val)
    return 'assert %s %s %s' % (_str(a), operator, _str(b))


def _equivs_param(a, b, timestamp_instants_only=False):
    def assert_equivalent():
        assert ion_equals(a, b, timestamp_instants_only)
        assert ion_equals(b, a, timestamp_instants_only)
    return _P(_desc(a, b, '=='), assert_equivalent)


_equivs_timestamp_instants_param = partial(_equivs_param, timestamp_instants_only=True)


def _nonequivs_param(a, b):
    def assert_not_equivalent():
        assert not ion_equals(a, b)
        assert not ion_equals(b, a)
    return _P(_desc(a, b, '!='), assert_not_equivalent)


_TEST_ANNOTATIONS = (
    (u'abc',),
    (u'abc', u'def'),
    (SymbolToken(text=None, sid=10), SymbolToken(text=None, sid=11)),
)


def _generate_annotations():
    """Circularly generates sequences of test annotations. The annotations sequence yielded from this generator must
    never be equivalent to the annotations sequence last yielded by this generator.
    """
    i = 0
    while True:
        yield _TEST_ANNOTATIONS[i]
        i += 1
        if i == len(_TEST_ANNOTATIONS):
            i = 0

_annotations_generator = iter(_generate_annotations())


def _add_annotations(val):
    val.ion_annotations = next(_annotations_generator)


def _generate_equivs(equivs, param_func=_equivs_param):
    for seq in equivs:
        seq_len = len(seq)
        for i in range(seq_len):
            for j in range(seq_len):
                yield param_func(seq[i], seq[j])
                # Now, for pairs that are otherwise equivalent, add differing annotation(s) if possible and make sure
                # they're no longer equivalent.
                has_ion_nature = False
                a = seq[i]
                b = seq[j]
                if isinstance(a, _IonNature):
                    has_ion_nature = True
                    a = a._copy()
                    _add_annotations(a)
                if isinstance(b, _IonNature):
                    has_ion_nature = True
                    b = b._copy()
                    _add_annotations(b)
                if has_ion_nature:
                    yield _nonequivs_param(a, b)


def _generate_nonequivs(nonequivs):
    for seq in nonequivs:
        seq_len = len(seq)
        for i in range(seq_len):
            for j in range(seq_len):
                if i != j:
                    yield _nonequivs_param(seq[i], seq[j])


def list_from(equivs, equiv_set_index):
    output = []
    for equiv_set in equivs:
        output.append(equiv_set[equiv_set_index])
    return output


def _generate_equiv_lists(equivs, type_func):
    a = list_from(equivs, 0)
    b = list_from(equivs, 1)
    yield _equivs_param(a, b)
    yield _equivs_param(a, type_func(b))
    yield _equivs_param(type_func(a), type_func(b))


def _generate_equiv_dicts(equivs):
    a = list_from(equivs, 0)
    b = list_from(equivs, 1)
    assert len(a) == len(b)
    field_names = [u'%d' % (i,) for i in range(len(a))]
    a = dict(zip(field_names, a))
    b = dict(zip(field_names, b))
    yield _equivs_param(a, b)
    yield _equivs_param(a, _struct(b))
    yield _equivs_param(_struct(a), _struct(b))


@parametrize(
    *tuple(chain(
        _generate_equivs(_EQUIVS),
        _generate_equivs(_EQUIVS_INSTANTS, _equivs_timestamp_instants_param),
        _generate_nonequivs(_NONEQUIVS),
        _generate_equiv_lists(_EQUIVS, _list),
        _generate_equiv_lists(_EQUIVS, _sexp),
        _generate_equiv_dicts(_EQUIVS),
    ))
)
def test_equivalence(p):
    p.assertion()
