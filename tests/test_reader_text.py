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

from decimal import Decimal
from itertools import chain

import six

from amazon.ion.core import timestamp, TimestampPrecision
from amazon.ion.exceptions import IonException
from amazon.ion.reader import ReadEventType, _NARROW_BUILD
from amazon.ion.reader_text import reader, _POS_INF, _NEG_INF, _NAN
from amazon.ion.symbols import SymbolToken
from amazon.ion.util import coroutine
from tests import listify, parametrize
from tests.event_aliases import *
from tests.reader_util import ReaderParameter, reader_scaffold, all_top_level_as_one_stream_params, value_iter

_P = ReaderParameter
_ts = timestamp
_tp = TimestampPrecision
_d = Decimal
_st = partial(SymbolToken, sid=None, location=None)


def _sid(sid):
    return SymbolToken(text=None, sid=sid, location=None)

_BAD_GRAMMAR = (
    (b'$ion_1_1 42',),
    (b'$ion_10_1 42',),
    (b'$ion_1_02 42',),
    (b'+1',),
    (b'01',),
    (b'1.23.4',),
    (b'1__0',),
    (b'1_e1',),
    (b'1e_1',),
    (b'1e1_',),
    (b'-infs',),
    (b'+infs',),
    (b'-in',),
    (b'1._0',),
    (b'1_.0',),
    (b'-_1',),
    (b'1_',),
    (b'0_x1',),
    (b'0b_1',),
    (b'1e0-1',),
    (b'1e0e-1',),
    (b'+inf-',),
    (b'null.strings',),
    (b'null.strn',),
    (b'null.flat',),
    (b'null.x',),
    (b'null.',),
    (b'200T',),
    (b'10000T',),
    (b'-2000T',),
    (b'-0001T',),
    (b'00-01T',),
    (b'2000-01',),
    (b'2000-001T',),
    (b'2000--01T',),
    (b'2000-01-123T',),
    (b'2000-01-12T1',),
    (b'2000-01--3T',),
    (b'2007-02-23T20:14:33.Z',),
    (b'2007-02-23T20:14:33.12.3Z',),
    (b'2007-02-23T20:14:33.12+00',),
    (b'1a',),
    (b'foo-',),
    (b'%',),
    (b'n%',),
    (b'"\n"',),
    (b'"\a"',),
    (b'"\\\a"',),
    (b'"a\b"',),
    (b'"\\\n\r"',),
    (b'"\0"',),
    (b"'\n'",),
    (b"'\a'",),
    (b"'\\\a'",),
    (b"'a\b'",),
    (b"'\\\n\r'",),
    (b"'\0'",),
    (b"'''\b'''",),
    (b"'''a\b'''",),
    (b'"\\udbff\\""',),  # Unpaired escaped surrogate.
    (b'"\\udbffabcdef',),  # Unpaired escaped surrogate.
    (b"'''\\udbff'''",),  # Splitting surrogate escapes across long string literal boundaries is illegal per the spec.
    (b'abc://',),
    (b'abc/**/://',),
    (b'{{/**/}}',),
    (b'{{//\n}}',),
    (b'{{/**/"abc"}}',),
    (b'{{"abc"//\n}}',),
    (b'{{\'\'\'abc\'\'\'//\n\'\'\'def\'\'\'}}',),
    (b'{{"\xf6"}}',),
    (b'{{"\n"}}',),
    (b"{{'''\0'''}}",),
    (b"{{'''\\u0000'''}}",),
    (b"{{'''\\u3000'''}}",),
    (b'{{"\\u0000"}}',),
    (b'{{"\\u3000"}}',),
    (b'{{"\\U0001f4a9"}}',),
    (b'{{ abcd} }',),
    (b'{ {abcd}}', e_start_struct()),
    (b'{{\'\' \'foo\'\'\'}}',),
    (b'{{\'"foo"}}',),
    (b'{{abc}de}}',),
    (b'{{"abc"}de}',),
    (b'{{ab}}',),
    (b'{{ab=}}',),
    (b'{{ab=}=}',),
    (b'{{ab===}}',),
    (b'{{====}}',),
    (b'{{abcd====}}',),
    (b'{{abc*}}',),
    (b'{foo:bar/**/baz:zar}', e_start_struct(), e_symbol(value=_st(u'bar'), field_name=_st(u'foo'))),
    (b'{foo:bar/**/baz}', e_start_struct(), e_symbol(value=_st(u'bar'), field_name=_st(u'foo'))),
    (b'[abc 123]', e_start_list(), e_symbol(value=_st(u'abc'))),
    (b'[abc/**/def]', e_start_list(), e_symbol(value=_st(u'abc'))),
    (b'{abc:}', e_start_struct()),
    (b'{abc :}', e_start_struct()),
    (b'{abc : //\n}', e_start_struct()),
    (b'[abc:]', e_start_list()),
    (b'(abc:)', e_start_sexp()),
    (b'[abc::]', e_start_list()),
    (b'(abc::)', e_start_sexp()),
    (b'{abc::}', e_start_struct()),
    (b'[abc ::]', e_start_list()),
    (b'(abc/**/::)', e_start_sexp()),
    (b'{abc//\n::}', e_start_struct()),
    (b'[abc::/**/]', e_start_list()),
    (b'(abc:: )', e_start_sexp()),
    (b'{abc:://\n}', e_start_struct()),
    (b'{foo:abc::}', e_start_struct()),
    (b'{foo:abc::/**/}', e_start_struct()),
    (b'{foo::bar}', e_start_struct()),
    (b'{foo::bar:baz}', e_start_struct()),
    (b'{foo, bar}', e_start_struct()),
    (b'{foo}', e_start_struct()),
    (b'{123}', e_start_struct()),
    (b'{42, 43}', e_start_struct()),
    (b'[abc, , 123]', e_start_list(), e_symbol(value=_st(u'abc'))),
    (b'[\'\'\'abc\'\'\'\'\']', e_start_list()),
    (b'[\'\'\'abc\'\'\'\'foo\']', e_start_list()),
    (b'[\'\'\'abc\'\'\'\'\', def]', e_start_list()),
    (b'{foo:\'\'\'abc\'\'\'\'\'}', e_start_struct()),
    (b'{foo:\'\'\'abc\'\'\'\'\', bar:def}', e_start_struct()),
    (b'[,]', e_start_list()),
    (b'(,)', e_start_sexp()),
    (b'{,}', e_start_struct()),
    (b'{foo:bar, ,}', e_start_struct(), e_symbol(value=_st(u'bar'), field_name=_st(u'foo'))),
    (b'{true:123}', e_start_struct()),
    (b'{false:123}', e_start_struct()),
    (b'{+inf:123}', e_start_struct()),
    (b'{-inf:123}', e_start_struct()),
    (b'{nan:123}', e_start_struct()),
    (b'{nan}', e_start_struct()),
    (b'{null.clob:123}', e_start_struct()),
    (b'{%:123}', e_start_struct()),
    (b'\'\'\'foo\'\'\'/\'\'\'bar\'\'\'',),  # Dangling slash at the top level.
    (b'{{\'\'\'foo\'\'\' \'\'bar\'\'\'}}',),
    (b'{\'\'\'foo\'\'\'/**/\'\'bar\'\'\':baz}', e_start_struct()),  # Missing an opening ' before "bar".
    (b'{\'\'\'foo\'\'\'/**/\'\'\'bar\'\'\'a:baz}', e_start_struct()),  # Character after field name, before colon.
    (b'{\'foo\'a:baz}', e_start_struct()),
    (b'{"foo"a:baz}', e_start_struct()),
    (b'(1..)', e_start_sexp()),
    (b'(1.a)', e_start_sexp()),
    (b'(1.23.)', e_start_sexp()),
    (b'(42/)', e_start_sexp()),
    (b'42/',),
    (b'0/',),
    (b'1.2/',),
    (b'1./',),
    (b'1.2e3/',),
    (b'1.2d3/',),
    (b'2000T/',),
    (b'/ ',),
    (b'/b',),
)

_BAD_VALUE = (
    (b'0000T',),  # Years must be 1..9999
    (b'2000-13T',),  # 2000 didn't have a thirteenth month.
    (b'2015-02-29T',),  # 2015 was not a leap year.
    (b'2000-01-01T24:00Z',),  # Hour is 0..23.
    (b'2000-01-01T00:60Z',),  # Minute is 0..59.
    (b'2000-01-01T00:00:60Z',),  # Second is 0..59.
    (b'2000-01-01T00:00:00.000+24:00',),  # Hour offset is 0..23.
    (b'2000-01-01T00:00:00.000+00:60',),  # Minute offset is 0..59.
    (b'"\\udbff\\u3000"',),  # Malformed surrogate pair (\u3000 is not a low surrogate).
    (b'"\\u3000\\udfff"',),  # Malformed surrogate pair (\u3000 is not a high surrogate).
)

_INCOMPLETE = (
    (b'{',),  # Might be a lob.
    (b'{ ', e_start_struct()),
    (b'[', e_start_list()),
    (b'(', e_start_sexp()),
    (b'[[]', e_start_list(), e_start_list(), e_end_list()),
    (b'(()', e_start_sexp(), e_start_sexp(), e_end_sexp()),
    (b'{foo:{}', e_start_struct(), e_start_struct(field_name=_st(u'foo')), e_end_struct()),
    (b'{foo:bar', e_start_struct(),),
    (b'{foo:bar::', e_start_struct(),),
    (b'{foo:bar,', e_start_struct(), e_symbol(_st(u'bar'), field_name=_st(u'foo'))),
    (b'[[],', e_start_list(), e_start_list(), e_end_list()),
    (b'{foo:{},', e_start_struct(), e_start_struct(field_name=_st(u'foo')), e_end_struct()),
    (b'foo',),  # Might be an annotation.
    (b'\'foo\'',),  # Might be an annotation.
    (b'\'\'\'foo\'\'\'/**/',),  # Might be followed by another triple-quoted string.
    (b'\'\'\'\'',),  # Might be followed by another triple-quoted string.
    (b"'''abc''''def'", e_string(u'abc'),),
    (b'123',),  # Might have more digits.
    (b'42/',),  # The / might start a comment
    (b'0/',),
    (b'1.2/',),
    (b'1./',),
    (b'1.2e3/',),
    (b'1.2d3/',),
    (b'2000T/',),
    (b'-',),
    (b'+',),
    (b'1.2',),
    (b'1.2e',),
    (b'1.2e-',),
    (b'+inf',),  # Might be followed by more characters, making it invalid at the top level.
    (b'-inf',),
    (b'nan',),
    (b'1.2d',),
    (b'1.2d3',),
    (b'1_',),
    (b'0b',),
    (b'0x',),
    (b'2000-01',),
    (b'"abc',),
    (b'false',),  # Might be a symbol with more characters.
    (b'true',),
    (b'null.string',),  # Might be a symbol with more characters.
    (b'/',),
    (b'/*',),
    (b'//',),
    (b'foo:',),
    (b'foo::',),
    (b'foo::bar',),
    (b'foo//\n',),
    (b'{foo', e_start_struct()),
    (b'{{',),
    (b'{{"',),
    (b'(foo-', e_start_sexp(), e_symbol(_st(u'foo'))),
    (b'(-foo', e_start_sexp(), e_symbol(_st(u'-'))),
)

_SKIP = (
    [(e_read(b'123 456 '), e_int(123)), (SKIP, TypeError)],  # Can't skip at top-level.
    [(e_read(b'[]'), e_start_list()), (SKIP, e_end_list()), (NEXT, END)],
    [(e_read(b'{//\n}'), e_start_struct()), (SKIP, e_end_struct()), (NEXT, END)],
    [(e_read(b'(/**/)'), e_start_sexp()), (SKIP, e_end_sexp()), (NEXT, END)],
    [(e_read(b'[a,b,c]'), e_start_list()), (NEXT, e_symbol(_st(u'a'))), (SKIP, e_end_list()), (NEXT, END)],
    [
        (e_read(b'{c:a,d:e::b}'), e_start_struct()),
        (NEXT, e_symbol(_st(u'a'), field_name=_st(u'c'))),
        (SKIP, e_end_struct()), (NEXT, END)
    ],
    [
        (e_read(b'(([{a:b}]))'), e_start_sexp()),
        (NEXT, e_start_sexp()),
        (SKIP, e_end_sexp()),
        (NEXT, e_end_sexp()), (NEXT, END)],
    [
        (e_read(b'['), e_start_list()),
        (SKIP, INC),
        (e_read(b'a,42'), INC),
        (e_read(b',]'), e_end_list()), (NEXT, END)
    ],
    [
        (e_read(b'{'), INC),
        (e_read(b'foo'), e_start_struct()),
        (SKIP, INC),
        (e_read(b':bar,baz:zar}'), e_end_struct()), (NEXT, END)
    ],
    [
        (e_read(b'('), e_start_sexp()),
        (SKIP, INC),
        (e_read(b'a+b'), INC),
        (e_read(b'//\n'), INC),
        (e_read(b')'), e_end_sexp()), (NEXT, END)
    ],
)

_NEXT_ERROR = (NEXT, IonException)
_NEXT_INC = (NEXT, INC)
_NEXT_END = (NEXT, END)


_GOOD_FLUSH = (
    [(e_read(b'0'), INC), (NEXT, e_int(0)), _NEXT_END],
    [(e_read(b'1'), INC), (NEXT, e_int(1)), _NEXT_END],
    [(e_read(b'-0'), INC), (NEXT, e_int(0)), _NEXT_END],
    [(e_read(b'123'), INC), (NEXT, e_int(123)), _NEXT_END],
    [(e_read(b'123.'), INC), (NEXT, e_decimal(_d(123))), _NEXT_END],
    [(e_read(b'1.23e-4'), INC), (NEXT, e_float(1.23e-4)), _NEXT_END],
    [(e_read(b'1.23d+4'), INC), (NEXT, e_decimal(_d(u'1.23e4'))), _NEXT_END],
    [(e_read(b'2000-01-01'), INC), (NEXT, e_timestamp(_ts(2000, 1, 1, precision=_tp.DAY))), _NEXT_END],
    [(e_read(b"a"), INC), (NEXT, e_symbol(_st(u'a'))), _NEXT_END],
    [(e_read(b"'abc'"), INC), (NEXT, e_symbol(_st(u'abc'))), _NEXT_END],
    [(e_read(b"$abc"), INC), (NEXT, e_symbol(_st(u'$abc'))), _NEXT_END],
    [(e_read(b"$"), INC), (NEXT, e_symbol(_st(u'$'))), _NEXT_END],
    [(e_read(b"$10"), INC), (NEXT, e_symbol(_sid(10))), _NEXT_END, (e_read(b'0'), INC), (NEXT, e_int(0)), _NEXT_END],
    [(e_read(b'abc'), INC), (NEXT, e_symbol(_st(u'abc'))), _NEXT_END, (e_read(b'def'), INC),
     (NEXT, e_symbol(_st(u'def'))), _NEXT_END],
    [(e_read(b"''"), INC), (NEXT, e_symbol(_st(u''))), _NEXT_END],
    [(e_read(b"'''abc'''"), INC), (NEXT, e_string(u'abc')), (NEXT, END), (e_read(b"'''def'''"), INC),
     (NEXT, e_string(u'def')), _NEXT_END],
    [(e_read(b"'''abc''''def'"), e_string(u'abc')), _NEXT_INC, (NEXT, e_symbol(_st(u'def'))), _NEXT_END],
    [(e_read(b"'''abc'''''"), INC), (NEXT, e_string(u'abc')), (NEXT, e_symbol(_st(u''))), _NEXT_END],
    [(e_read(b"'''abc'''//\n'def'"), e_string(u'abc')), _NEXT_INC, (NEXT, e_symbol(_st(u'def'))), _NEXT_END],
    [(e_read(b"'''abc'''/**/''"), INC), (NEXT, e_string(u'abc')), (NEXT, e_symbol(_st(u''))), _NEXT_END],
    [(e_read(b"'''abc'''//\n/**/''"), INC), (NEXT, e_string(u'abc')), (NEXT, e_symbol(_st(u''))), _NEXT_END],
    [(e_read(b'null'), INC), (NEXT, e_null()), _NEXT_END],
    [(e_read(b'null.string'), INC), (NEXT, e_string()), _NEXT_END],
    [(e_read(b'+inf'), INC), (NEXT, e_float(_POS_INF)), _NEXT_END],
    [(e_read(b'nan'), INC), (NEXT, e_float(_NAN)), _NEXT_END],
    [(e_read(b'true'), INC), (NEXT, e_bool(True)), _NEXT_END],
    [(e_read(b'//'), INC), _NEXT_END],  # Matches ion-java - termination of line comment with newline not required.
    [(e_read(b'abc//123\n'), INC), (NEXT, e_symbol(_st(u'abc'))), _NEXT_END],
    [(e_read(b"'abc'//123\n"), INC), (NEXT, e_symbol(_st(u'abc'))), _NEXT_END],
    [(e_read(b'abc//123'), INC), (NEXT, e_symbol(_st(u'abc'))), _NEXT_END],
    [(e_read(b"'abc'//123"), INC), (NEXT, e_symbol(_st(u'abc'))), _NEXT_END],
)

_BAD_FLUSH = (
    [(e_read(b'$ion_1_1'), INC), _NEXT_ERROR],
    [(e_read(b'123_'), INC), _NEXT_ERROR],
    [(e_read(b'123e'), INC), _NEXT_ERROR],
    [(e_read(b'123e-'), INC), _NEXT_ERROR],
    [(e_read(b'123d+'), INC), _NEXT_ERROR],
    [(e_read(b'0x'), INC), _NEXT_ERROR],
    [(e_read(b'2000-01-'), INC), _NEXT_ERROR],
    [(e_read(b'"'), INC), _NEXT_ERROR],
    [(e_read(b'/'), INC), _NEXT_ERROR],
    [(e_read(b'abc/'), INC), _NEXT_ERROR],
    [(e_read(b'{/'), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b'/*'), INC), _NEXT_ERROR],
    [(e_read(b'abc/*'), INC), _NEXT_ERROR],
    [(e_read(b'[/*'), e_start_list()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b'(//'), e_start_sexp()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b'(//\n'), e_start_sexp()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b'+in'), INC), _NEXT_ERROR],
    [(e_read(b'null.'), INC), _NEXT_ERROR],
    [(e_read(b'null.str'), INC), _NEXT_ERROR],
    [(e_read(b'"abc'), INC), _NEXT_ERROR],
    [(e_read(b"'abc"), INC), _NEXT_ERROR],
    [(e_read(b"'''abc"), INC), _NEXT_ERROR],
    [(e_read(b"'''abc''''"), INC), _NEXT_ERROR],
    [(e_read(b"{{abc"), INC), _NEXT_ERROR],
    [(e_read(b'{{"abc"'), INC), _NEXT_ERROR],
    [(e_read(b"(abc"), e_start_sexp()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"[abc "), e_start_list()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"['abc' "), e_start_list()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc:def "), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc "), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc: "), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc://"), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc:/**/"), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc:/*"), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc//\n:"), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc/**/:"), e_start_struct()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"(abc "), e_start_sexp()), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"[abc,"), e_start_list()), (NEXT, e_symbol(_st(u'abc'))), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"[abc/**/,"), e_start_list()), (NEXT, e_symbol(_st(u'abc'))), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"[abc,//"), e_start_list()), (NEXT, e_symbol(_st(u'abc'))), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"[abc,/**/"), e_start_list()), (NEXT, e_symbol(_st(u'abc'))), _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"{abc:def,"), e_start_struct()), (NEXT, e_symbol(_st(u'def'), field_name=_st(u'abc'))),
     _NEXT_INC, _NEXT_ERROR],
    [(e_read(b"abc:"), INC), _NEXT_ERROR],
    [(e_read(b"abc/**/:"), INC), _NEXT_ERROR],
    [(e_read(b"abc//\n:"), INC), _NEXT_ERROR],
    [(e_read(b"abc::"), INC), _NEXT_ERROR],
    [(e_read(b"'abc'::"), INC), _NEXT_ERROR],
    [(e_read(b"abc:: //"), INC), _NEXT_ERROR],
    [(e_read(b"'abc' ::/**/"), INC), _NEXT_ERROR],
    [(e_read(b"abc//\n:: "), INC), _NEXT_ERROR],
    [(e_read(b"'abc'/**/::"), INC), _NEXT_ERROR],
    [(e_read(b"abc//\n::/**/"), INC), _NEXT_ERROR],
    [(e_read(b"'abc'/**/:://"), INC), _NEXT_ERROR],
    [(e_read(b'abc'), INC), (NEXT, e_symbol(_st(u'abc'))), _NEXT_END, (e_read(b'::123 '), IonException)],
    [(e_read(b'$10'), INC), (NEXT, e_symbol(_sid(10))), _NEXT_END, (e_read(b'::123 '), IonException)],
)


def _good_container(start, end, *events):
    return (start(),) + events + (end(),)

_good_sexp = partial(_good_container, e_start_sexp, e_end_sexp)
_good_struct = partial(_good_container, e_start_struct, e_end_struct)
_good_list = partial(_good_container, e_start_list, e_end_list)


_GOOD = (
    (b'$ion_1_0 42 ', IVM, e_int(42)),
    (b'$ion_1_0_ 42 ', e_symbol(_st(u'$ion_1_0_')), e_int(42)),
    (b'$ion_1_0a 42 ', e_symbol(_st(u'$ion_1_0a')), e_int(42)),
    (b'$ion_1_ 42 ', e_symbol(_st(u'$ion_1_')), e_int(42)),
    (b'$ion_a_b 42 ', e_symbol(_st(u'$ion_a_b')), e_int(42)),
    (b'$ion_1_b 42 ', e_symbol(_st(u'$ion_1_b')), e_int(42)),
    (b'ann::$ion_1_0 42 ', e_symbol(_st(u'$ion_1_0'), annotations=(_st(u'ann'),)), e_int(42)),
    (b'$ion_1234_1::$ion_1_0 42 ', e_symbol(_st(u'$ion_1_0'), annotations=(_st(u'$ion_1234_1'),)), e_int(42)),
    (b'$ion_1_0::$ion_1234_1 42 ', e_symbol(_st(u'$ion_1234_1'), annotations=(_st(u'$ion_1_0'),)), e_int(42)),
    (b'{$ion_1_0:abc}',) + _good_struct(e_symbol(_st(u'abc'), field_name=_st(u'$ion_1_0'))),
    (b'($ion_1_0)',) + _good_sexp(e_symbol(_st(u'$ion_1_0'))),
    (b'42[]', e_int(42)) + _good_list(),
    (b'\'foo\'123 ', e_symbol(_st(u'foo')), e_int(123)),
    (b'null()', e_null()) + _good_sexp(),
    (b'tru{}', e_symbol(_st(u'tru'))) + _good_struct(),
    (b'{{"foo"}}42{{}}', e_clob(b'foo'), e_int(42), e_blob(b'')),
    (b'+inf"bar"', e_float(_POS_INF), e_string(u'bar')),
    (b'foo\'bar\'"baz"', e_symbol(_st(u'foo')), e_symbol(_st(u'bar')), e_string(u'baz')),
    (b'\'\'\'foo\'\'\'\'\'123 ', e_string(u'foo'), e_symbol(_st(u'')), e_int(123)),
    (b'\'\'\'foo\'\'\'\'abc\'123 ', e_string(u'foo'), e_symbol(_st(u'abc')), e_int(123)),
    (b'[]',) + _good_list(),
    (b'()',) + _good_sexp(),
    (b'{}',) + _good_struct(),
    (b'{/**/}',) + _good_struct(),
    (b'(/**/)',) + _good_sexp(),
    (b'[/**/]',) + _good_list(),
    (b'{//\n}',) + _good_struct(),
    (b'(//\n)',) + _good_sexp(),
    (b'[//\n]',) + _good_list(),
    (b'{/**///\n}',) + _good_struct(),
    (b'(/**///\n)',) + _good_sexp(),
    (b'[/**///\n]',) + _good_list(),
    (b'(foo)',) + _good_sexp(e_symbol(_st(u'foo'))),
    (b'[foo]',) + _good_list(e_symbol(_st(u'foo'))),
    (b'(\'\')',) + _good_sexp(e_symbol(_st(u''))),
    (b'[\'\']',) + _good_list(e_symbol(_st(u''))),
    (b'(\'foo\')',) + _good_sexp(e_symbol(_st(u'foo'))),
    (b'[\'foo\']',) + _good_list(e_symbol(_st(u'foo'))),
    (b'/*foo*///bar\n/*baz*/',),
    (b'/*\\n*///\\u3000\n',),
    (b'\'\'::123 ', e_int(123, annotations=(_st(u''),))),
    (b'{foo:zar::[], bar: (), baz:{}}',) + _good_struct(
        e_start_list(field_name=_st(u'foo'), annotations=(_st(u'zar'),)), e_end_list(),
        e_start_sexp(field_name=_st(u'bar')), e_end_sexp(),
        e_start_struct(field_name=_st(u'baz')), e_end_struct()
    ),
    (b'[[], zar::{}, ()]',) + _good_list(
        e_start_list(), e_end_list(),
        e_start_struct(annotations=(_st(u'zar'),)), e_end_struct(),
        e_start_sexp(), e_end_sexp(),
    ),
    (b'{\'\':bar,}',) + _good_struct(e_symbol(_st(u'bar'), field_name=_st(u''))),
    (b'{\'\':bar}',) + _good_struct(e_symbol(_st(u'bar'), field_name=_st(u''))),
    (b'{\'\'\'foo\'\'\'/**/\'\'\'bar\'\'\':baz}',) + _good_struct(e_symbol(_st(u'baz'), field_name=_st(u'foobar')))
)


_GOOD_UNICODE = (
    (u'{foo:bar}',) + _good_struct(e_symbol(_st(u'bar'), field_name=_st(u'foo'))),
    (u'{foo:"b\xf6\u3000r"}',) + _good_struct(e_string(u'b\xf6\u3000r', field_name=_st(u'foo'))),
    (u'{\'b\xf6\u3000r\':"foo"}',) + _good_struct(e_string(u'foo', field_name=_st(u'b\xf6\u3000r'))),
    (u'\x7b\x7d',) + _good_struct(),
    (u'\u005b\u005d',) + _good_list(),
    (u'\u0028\x29',) + _good_sexp(),
    (u'\u0022\u0061\u0062\u0063\u0022', e_string(u'abc')),
    (u'{foo:"b\xf6\U0001f4a9r"}',) + _good_struct(e_string(u'b\xf6\U0001f4a9r', field_name=_st(u'foo'))),
    (u'{\'b\xf6\U0001f4a9r\':"foo"}',) + _good_struct(e_string(u'foo', field_name=_st(u'b\xf6\U0001f4a9r'))),
    (u'{"b\xf6\U0001f4a9r\":"foo"}',) + _good_struct(e_string(u'foo', field_name=_st(u'b\xf6\U0001f4a9r'))),
    (u'{\'\'\'\xf6\'\'\' \'\'\'\U0001f4a9r\'\'\':"foo"}',) + _good_struct(
        e_string(u'foo', field_name=_st(u'\xf6\U0001f4a9r'))
    ),
    (u'\'b\xf6\U0001f4a9r\'::"foo"', e_string(u'foo', annotations=(_st(u'b\xf6\U0001f4a9r'),))),
    (u'"\t\v\f\'"', e_string(u'\t\v\f\'')),
    (u"'''\t\v\f\"\n\r'''42 ", e_string(u'\t\v\f\"\n\n'), e_int(42))
)

_BAD_UNICODE = (
    (u'\xf6',),  # Not an acceptable identifier symbol.
    (u'r\U0001f4a9',),
    (u'{foo:b\xf6\u3000r}', e_start_struct()),
    (u'{b\xf6\u3000:"foo"}', e_start_struct()),
    (u'{br\U0001f4a9:"foo"}', e_start_struct()),
    (u'{br\U0001f4a9r:"foo"}', e_start_struct()),
    (u'{\'\'\'\xf6\'\'\' \'\'\'\U0001f4a9r\'\'\'a:"foo"}', e_start_struct()),
    (u'b\xf6\U0001f4a9r::"foo"',),
    (u"'''\a'''",),
    (u"{{'''\xf6'''}}",),
    (u'{{"\u3000"}}',),
)

_GOOD_ESCAPES_FROM_UNICODE = (
    (u'"\\xf6"', e_string(u'\xf6')),
    (u'"\\a"', e_string(u'\a')),
    (u'"a\\b"', e_string(u'a\b')),
    (u'"\\r"', e_string(u'\r')),
    (u"'\\xf6'42 ", e_symbol(_st(u'\xf6')), e_int(42)),
    (u"'\\a'42 ", e_symbol(_st(u'\a')), e_int(42)),
    (u"'a\\b'42 ", e_symbol(_st(u'a\b')), e_int(42)),
    (u"'\\r'42 ", e_symbol(_st(u'\r')), e_int(42)),
    (u"'''\\b'''42 ", e_string(u'\b'), e_int(42)),
    (u"'''a\\b'''42 ", e_string(u'a\b'), e_int(42)),
    (u'"\\u3000"', e_string(u'\u3000')),
    (u'"\\udbff\\udfff"', e_string(u'\U0010ffff')),  # Escaped surrogate pair.
    (u'["\\U0001F4a9"]',) + _good_list(e_string(u'\U0001f4a9')),
    (u'"\\t "\'\\\'\'"\\v"', e_string(u'\t '), e_symbol(_st(u'\'')), e_string(u'\v')),
    (u'(\'\\/\')',) + _good_sexp(e_symbol(_st(u'/'))),
    (u'{\'\\f\':foo,"\\?":\'\\\\\'::"\\v\\t"}',) + _good_struct(
        e_symbol(_st(u'foo'), field_name=_st(u'\f')), e_string(u'\v\t', field_name=_st(u'?'), annotations=(_st(u'\\'),))
    ),
    (u'\'\\?\\f\'::\'\\xF6\'::"\\\""', e_string(u'"', annotations=(_st(u'?\f'), _st(u'\xf6')))),
    (u"'''\\\'\\\'\\\''''\"\\\'\"", e_string(u"'''"), e_string(u"'")),
    (u"'''a''\\\'b'''\n'''\\\''''/**/''''\'c'''\"\"", e_string(u"a'''b'''c"), e_string(u'')),
    (u"'''foo''''\\U0001f4a9'42 ", e_string(u'foo'), e_symbol(_st(u'\U0001f4a9')), e_int(42)),
    (u"''''\\\r\n'''42 ", e_string(u"'"), e_int(42)),
    (u'"\\\n"', e_string(u'')),
    (u'"\\\r\n"', e_string(u'')),
    (u'"\\\r"', e_string(u'')),
    (u'"\\\r\\xf6"', e_string(u'\xf6')),
    (u'"\\\rabc"', e_string(u'abc')),
    (u"'\\\r\n'::42 ", e_int(42, annotations=(_st(u''),))),
    (u"{'''\\\rfoo\\\n\r''':bar}",) + _good_struct(e_symbol(_st(u'bar'), field_name=_st(u'foo\n'))),
    (u"{{'''\\x00''''''\\x7e'''}}", e_clob(b'\0~')),
    (u"{{'''\\xff'''}}", e_clob(b'\xff')),
    (u'{{"\\t"}}', e_clob(b'\t')),
    (u'{{"\\\n"}}', e_clob(b'')),
    (u"{{'''\\\r\n'''}}", e_clob(b'')),
)

_GOOD_ESCAPES_FROM_BYTES = (
    (br'"\xf6"', e_string(u'\xf6')),
    (br'"\a"', e_string(u'\a')),
    (br'"a\b"', e_string(u'a\b')),
    (br'"\r"', e_string(u'\r')),
    (br"'\xf6'42 ", e_symbol(_st(u'\xf6')), e_int(42)),
    (br"'\a'42 ", e_symbol(_st(u'\a')), e_int(42)),
    (br"'a\b'42 ", e_symbol(_st(u'a\b')), e_int(42)),
    (br"'\r'42 ", e_symbol(_st(u'\r')), e_int(42)),
    (br"'''\b'''42 ", e_string(u'\b'), e_int(42)),
    (br"'''a\b'''42 ", e_string(u'a\b'), e_int(42)),
    (br'"\u3000"', e_string(u'\u3000')),
    (br'"\udbff\udfff"', e_string(u'\U0010ffff')),  # Escaped surrogate pair.
    (br'["\U0001F4a9"]',) + _good_list(e_string(u'\U0001f4a9')),
    (b'"\\t "\'\\\'\'"\\v"', e_string(u'\t '), e_symbol(_st(u'\'')), e_string(u'\v')),
    (b'(\'\\/\')',) + _good_sexp(e_symbol(_st(u'/'))),
    (b'{\'\\f\':foo,"\\?":\'\\\\\'::"\\v\\t"}',) + _good_struct(
        e_symbol(_st(u'foo'), field_name=_st(u'\f')), e_string(u'\v\t', field_name=_st(u'?'), annotations=(_st(u'\\'),))
    ),
    (b'\'\\?\\f\'::\'\\xF6\'::"\\\""', e_string(u'"', annotations=(_st(u'?\f'), _st(u'\xf6')))),
    (b"'''\\\'\\\'\\\''''\"\\\'\"", e_string(u"'''"), e_string(u"'")),
    (b"'''a''\\\'b'''\n'''\\\''''/**/''''\'c'''\"\"", e_string(u"a'''b'''c"), e_string(u'')),
    (b"'''foo''''\\U0001f4a9'42 ", e_string(u'foo'), e_symbol(_st(u'\U0001f4a9')), e_int(42)),
    (b"''''\\\r\n'''42 ", e_string(u"'"), e_int(42)),
    (b'"\\\n"', e_string(u'')),
    (b'"\\\r\n"', e_string(u'')),
    (b'"\\\r"', e_string(u'')),
    (b'"\\\r\\xf6"', e_string(u'\xf6')),
    (b'"\\\rabc"', e_string(u'abc')),
    (b"'\\\r\n'::42 ", e_int(42, annotations=(_st(u''),))),
    (b"{'''\\\rfoo\\\n\r''':bar}",) + _good_struct(e_symbol(_st(u'bar'), field_name=_st(u'foo\n'))),
    (b"{{'''\\x00''''''\\x7e'''}}", e_clob(b'\0~')),
    (b"{{'''\\xff'''}}", e_clob(b'\xff')),
    (b'{{"\\t"}}', e_clob(b'\t')),
    (b'{{"\\\n"}}', e_clob(b'')),
    (b"{{'''\\\r\n'''}}", e_clob(b'')),
)

_INCOMPLETE_ESCAPES = (
    [(e_read(u'"\\'), INC), (e_read(u't"'), e_string(u'\t')), (NEXT, END)],
    [
        (e_read(u'\'\\x'), INC), (e_read(u'f'), INC), (e_read(u'6\'42 '), e_symbol(_st(u'\xf6'))),
        (NEXT, e_int(42)), (NEXT, END)
    ],
    [
        (e_read(u'{"\\U0001'), e_start_struct()), (NEXT, INC), (e_read(u'f4a9"'), INC),
        (e_read(u':bar}'), e_symbol(_st(u'bar'), field_name=_st(u'\U0001f4a9'))), (NEXT, e_end_struct()), (NEXT, END)
    ],
    [
        (e_read(u"'''\\\r"), INC), (e_read(u"\n'''//\n"), INC), (e_read(u"'''abc'''42 "), e_string(u'abc')),
        (NEXT, e_int(42)), (NEXT, END)
    ],
)

_UNICODE_SURROGATES = (
    # Note: Surrogates only allowed with UCS2.
    [(e_read(u'"\ud83d\udca9"'), e_string(u'\U0001f4a9')), (NEXT, END)],
    [(e_read(u'"\ud83d'), INC), (e_read(u'\udca9"'), e_string(u'\U0001f4a9')), (NEXT, END)],
)

_BAD_ESCAPES_FROM_UNICODE = (
    (u'"\\g"',),
    (u'\'\\q\'',),
    (u'\\t',),
    (u'"abc"\\t', e_string(u'abc')),
    (u'\'abc\'\\n', e_symbol(_st(u'abc'))),
    (u'\'abc\'\\xf6', e_symbol(_st(u'abc'))),
    (u"'''abc'''\\U0001f4a9", e_string(u'abc')),
    (u"''\\u3000", e_symbol(_st(u''))),
    (u"'''\\u3''' '''000'''42 ",),
    (u'"\\U0001f4aQ"',),
    (u"{{'''abc'''\\n}}",),
    (u'{{"abc"\\n}}',),
    (u'{\'foo\'\\v:bar}', e_start_struct()),
    (u'{\'\'\'foo\'\'\'\\xf6:bar}', e_start_struct()),
)

_BAD_ESCAPES_FROM_BYTES = (
    (br'"\g"',),
    (br'\'\q\'',),
    (b'\\t',),
    (b'"abc"\\t', e_string(u'abc')),
    (b'\'abc\'\\n', e_symbol(_st(u'abc'))),
    (b'\'abc\'\\xf6', e_symbol(_st(u'abc'))),
    (b"'''abc'''\\U0001f4a9", e_string(u'abc')),
    (b"''\\u3000", e_symbol(_st(u''))),
    (b"'''\\u3''' '''000'''42 ",),
    (b'"\\U0001f4aQ"',),
    (b"{{'''abc'''\\n}}",),
    (b'{{"abc"\\n}}',),
    (b'{\'foo\'\\v:bar}', e_start_struct()),
    (b'{\'\'\'foo\'\'\'\\xf6:bar}', e_start_struct()),
)

_UNSPACED_SEXPS = (
    (b'(a/b)',) + _good_sexp(e_symbol(_st(u'a')), e_symbol(_st(u'/')), e_symbol(_st(u'b'))),
    (b'(a+b)',) + _good_sexp(e_symbol(_st(u'a')), e_symbol(_st(u'+')), e_symbol(_st(u'b'))),
    (b'(a-b)',) + _good_sexp(e_symbol(_st(u'a')), e_symbol(_st(u'-')), e_symbol(_st(u'b'))),
    (b'(/%)',) + _good_sexp(e_symbol(_st(u'/%'))),
    (b'(foo //bar\n::baz)',) + _good_sexp(e_symbol(_st(u'baz'), annotations=(_st(u'foo'),))),
    (b'(foo/*bar*/ ::baz)',) + _good_sexp(e_symbol(_st(u'baz'), annotations=(_st(u'foo'),))),
    (b'(\'a b\' //\n::cd)',) + _good_sexp(e_symbol(_st(u'cd'), annotations=(_st(u'a b'),))),
    (b'(abc//baz\n-)',) + _good_sexp(e_symbol(_st(u'abc')), e_symbol(_st(u'-'))),
    (b'(null-100/**/)',) + _good_sexp(e_null(), e_int(-100)),
    (b'(//\nnull//\n)',) + _good_sexp(e_null()),
    (b'(abc/*baz*/123)',) + _good_sexp(e_symbol(_st(u'abc')), e_int(123)),
    (b'(abc/*baz*/-)',) + _good_sexp(e_symbol(_st(u'abc')), e_symbol(_st(u'-'))),
    (b'(abc//baz\n123)',) + _good_sexp(e_symbol(_st(u'abc')), e_int(123)),
    (b'(abc//\n/123)',) + _good_sexp(e_symbol(_st(u'abc')), e_symbol(_st(u'/')), e_int(123)),
    (b'(abc/////\n/123)',) + _good_sexp(e_symbol(_st(u'abc')), e_symbol(_st(u'/')), e_int(123)),
    (b'(abc/**//123)',) + _good_sexp(e_symbol(_st(u'abc')), e_symbol(_st(u'/')), e_int(123)),
    (b'(foo%+null-//\n)',) + _good_sexp(
        e_symbol(_st(u'foo')), e_symbol(_st(u'%+')), e_null(), e_symbol(_st(u'-//'))  # Matches java.
    ),
    (b'(null-100)',) + _good_sexp(e_null(), e_int(-100)),
    (b'(null\'a\')',) + _good_sexp(e_null(), e_symbol(_st(u'a'))),
    (b'(null\'a\'::b)',) + _good_sexp(e_null(), e_symbol(_st(u'b'), annotations=(_st(u'a'),))),
    (b'(null.string.b)',) + _good_sexp(e_string(None), e_symbol(_st(u'.')), e_symbol(_st(u'b'))),
    (b'(\'\'\'abc\'\'\'\'\')',) + _good_sexp(e_string(u'abc'), e_symbol(_st(u''))),
    (b'(\'\'\'abc\'\'\'\'foo\')',) + _good_sexp(e_string(u'abc'), e_symbol(_st(u'foo'))),
    (b'(\'\'\'abc\'\'\'\'\'42)',) + _good_sexp(e_string(u'abc'), e_symbol(_st(u'')), e_int(42)),
    (b'(42\'a\'::b)',) + _good_sexp(e_int(42), e_symbol(_st(u'b'), annotations=(_st(u'a'),))),
    (b'(1.23[])',) + _good_sexp(e_decimal(_d(u'1.23')), e_start_list(), e_end_list()),
    (b'(\'\'\'foo\'\'\'/\'\'\'bar\'\'\')',) + _good_sexp(e_string(u'foo'), e_symbol(_st(u'/')), e_string(u'bar')),
    (b'(-100)',) + _good_sexp(e_int(-100)),
    (b'(-1.23 .)',) + _good_sexp(e_decimal(_d(u'-1.23')), e_symbol(_st(u'.'))),
    (b'(1.)',) + _good_sexp(e_decimal(_d(u'1.'))),
    (b'(1. .1)',) + _good_sexp(e_decimal(_d(u'1.')), e_symbol(_st(u'.')), e_int(1)),
    (b'(2001-01-01/**/a)',) + _good_sexp(e_timestamp(_ts(2001, 1, 1, precision=_tp.DAY)), e_symbol(_st(u'a'))),
    (b'(nul)',) + _good_sexp(e_symbol(_st(u'nul'))),
    (b'(foo::%-bar)',) + _good_sexp(e_symbol(_st(u'%-'), annotations=(_st(u'foo'),)), e_symbol(_st(u'bar'))),
    (b'(true.False+)',) + _good_sexp(e_bool(True), e_symbol(_st(u'.')), e_symbol(_st(u'False')), e_symbol(_st(u'+'))),
    (b'(false)',) + _good_sexp(e_bool(False)),
    (b'(-inf)',) + _good_sexp(e_float(_NEG_INF)),
    (b'(+inf)',) + _good_sexp(e_float(_POS_INF)),
    (b'(nan)',) + _good_sexp(e_float(_NAN)),
    (b'(-inf+inf)',) + _good_sexp(e_float(_NEG_INF), e_float(_POS_INF)),
    (b'(+inf\'foo\')',) + _good_sexp(e_float(_POS_INF), e_symbol(_st(u'foo'))),
    (b'(-inf\'foo\'::bar)',) + _good_sexp(e_float(_NEG_INF), e_symbol(_st(u'bar'), annotations=(_st(u'foo'),))),
    # TODO the inf tests do not match ion-java's behavior. They should be reconciled. I believe this is more correct.
    (b'(- -inf-inf-in-infs-)',) + _good_sexp(
        e_symbol(_st(u'-')), e_float(_NEG_INF), e_float(_NEG_INF), e_symbol(_st(u'-')),
        e_symbol(_st(u'in')), e_symbol(_st(u'-')), e_symbol(_st(u'infs')), e_symbol(_st(u'-'))
    ),
    (b'(+ +inf+inf+in+infs+)',) + _good_sexp(
        e_symbol(_st(u'+')), e_float(_POS_INF), e_float(_POS_INF), e_symbol(_st(u'+')),
        e_symbol(_st(u'in')), e_symbol(_st(u'+')), e_symbol(_st(u'infs')), e_symbol(_st(u'+'))
    ),
    (b'(nan-nan+nan)',) + _good_sexp(
        e_float(_NAN), e_symbol(_st(u'-')), e_float(_NAN), e_symbol(_st(u'+')),
        e_float(_NAN)
    ),
    (b'(nans-inf+na-)',) + _good_sexp(
        e_symbol(_st(u'nans')), e_float(_NEG_INF), e_symbol(_st(u'+')),
        e_symbol(_st(u'na')), e_symbol(_st(u'-'))
    ),
    (b'({}()zar::[])',) + _good_sexp(
        e_start_struct(), e_end_struct(),
        e_start_sexp(), e_end_sexp(),
        e_start_list(annotations=(_st(u'zar'),)), e_end_list()
    ),
)

_GOOD_SCALARS = (
    (b'null', e_null()),

    (b'false', e_bool(False)),
    (b'true', e_bool(True)),
    (b'null.bool', e_bool()),

    (b'null.int', e_int()),
    (b'0', e_int(0)),
    (b'1_2_3', e_int(123)),
    (b'0xfe', e_int(254)),
    (b'0b101', e_int(5)),
    (b'0b10_1', e_int(5)),
    (b'-0b101', e_int(-5)),
    (b'-0b10_1', e_int(-5)),
    (b'1', e_int(1)),
    (b'-1', e_int(-1)),
    (b'0xc1c2', e_int(49602)),
    (b'0xc1_c2', e_int(49602)),
    (b'-0xc1c2', e_int(-49602)),
    (b'-0xc1_c2', e_int(-49602)),
    (b'9223372036854775808', e_int(9223372036854775808)),
    (b'-9223372036854775809', e_int(-9223372036854775809)),

    (b'null.float', e_float()),
    (b'0.0e1', e_float(0.)),
    (b'-0.0e-1', e_float(-0.)),
    (b'0.0e+1', e_float(0.)),
    (b'0.0E1', e_float(0.)),
    (b'-inf', e_float(_NEG_INF)),
    (b'+inf', e_float(_POS_INF)),
    (b'nan', e_float(_NAN)),

    (b'null.decimal', e_decimal()),
    (b'0.0', e_decimal(_d(u'0.0'))),
    (b'0.', e_decimal(_d(u'0.'))),
    (b'-0.0', e_decimal(_d(u'-0.0'))),
    (b'0d-1000', e_decimal(_d(u'0e-1000'))),
    (b'0d1000', e_decimal(_d(u'0e1000'))),
    (b'1d1', e_decimal(_d(u'1e1'))),
    (b'1D1', e_decimal(_d(u'1e1'))),
    (b'1234d-20', e_decimal(_d(u'1234e-20'))),
    (b'1234d+20', e_decimal(_d(u'1234e20'))),
    (b'1d0', e_decimal(_d(u'1e0'))),
    (b'1d-1', e_decimal(_d(u'1e-1'))),
    (b'0d-1', e_decimal(_d(u'0e-1'))),
    (b'0d1', e_decimal(_d(u'0e1'))),
    (b'-1d1', e_decimal(_d(u'-1e1'))),
    (b'-1d0', e_decimal(_d(u'-1e0'))),
    (b'-1d-1', e_decimal(_d(u'-1e-1'))),
    (b'-0d-1', e_decimal(_d(u'-0e-1'))),
    (b'-0d1', e_decimal(_d(u'-0e1'))),

    (b'null.timestamp', e_timestamp()),
    (b'2007-01T', e_timestamp(_ts(2007, 1, precision=_tp.MONTH))),
    (b'2007T', e_timestamp(_ts(2007, precision=_tp.YEAR))),
    (b'2007-01-01', e_timestamp(_ts(2007, 1, 1, precision=_tp.DAY))),
    (
        b'2000-01-01T00:00:00.0Z',
        e_timestamp(_ts(
            2000, 1, 1, 0, 0, 0, 0, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=1
        ))
    ),
    (
        b'2000-01-01T00:00:00.000Z',
        e_timestamp(_ts(
            2000, 1, 1, 0, 0, 0, 0, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=3
        ))
    ),
    (
        b'2000-01-01T00:00:00.999999Z',
        e_timestamp(_ts(
            2000, 1, 1, 0, 0, 0, 999999, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=6
        ))
    ),
    (
        b'2000-01-01T00:00:00.99999900000Z',
        e_timestamp(_ts(
            2000, 1, 1, 0, 0, 0, 999999, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=6
        ))
    ),
    (
        b'2000-01-01T00:00:00.9999999Z',
        e_timestamp(_ts(
            2000, 1, 1, 0, 0, 0, None, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=None,
            fractional_seconds=Decimal('0.9999999')
        ))
    ),
    (
        b'2000-01-01T00:00:00.1234567Z',
        e_timestamp(_ts(
            2000, 1, 1, 0, 0, 0, None, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=None,
            fractional_seconds=Decimal('0.1234567')
        ))
    ),
    (
        b'2000-01-01T00:00:00.1234567800Z',
        e_timestamp(_ts(
            2000, 1, 1, 0, 0, 0, None, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=None,
            fractional_seconds=Decimal('0.1234567800')
        ))
    ),
    (
        b'2000-01-01T00:00:00.000-00:00',
        e_timestamp(_ts(2000, 1, 1, 0, 0, 0, 0, precision=_tp.SECOND, fractional_precision=3))
    ),
    (
        b'2007-02-23T00:00+00:00',
        e_timestamp(_ts(2007, 2, 23, 0, 0, off_hours=0, off_minutes=0, precision=_tp.MINUTE))
    ),
    (b'2007-01-01T', e_timestamp(_ts(2007, 1, 1, precision=_tp.DAY))),
    (b'2000-01-01T00:00:00Z', e_timestamp(_ts(2000, 1, 1, 0, 0, 0, off_hours=0, off_minutes=0, precision=_tp.SECOND))),
    (
        b'2007-02-23T00:00:00-00:00',
        e_timestamp(_ts(2007, 2, 23, 0, 0, 0, precision=_tp.SECOND))
    ),
    (
        b'2007-02-23T12:14:33.079-08:00',
        e_timestamp(_ts(
            2007, 2, 23, 12, 14, 33, 79000, off_hours=-8, off_minutes=0, precision=_tp.SECOND, fractional_precision=3
        ))
    ),
    (
        b'2007-02-23T20:14:33.079Z',
        e_timestamp(_ts(
            2007, 2, 23, 20, 14, 33, 79000, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=3
        ))
    ),
    (
        b'2007-02-23T20:14:33.079+00:00',
        e_timestamp(_ts(
            2007, 2, 23, 20, 14, 33, 79000, off_hours=0, off_minutes=0, precision=_tp.SECOND, fractional_precision=3
        ))
    ),
    (b'0001T', e_timestamp(_ts(1, precision=_tp.YEAR))),
    (b'0001-01-01T00:00:00Z', e_timestamp(_ts(1, 1, 1, 0, 0, 0, off_hours=0, off_minutes=0, precision=_tp.SECOND))),
    (b'2016-02-29T', e_timestamp(_ts(2016, 2, 29, precision=_tp.DAY))),

    (b'null.symbol', e_symbol()),
    (b'nul', e_symbol(_st(u'nul'))),  # See the logic in the event generators that forces these to emit an event.
    (b'$foo', e_symbol(_st(u'$foo'))),
    (b'$10', e_symbol(_sid(10))),
    (b'$10n', e_symbol(_st(u'$10n'))),
    (b'$2', e_symbol(_sid(2))),  # Note: NOT an IVM event
    (b"'$ion_1_0'", e_symbol(_st(u'$ion_1_0'))),  # Note: NOT an IVM event
    (b'$', e_symbol(_st(u'$'))),
    (b'\'a b\'', e_symbol(_st(u'a b'))),
    (b'\'\'', e_symbol(_st(u''))),

    (b'null.string', e_string()),
    (b'" "', e_string(u' ')),
    (b'\'\'\'foo\'\'\' \'\'\'\'\'\' \'\'\'""\'\'\'', e_string(u'foo""')),
    (b'\'\'\'ab\'\'cd\'\'\'', e_string(u'ab\'\'cd')),
    (b"'''\r\n \r \n \n\r'''", e_string(u'\n \n \n \n\n')),

    (b'null.clob', e_clob()),
    (b'{{""}}', e_clob(b'')),
    (b'{{ "abcd" }}', e_clob(b'abcd')),
    (b'{{"abcd"}}', e_clob(b'abcd')),
    (b'{{"abcd"\n}}', e_clob(b'abcd')),
    (b'{{\'\'\'ab\'\'\' \'\'\'cd\'\'\'}}', e_clob(b'abcd')),
    (b'{{\'\'\'ab\'\'\'\n\'\'\'cd\'\'\'}}', e_clob(b'abcd')),

    (b'null.blob', e_blob()),
    (b'{{}}', e_blob(b'')),
    (b'{{ YW1heg== }}', e_blob(b'amaz')),
    (b'{{ YW1hem8= }}', e_blob(b'amazo')),
    (b'{{ YW1hem9u }}', e_blob(b'amazon')),
    (b'{{ YW1heg = = }}', e_blob(b'amaz')),
    (b'{{aW\n9u}}', e_blob(b'ion')),
    (b'{{aW9u}}', e_blob(b'ion')),

    (b'null.list', e_null_list()),

    (b'null.sexp', e_null_sexp()),

    (b'null.struct', e_null_struct()),
)


def _scalar_event_pairs(data, events, info):
    """Generates event pairs for all scalars.

    Each scalar is represented by a sequence whose first element is the raw data and whose following elements are the
    expected output events.
    """
    first = True
    delimiter, in_container = info
    space_delimited = not (b',' in delimiter)
    for event in events:
        input_event = NEXT
        if first:
            input_event = e_read(data + delimiter)
            if space_delimited and event.value is not None \
                and ((event.ion_type is IonType.SYMBOL) or
                     (event.ion_type is IonType.STRING and
                      six.byte2int(b'"') != six.indexbytes(data, 0))):  # triple-quoted strings
                # Because annotations and field names are symbols, a space delimiter after a symbol isn't enough to
                # generate a symbol event immediately. Similarly, triple-quoted strings may be followed by another
                # triple-quoted string if only delimited by whitespace or comments.
                yield input_event, INC
                if in_container:
                    # Within s-expressions, these types are delimited in these tests by another value - in this case,
                    # int 0 (but it could be anything).
                    yield e_read(b'0' + delimiter), event
                    input_event, event = (NEXT, e_int(0))
                else:
                    # This is a top-level value, so it may be flushed with NEXT after INCOMPLETE.
                    input_event, event = (NEXT, event)
            first = False
        yield input_event, event


_scalar_iter = partial(value_iter, _scalar_event_pairs, _GOOD_SCALARS)


@coroutine
def _scalar_params():
    """Generates scalars as reader parameters."""
    while True:
        info = yield
        for data, event_pairs in _scalar_iter(info):
            yield _P(
                desc=data,
                event_pairs=event_pairs + [(NEXT, INC)]
            )


def _top_level_value_params(delimiter=b' ', is_delegate=False):
    """Converts the top-level tuple list into parameters with appropriate ``NEXT`` inputs.

    The expectation is starting from an end of stream top-level context.
    """
    info = (delimiter, False)
    for data, event_pairs in _scalar_iter(info):
        _, first = event_pairs[0]
        if first.event_type is IonEventType.INCOMPLETE:  # Happens with space-delimited symbol values.
            _, first = event_pairs[1]
        yield _P(
            desc='TL %s - %s - %r' %
                 (first.event_type.name, first.ion_type.name, data),
            event_pairs=[(NEXT, END)] + event_pairs + [(NEXT, END)],
        )
    if is_delegate:
        yield


@coroutine
def _all_scalars_in_one_container_params():
    """Generates one parameter that contains all scalar events in a single container. """
    while True:
        info = yield

        @listify
        def generate_event_pairs():
            for data, event_pairs in _scalar_iter(info):
                pairs = ((i, o) for i, o in event_pairs)
                while True:
                    try:
                        input_event, output_event = next(pairs)
                        yield input_event, output_event
                        if output_event is INC:
                            # This is a symbol value.
                            yield next(pairs)  # Input: a scalar. Output: the symbol value's event.
                            yield next(pairs)  # Input: NEXT. Output: the previous scalar's event.
                        yield (NEXT, INC)
                    except StopIteration:
                        break

        yield _P(
            desc='ALL',
            event_pairs=generate_event_pairs()
        )


def _collect_params(param_generator, info):
    """Collects all output of the given coroutine into a single list."""
    params = []
    while True:
        param = param_generator.send(info)
        if param is None:
            return params
        params.append(param)


_TEST_SYMBOLS = (
    (
        b'foo',
        b'$foo',
        b'$ios',
        b'$',
        b'$10',
        b'\'a b\'',
        b'foo ',
        b'\'a b\' ',
        b'foo/*bar*/',
        b'\'a b\' //bar\r',
        b'\'\'',
        b'\'\\U0001f4a9\'',
    ),
    (
        _st(u'foo'),
        _st(u'$foo'),
        _st(u'$ios'),
        _st(u'$'),
        _sid(10),
        _st(u'a b'),
        _st(u'foo'),
        _st(u'a b'),
        _st(u'foo'),
        _st(u'a b'),
        _st(u''),
        _st(u'\U0001f4a9'),
    )
)

_TEST_FIELD_NAMES = (
    _TEST_SYMBOLS[0] +
    (
        b'"foo"',
        b'"foo"//bar\n',
        b'\'\'\'foo\'\'\'/*bar*/\'\'\'baz\'\'\'',
        b'//zar\n\'\'\'foo\'\'\'/*bar*/\'\'\'baz\'\'\'',
        b'\'\'\'a \'\'\'\t\'\'\'b\'\'\'',
        b'\'\'\'a \'\'\'\'\'\'b\'\'\'/*zar*/',
        b'\'\'\'\'\'\'',
        b'"\\xf6"',
        b"'''\r\n \r \n \n\r'''",
    ),
    _TEST_SYMBOLS[1] +
    (
        _st(u'foo'),
        _st(u'foo'),
        _st(u'foobaz'),
        _st(u'foobaz'),
        _st(u'a b'),
        _st(u'a b'),
        _st(u''),
        _st(u'\xf6'),
        _st(u'\n \n \n \n\n'),
    )
)


def _generate_annotations():
    """Circularly generates annotations."""
    assert len(_TEST_SYMBOLS[0]) == len(_TEST_SYMBOLS[1])
    i = 1
    num_symbols = len(_TEST_SYMBOLS[0])
    while True:
        yield _TEST_SYMBOLS[0][0:i], _TEST_SYMBOLS[1][0:i]
        i += 1
        if i == num_symbols:
            i = 0


_annotations_generator = _generate_annotations()


@coroutine
def _annotate_params(params, is_delegate=False):
    """Adds annotation wrappers for a given iterator of parameters."""

    while True:
        info = yield
        params_list = _collect_params(params, info)
        test_annotations, expected_annotations = next(_annotations_generator)
        for param in params_list:
            @listify
            def annotated():
                pairs = ((i, o) for i, o in param.event_pairs)
                while True:
                    try:
                        input_event, output_event = next(pairs)
                        if input_event.type is ReadEventType.DATA:
                            data = b''
                            for test_annotation in test_annotations:
                                data += test_annotation + b'::'
                            data += input_event.data
                            input_event = read_data_event(data)
                            if output_event is INC:
                                yield input_event, output_event
                                input_event, output_event = next(pairs)
                            output_event = output_event.derive_annotations(expected_annotations)
                        yield input_event, output_event
                    except StopIteration:
                        break

            yield _P(
                desc='ANN %r on %s' % (expected_annotations, param.desc),
                event_pairs=annotated(),
            )
        if not is_delegate:
            break


def _generate_field_name():
    """Circularly generates field names."""
    assert len(_TEST_FIELD_NAMES[0]) == len(_TEST_FIELD_NAMES[1])
    i = 0
    num_symbols = len(_TEST_FIELD_NAMES[0])
    while True:
        yield _TEST_FIELD_NAMES[0][i], _TEST_FIELD_NAMES[1][i]
        i += 1
        if i == num_symbols:
            i = 0


_field_name_generator = _generate_field_name()


@coroutine
def _containerize_params(param_generator, with_skip=True, is_delegate=False, top_level=True):
    """Adds container wrappers for a given iteration of parameters."""
    while True:
        yield
        for info in ((IonType.LIST, b'[', b']', b','),
                     (IonType.SEXP, b'(', b')', b' '),  # Sexps without delimiters are tested separately
                     (IonType.STRUCT, b'{ ', b'}', b','),  # Space after opening bracket for instant event.
                     (IonType.LIST, b'[/**/', b'//\n]', b'//\r,'),
                     (IonType.SEXP, b'(//\n', b'/**/)', b'/**/'),
                     (IonType.STRUCT, b'{/**/', b'//\r}', b'/**/,')):
            ion_type = info[0]
            params = _collect_params(param_generator, (info[3], True))
            for param in params:
                @listify
                def add_field_names(event_pairs):
                    container = False
                    first = True
                    for read_event, ion_event in event_pairs:
                        if not container and read_event.type is ReadEventType.DATA:
                            field_name, expected_field_name = next(_field_name_generator)
                            data = field_name + b':' + read_event.data
                            read_event = read_data_event(data)
                            ion_event = ion_event.derive_field_name(expected_field_name)
                        if first and ion_event.event_type is IonEventType.CONTAINER_START:
                            # For containers within a struct--only the CONTAINER_START event gets adorned with a
                            # field name
                            container = True
                        first = False
                        yield read_event, ion_event
                start = []
                end = [(e_read(info[2]), e_end(ion_type))]
                if top_level:
                    start = [(NEXT, END)]
                    end += [(NEXT, END)]
                else:
                    end += [(NEXT, INC)]
                start += [
                    (e_read(info[1]), e_start(ion_type)),
                    (NEXT, INC)
                ]
                if ion_type is IonType.STRUCT:
                    mid = add_field_names(param.event_pairs)
                else:
                    mid = param.event_pairs
                desc = 'CONTAINER %s - %s' % (ion_type.name, param.desc)
                yield _P(
                    desc=desc,
                    event_pairs=start + mid + end,
                )
                if with_skip:
                    @listify
                    def only_data_inc(event_pairs):
                        for read_event, ion_event in event_pairs:
                            if read_event.type is ReadEventType.DATA:
                                yield read_event, INC

                    start = start[:-1] + [(SKIP, INC)]
                    mid = only_data_inc(mid)
                    yield _P(
                        desc='SKIP %s' % desc,
                        event_pairs=start + mid + end,
                    )
        if not is_delegate:
            break


def _expect_event(expected_event, data, events, delimiter):
    """Generates event pairs for a stream that ends in an expected event (or exception), given the text and the output
    events preceding the expected event.
    """
    events += (expected_event,)
    outputs = events[1:]
    event_pairs = [(e_read(data + delimiter), events[0])] + list(zip([NEXT] * len(outputs), outputs))
    return event_pairs


@coroutine
def _basic_params(event_func, desc, delimiter, data_event_pairs, is_delegate=False, top_level=True):
    """Generates parameters from a sequence whose first element is the raw data and the following
    elements are the expected output events.
    """
    while True:
        yield
        params = list(zip(*list(value_iter(event_func, data_event_pairs, delimiter))))[1]
        for param in _paired_params(params, desc, top_level):
            yield param
        if not is_delegate:
            break


def _paired_params(params, desc, top_level=True):
    """Generates reader parameters from sequences of input/output event pairs."""
    for event_pairs in params:
        data = event_pairs[0][0].data
        if top_level:
            event_pairs = [(NEXT, END)] + event_pairs
        yield _P(
            desc='%s %s' % (desc, data),
            event_pairs=event_pairs,
            is_unicode=isinstance(data, six.text_type)
        )


_ion_exception = partial(_expect_event, IonException)
_bad_grammar_params = partial(_basic_params, _ion_exception, 'BAD GRAMMAR', b' ')
_bad_unicode_params = partial(_basic_params, _ion_exception, 'BAD GRAMMAR - UNICODE', u' ')
_value_error = partial(_expect_event, ValueError)
_bad_value_params = partial(_basic_params, _value_error, 'BAD VALUE', b' ')
_incomplete = partial(_expect_event, INC)
_incomplete_params = partial(_basic_params, _incomplete, 'INC', b'')
_end = partial(_expect_event, END)
_good_params = partial(_basic_params, _end, 'GOOD', b'')
_good_unicode_params = partial(_basic_params, _end, 'GOOD - UNICODE', u'')


@parametrize(*chain(
    _good_params(_GOOD),
    _bad_grammar_params(_BAD_GRAMMAR),
    _bad_value_params(_BAD_VALUE),
    _incomplete_params(_INCOMPLETE),
    _good_unicode_params(_GOOD_UNICODE),
    _good_unicode_params(_GOOD_ESCAPES_FROM_UNICODE),
    _good_params(_GOOD_ESCAPES_FROM_BYTES),
    _bad_unicode_params(_BAD_UNICODE),
    _bad_unicode_params(_BAD_ESCAPES_FROM_UNICODE),
    _bad_grammar_params(_BAD_ESCAPES_FROM_BYTES),
    _paired_params(_INCOMPLETE_ESCAPES, 'INCOMPLETE ESCAPES'),
    _paired_params(_UNICODE_SURROGATES, 'UNICODE SURROGATES') if _NARROW_BUILD else (),
    _good_params(_UNSPACED_SEXPS),
    _paired_params(_SKIP, 'SKIP'),
    _paired_params(_GOOD_FLUSH, 'GOOD FLUSH'),
    _paired_params(_BAD_FLUSH, 'BAD FLUSH'),
    # All top-level values as individual data events, space-delimited.
    _top_level_value_params(),
    # All top-level values as one data event, space-delimited.
    all_top_level_as_one_stream_params(_scalar_iter, (b' ', False)),
    # All top-level values as one data event, block comment-delimited.
    all_top_level_as_one_stream_params(_scalar_iter, (b'/*foo*/', False)),
    # All top-level values as one data event, line comment-delimited.
    all_top_level_as_one_stream_params(_scalar_iter, (b'//foo\n', False)),
    # All annotated top-level values, space-delimited.
    _annotate_params(_top_level_value_params(is_delegate=True)),
    # All annotated top-level values, comment-delimited.
    _annotate_params(_top_level_value_params(b'//foo\n/*bar*/', is_delegate=True)),
    _annotate_params(_good_params(_UNSPACED_SEXPS, is_delegate=True)),
    # All values, each as the only value within a container.
    _containerize_params(_scalar_params()),
    _containerize_params(_containerize_params(_scalar_params(), is_delegate=True, top_level=False), with_skip=False),
    # All values, annotated, each as the only value within a container.
    _containerize_params(_annotate_params(_scalar_params(), is_delegate=True)),
    # All values within a single container.
    _containerize_params(_all_scalars_in_one_container_params()),
    # Annotated containers.
    _containerize_params(_annotate_params(_all_scalars_in_one_container_params(), is_delegate=True)),
    # All unspaced sexps, annotated, in containers.
    _containerize_params(_annotate_params(_incomplete_params(
        _UNSPACED_SEXPS, is_delegate=True, top_level=False), is_delegate=True
    )),
))
def test_raw_reader(p):
    reader_scaffold(reader(is_unicode=p.is_unicode), p.event_pairs)
