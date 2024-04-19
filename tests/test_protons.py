from typing import Tuple, List

import pytest

from amazon.protonic.protons import *
from amazon.protonic.protons import _failure, _success


def expect_value(v, next=None):
    """
    If next is None expects that the result context is exhausted.
    Otherwise expects next to match the first bytes in the buffer.
    """
    def expect(_, result):
        assert result.type is ResultType.SUCCESS
        assert v == result.value
        if next:
            n = len(next)
            assert result.context.read(n) == next
        else:
            assert not result.context.avail()
    return expect


def expect_value_if_done(v):
    """
    Expects v if the source is complete, otherwise incomplete.
    """
    def expect(ctx, result):
        if ctx.source.is_complete():
            expect_value(v)(ctx, result)
        else:
            expect_incomplete()(ctx, result)
    return expect


def expect_failure():
    def expect(ctx, result):
        assert result.type is ResultType.FAILURE
        assert ctx.avail() == result.context.avail()
    return expect


def expect_incomplete():
    def expect(ctx, result):
        assert result.type is ResultType.INCOMPLETE
        assert ctx.avail() == result.context.avail()
    return expect


def expect_inc_or_fail():
    def expect(ctx, result):
        if ctx.source.is_complete():
            assert result.type is ResultType.FAILURE
        else:
            assert result.type is ResultType.INCOMPLETE
        assert ctx.avail() == result.context.avail()
    return expect


def parameterify(*tests: Tuple[Parser, List]):
    return (["rule", "data", "expect"],
            [(rule, data, expect) for (rule, ts) in tests for (data, expect) in ts])


@pytest.mark.parametrize(*parameterify(
    (tag(b"spam"), [
        ("spam", expect_value(b"spam")),
        ("spam musubi", expect_value(b"spam", next=b" ")),
        ("eggs", expect_failure()),
        ("", expect_inc_or_fail()),
        ("spa", expect_inc_or_fail())
    ]),
    (one_of(b"abc"), [
        ("b", expect_value(b"b")),
        ("abc", expect_value(b"a", next=b"b")),
        ("d", expect_failure()),
        ("", expect_inc_or_fail())
    ]),
    (value(tag(b"spam"), "eggs"), [
        ("spam", expect_value("eggs")),
        ("spam musubi", expect_value("eggs", next=b" ")),
        ("beef", expect_failure())
    ]),
    (delim(tag(b"{"), tag(b" "), tag(b"}")), [
        ("{ }", expect_value(b" ")),
        ("{ };", expect_value(b" ", next=b";")),
        ("{}", expect_failure()),
        ("{", expect_inc_or_fail()),
        ("{ ", expect_inc_or_fail()),
        ("", expect_inc_or_fail()),
        ("[]", expect_failure()),
        (" }", expect_failure()),
        ("{bad}", expect_failure())
    ]),
    (take_while(lambda b: ord(b'a') <= b <= ord(b'c')), [
        ("abc", expect_value_if_done(b"abc")),
        ("abc123", expect_value(b"abc", next=b"1")),
        ("", expect_value_if_done(b"")),
        ("123", expect_value(b"", next=b"1"))
    ]),
    (terminated(tag(b"foo"), tag(b";")), [
        ("foo;", expect_value(b"foo")),
        ("foo|", expect_failure()),
        ("foo", expect_inc_or_fail()),
        ("qux", expect_failure()),
        ("", expect_inc_or_fail())
    ]),
    (preceded(tag(b"> "), tag(b"spam")), [
        ("> spam", expect_value(b"spam")),
        ("$ spam", expect_failure()),
        ("> eggs", expect_failure()),
        ("", expect_inc_or_fail()),
        ("> ", expect_inc_or_fail())
    ]),
    (alt(tag(b"spam"), tag(b"spa"), tag(b"eggs")), [
        ("spa", expect_value_if_done(b"spa")),
        ("eggs", expect_value(b"eggs")),
        ("ham", expect_failure())
    ]),
    (is_eof(), [
        ("", expect_value_if_done(None)),
        ("a", expect_failure())
    ])
))
def test_rule(rule, data, expect):
    """
    Tests rule against both complete and incomplete data sources.
    """
    ctx = context_from(data)
    result = rule(ctx)
    expect(ctx, result)

    ctx = context_from(data, True)
    result = rule(ctx)
    expect(ctx, result)


def context_from(strdata, end=False):
    source = DataSource()
    source.extend(bytes(strdata, "utf-8"))
    if end:
        source.eof()

    return ParserContext(source)


def expect_next(next) -> Parser:
    def p(c: ParserContext):
        if not next:
            assert not c.avail()
        elif not c.avail():
            return _failure(c)
        else:
            # bytes() is not necessary but improves failure message
            assert next == bytes(c.read(1))
        return _success(c, b"")
    return p
