from dataclasses import dataclass
from enum import Enum
from typing import Callable, Any

from amazon.protonic.data_source import DataSource

"""
"protonic" is a parser combinator library for parsing ion and similar document 
formats in python.

Parser combinators abstract a lot of the undifferentiated bits of parsing. They 
represent a good middle ground between a direct transformation of an abstract
grammar representation and a bunch of hand-written parsing code.

The goals:
- Keep it simple: avoid unnecessary indirection, keep scope small, avoid fancy
  ast manipulations.
- Support streaming: receive incremental inputs and produce incremental results
- Reasonably performant: avoid data copying, reduce call overhead and reference
  counting.
- Enable good error messaging. TODO more here!
- Simple to extend: users can write their own functions and combinators.
"""


class ParseError(Exception):

    def __init__(self, message):
        self.message = message


@dataclass
class ParserContext:
    """
    Wraps a mutable DataSource to provide an immutable facade.

    Intended to hold other metadata information if/when the need
    arises.

    FIXME: As implemented now this is super dangerous because if you
     advance the underlying buffer it silently invalidates all of your
     outstanding ParserContexts. I need to fix this but for now I am
     doing this because it gives me the api in the combinators that i
     want without either a bunch of data copying or doing something
     more complex like weakrefs.
    """

    # TODO: add col/line or other context info for errors
    source: DataSource
    cursor: int = 0

    def avail(self):
        return len(self.source) - self.cursor

    def read(self, n):
        start = self.cursor
        end = start + n
        return self.source[start:end]

    def remaining(self, n):
        return ParserContext(self.source, self.cursor + n)

    def advance(self):
        """
        drop anything from the buffer before cursor and reset cursor
        to start of current buffer
        """
        self.source.advance(self.cursor)
        self.cursor = 0


class ResultType(Enum):
    SUCCESS = "Success"
    FAILURE = "Failure"
    INCOMPLETE = "Incomplete"
    DONE = "Done"


@dataclass
class ParseResult:
    """
    base-class for parse results.
    """
    type: ResultType
    context: ParserContext


@dataclass
class SuccessResult(ParseResult):
    value: Any
    type = ResultType.SUCCESS

    def __init__(self, context, value):
        self.context = context
        self.value = value


Parser = Callable[[ParserContext], ParseResult]


def tag(tag: bytes) -> Parser:
    """
    Match a sequence of bytes, a "tag"
    """
    l = len(tag)

    def p(ctx: ParserContext):
        avail = ctx.avail()
        if avail < l:
            # todo what happens when 0 is avail? that should be inomplete right?
            if ctx.read(avail) == tag[:avail]:
                return _inc_or_fail(ctx)
            else:
                return _failure(ctx)

        elif ctx.read(l) == tag:
            return _success(ctx.remaining(l), tag)
        else:
            return _failure(ctx)
    return p


def one_of(items: bytes) -> Parser:
    """
    Match one of the bytes passed.
    """
    def p(ctx):
        if not ctx.avail():
            return _inc_or_fail(ctx)

        b = ctx.read(1)

        if b[0] in items:
            return _success(ctx.remaining(1), b)
        else:
            return _failure(ctx)
    return p


def terminated(item: Parser, terminal: Parser) -> Parser:
    """
    checks that the value is terminated with terminal, which is not
    consumed. result is that produced by item if both succeed.
    """
    def p(ctx: ParserContext):
        result = item(ctx)
        if result.type is not ResultType.SUCCESS:
            return ParseResult(result.type, ctx)

        ended = terminal(result.context)
        if ended.type is not ResultType.SUCCESS:
            return ParseResult(ended.type, ctx)

        return _success(ended.context, result.value)
    return p


def delim(start: Parser, value: Parser, end: Parser) -> Parser:
    """
    result of value is returned

    if start matches.

    this doesn't do any checks about incomplete vs done itself: it delegates that to it's component parsers
    """
    def p(ctx):
        started = start(ctx)
        if started.type is not ResultType.SUCCESS:
            return ParseResult(started.type, ctx)

        body = value(started.context)
        if body.type is not ResultType.SUCCESS:
            return ParseResult(body.type, ctx)

        ended = end(body.context)
        if ended.type is not ResultType.SUCCESS:
            return ParseResult(ended.type, ctx)

        return _success(ended.context, body.value)
    return p


def take_while(pred) -> Parser:
    """
    pred is fed one byte at a time. Why is it written this way?
    I don't want to add the complexity in surface area to generalize
    it to take a parser _and_ doing so would mean i'd need to combine
    the results in a general way, _and_ ensure the parser made progress...
    so this is the simplest and more performant thing i can think to do
    while still having some generality.

    if the data ends before pred returns False _and_ there may be more
    then it will be Incomplete.

    pred can signal an error by raising ParserError.
    """
    def p(ctx: ParserContext):
        initial_ctx = ctx
        n = 0

        while ctx.avail():
            result = pred(ctx.read(1)[0])
            if not result:
                return _success(ctx, initial_ctx.read(n))

            ctx = ctx.remaining(1)
            n += 1

        if ctx.source.is_complete():
            return _success(ctx, initial_ctx.read(n))
        else:
            return _incomplete(initial_ctx)
    return p


def value(parser: Parser, value: Any) -> Parser:
    """
    If the parser succeeds, the resulting value will be replaced by the value.
    """
    return map_value(parser, lambda _: value)


def map_value(parser: Parser, mapper: Callable) -> Parser:
    """
    If the parser succeeds, the mapper will be called with the value
    of the result and the return value will replace the value.
    """
    def p(ctx):
        result = parser(ctx)
        if result.type is ResultType.SUCCESS:
            return _success(result.context, mapper(result.value))
        else:
            return result

    return p


def alt(*parsers: Parser) -> Parser:
    """
    A switch between a variable number of parsers.

    A success or incomplete will propagate from the first that matchers.
    Users are responsible for ordering parsers so that any that may complete
    will come before potential incompletes.
    """
    def p(ctx):
        for parser in parsers:
            result = parser(ctx)
            if result.type is ResultType.SUCCESS or result.type is ResultType.INCOMPLETE:
                return result
        return _failure(ctx)

    return p


def is_eof() -> Parser:
    """
    Results in the Successful None value when source is exhausted and marked EOF.

    Incomplete when source is exhausted but not EOF.

    Failure when there is data.
    """
    def p(ctx):
        if not ctx.avail():
            if ctx.source.is_complete():
                return _success(ctx, None)
            else:
                return _incomplete(ctx)
        else:
            return _failure(ctx)
    return p


def preceded(prefix, item) -> Parser:
    """
    checks that the prefix matches then returns item if it matches.
    """
    def p(ctx: ParserContext):

        prior = prefix(ctx)
        if prior.type is not ResultType.SUCCESS:
            return prior

        result = item(prior.context)
        if result.type is ResultType.SUCCESS:
            return result
        else:
            return ParseResult(result.type, ctx)
    return p


def succeed(value=None) -> Parser:
    """
    Always succeeds. Context will be returned as-is, value will be value
    """
    return lambda ctx: _success(ctx, value)


def fail() -> Parser:
    """
    Always fails. Context will be returned as-is.

    todo: consider adding message
    """
    return lambda ctx: _failure(ctx)


def error(message: str) -> Parser:
    """
    Raises a ParseError with message
    """
    def p(ctx):
        raise ParseError(message)
    return p


def ignore(parser: Parser) -> Parser:
    """
    If the parser succeeds, throw away any value but keep the context.
    """
    def p(ctx):
        result = parser(ctx)
        if result.type is ResultType.SUCCESS:
            return _success(result.context, None)
        else:
            return result
    return p


def peek(parser: Parser) -> Parser:
    """
    Peek at the stream, result propagates but the context doesn't move.
    """
    def p(ctx):
        result = parser(ctx)
        if result.type is ResultType.SUCCESS:
            return _success(ctx, result.value)
        else:
            return result
    return p


def _incomplete(context):
    return ParseResult(ResultType.INCOMPLETE, context)


def _failure(context):
    return ParseResult(ResultType.FAILURE, context)


def _success(context, value):
    return SuccessResult(context, value)


def _inc_or_fail(context):
    if context.source.is_complete():
        return _failure(context)
    else:
        return _incomplete(context)
