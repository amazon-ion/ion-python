from dataclasses import dataclass
from enum import Enum
from typing import Callable, Any

from amazon.ion.sliceable_buffer import SliceableBuffer

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
    buffer: SliceableBuffer
    value: Any = None


Parser = Callable[[SliceableBuffer], ParseResult]


def tag(tag: bytes) -> Parser:
    """
    Match a sequence of bytes, a "tag".


    """
    length = len(tag)

    def p(buffer: SliceableBuffer):
        avail = buffer.size

        if avail >= length:
            (data, buffer) = buffer.read_slice(length)
            if data == tag:
                return ParseResult(ResultType.SUCCESS, buffer, tag)
            else:
                return ParseResult(ResultType.FAILURE, buffer)

        if avail:
            (data, buffer) = buffer.read_slice(avail)
        else:
            data = b""

        if data == tag[:avail] and not buffer.is_eof():
            return ParseResult(ResultType.INCOMPLETE, buffer)
        else:
            return ParseResult(ResultType.FAILURE, buffer)

    return p


# def one_of(items: bytes) -> Parser:
#     """
#     Match one of the bytes passed.
#     """
#     def p(ctx):
#         if not ctx.avail():
#             return _inc_or_fail(ctx)
#
#         b = ctx.read(1)
#
#         if b[0] in items:
#             return _success(ctx.remaining(1), b)
#         else:
#             return _failure(ctx)
#     return p
#

def terminated(item: Parser, terminal: Parser) -> Parser:
    """
    checks that the value is terminated with terminal, which is not
    consumed. result is that produced by item if both succeed.
    """
    def p(buffer: SliceableBuffer):
        body = item(buffer)
        if body.type is not ResultType.SUCCESS:
            return body

        term = terminal(body.buffer)
        if term.type is not ResultType.SUCCESS:
            return term

        return ParseResult(ResultType.SUCCESS, term.buffer, body.value)
    return p


# def delim(start: Parser, value: Parser, end: Parser) -> Parser:
#     """
#     result of value is returned
#
#     if start matches.
#
#     this doesn't do any checks about incomplete vs done itself: it delegates that to it's component parsers
#     """
#     def p(ctx):
#         started = start(ctx)
#         if started.type is not ResultType.SUCCESS:
#             return ParseResult(started.type, ctx)
#
#         body = value(started.context)
#         if body.type is not ResultType.SUCCESS:
#             return ParseResult(body.type, ctx)
#
#         ended = end(body.context)
#         if ended.type is not ResultType.SUCCESS:
#             return ParseResult(ended.type, ctx)
#
#         return _success(ended.context, body.value)
#     return p


# def take_while(pred) -> Parser:
#     """
#     pred is fed one byte at a time. Why is it written this way?
#     I don't want to add the complexity in surface area to generalize
#     it to take a parser _and_ doing so would mean i'd need to combine
#     the results in a general way, _and_ ensure the parser made progress...
#     so this is the simplest and more performant thing i can think to do
#     while still having some generality.
#
#     if the data ends before pred returns False _and_ there may be more
#     then it will be Incomplete.
#
#     pred can signal an error by raising ParserError.
#     """
#     def p(buffer: ParserContext):
#         initial_ctx = ctx
#         n = 0
#
#         while ctx.avail():
#             result = pred(ctx.read(1)[0])
#             if not result:
#                 return _success(ctx, initial_ctx.read(n))
#
#             ctx = ctx.remaining(1)
#             n += 1
#
#         if ctx.source.is_complete():
#             return _success(ctx, initial_ctx.read(n))
#         else:
#             return _incomplete(initial_ctx)
#     return p


def constant(parser: Parser, constant: Any) -> Parser:
    """
    If the parser succeeds, the resulting value will be replaced by the constant.
    """
    return map_value(parser, lambda _: constant)


def map_value(parser: Parser, mapper: Callable) -> Parser:
    """
    If the parser succeeds, the mapper will be called with the value
    of the result and the return value will replace the value.
    """
    def p(buffer: SliceableBuffer):
        result = parser(buffer)
        if result.type is ResultType.SUCCESS:
            return ParseResult(ResultType.SUCCESS, result.buffer, mapper(result.value))
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
    def p(buffer: SliceableBuffer):
        for parser in parsers:
            result = parser(buffer)
            if result.type is ResultType.SUCCESS or result.type is ResultType.INCOMPLETE:
                return result
        return ParseResult(ResultType.FAILURE, buffer)

    return p


def is_eof() -> Parser:
    """
    Results in the Successful None value when source is exhausted and marked EOF.

    Incomplete when source is exhausted but not EOF.

    Failure when there is data.
    """
    def p(buffer: SliceableBuffer):
        if not buffer.size:
            if buffer.is_eof():
                return ParseResult(ResultType.SUCCESS, buffer)
            else:
                return ParseResult(ResultType.INCOMPLETE, buffer)
        else:
            return ParseResult(ResultType.FAILURE, buffer)
    return p


def preceded(prefix: Parser, item: Parser) -> Parser:
    """
    checks that the prefix matches then returns item if it matches.

    if both match then the buffer returned from item is returned
    """
    def p(buffer: SliceableBuffer):

        prior = prefix(buffer)
        if prior.type is not ResultType.SUCCESS:
            return prior

        return item(prior.buffer)
    return p


# def succeed(value=None) -> Parser:
#     """
#     Always succeeds. Context will be returned as-is, value will be value
#     """
#     return lambda ctx: _success(ctx, value)
#
#
# def fail() -> Parser:
#     """
#     Always fails. Context will be returned as-is.
#
#     todo: consider adding message
#     """
#     return lambda ctx: _failure(ctx)
#
#
# def error(message: str) -> Parser:
#     """
#     Raises a ParseError with message
#     """
#     def p(ctx):
#         raise ParseError(message)
#     return p


#def ignore(parser: Parser) -> Parser:
#    """
#    If the parser succeeds, throw away any value but keep the context.
#    """
#    def p(ctx):
#        result = parser(ctx)
#        if result.type is ResultType.SUCCESS:
#            return _success(result.context, None)
#        else:
#            return result
#    return p


def peek(parser: Parser) -> Parser:
    """
    Peek at the stream, result propagates but the context doesn't move forward.
    """
    def p(buffer: SliceableBuffer):
        result = parser(buffer)
        return ParseResult(result.type, buffer, result.value)
    return p


def _incomplete(buffer: SliceableBuffer) -> ParseResult:
    raise NotImplementedError("todo")


def _failure(buffer: SliceableBuffer) -> ParseResult:
    raise NotImplementedError("todo")


def _success(buffer: SliceableBuffer, value: Any) -> ParseResult:
    raise NotImplementedError("todo")
