from collections import namedtuple
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Any, NamedTuple

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
- Support streaming: receive incremental inputs and produce possibly incomplete results
- Reasonably performant: avoid data copying, reduce call overhead and reference
  counting.
- Enable good error messaging. TODO: more here!
- Simple to extend: users can easily mix their own functions and combinators.
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
    if not length:
        raise ValueError("tag must not be empty")

    def p(buffer: SliceableBuffer):
        avail = buffer.size

        if avail < length:
            if buffer.is_eof():
                return ParseResult(ResultType.FAILURE, buffer)

            if avail > 0:
                (data, buffer) = buffer.read_slice(avail)
            else:
                data = b""

            if data == tag[:avail]:
                return ParseResult(ResultType.INCOMPLETE, buffer)
            else:
                return ParseResult(ResultType.FAILURE, buffer)

        (data, buffer) = buffer.read_slice(length)
        if data == tag:
            return ParseResult(ResultType.SUCCESS, buffer, tag)
        else:
            return ParseResult(ResultType.FAILURE, buffer)

    return p


def one_of(items: bytes) -> Parser:
    """
    Match the next byte to one of the bytes passed.
    """
    def p(buffer: SliceableBuffer):
        if not buffer.size:
            if buffer.is_eof():
                return ParseResult(ResultType.FAILURE, buffer)
            else:
                return ParseResult(ResultType.INCOMPLETE, buffer)

        (b, buffer) = buffer.read_byte()

        if b in items:
            return ParseResult(ResultType.SUCCESS, buffer, b)
        else:
            return ParseResult(ResultType.FAILURE, buffer)
    return p


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


def debug(name: str, parser: Parser) -> Parser:
    """
    prints the input buffer and result of ``parser`` to stdout.

    also provides a nice place to put a debug breakpoint.
    """
    def p(buffer: SliceableBuffer):
        print(f"{name} input {buffer}")
        result = parser(buffer)
        print(f"{name} result {result}")

        return result
    return p


def delim(start: Parser, value: Parser, end: Parser) -> Parser:
    """
    result of value is returned

    if start matches.

    this doesn't do any checks about incomplete vs done itself: it delegates that to it's component parsers
    """
    def p(buffer: SliceableBuffer):
        started = start(buffer)
        if started.type is not ResultType.SUCCESS:
            return ParseResult(started.type, buffer)

        body = value(started.buffer)
        if body.type is not ResultType.SUCCESS:
            return ParseResult(body.type, buffer)

        ended = end(body.buffer)
        if ended.type is not ResultType.SUCCESS:
            return ParseResult(ended.type, buffer)

        return ParseResult(ResultType.SUCCESS, ended.buffer, body.value)
    return p


def take_while(pred: Callable[[int], bool]) -> Parser:
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
    def p(buffer: SliceableBuffer):
        (data, buffer) = buffer.read_while(pred)
        if not buffer.size and not buffer.is_eof():
            return ParseResult(ResultType.INCOMPLETE, buffer)

        return ParseResult(ResultType.SUCCESS, buffer, data)

    return p


def take_while_n(n: int, pred: Callable[[int], bool]) -> Parser:
    """
    same as take_while but expects to, and only reads n bytes
    """
    def p(buffer: SliceableBuffer):
        ct = 0

        def wrapped_pred(byte):
            nonlocal ct
            if ct > n:
                return False
            ct += 1
            return pred(byte)

        avail = buffer.size
        (data, buffer) = buffer.read_while(wrapped_pred)

        if len(data) == n:
            return ParseResult(ResultType.SUCCESS, buffer, data)
        elif len(data) == avail:
            return ParseResult(ResultType.INCOMPLETE, buffer)
        else:
            return ParseResult(ResultType.FAILURE, buffer)

    return p


def constant(parser: Parser, value: Any) -> Parser:
    """
    If the parser succeeds, the resulting value will be replaced by the constant.
    """
    return map_value(parser, lambda _: value)


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


class Exact(NamedTuple):
    value: int

class Range(NamedTuple):
    low: int
    high: int


SwitchRule = Exact | Range


def table(*mappings: tuple[SwitchRule, Parser]) -> Parser:
    """The parsers should expect to "re-parse" the byte, at least for now.
    """

    table: list[Parser | None] = [None] * 256
    for rule, parser in mappings:
        if type(rule) is Exact:
            keys = [rule.value]
        else:
            keys = [i for i in range(rule.low, rule.high + 1)]
        for key in keys:
            if table[key]:
                raise ValueError(f"mapping for int value: {key} already present!")
            table[key] = parser

    def p(buffer: SliceableBuffer):
        if not buffer.size:
            if buffer.is_eof():
                return ParseResult(ResultType.FAILURE, buffer)
            else:
                return ParseResult(ResultType.INCOMPLETE, buffer)
        byte = buffer.peek_byte()
        parser = table[byte]
        if not parser:
            return ParseResult(ResultType.FAILURE, buffer)
        else:
            return parser(buffer)

    return p


def preceded(prefix: Parser, item: Parser) -> Parser:
    """
    check that the prefix matches then returns item if it matches.

    if both match then the buffer returned from item is returned
    """
    def p(buffer: SliceableBuffer):

        prior = prefix(buffer)
        if prior.type is not ResultType.SUCCESS:
            return ParseResult(prior.type, buffer)

        value = item(prior.buffer)
        if value.type is not ResultType.SUCCESS:
            return ParseResult(value.type, buffer)
        else:
            return value

    return p


def pair(left: Parser, right: Parser) -> Parser:
    """
    returns a tuple of the left and right values
    """
    def p(buffer: SliceableBuffer):
        l = left(buffer)
        if l.type is not ResultType.SUCCESS:
            return ParseResult(l.type, buffer)

        r = right(l.buffer)
        if r.type is not ResultType.SUCCESS:
            return ParseResult(r.type, buffer)

        return ParseResult(ResultType.SUCCESS, r.buffer, (l.value, r.value))

    return p

def delim_pair(left: Parser, delim: Parser, right: Parser) -> Parser:
    """
    returns a tuple of the left and right values
    """
    def p(buffer: SliceableBuffer):
        l = left(buffer)
        if l.type is not ResultType.SUCCESS:
            return ParseResult(l.type, buffer)

        d = delim(l.buffer)
        if d.type is not ResultType.SUCCESS:
            return ParseResult(d.type, buffer)

        r = right(d.buffer)
        if r.type is not ResultType.SUCCESS:
            return ParseResult(r.type, buffer)

        return ParseResult(ResultType.SUCCESS, r.buffer, (l.value, r.value))

    return p


def peek(parser: Parser) -> Parser:
    """
    Peek at the stream, result propagates but the context doesn't move forward.
    """
    def p(buffer: SliceableBuffer):
        result = parser(buffer)
        return ParseResult(result.type, buffer, result.value)
    return p


def is_eof() -> Parser:
    """
    returns Success if the buffer is empty and marked EOF

    Should this return Incomplete if the buffer is empty but not EOF?
    It does for now, I guess.
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
