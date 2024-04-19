from typing import Callable, Tuple, Any, NamedTuple

from amazon.ion.sliceable_buffer import SliceableBuffer

Parser = Callable[[SliceableBuffer], Tuple[Any, SliceableBuffer]]

class IncompleteParseError(Exception):
    pass


Fail = 'fail'
Eof = 'eof'


class Tag(NamedTuple):
    bytes: bytes


class Range(NamedTuple):
    low: int
    high: int


class Alt(NamedTuple):
    branches: list["ParserRule"]


class TakeWhile(NamedTuple):
    byte_checker: Callable[[int], bool]


class Peek(NamedTuple):
    parser: "ParserRule"


class Terminated(NamedTuple):
    """ Indicates the rule must end with the terminator or the result is incomplete.

    The result of ``rule`` is the result of the operation.
    """
    rule: "ParserRule"
    terminal: "ParserRule"


class Preceded(NamedTuple):
    """ Indicates the rule may be preceded by another rule."""
    precedes: "ParserRule"
    rule: "ParserRule"


class MapValue(NamedTuple):
    parser: "ParserRule"
    mapper: Callable[[bytes], Any]


class Constant(NamedTuple):
    parser: "ParserRule"
    value: Any


ParserRule = Tag | Range | Alt | TakeWhile | Peek | Terminated | Preceded | MapValue | Constant | Fail | Eof

def rule(parser: "ParserRule") -> Parser:
