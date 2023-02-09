from collections import deque

from amazon.ion.core import IonEvent, IonEventType, IonType, DataEvent, Transition, ION_STREAM_INCOMPLETE_EVENT, \
    ION_STREAM_END_EVENT
from amazon.ion.exceptions import IonException
from amazon.ion.reader import ReadEventType
from amazon.ion.util import coroutine
from amazon.protonic.data_source import DataSource
from amazon.protonic.protons import one_of, delim, tag, alt, value, terminated, map_value, peek, ResultType, \
    ParseResult, take_while, ParserContext, is_eof, preceded


@coroutine
def wrap_parser(parser_factory):
    """
    Acts as intermediary between the text_parser that is a generator and knows nothing
    of ReadEvents and the Ion general coroutine based components.

    Intercepts data handling events to deal with the growing and marking end of the buffer.

    Assumes that state invariants will be managed by the `reader_trampoline`.

    Asks the parser for values when expected to produce a value.
    """
    data_source = DataSource()
    ctx = ParserContext(data_source)
    parser = parser_factory(ctx)

    read_event, self = yield
    assert read_event.type is ReadEventType.NEXT

    ion_event_type = None
    while True:
        if not read_event or read_event.type is ReadEventType.NEXT:
            if ion_event_type is IonEventType.INCOMPLETE:
                data_source.eof()

            ion_event = next(parser)
            ion_event_type = ion_event.event_type

            if ion_event_type is IonEventType.STREAM_END:
                data_source.clear()

            read_event, self = yield Transition(ion_event, self)
        elif read_event.type is ReadEventType.DATA:
            data_source.extend(read_event.data)
            ion_event_type = None

            read_event, self = yield Transition(None, self)
        else:
            raise ValueError("Only ReadEventTypes of NEXT or DATA handled here!")


def parse_text(ctx):
    """
    A note about streaming, incremental production and state...
    We could model the stepping into containers within the combinators and the
    combinators as generators. Then the stack of combinators at runtime mirrors
    the nesting of the containers you have parsed into. That
    - seems nice in that you can 'preserve state' across buffer boundaries
    - means you can define entirety of grammar in combinators (sort of)

    but...
    - makes the combinators more complicated (cardinality explosion of sequences
      instead of values)
    - hurts performance:
      - more pushing and popping through stack frames
      - generators have some overhead
      - python stack frame limit (defaults means that some deeply nested docs
        may max out stack)

    So this function holds the stack of containers as a deque and owns the logic
    around well-formedness
    """
    containers = deque()
    containers.append(None)

    while True:
        parent = containers[-1]
        if parent is IonType.LIST:
            parser = list_item
        elif parent is IonType.STRUCT:
            raise NotImplementedError("Structs are not supported yet!")
        elif not parent:
            parser = tlv
        else:
            raise ValueError(f"{parent} is not a container type!")

        if not ctx.avail() and ctx.source.is_complete():
            if parent:
                raise IonException(f'Encountered stream end while still inside parent: {parent}!', ctx)
            else:
                yield ION_STREAM_END_EVENT

        result: ParseResult = parser(ctx)

        if result.type is ResultType.FAILURE:
            # handle trailing whitespace. I considered structuring this as part of the rules,
            # that seems to add complexity to the rules for a stream processing problem. idk.
            if ctx.source.is_complete() and whitespace(ctx).type is ResultType.SUCCESS:
                event = ION_STREAM_END_EVENT
            else:
                raise IonException(f'Parsing failed!', ctx)
        elif result.type is ResultType.INCOMPLETE:
            event = ION_STREAM_INCOMPLETE_EVENT
        else:
            ctx = result.context
            event: IonEvent = result.value
            # todo: a fully functional slicing buffer would be great
            #       this is the best I can do for now.
            ctx.advance()

        if event.event_type is IonEventType.CONTAINER_START:
            containers.append(event.ion_type)
        elif event.event_type is IonEventType.CONTAINER_END:
            if event.ion_type is not parent:
                raise IonException(f'Encountered {event.type} end when {parent} was expected!', ctx)
            containers.pop()
        elif event.event_type is IonEventType.STREAM_END and parent:
            raise IonException(f"Encountered Stream End inside Container: {parent}")

        yield event.derive_depth(len(containers) - 1)


"""
*** Rules below here ***
"""

stop = peek(
    alt(
        one_of(b" \n\t\r\f{}[](),"),
        is_eof()
    )
)

whitespace = take_while(lambda b: b in bytearray(b" \n\t\r\f"))

# todo: obviously this is just ascii and doesn't handle unicode escapes
#       also doesn't handle escaped double quotes!
string = delim(tag(b'"'), take_while(lambda b: 0 <= b <= 127 and b != 34), tag(b'"'))
# todo: unquoted symbols, escaping of single quotes?
# todo: integer symbol Ids!
symbol = delim(tag(b"'"), take_while(lambda b: 0 <= b <= 127 and b != 39), tag(b"'"))

# todo: annotations!

tlv = preceded(
    whitespace,
    alt(
        # scalars
        # todo: typed null, numbers, timestamps
        value(terminated(tag(b"null"), stop), IonEvent(IonEventType.SCALAR, IonType.NULL, None)),
        value(terminated(tag(b"true"), stop), IonEvent(IonEventType.SCALAR, IonType.BOOL, True)),
        value(terminated(tag(b"false"), stop), IonEvent(IonEventType.SCALAR, IonType.BOOL, False)),
        map_value(string, lambda v: IonEvent(IonEventType.SCALAR, IonType.STRING, v.tobytes().decode("utf-8"))),
        map_value(symbol, lambda v: IonEvent(IonEventType.SCALAR, IonType.SYMBOL, v.tobytes().decode("utf-8"))),

        # containers
        # todo: s-exps!
        value(tag(b'['), IonEvent(IonEventType.CONTAINER_START, IonType.LIST, None)),
        value(tag(b'{'), IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT, None)),

        # the ends will be captured by the container parsers in the happy case
        # they are here to catch the unhappy cases with clear errors.
        value(tag(b']'), IonEvent(IonEventType.CONTAINER_END, IonType.LIST, None)),
        value(tag(b'}'), IonEvent(IonEventType.CONTAINER_END, IonType.STRUCT, None)),

    )
)
list_item = alt(
    value(tag(b']'), IonEvent(IonEventType.CONTAINER_END, IonType.LIST, None)),
    terminated(
        tlv,
        preceded(whitespace, alt(tag(b','), peek(tag(b']'))))
    )
)
