from amazon.ion.reader import blocking_reader, NEXT_EVENT, reader_trampoline
from amazon.ion.core import IonEvent, IonEventType
from amazon.ion.protonic_text_reader import wrap_parser, parse_text

if __name__ == '__main__':
    # fp = open('/Users/calhounr/python_bm.ion')
    fp = open('/Users/calhounr/simple.ion', 'rb')
    # g = blocking_reader(managed_reader(text_reader(is_unicode=True), None), fp)
    # todo: wrap with skipping and symbol table mgmt
    g = blocking_reader(reader_trampoline(wrap_parser(parse_text), True), fp)

    types = set()
    ct = 0
    while 1:
        e: IonEvent = g.send(NEXT_EVENT)
        print(f"event: {e}")
        if e.event_type is IonEventType.STREAM_END:
            print("Done!")
            break
        types.add(e.ion_type)

    for t in types:
        print(t)
