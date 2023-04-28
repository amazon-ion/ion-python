import time
from pstats import SortKey

from amazon.ion.reader import blocking_reader, NEXT_EVENT, reader_trampoline
from amazon.ion.core import IonEvent, IonEventType
from amazon.ion.reader_binary import binary_reader, stream_handler
from amazon.ion.reader_managed import managed_reader


def do_it(fp):
    g = blocking_reader(managed_reader(stream_handler()), fp)

    types = set()
    ct = 0
    hash = 0
    start = time.time()
    print(start)
    while 1:
        e: IonEvent = g.send(NEXT_EVENT)
        #print(f"event: {e}")
        if e.event_type is IonEventType.STREAM_END:
            #print("Done!")
            break
        types.add(e.ion_type)
        if e.value is not None:
            hash += id(e.value)

    for t in types:
        print(t)
    print(hash)
    end = time.time()
    print(end)
    print(end - start)


if __name__ == '__main__':

    import cProfile

    # fp = open('/Users/calhounr/python_bm.ion')
    fp = open('/Users/calhounr/python_bm.i0n', 'rb')
    # g = blocking_reader(managed_reader(text_reader(is_unicode=True), None), fp)
    # todo: wrap with skipping and symbol table mgmt
    #do_it(fp)

    with cProfile.Profile() as p:
        do_it(fp)

    p.print_stats(sort=SortKey.TIME)
