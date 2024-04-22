import time
from pstats import SortKey

from amazon.ion.reader import blocking_reader, NEXT_EVENT, reader_trampoline
from amazon.ion.core import IonEvent, IonEventType
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader_managed import managed_reader

from amazon.ion import simpleion


def ionc_load(fp):
    iter = simpleion.load(fp, single_value=False, parse_eagerly=False)
    ct = 0
    for v in iter:
        # print(v)
        ct += 1
    print(ct)

def do_it(fp):
    g = blocking_reader(managed_reader(binary_reader()), fp)

    while 1:
        e: IonEvent = g.send(NEXT_EVENT)
        print(f"event: {e}")
        if e.event_type is IonEventType.STREAM_END:
            print("Done!")
            break


if __name__ == '__main__':

    import cProfile

    # fp = open('/Users/calhounr/python_bm.ion')
    # g = blocking_reader(managed_reader(text_reader(is_unicode=True), None), fp)
    # todo: wrap with skipping and symbol table mgmt
    for i in range(0, 1):
        fp = open('service_log_legacy.i0n', 'rb')
        ionc_load(fp)

    # with cProfile.Profile() as p:
    #     # do_it(fp)
    #     ct = 0
    #     iter = simpleion.load(fp, single_value=False, parse_eagerly=False)
    #     for v in iter:
    #         #print(v)
    #         ct += 1
    #     print(ct)
    #
    # p.print_stats(sort=SortKey.TIME)
