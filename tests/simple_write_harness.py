import time
from pstats import SortKey

from amazon.ion.reader import blocking_reader, NEXT_EVENT, reader_trampoline
from amazon.ion.core import IonEvent, IonEventType
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader_managed import managed_reader

from amazon.ion import simpleion


def ionc_load(fp):
    iter = simpleion.load(fp, single_value=False, parse_eagerly=False)
    data = []
    for v in iter:
        data.append(v)
        if len(data) >= 10000:
            break
    return data


if __name__ == '__main__':

    import cProfile

    # fp = open('/Users/calhounr/python_bm.ion')
    # g = blocking_reader(managed_reader(text_reader(is_unicode=True), None), fp)
    # todo: wrap with skipping and symbol table mgmt
    fp = open('service_log_legacy.i0n', 'rb')
    data = ionc_load(fp)

    wfp = open('out.i0n', 'wb')
    simpleion.dump(data, wfp, sequence_as_stream=True)

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
