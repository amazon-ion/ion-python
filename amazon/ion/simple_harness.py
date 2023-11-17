import time
import timeit
from pstats import SortKey

from amazon.ion.reader import blocking_reader, NEXT_EVENT, reader_trampoline
from amazon.ion.core import IonEvent, IonEventType
from amazon.ion.reader_binary import binary_reader#, stream_handler
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_text import text_reader
from amazon.ion.simpleion import load, load_extension, load_python

import time

def timeit(f):

    def timed(*args, **kw):

        ts = time.time()
        result = f(*args, **kw)
        te = time.time()

        print(f"Time: {te - ts}")
        return result

    return timed

@timeit
def stream_it(fp, materialize=True):
    g = blocking_reader(managed_reader(text_reader()), fp)

    types = set()
    hash = 0
    ct = 0
    while 1:
        e: IonEvent = g.send(NEXT_EVENT)
        #print(f"event: {e}")
        if e.event_type is IonEventType.STREAM_END:
            #print("Done!")
            break
        types.add(e.ion_type)
        if materialize and e.value is not None:
            hash += id(e.value)
        if e.depth == 0:
            ct += 1
            if ct >= 2000:
                break

    for t in types:
        print(t)
    print(hash)

@timeit
def load_it(fp):
    hash = 0
    ct = 0
    for record in load_python(fp, parse_eagerly=False, single_value=False):
        hash += id(record)
        ct += 1
        if ct >= 2000:
            break
    print(hash)



if __name__ == '__main__':

    import cProfile

    # fp = open('/Users/calhounr/python_bm.ion')
    fp = open('/Users/calhounr/python_bm.i0n', 'rb')
    # g = blocking_reader(managed_reader(text_reader(is_unicode=True), None), fp)
    # todo: wrap with skipping and symbol table mgmt
    stream_it(open('/Users/calhounr/python_bm.ion', 'rb'))
    stream_it(open('/Users/calhounr/python_bm.ion', 'rb'))
    stream_it(open('/Users/calhounr/python_bm.ion', 'rb'))

    exit(0)

    with cProfile.Profile() as p:
        load_it(fp)

    p.print_stats(sort=SortKey.TIME)
