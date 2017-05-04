from io import BytesIO

import time

from amazon.ion.equivalence import ion_equals
from amazon.ion.simpleion import load, dump, _IVM


def test_dump_load_binary(obj, single_value):
    # test dump
    time.sleep(15)
    out = BytesIO()
    dump(obj, out, binary=True, sequence_as_stream=(not single_value))
    # res = out.getvalue()
    # if not p.has_symbols:
    #     assert (_IVM + p.expected) == res
    # else:
    #     # The payload contains a LST. The value comes last, so compare the end bytes.
    #     assert p.expected == res[len(res) - len(p.expected):]
    # test load
    out.seek(0)
    res = load(out, single_value=single_value)
    assert ion_equals(obj, res)

if __name__ == "__main__":
    test_dump_load_binary(0xFFFFFFFF, True)
