from io import BytesIO

import time

from decimal import Decimal

from datetime import datetime
from os.path import abspath, join

from amazon.ion.core import IonType
from amazon.ion.equivalence import ion_equals
from amazon.ion.simple_types import IonPyBytes, IonPyInt, IonPyTimestamp
from amazon.ion.simpleion import load, dump, _IVM
from amazon.ion.symbols import SymbolToken


def test_dump_load_binary(obj, binary, single_value):
    # test dump
    #time.sleep(15)
    out = BytesIO()
    dump(obj, out, binary=binary, sequence_as_stream=(not single_value))
    #print(out.getvalue().encode('utf-8'))
    print(out.getvalue().encode('hex'))
    # if not p.has_symbols:
    #     assert (_IVM + p.expected) == res
    # else:
    #     # The payload contains a LST. The value comes last, so compare the end bytes.
    #     assert p.expected == res[len(res) - len(p.expected):]
    # test load
    out.seek(0)
    res = load(out, single_value=single_value)
    assert ion_equals(obj, res)


def test_load_text_from_file(file):
    time.sleep(15)
    with open(file, mode='r') as vector:
        res = load(vector, single_value=False)
    print(res)


if __name__ == "__main__":
    #test_dump_load_binary(SymbolToken(text=None, sid=0, location=None), binary=True, single_value=True)
    test_load_text_from_file(abspath(join(abspath(__file__), u'..', u'..', u'vectors', u'iontestdata', u'good', u'floatDblMin.ion')))
