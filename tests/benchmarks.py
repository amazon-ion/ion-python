# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the
# License.

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from functools import partial
from io import BytesIO

from datetime import datetime
import simplejson as json
import six

import amazon.ion.simpleion
from amazon.ion.equivalence import ion_equals
from amazon.ion.ionc_ctypes import C_ELAPSED_TIME


class Timer:
    def __enter__(self):
        self.start = datetime.now()
        return self

    def __exit__(self, *args):
        self.end = datetime.now()
        self.elapsed = self.end - self.start


def _prepare_data(f):
    with open(f) as fp:
        return json.load(fp)


def _write(dump_func, obj, fp, *args, **kwargs):
    dump_func(obj, fp, *args, **kwargs)

_write_ion_binary = partial(_write, amazon.ion.simpleion.dump, binary=True)
_write_ion_text = partial(_write, amazon.ion.simpleion.dump, binary=False)
_write_json = partial(_write, json.dump)


def _read(load_func, fp, *args, **kwargs):
    return load_func(fp, *args, **kwargs)

_read_ion = partial(_read, amazon.ion.simpleion.load)
_read_json = partial(_read, json.load)


def _read_benchmark(write_func, read_func, io_type, iterations, f):
    obj = _prepare_data(f)
    source = io_type()
    write_func(obj, source)
    read_obj = None
    with Timer() as t:
        for i in range(iterations):
            source.seek(0)
            read_obj = read_func(source)
    assert ion_equals(obj, read_obj)
    return t.elapsed

_read_benchmark_ion_binary = partial(_read_benchmark, _write_ion_binary, _read_ion, BytesIO)
_read_benchmark_ion_text = partial(_read_benchmark, _write_ion_text, _read_ion, BytesIO)
_read_benchmark_json = partial(_read_benchmark, _write_json, _read_json, six.StringIO)


def _write_benchmark(write_func, read_func, io_type, iterations, f):
    obj = _prepare_data(f)
    sink = io_type()
    with Timer() as t:
        for i in range(iterations):
            write_func(obj, sink)
            sink.seek(0)
    assert ion_equals(obj, read_func(sink))
    return t.elapsed

_write_benchmark_ion_binary = partial(_write_benchmark, _write_ion_binary, _read_ion, BytesIO)
_write_benchmark_ion_text = partial(_write_benchmark, _write_ion_text, _read_ion, BytesIO)
_write_benchmark_json = partial(_write_benchmark, _write_json, _read_json, six.StringIO)


if __name__ == "__main__":
    repeats = 1000
    filepath = '/Users/greggt/Desktop/generated_short.json'
    print('Read ion binary:   %s' % _read_benchmark_ion_binary(repeats, filepath))
    print('Write ion binary:  %s' % _write_benchmark_ion_binary(repeats, filepath))
    print('Read json native:  %s' % _read_benchmark_json(repeats, filepath))
    print('Write json native: %s' % _write_benchmark_json(repeats, filepath))
    json._toggle_speedups(False)
    print('Read json pure:    %s' % _read_benchmark_json(repeats, filepath))
    print('Write json pure:   %s' % _write_benchmark_json(repeats, filepath))
