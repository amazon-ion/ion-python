# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
"""A repeatable benchmark tool for ion-python implementation.

Usage:
    ion_python_benchmark_cli.py write [--api <api>] [--warmups <int>] [--c-extension <bool>] [--iterations <int>]
    [--format <format>] <input_file>
    ion_python_benchmark_cli.py read [--api <api>] [--iterator <bool>] [--warmups <int>] [--iterations <int>]
    [--c-extension <bool>] [--format <format>] <input_file>
    ion_python_benchmark_cli.py (-h | --help)
    ion_python_benchmark_cli.py (-v | --version)

Command:
    write       Benchmark writing the given input file to the given output format(s). In order to isolate
    writing from reading, during the setup phase write instructions are generated from the input file
    and stored in memory. For large inputs, this can consume a lot of resources and take a long time
    to execute.

    read        First, re-write the given input file to the given output format(s) (if necessary), then
    benchmark reading the resulting log files.

Options:
     -h, --help                          Show this screen.

     --api <api>                        The API to exercise (simpleIon, event). `simpleIon` refers to
                                        simpleIon's load method. `event` refers to ion-python's event
                                        based non-blocking API. Default to `simpleIon`.

     -t --iterator <bool>               If returns an iterator for simpleIon C extension read API. [default: False]

     -w --warmups <int>                 Number of benchmark warm-up iterations. [default: 10]

     -i --iterations <int>              Number of benchmark iterations. [default: 10]

     -c --c-extension <bool>            If the C extension is enabled, note that it only applies to simpleIon module.
                                        [default: True]

     -f --format <format>               Format to benchmark, from the set (ion_binary | ion_text). May be specified
                                        multiple times to compare different formats. [default: ion_binary]

     -p --profile                       (NOT SUPPORTED YET) Initiates a single iteration that repeats indefinitely until
                                        terminated, allowing users to attach profiling tools. If this option is
                                        specified, the --warmups, --iterations, and --forks options are ignored. An
                                        error will be raised if this option is used when multiple values are specified
                                        for other options. Not enabled by default.

     -u --time-unit <unit>              (NOT SUPPORTED YET)
     -o --results-file <path>           (NOT SUPPORTED YET)
     -I --ion-imports-for-input <file>  (NOT SUPPORTED YET)
     -n --limit <int>                   (NOT SUPPORTED YET)

"""
import timeit
from pathlib import Path
import platform

import amazon.ion.simpleion as ion
from docopt import docopt
from tabulate import tabulate

from amazon.ionbenchmark.API import API
from amazon.ionbenchmark.Format import Format
from amazon.ionbenchmark.util import str_to_bool, format_percentage, format_decimal, TOOL_VERSION
pypy = platform.python_implementation() == 'PyPy'
if not pypy:
    import tracemalloc


BYTES_TO_MB = 1024 * 1024
_IVM = b'\xE0\x01\x00\xEA'
write_memory_usage_peak = 0
read_memory_usage_peak = 0


def generate_simpleion_load_test_code(file, memory_profiling, iterator=False, single_value=False,
                                      emit_bare_values=False):
    if not memory_profiling:
        if not iterator:
            def test_func():
                with open(file, "br") as fp:
                    data = ion.load(fp, single_value=single_value, emit_bare_values=emit_bare_values)
                return data
        else:
            def test_func():
                with open(file, "br") as fp:
                    it = ion.load(fp, single_value=single_value, emit_bare_values=emit_bare_values,
                                  parse_eagerly=False)
                    while True:
                        try:
                            next(it)
                        except StopIteration:
                            break
                return it
    else:
        if not iterator:
            def test_func():
                tracemalloc.start()
                with open(file, "br") as fp:
                    data = ion.load(fp, single_value=single_value, emit_bare_values=emit_bare_values)
                global read_memory_usage_peak
                read_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()
                return data
        else:
            def test_func():
                tracemalloc.start()
                with open(file, "br") as fp:
                    it = ion.load(fp, single_value=single_value, emit_bare_values=emit_bare_values,
                                  parse_eagerly=False)
                    while True:
                        try:
                            next(it)
                        except StopIteration:
                            break
                global read_memory_usage_peak
                read_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()
                return it
    return test_func


def generate_event_test_code(file):
    pass


def generate_simpleion_setup(c_extension, memory_profiling, gc=True):
    rtn = f'import amazon.ion.simpleion as ion;from amazon.ion.simple_types import IonPySymbol; ion.c_ext ={c_extension}'
    if memory_profiling:
        rtn += '; import tracemalloc'
    if gc:
        rtn += '; import gc; gc.enable()'

    return rtn


def generate_event_setup(file, gc=True):
    pass


def read_micro_benchmark_simpleion(iterations, warmups, c_extension, file, memory_profiling, iterator=False):
    file_size = Path(file).stat().st_size / BYTES_TO_MB

    setup_with_gc = generate_simpleion_setup(c_extension=c_extension, gc=False, memory_profiling=memory_profiling)

    test_code = generate_simpleion_load_test_code(file, emit_bare_values=False, memory_profiling=memory_profiling,
                                                  iterator=iterator)
    test_code_without_wrapper = generate_simpleion_load_test_code(file, emit_bare_values=True,
                                                                  memory_profiling=memory_profiling,
                                                                  iterator=iterator)

    # warm up
    timeit.timeit(stmt=test_code, setup=setup_with_gc, number=warmups)
    timeit.timeit(stmt=test_code_without_wrapper, setup=setup_with_gc, number=warmups)

    # iteration
    result_with_gc = timeit.timeit(stmt=test_code, setup=setup_with_gc, number=iterations) / iterations
    result_with_raw_value = \
        (timeit.timeit(stmt=test_code_without_wrapper, setup=setup_with_gc, number=iterations) / iterations) \
            if c_extension else result_with_gc

    return file_size, result_with_gc, result_with_raw_value


def read_micro_benchmark_event(iterations, warmups, c_extension, file=None):
    pass


def read_micro_benchmark_and_profiling(read_micro_benchmark_function, iterations, warmups, file, c_extension, iterator):
    if not file:
        raise Exception("Invalid file: file can not be none.")
    if not read_micro_benchmark_function:
        raise Exception("Invalid micro benchmark function: micro benchmark function can not be none.")

    # memory profiling
    if not pypy:
        read_micro_benchmark_function(iterations=1, warmups=0, file=file, c_extension=c_extension, memory_profiling=True,
                                    iterator=iterator)

    # performance benchmark
    file_size, result_with_gc, result_with_raw_value = \
        read_micro_benchmark_function(iterations=iterations, warmups=warmups, file=file, c_extension=c_extension,
                                      memory_profiling=False, iterator=iterator)

    # calculate metrics
    conversion_time = result_with_gc - result_with_raw_value
    # generate report
    read_generate_report(file_size, result_with_gc,
                         conversion_time if conversion_time > 0 else 0,
                         (conversion_time / result_with_gc) if conversion_time > 0 else 0,
                         read_memory_usage_peak)

    return file_size, result_with_gc, conversion_time, read_memory_usage_peak


def read_generate_report(file_size, total_time, conversion_time, wrapper_time_percentage, memory_usage_peak):
    table = [['file_size (MB)', 'total_time (s)', 'conversion_\ntime (s)', 'conversion_time/\ntotal_time (%)',
              'memory_usage_peak (MB)'],
             [format_decimal(file_size),
              format_decimal(total_time),
              format_decimal(conversion_time),
              format_percentage(wrapper_time_percentage),
              format_decimal(memory_usage_peak)]]
    print('\n')
    print(tabulate(table, tablefmt='fancy_grid'))


def generate_simpleion_dump_test_code(obj, memory_profiling, binary=True):
    if not memory_profiling:
        def test_func():
            return ion.dumps(obj=obj, binary=binary)
    else:
        def test_func():
            tracemalloc.start()
            data = ion.dumps(obj=obj, binary=binary)
            global write_memory_usage_peak
            write_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
            tracemalloc.stop()

            return data

    return test_func


def write_micro_benchmark_simpleion(iterations, warmups, c_extension, obj, file, binary, memory_profiling):
    file_size = Path(file).stat().st_size / BYTES_TO_MB

    # GC refers to reference cycles, not reference count
    setup_with_gc = generate_simpleion_setup(gc=True, c_extension=c_extension, memory_profiling=memory_profiling)

    test_func = generate_simpleion_dump_test_code(obj, memory_profiling=memory_profiling, binary=binary)

    # warm up
    timeit.timeit(stmt=test_func, setup=setup_with_gc, number=warmups)

    # iteration
    result_with_gc = timeit.timeit(stmt=test_func, setup=setup_with_gc, number=iterations) / iterations

    return file_size, result_with_gc


def write_micro_benchmark_event(iterations, warmups, c_extension, obj, binary, file=None):
    pass


def write_micro_benchmark_and_profiling(write_micro_benchmark_function, iterations, warmups, obj, c_extension, binary, file):
    if not obj:
        raise Exception("Invalid obj: object can not be none.")
    if not write_micro_benchmark_function:
        raise Exception("Invalid micro benchmark function: micro benchmark function can not be none.")
    # Memory Profiling
    if not pypy:
        write_micro_benchmark_function(iterations=1, warmups=0, obj=obj, c_extension=c_extension, file=file, binary=binary,
                                       memory_profiling=True)

    # Performance Benchmark
    file_size, result_with_gc = \
        write_micro_benchmark_function(iterations=iterations, warmups=warmups, obj=obj, c_extension=c_extension, file=file,
                                       binary=binary, memory_profiling=False)

    # generate report
    write_generate_report(file_size, result_with_gc, write_memory_usage_peak)

    return file_size, result_with_gc, write_memory_usage_peak


def write_generate_report(file_size, total_time, memory_usage_peak):
    table = [['file_size (MB)', 'total_time (s)', 'memory_usage_peak (MB)'],
             [format_decimal(file_size),
              format_decimal(total_time),
              format_decimal(memory_usage_peak)]]
    print('\n')
    print(tabulate(table, tablefmt='fancy_grid'))


def ion_python_benchmark_cli(arguments):
    if arguments['--version'] or arguments['-v']:
        print(TOOL_VERSION)
        return TOOL_VERSION
    if not arguments['<input_file>']:
        raise Exception('Invalid input file')
    file = arguments['<input_file>']
    iterations = int(arguments['--iterations'])
    warmups = int(arguments['--warmups'])
    c_extension = str_to_bool(arguments['--c-extension']) if not pypy else False
    binary = arguments['--format'] == Format.ION_BINARY.value
    iterator = str_to_bool(arguments['--iterator'])

    if arguments['read']:
        api = arguments['--api']
        if not api or api == API.SIMPLE_ION.value:
            read_micro_benchmark_function = read_micro_benchmark_simpleion
        elif api == API.EVENT.value:
            read_micro_benchmark_function = read_micro_benchmark_event
        else:
            raise Exception(f'Invalid API option {api}.')

        return read_micro_benchmark_and_profiling(read_micro_benchmark_function, iterations, warmups, file, c_extension,
                                                  iterator)

    elif arguments['write']:
        api = arguments['--api']
        if not api or api == API.SIMPLE_ION.value:
            write_micro_benchmark_function = write_micro_benchmark_simpleion
        elif api == API.EVENT.value:
            write_micro_benchmark_function = write_micro_benchmark_event
        else:
            raise Exception(f'Invalid API option {api}.')

        with open(file) as fp:
            obj = ion.load(fp, parse_eagerly=True, single_value=False)

        return write_micro_benchmark_and_profiling(write_micro_benchmark_function, iterations, warmups, obj, c_extension,
                                                   binary, file)


if __name__ == '__main__':
    ion_python_benchmark_cli(docopt(__doc__, help=True))
