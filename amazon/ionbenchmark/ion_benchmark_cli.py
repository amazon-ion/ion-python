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
    ion_python_benchmark_cli.py write [--api <api>]... [--warmups <int>] [--c-extension <bool>] [--iterations <int>]
    [--format <format>]... [--io-type <io_type>]... <input_file>
    ion_python_benchmark_cli.py read [--api <api>]... [--iterator <bool>] [--warmups <int>] [--iterations <int>]
    [--c-extension <bool>] [--format <format>]... [--io-type <io_type>]... <input_file>
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
     -h, --help                         Show this screen.

     --api <api>                        The API to exercise (simple_ion, event). `simple_ion` refers to
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

     -i --io-type <io_type>             The source or destination type, from the set (buffer | file). If buffer is
                                        selected, buffers the input data in memory before reading and writes the output
                                        data to an in-memory buffer instead of a file. [default: file]

     -u --time-unit <unit>              (NOT SUPPORTED YET)
     -o --results-file <path>           (NOT SUPPORTED YET)
     -I --ion-imports-for-input <file>  (NOT SUPPORTED YET)
     -n --limit <int>                   (NOT SUPPORTED YET)

"""
import itertools
import json
import os
import timeit
from pathlib import Path
import platform

import cbor2
import orjson
import rapidjson
import simplejson
import ujson
from cbor import cbor

import amazon.ion.simpleion as ion
from docopt import docopt
from tabulate import tabulate

from amazon.ionbenchmark.API import API
from amazon.ionbenchmark.Command import Command
from amazon.ionbenchmark.Format import Format, format_is_ion, format_is_json, format_is_cbor, rewrite_file_to_format, \
    format_is_binary
from amazon.ionbenchmark.util import str_to_bool, format_percentage, format_decimal, TOOL_VERSION
from amazon.ionbenchmark.Io_type import Io_type

# Relate pypy incompatible issue - https://github.com/amazon-ion/ion-python/issues/227
pypy = platform.python_implementation() == 'PyPy'
if not pypy:
    import tracemalloc

BYTES_TO_MB = 1024 * 1024
_IVM = b'\xE0\x01\x00\xEA'
write_memory_usage_peak = 0
read_memory_usage_peak = 0

JSON_PRIMARY_BASELINE = Format.JSON
CBOR_PRIMARY_BASELINE = Format.CBOR2

output_file = 'dump_output'


# Generates benchmark code for simpleion load/loads APIs
def generate_simpleion_read_test_code(file, memory_profiling, io_type, iterator=False, single_value=False,
                                      emit_bare_values=False):
    if io_type == Io_type.BUFFER.value:
        with open(file, "br") as fp:
            benchmark_data = fp.read()
        if not memory_profiling:
            if not iterator:
                def test_func():
                    data = ion.loads(benchmark_data, single_value=single_value, emit_bare_values=emit_bare_values)
                    return data
            else:
                def test_func():
                    it = ion.loads(benchmark_data, single_value=single_value, emit_bare_values=emit_bare_values,
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
                    data = ion.loads(benchmark_data, single_value=single_value, emit_bare_values=emit_bare_values)
                    global read_memory_usage_peak
                    read_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                    tracemalloc.stop()
                    return data
            else:
                def test_func():
                    tracemalloc.start()
                    it = ion.loads(benchmark_data, single_value=single_value, emit_bare_values=emit_bare_values,
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
    else:
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


# Generates benchmark code for json/cbor load/loads APIs
def generate_read_test_code(file, memory_profiling, format_option, binary, io_type):
    if format_option == Format.JSON.value:
        benchmark_api = json.loads if io_type == Io_type.BUFFER.value else json.load
    elif format_option == Format.SIMPLEJSON.value:
        benchmark_api = simplejson.loads if io_type == Io_type.BUFFER.value else simplejson.load
    elif format_option == Format.UJSON.value:
        benchmark_api = ujson.loads if io_type == Io_type.BUFFER.value else ujson.load
    elif format_option == Format.RAPIDJSON.value:
        benchmark_api = rapidjson.loads if io_type == Io_type.BUFFER.value else rapidjson.load
    elif format_option == Format.ORJSON.value:
        # orjson doesn't provide load API, so use loads for both file and buffer io_types.
        benchmark_api = orjson.loads
    elif format_option == Format.CBOR.value:
        benchmark_api = cbor.loads if io_type == Io_type.BUFFER.value else cbor.load
    elif format_option == Format.CBOR2.value:
        benchmark_api = cbor2.loads if io_type == Io_type.BUFFER.value else cbor2.load
    else:
        raise Exception('unknown JSON/CBOR format to generate setup code.')

    if io_type == Io_type.BUFFER.value:
        with open(file, 'br') as fp:
            benchmark_data = fp.read()

        if not memory_profiling:
            def test_func():
                data = benchmark_api(benchmark_data)
                return data
        else:
            def test_func():
                tracemalloc.start()
                data = benchmark_api(benchmark_data)
                global read_memory_usage_peak
                read_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()
                return data
    elif format_option == Format.ORJSON.value:
        if not memory_profiling:
            def test_func():
                with open(file, 'br') as benchmark_file:
                    data = benchmark_api(benchmark_file.read())
                return data
        else:
            def test_func():
                tracemalloc.start()
                with open(file, 'br') as benchmark_file:
                    data = benchmark_api(benchmark_file.read())
                global read_memory_usage_peak
                read_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()
                return data
    else:
        if not memory_profiling:
            def test_func():
                with open(file, 'br' if binary else 'r') as benchmark_file:
                    data = benchmark_api(benchmark_file)
                return data
        else:
            def test_func():
                tracemalloc.start()
                with open(file, 'br' if binary else 'r') as benchmark_file:
                    data = benchmark_api(benchmark_file)
                global read_memory_usage_peak
                read_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()
                return data

    return test_func


# Generates benchmark code for event based API
def generate_event_test_code(file):
    pass


# Generates setup code for simpleion benchmark code
def generate_simpleion_setup(c_extension, gc=False):
    rtn = f'import amazon.ion.simpleion as ion;from amazon.ion.simple_types import IonPySymbol; ion.c_ext = ' \
          f'{c_extension}; import tracemalloc'
    if gc:
        rtn += '; import gc; gc.enable()'

    return rtn


# Generates setup code for json/cbor benchmark code
def generate_setup(gc=False):
    rtn = 'import tracemalloc'
    if gc:
        rtn += '; import gc; gc.enable()'

    return rtn


# Generates setup code for event based non_blocking benchmark code
def generate_event_setup(file, gc=False):
    pass


# Benchmarks json/cbor loads/load APIs
def read_micro_benchmark(iterations, warmups, c_extension, file, memory_profiling, format_option, binary, io_type,
                         iterator=False):
    file_size = Path(file).stat().st_size / BYTES_TO_MB

    setup_with_gc = generate_setup(gc=False)

    test_code = generate_read_test_code(file, memory_profiling=memory_profiling,
                                        format_option=format_option, io_type=io_type, binary=binary)

    # warm up
    timeit.timeit(stmt=test_code, setup=setup_with_gc, number=warmups)

    # iteration
    result_with_gc = timeit.timeit(stmt=test_code, setup=setup_with_gc, number=iterations) / iterations

    return file_size, result_with_gc


# Benchmarks simpleion load/loads APIs
def read_micro_benchmark_simpleion(iterations, warmups, c_extension, file, memory_profiling, format_option, binary,
                                   io_type, iterator=False):
    file_size = Path(file).stat().st_size / BYTES_TO_MB

    setup_with_gc = generate_simpleion_setup(c_extension=c_extension, gc=False)

    test_code = generate_simpleion_read_test_code(file, emit_bare_values=False, memory_profiling=memory_profiling,
                                                  iterator=iterator, io_type=io_type)

    # warm up
    timeit.timeit(stmt=test_code, setup=setup_with_gc, number=warmups)

    # iteration
    result_with_gc = timeit.timeit(stmt=test_code, setup=setup_with_gc, number=iterations) / iterations

    return file_size, result_with_gc


# Benchmarks pure python implementation event based APIs
# https://github.com/amazon-ion/ion-python/issues/236
def read_micro_benchmark_event(iterations, warmups, c_extension, file, memory_profiling, format_option, binary, io_type,
                               iterator=False):
    return 0, 0


# Framework for benchmarking read methods, this functions includes
# 1. profile memory usage,
# 2. benchmark performance,
# 3. generate report
def read_micro_benchmark_and_profiling(table, read_micro_benchmark_function, iterations, warmups, file, c_extension,
                                       binary, iterator, each_option, io_type):
    if not file:
        raise Exception("Invalid file: file can not be none.")
    if not read_micro_benchmark_function:
        raise Exception("Invalid micro benchmark function: micro benchmark function can not be none.")

    # memory profiling
    if not pypy:
        read_micro_benchmark_function(iterations=1, warmups=0, file=file, c_extension=c_extension,
                                      memory_profiling=True, iterator=iterator, format_option=each_option[1],
                                      io_type=io_type, binary=binary)

    # performance benchmark
    file_size, result_with_gc = \
        read_micro_benchmark_function(iterations=iterations, warmups=warmups, file=file, c_extension=c_extension,
                                      memory_profiling=False, iterator=iterator, format_option=each_option[1],
                                      io_type=io_type, binary=binary)

    # generate report
    read_generate_report(table, file_size, each_option, result_with_gc, read_memory_usage_peak)

    return file_size, result_with_gc, read_memory_usage_peak


# Generates and prints benchmark report
def read_generate_report(table, file_size, each_option, total_time, memory_usage_peak):
    insert_into_report_table(table, [format_decimal(file_size),
                                     each_option,
                                     format_decimal(total_time),
                                     format_decimal(memory_usage_peak)])


# Generates benchmark code for simpleion dump API
def generate_simpleion_write_test_code(obj, memory_profiling, io_type, binary):
    if io_type == Io_type.BUFFER.value:
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
    else:
        if not memory_profiling:
            def test_func():
                with open(output_file, 'bw') as fp:
                    ion.dump(obj, fp, binary=binary)
        else:
            def test_func():
                tracemalloc.start()
                with open(output_file, 'bw') as fp:
                    ion.dump(obj, fp, binary=binary)
                global write_memory_usage_peak
                write_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()

    return test_func


# Benchmarks simpleion dump API
def write_micro_benchmark_simpleion(iterations, warmups, c_extension, file, binary, memory_profiling,
                                    format_option, io_type):
    file_size = Path(file).stat().st_size / BYTES_TO_MB
    with open(file) as fp:
        obj = ion.load(fp, parse_eagerly=True, single_value=False)

    # GC refers to reference cycles, not reference count
    setup_with_gc = generate_simpleion_setup(gc=False, c_extension=c_extension)

    test_func = generate_simpleion_write_test_code(obj, memory_profiling=memory_profiling, binary=binary,
                                                   io_type=io_type)

    # warm up
    timeit.timeit(stmt=test_func, setup=setup_with_gc, number=warmups)

    # iteration
    result_with_gc = timeit.timeit(stmt=test_func, setup=setup_with_gc, number=iterations) / iterations

    return file_size, result_with_gc


# Benchmarks JSON/CBOR APIs
def write_micro_benchmark(iterations, warmups, c_extension, file, binary, memory_profiling, format_option, io_type):
    file_size = Path(file).stat().st_size / BYTES_TO_MB
    obj = generate_json_and_cbor_obj_for_write(file, format_option)
    # GC refers to reference cycles, not reference count
    setup_with_gc = generate_setup(gc=False)

    test_func = generate_write_test_code(obj, memory_profiling=memory_profiling, format_option=format_option,
                                         io_type=io_type, binary=binary)

    # warm up
    timeit.timeit(stmt=test_func, setup=setup_with_gc, number=warmups)

    # iteration
    result_with_gc = timeit.timeit(stmt=test_func, setup=setup_with_gc, number=iterations) / iterations

    return file_size, result_with_gc


# Generates benchmark code for json dump API
def generate_write_test_code(obj, memory_profiling, format_option, io_type, binary):
    if format_option == Format.JSON.value:
        benchmark_api = json.dumps if io_type == Io_type.BUFFER.value else json.dump
    elif format_option == Format.SIMPLEJSON.value:
        benchmark_api = simplejson.dumps if io_type == Io_type.BUFFER.value else simplejson.dump
    elif format_option == Format.UJSON.value:
        benchmark_api = ujson.dumps if io_type == Io_type.BUFFER.value else ujson.dump
    elif format_option == Format.RAPIDJSON.value:
        benchmark_api = rapidjson.dumps if io_type == Io_type.BUFFER.value else rapidjson.dump
    elif format_option == Format.ORJSON.value:
        benchmark_api = orjson.dumps
    elif format_option == Format.CBOR.value:
        benchmark_api = cbor.dumps if io_type == Io_type.BUFFER.value else cbor.dump
    elif format_option == Format.CBOR2.value:
        benchmark_api = cbor2.dumps if io_type == Io_type.BUFFER.value else cbor2.dump
    else:
        raise Exception('unknown JSON/CBOR format to generate setup code.')

    if io_type == Io_type.BUFFER.value:
        if not memory_profiling:
            def test_func():
                return benchmark_api(obj)
        else:
            def test_func():
                tracemalloc.start()
                data = benchmark_api(obj)
                global write_memory_usage_peak
                write_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()

                return data
    elif format_option == Format.ORJSON.value:
        if not memory_profiling:
            def test_func():
                with open(output_file, 'bw' if binary else 'w') as fp:
                    data = benchmark_api(obj)
                    fp.write(data)
        else:
            def test_func():
                tracemalloc.start()
                with open(output_file, 'bw' if binary else 'w') as fp:
                    data = benchmark_api(obj)
                    fp.write(data)
                global write_memory_usage_peak
                write_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()

                return data
    else:
        if not memory_profiling:
            def test_func():
                with open(output_file, 'bw' if binary else 'w') as fp:
                    benchmark_api(obj, fp)
        else:
            def test_func():
                tracemalloc.start()
                with open(output_file, 'bw' if binary else 'w') as fp:
                    benchmark_api(obj, fp)
                global write_memory_usage_peak
                write_memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
                tracemalloc.stop()

    return test_func


# Generates setup code for json benchmark code
def generate_setup(gc=False):
    rtn = 'import tracemalloc; import gc'
    if gc:
        rtn += '; gc.enable()'

    return rtn


# Benchmarks pure python event based write API
# https://github.com/amazon-ion/ion-python/issues/236
def write_micro_benchmark_event(iterations, warmups, c_extension, file, binary, memory_profiling, io_type,
                                format_option):
    return 0, 0


# Framework for benchmarking write methods, this functions includes
# 1. profile memory usage,
# 2. benchmark performance,
# 3. generate report
def write_micro_benchmark_and_profiling(table, write_micro_benchmark_function, iterations, warmups, file, c_extension,
                                        binary, each_option, io_type):
    if not write_micro_benchmark_function:
        raise Exception("Invalid micro benchmark function: micro benchmark function can not be none.")
    # Memory Profiling
    if not pypy:
        write_micro_benchmark_function(iterations=1, warmups=0, c_extension=c_extension, file=file,
                                       binary=binary, memory_profiling=True, format_option=each_option[1],
                                       io_type=io_type)

    # Performance Benchmark
    file_size, result_with_gc = \
        write_micro_benchmark_function(iterations=iterations, warmups=warmups, c_extension=c_extension,
                                       file=file, binary=binary, memory_profiling=False, format_option=each_option[1],
                                       io_type=io_type)

    # generate report
    write_generate_report(table, file_size, each_option, result_with_gc, write_memory_usage_peak)

    return file_size, each_option, result_with_gc, write_memory_usage_peak


# Generates and prints benchmark report
def write_generate_report(table, file_size, each_option, total_time, memory_usage_peak):
    insert_into_report_table(table,
                             [format_decimal(file_size),
                              each_option,
                              format_decimal(total_time),
                              format_decimal(memory_usage_peak)])


# Insert a benchmark result row into the benchmark report (table)
def insert_into_report_table(table, row):
    if not isinstance(row, list):
        raise Exception('row must be a list')
    table += [row]


# Create a report table by given description
def identify_report_table(command):
    if command == 'read':
        return identify_report_table_helper(
            ['file_size (MB)', 'options', 'total_time (s)', 'memory_usage_peak (MB)'])
    elif command == 'write':
        return identify_report_table_helper(
            ['file_size (MB)', 'options', 'total_time (s)', 'memory_usage_peak (MB)']
        )
    else:
        raise Exception('Command should be either read or write.')


def identify_report_table_helper(row_description):
    return [row_description]


# reset configuration options for each execution
def reset_for_each_execution(each_option):
    global read_memory_usage_peak
    read_memory_usage_peak = 0
    global write_memory_usage_peak
    write_memory_usage_peak = 0
    api = each_option[0]
    format_option = each_option[1]
    io_type = each_option[2]

    return api, format_option, io_type


def generate_json_and_cbor_obj_for_write(file, format_option):
    with open(file) as fp:
        if format_option == Format.JSON.value:
            return json.load(fp)
        elif format_option == Format.SIMPLEJSON.value:
            return simplejson.load(fp)
        elif format_option == Format.UJSON.value:
            return ujson.load(fp)
        elif format_option == Format.RAPIDJSON.value:
            return rapidjson.load(fp)
        elif format_option == Format.ORJSON.value:
            return orjson.loads(fp.read())
        elif format_option == Format.CBOR.value:
            return cbor.load(fp)
        elif format_option == Format.CBOR2.value:
            return cbor2.load(fp)
        else:
            raise Exception('unknown JSON format to generate setup code.')


def clean_up():
    if os.path.exists(output_file):
        os.remove(output_file)


def ion_python_benchmark_cli(arguments):
    if arguments['--version'] or arguments['-v']:
        print(TOOL_VERSION)
        return TOOL_VERSION
    if not arguments['<input_file>']:
        raise Exception('Invalid input file')
    file = arguments['<input_file>']
    command = Command.READ.value if arguments['read'] else Command.WRITE.value
    iterations = int(arguments['--iterations'])
    warmups = int(arguments['--warmups'])
    c_extension = str_to_bool(arguments['--c-extension']) if not pypy else False
    iterator = str_to_bool(arguments['--iterator'])

    # For options may show up more than once, initialize them as below and added them into list option_configuration.
    # initialize options that might show up multiple times
    api = [*set(arguments['--api'])] if arguments['--api'] else [API.DEFAULT.value]
    format_option = [*set(arguments['--format'])] if arguments['--format'] else [Format.DEFAULT.value]
    io_type = [*set(arguments['--io-type'])] if arguments['--io-type'] else [Io_type.DEFAULT.value]
    # option_configuration is used for tracking options may show up multiple times.
    option_configuration = [api, format_option, io_type]
    option_configuration_combination = list(itertools.product(*option_configuration))

    # initialize benchmark report table
    table = identify_report_table(command)

    for each_option in option_configuration_combination:
        print(f'Generating option {each_option}...')
        # reset each option configuration
        api, format_option, io_type = reset_for_each_execution(each_option)
        binary = format_is_binary(format_option)
        # TODO. currently, we must provide the tool a corresponding file format for read benchmarking. For example,
        #  we must provide a CBOR file for CBOR APIs benchmarking. We cannot benchmark CBOR APIs by giving a JSON
        #  file. Lack of format conversion prevents us from benchmarking different formats concurrently.
        file = rewrite_file_to_format(file, format_option)

        # Generate microbenchmark API according to read/write command
        if format_is_ion(format_option):
            if not api or api == API.SIMPLE_ION.value:
                micro_benchmark_function = read_micro_benchmark_simpleion if command == 'read' \
                    else write_micro_benchmark_simpleion
            elif api == API.EVENT.value:
                micro_benchmark_function = read_micro_benchmark_event if command == 'read' \
                    else write_micro_benchmark_event
            else:
                raise Exception(f'Invalid API option {api}.')
        elif format_is_json(format_option):
            micro_benchmark_function = read_micro_benchmark if command == 'read' else write_micro_benchmark
        elif format_is_cbor(format_option):
            micro_benchmark_function = read_micro_benchmark if command == 'read' else write_micro_benchmark
        else:
            raise Exception(f'Invalid format option {format_option}.')

        if command == 'read':
            read_micro_benchmark_and_profiling(table, micro_benchmark_function, iterations, warmups, file,
                                               c_extension, binary, iterator, each_option, io_type)
        else:
            write_micro_benchmark_and_profiling(table, micro_benchmark_function, iterations, warmups, file,
                                                c_extension, binary, each_option, io_type)

    print(tabulate(table, tablefmt='fancy_grid'))
    clean_up()

    return table


if __name__ == '__main__':
    ion_python_benchmark_cli(docopt(__doc__, help=True))
