import time
from itertools import chain
from os.path import abspath, join, dirname, getsize

from docopt import docopt

from amazon.ion import simpleion
from amazon.ionbenchmark import ion_benchmark_cli
from amazon.ionbenchmark.ion_benchmark_cli import generate_simpleion_load_test_code, generate_simpleion_dump_test_code, \
    BYTES_TO_MB, ion_python_benchmark_cli
from amazon.ionbenchmark.util import str_to_bool, TOOL_VERSION
from tests import parametrize
from tests.test_simpleion import generate_scalars_text
from tests.writer_util import SIMPLE_SCALARS_MAP_TEXT

doc = ion_benchmark_cli.__doc__


@parametrize(
    '1',
    'true',
    'True',
    'TRue',
)
def test_str_to_bool_true(p):
    assert str_to_bool(p) == True


@parametrize(
    '0',
    '2',
    'false',
    '?',
    'test'
)
def test_str_to_bool_true(p):
    assert str_to_bool(p) == False


def generate_test_path(p):
    return join(dirname(abspath(__file__)), 'benchmark_sample_data', p)


@parametrize(
    generate_test_path('integers.ion')
)
def test_generate_simpleion_load_test_code(path):
    actual = generate_simpleion_load_test_code(path, memory_profiling=False, single_value=False, emit_bare_values=False)

    # make sure we generated the desired load function
    with open(path) as fp:
        expect = simpleion.load(fp, single_value=False, parse_eagerly=True)

    # make sure the return values are same
    assert actual() == expect


@parametrize(
    *tuple(chain(
        generate_scalars_text(SIMPLE_SCALARS_MAP_TEXT),
    ))
)
def test_generate_simpleion_dump_test_code(obj):
    actual = generate_simpleion_dump_test_code(obj, memory_profiling=False, binary=False)

    # make sure we generated the desired dumps function
    expect = simpleion.dumps(obj, binary=False)

    # make sure the return values are same
    assert actual() == expect


def execution_with_command(c):
    return ion_python_benchmark_cli(docopt(doc, argv=c))


def test_option_version():
    assert execution_with_command('-v') == TOOL_VERSION


def test_option_write(file=generate_test_path('integers.ion')):
    # make sure it reads successfully
    file_size, result_with_gc, write_memory_usage_peak = \
        execution_with_command(['write', file])

    assert file_size == getsize(file) / BYTES_TO_MB
    assert result_with_gc > 0
    assert write_memory_usage_peak >= 0


def test_option_read(file=generate_test_path('integers.ion')):
    # make sure it reads successfully
    file_size, result_with_gc, conversion_time, read_memory_usage_peak = \
        execution_with_command(['read', file])

    assert file_size == getsize(file) / BYTES_TO_MB
    assert result_with_gc > 0
    assert read_memory_usage_peak >= 0


def test_option_write_c_extension(file=generate_test_path('integers.ion')):
    file_size, result_with_gc, write_memory_usage_peak = \
        execution_with_command(['write', file, '--c-extension', 'true'])

    file_size_2, result_with_gc_2, write_memory_usage_peak_2 = \
        execution_with_command(['write', file, '--c-extension', 'false'])

    assert file_size == getsize(file) / BYTES_TO_MB
    assert file_size_2 == getsize(file) / BYTES_TO_MB


def test_option_read_c_extension(file=generate_test_path('integers.ion')):
    file_size, result_with_gc, conversion_time, read_memory_usage_peak = \
        execution_with_command(['read', file, '--c-extension', 'true'])

    file_size_2, result_with_gc_2, conversion_time_2, read_memory_usage_peak_2 = \
        execution_with_command(['read', file, '--c-extension', 'false'])

    assert file_size == getsize(file) / BYTES_TO_MB


def test_option_read_iterations(file=generate_test_path('integers.ion')):
    # warmup
    execution_with_command(['read', file, '--c-extension', 'true', '--iterations', '10'])

    start = time.perf_counter()
    execution_with_command(['read', file, '--c-extension', 'true', '--iterations', '1'])
    stop = time.perf_counter()
    time_1 = stop - start

    start = time.perf_counter()
    execution_with_command(['read', file, '--c-extension', 'true', '--iterations', '100'])
    stop = time.perf_counter()
    time_2 = stop - start

    # Executing 100 times should be longer than benchmark only once, but don't have to be exact 100x faster.
    assert time_2 > time_1


def test_option_write_iterations(file=generate_test_path('integers.ion')):
    # warmup
    execution_with_command(['write', file, '--c-extension', 'true', '--iterations', '10'])

    start = time.perf_counter()
    execution_with_command(['write', file, '--c-extension', 'true', '--iterations', '1'])
    stop = time.perf_counter()
    time_1 = stop - start

    start = time.perf_counter()
    execution_with_command(['write', file, '--c-extension', 'true', '--iterations', '100'])
    stop = time.perf_counter()
    time_2 = stop - start

    # Executing 100 times should be longer than benchmark only once, but don't have to be exact 100x faster.
    assert time_2 > time_1

