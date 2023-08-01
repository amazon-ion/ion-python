import os
import time
from os.path import abspath, join, dirname

from amazon.ion import simpleion
from amazon.ion.equivalence import ion_equals
from amazon.ionbenchmark import Format
from amazon.ionbenchmark.Format import format_is_ion, format_is_cbor, format_is_json, rewrite_file_to_format
from amazon.ionbenchmark.ion_benchmark_cli import TOOL_VERSION
from tests import parametrize


def generate_test_path(p):
    return join(dirname(abspath(__file__)), 'benchmark_sample_data', p)


def run_cli(c):
    import subprocess
    cmd = ["python", "./amazon/ionbenchmark/ion_benchmark_cli.py"] + c
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    error_code = proc.wait()
    (out, err) = proc.communicate()
    return error_code, out, err


def test_option_version():
    (error_code, out, _) = run_cli(["--version"])
    assert not error_code
    assert out.strip() == TOOL_VERSION


@parametrize(
    ('write', 'buffer'),
    ('write', 'file'),
    ('read', 'buffer'),
    ('read', 'file'),
)
def test_run_benchmark_spec(args):
    (command, io_type) = args
    (error_code, out, _) = run_cli(['spec', './tests/benchmark_sample_data/sample_spec/spec.ion', '-O', f'{{io_type:{io_type},command:{command},warmups:1,iterations:10}}'])
    assert not error_code



def test_option_write(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['write', file])
    assert not error_code


def test_option_read(file=generate_test_path('integers.ion')):
    # make sure it reads successfully
    (error_code, _, _) = run_cli(['read', file])
    assert not error_code


def test_option_write_no_c_extension(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['write', file, '--no-c-extension'])
    assert not error_code


def test_option_read_no_c_extension(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['read', file, '--no-c-extension'])
    assert not error_code


def test_option_read_iterations(file=generate_test_path('integers.ion')):
    # This is a potentially flaky test due to the overhead of running the CLI as a new process.
    start = time.perf_counter()
    (error_code, _, _) = run_cli(['read', file, '--iterations', '3'])
    stop = time.perf_counter()
    assert not error_code
    time_1 = stop - start

    start = time.perf_counter()
    (error_code, _, _) = run_cli(['read', file, '--iterations', '300'])
    stop = time.perf_counter()
    assert not error_code
    time_2 = stop - start

    assert time_2 > time_1


def test_option_write_iterations(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['write', file, '--iterations', '100'])
    assert not error_code


# Streaming not supported yet
# def test_read_multi_api(file=generate_test_path('integers.ion')):
#     execution_with_command(['read', file, '--api', 'load_dump', '--api', 'streaming'])

# Streaming not supported yet
# def test_write_multi_api(file=generate_test_path('integers.ion')):
#     execution_with_command(['write', file, '--api', 'load_dump', '--api', 'streaming'])


def test_read_duplicated_api(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['read', file, '--api', 'load_dump', '--api', 'load_dump'])
    assert not error_code


def test_write_duplicated_api(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['write', file, '--api', 'load_dump', '--api', 'load_dump'])
    assert not error_code


def test_read_multi_format(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['read', file, '--format', 'ion_text', '--format', 'ion_binary'])
    assert not error_code


def test_write_multi_format(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['write', file, '--format', 'ion_text', '--format', 'ion_binary'])
    assert not error_code


def test_read_multi_duplicated_format(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['read', file, '--format', 'ion_text', '--format', 'ion_binary', '--format', 'ion_text'])
    assert not error_code


def test_write_multi_duplicated_format(file=generate_test_path('integers.ion')):
    (error_code, _, _) = run_cli(['write', file, '--format', 'ion_text', '--format', 'ion_binary', '--format', 'ion_text', ])
    assert not error_code


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_json(f.value)))
)
def test_write_json_format(f):
    (error_code, _, _) = run_cli(['write', generate_test_path('json/object.json'), '--format', f'{f}'])
    assert not error_code


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_json(f.value)))
)
def test_read_json_format(f):
    (error_code, _, _) = run_cli(['read', generate_test_path('json/object.json'), '--format', f'{f}'])
    assert not error_code


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_cbor(f.value)))
)
def test_write_cbor_format(f):
    (error_code, _, _) = run_cli(['write', generate_test_path('cbor/sample'), '--format', f'{f}'])
    assert not error_code


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_cbor(f.value)))
)
def test_read_cbor_format(f):
    (error_code, _, _) = run_cli(['read', generate_test_path('cbor/sample'), '--format', f'{f}'])
    assert not error_code



@parametrize(*['buffer', 'file'])
def test_write_io_type(f):
    (error_code, _, _) = run_cli(['write', generate_test_path('integers.ion'), '--io-type', f'{f}', '--format', 'json'])
    assert not error_code



@parametrize(*['buffer', 'file'])
def test_read_io_type(f):
    (error_code, _, _) = run_cli(['read', '--io-type', f'{f}', '--format', 'ion_text', '--format', 'ion_binary', generate_test_path('integers.ion')])
    assert not error_code


@parametrize(
    *tuple((Format.Format.ION_TEXT, Format.Format.ION_BINARY))
)
def test_format_is_ion(f):
    assert format_is_ion(f.value) is True


@parametrize(
    *tuple((Format.Format.JSON,
            Format.Format.UJSON,
            Format.Format.RAPIDJSON,
            Format.Format.SIMPLEJSON
            ))
)
def test_format_is_json(f):
    assert format_is_json(f.value) is True


@parametrize(
    Format.Format.CBOR,
    Format.Format.CBOR2
)
def test_format_is_cbor(f):
    assert format_is_cbor(f.value) is True


def assert_ion_string_equals(act, exp):
    assert ion_equals(simpleion.loads(act), simpleion.loads(exp))


def test_compare_without_regression():
    (error_code, _, _) = run_cli(['compare', generate_test_path('compare/cats_baseline.ion'), generate_test_path('compare/cats_baseline.ion'), '--fail'])
    assert not error_code


def test_compare_with_small_regression():
    (error_code, _, _) = run_cli(['compare', generate_test_path('compare/cats_baseline.ion'), generate_test_path('compare/cats_small_regression.ion'), '--fail'])
    assert not error_code


def test_compare_with_large_regression():
    (error_code, _, _) = run_cli(['compare', './tests/benchmark_sample_data/compare/cats_baseline.ion', generate_test_path('compare/cats_large_regression.ion'), '--fail'])
    assert error_code


def test_format_conversion_ion_binary_to_ion_text():
    rewrite_file_to_format(generate_test_path('integers.ion'), Format.Format.ION_BINARY.value)
    assert os.path.exists('temp_integers.10n')
    os.remove('temp_integers.10n')


def test_format_conversion_ion_text_to_ion_binary():
    rewrite_file_to_format(generate_test_path('integers.10n'), Format.Format.ION_TEXT.value)
    assert os.path.exists('temp_integers.ion')
    os.remove('temp_integers.ion')
