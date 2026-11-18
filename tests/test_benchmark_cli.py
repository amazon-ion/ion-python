import os
import time
from os.path import abspath, join, dirname
from pathlib import Path

import pytest

from amazon.ion import simpleion
from amazon.ion.equivalence import ion_equals
from amazon.ionbenchmark import Format, benchmark_spec
from amazon.ionbenchmark.Format import format_is_ion, format_is_cbor, format_is_json, rewrite_file_to_format
from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec
from amazon.ionbenchmark.ion_benchmark_cli import TOOL_VERSION
from tests import parametrize


def generate_test_path(p):
    return join(dirname(abspath(__file__)), 'benchmark_sample_data', p)


def _ion_bytes(value, format_option):
    data = simpleion.dumps(value, binary=format_option == 'ion_binary')
    return data.encode('utf-8') if isinstance(data, str) else data


def run_cli(c):
    import subprocess
    cmd = ["python", abspath(join(dirname(os.path.abspath(__file__)), '../src-python/amazon/ionbenchmark/ion_benchmark_cli.py'))] + c
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
    (error_code, _, _) = run_cli(['read', file, '--iterations', '3'])
    assert not error_code

    (error_code, _, _) = run_cli(['read', file, '--iterations', '300'])
    assert not error_code



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


@pytest.mark.parametrize('format_option,suffix', [('ion_text', '.ion'), ('ion_binary', '.10n')])
@pytest.mark.parametrize('path_type', [str, Path])
def test_format_conversion_reuses_matching_input(tmp_path, monkeypatch, format_option, suffix, path_type):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / ('temp_sample' + suffix)
    source.write_bytes(_ion_bytes(1, format_option))
    original = source.read_bytes()
    stale_temp = tmp_path / ('temp_temp_sample' + suffix)
    stale_temp.write_bytes(b'previous benchmark')

    def unexpected_copy(*args):
        pytest.fail('Matching Ion input should not be copied')

    monkeypatch.setattr(Format.shutil, 'copy', unexpected_copy)
    source_path = path_type(source)
    assert rewrite_file_to_format(source_path, format_option) is source_path
    assert source.read_bytes() == original
    assert stale_temp.read_bytes() == b'previous benchmark'


@pytest.mark.parametrize('format_option,suffix', [('ion_text', '.ion'), ('ion_binary', '.10n')])
def test_format_conversion_reuses_same_basename(tmp_path, monkeypatch, format_option, suffix):
    monkeypatch.chdir(tmp_path)
    sources = []
    for directory in ('first', 'second'):
        parent = tmp_path / directory
        parent.mkdir()
        source = parent / ('sample' + suffix)
        source.write_bytes(_ion_bytes(directory, format_option))
        sources.append(source)

    for source in sources * 2:
        assert rewrite_file_to_format(source, format_option) == source
    assert not (tmp_path / ('temp_sample' + suffix)).exists()
    assert sources[0].read_bytes() != sources[1].read_bytes()


@pytest.mark.parametrize('format_option', ['ion_text', 'ion_binary'])
def test_format_conversion_copies_unknown_extension(tmp_path, monkeypatch, format_option):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / 'sample.data'
    source.write_bytes(_ion_bytes(1, format_option))
    result = rewrite_file_to_format(source, format_option)
    assert result != source
    assert Path(result).read_bytes() == source.read_bytes()


@pytest.mark.parametrize('format_option,suffix', [('ion_text', '.ion'), ('ion_binary', '.10n')])
@pytest.mark.parametrize('command', ['read', 'write'])
@pytest.mark.parametrize('io_type', ['buffer', 'file'])
def test_benchmark_reused_input_is_read_only(tmp_path, monkeypatch, format_option, suffix, command, io_type):
    from amazon.ionbenchmark.benchmark_runner import run_benchmark

    monkeypatch.chdir(tmp_path)
    source = tmp_path / ('sample' + suffix)
    original = _ion_bytes(1, format_option)
    source.write_bytes(original)
    source.chmod(0o444)
    try:
        spec = BenchmarkSpec({'input_file': str(source), 'format': format_option, 'command': command,
                              'io_type': io_type, 'iterations': 2, 'warmups': 0, 'py_c_extension': False})
        spec['input_file'] = rewrite_file_to_format(spec.get_input_file(), spec.get_format())
        assert spec.get_input_file() == str(source)
        result = run_benchmark(spec)
        assert len(result.nanos_per_op) == 2
        assert source.read_bytes() == original
        assert not (tmp_path / ('temp_sample' + suffix)).exists()
    finally:
        source.chmod(0o644)


def test_format_conversion_ion_binary_to_ion_text():
    rewrite_file_to_format(generate_test_path('integers.ion'), Format.Format.ION_BINARY.value)
    assert os.path.exists('temp_integers.10n')
    os.remove('temp_integers.10n')


def test_format_conversion_ion_text_to_ion_binary():
    rewrite_file_to_format(generate_test_path('integers.10n'), Format.Format.ION_TEXT.value)
    assert os.path.exists('temp_integers.ion')
    os.remove('temp_integers.ion')


@parametrize(
    ('write', 'json', generate_test_path('./sample_spec/multiple_top_level_object.json')),
    ('write', 'cbor2', generate_test_path('./sample_spec/multiple_top_level_object.cbor')),
    ('write', 'ion_binary', generate_test_path('./sample_spec/multiple_top_level_object.ion')),
    ('read', 'json', generate_test_path('./sample_spec/multiple_top_level_object.json')),
    ('read', 'cbor2', generate_test_path('./sample_spec/multiple_top_level_object.cbor')),
    ('read', 'ion_binary', generate_test_path('./sample_spec/multiple_top_level_object.ion')),
)
def test_multiple_top_level_values(args):
    (command, format_option, file) = args
    (error_code, _, _) = run_cli([f'{command}', file, '--format', f'{format_option}', '--io-type', 'file'])
    assert not error_code

