import json
import time
from itertools import chain
from os.path import abspath, join, dirname

import cbor2
from docopt import docopt

from amazon.ion import simpleion
from amazon.ionbenchmark import ion_benchmark_cli, Format, Io_type
from amazon.ionbenchmark.Format import format_is_ion, format_is_cbor, format_is_json
from amazon.ionbenchmark.ion_benchmark_cli import generate_read_test_code, \
    generate_write_test_code, ion_python_benchmark_cli
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
def test_generate_simpleion_read_test_code(path):
    actual = generate_read_test_code(path, memory_profiling=False, single_value=False,
                                     format_option=Format.Format.ION_TEXT.value, emit_bare_values=False,
                                     io_type=Io_type.Io_type.FILE, binary=False)

    # make sure we generated the desired load function
    with open(path) as fp:
        expect = simpleion.load(fp, single_value=False, emit_bare_values=False, parse_eagerly=True)

    # make sure the return values are same
    assert actual() == expect


@parametrize(
    generate_test_path('integers.ion')
)
def test_generate_json_read_test_code(path):
    actual = generate_read_test_code(path, memory_profiling=False, single_value=False,
                                     format_option=Format.Format.JSON.value, emit_bare_values=False,
                                     io_type=Io_type.Io_type.FILE, binary=False)

    # make sure we generated the desired load function
    with open(path) as fp:
        expect = json.load(fp)

    # make sure the return values are same
    assert actual() == expect


@parametrize(
    generate_test_path('integers.ion')
)
def test_generate_cbor_read_test_code(path):
    actual = generate_read_test_code(path, memory_profiling=False, single_value=False,
                                     format_option=Format.Format.CBOR2.value, emit_bare_values=False,
                                     io_type=Io_type.Io_type.FILE, binary=True)

    # make sure we generated the desired load function
    with open(path, 'br') as fp:
        expect = cbor2.load(fp)

    # make sure the return values are same
    assert actual() == expect


@parametrize(
    *tuple(chain(
        generate_scalars_text(SIMPLE_SCALARS_MAP_TEXT),
    ))
)
def test_generate_simpleion_write_test_code(obj):
    actual = generate_write_test_code(obj, format_option=Format.Format.ION_TEXT.value, memory_profiling=False,
                                      binary=False, io_type=Io_type.Io_type.BUFFER.value)

    # make sure we generated the desired dumps function
    expect = simpleion.dumps(obj, binary=False)

    # make sure the return values are same
    assert actual() == expect


@parametrize(
    generate_test_path('json/object.json'),
)
def test_generate_json_write_test_code(file):
    with open(file) as fp:
        obj = json.load(fp)
    actual = generate_write_test_code(obj, format_option=Format.Format.JSON.value, memory_profiling=False, binary=False,
                                      io_type=Io_type.Io_type.BUFFER.value)

    # make sure we generated the desired dumps function
    expect = json.dumps(obj)

    # make sure the return values are same
    assert actual() == expect


@parametrize(
    generate_test_path('cbor/sample')
)
def test_generate_cbor_write_test_code(file):
    with open(file, 'br') as fp:
        obj = cbor2.load(fp)
    actual = generate_write_test_code(obj, format_option=Format.Format.CBOR2.value, memory_profiling=False,
                                      binary=False, io_type=Io_type.Io_type.BUFFER.value)

    # make sure we generated the desired dumps function
    expect = cbor2.dumps(obj)

    # make sure the return values are same
    assert actual() == expect


def execution_with_command(c):
    return ion_python_benchmark_cli(docopt(doc, argv=c))


def test_option_version():
    assert execution_with_command('-v') == TOOL_VERSION


def test_option_write(file=generate_test_path('integers.ion')):
    execution_with_command(['write', file])


def test_option_read(file=generate_test_path('integers.ion')):
    # make sure it reads successfully
    execution_with_command(['read', file])


def test_option_write_c_extension(file=generate_test_path('integers.ion')):
    execution_with_command(['write', file, '--c-extension', 'true'])
    execution_with_command(['write', file, '--c-extension', 'false'])


def test_option_read_c_extension(file=generate_test_path('integers.ion')):
    execution_with_command(['read', file, '--c-extension', 'true'])
    execution_with_command(['read', file, '--c-extension', 'false'])


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
    execution_with_command(['write', file, '--c-extension', 'true', '--iterations', '1'])
    execution_with_command(['write', file, '--c-extension', 'true', '--iterations', '100'])


def gather_all_options_in_list(table):
    rtn = []
    count = 1
    if len(table) < 1:
        return []
    while count < len(table):
        rtn += [table[count][1]]
        count += 1
    return sorted(rtn)


def test_read_multi_api(file=generate_test_path('integers.ion')):
    table = execution_with_command(['read', file, '--api', 'load_dump', '--api', 'streaming'])
    assert gather_all_options_in_list(table) == sorted(
        [('streaming', 'ion_binary', 'file'), ('load_dump', 'ion_binary', 'file')])


def test_write_multi_api(file=generate_test_path('integers.ion')):
    table = execution_with_command(['write', file, '--api', 'load_dump', '--api', 'streaming'])
    assert gather_all_options_in_list(table) == sorted(
        [('streaming', 'ion_binary', 'file'), ('load_dump', 'ion_binary', 'file')])


def test_read_multi_duplicated_api(file=generate_test_path('integers.ion')):
    table = execution_with_command(['read', file, '--api', 'load_dump', '--api', 'streaming', '--api', 'streaming'])
    assert gather_all_options_in_list(table) == sorted(
        [('streaming', 'ion_binary', 'file'), ('load_dump', 'ion_binary', 'file')])


def test_write_multi_duplicated_api(file=generate_test_path('integers.ion')):
    table = execution_with_command(['write', file, '--api', 'load_dump', '--api', 'streaming', '--api', 'streaming'])
    assert gather_all_options_in_list(table) == sorted(
        [('streaming', 'ion_binary', 'file'), ('load_dump', 'ion_binary', 'file')])


def test_read_multi_format(file=generate_test_path('integers.ion')):
    table = execution_with_command(['read', file, '--format', 'ion_text', '--format', 'ion_binary'])
    assert gather_all_options_in_list(table) == sorted(
        [('load_dump', 'ion_binary', 'file'), ('load_dump', 'ion_text', 'file')])


def test_write_multi_format(file=generate_test_path('integers.ion')):
    table = execution_with_command(['write', file, '--format', 'ion_text', '--format', 'ion_binary'])
    assert gather_all_options_in_list(table) == sorted(
        [('load_dump', 'ion_text', 'file'), ('load_dump', 'ion_binary', 'file')])


def test_read_multi_duplicated_format(file=generate_test_path('integers.ion')):
    table = execution_with_command(
        ['read', file, '--format', 'ion_text', '--format', 'ion_binary', '--format', 'ion_text'])
    assert gather_all_options_in_list(table) == sorted(
        [('load_dump', 'ion_text', 'file'), ('load_dump', 'ion_binary', 'file')])


def test_write_multi_duplicated_format(file=generate_test_path('integers.ion')):
    table = execution_with_command(
        ['write', file, '--format', 'ion_text', '--format', 'ion_binary', '--format', 'ion_text', ])
    assert gather_all_options_in_list(table) == sorted(
        [('load_dump', 'ion_text', 'file'), ('load_dump', 'ion_binary', 'file')])


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_json(f.value)))
)
def test_write_json_format(f):
    table = execution_with_command(['write', generate_test_path('json/object.json'), '--format', f'{f}'])
    assert gather_all_options_in_list(table) == sorted([('load_dump', f'{f}', 'file')])


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_json(f.value)))
)
def test_read_json_format(f):
    table = execution_with_command(['read', generate_test_path('json/object.json'), '--format', f'{f}'])
    assert gather_all_options_in_list(table) == sorted([('load_dump', f'{f}', 'file')])


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_cbor(f.value)))
)
def test_write_cbor_format(f):
    table = execution_with_command(['write', generate_test_path('cbor/sample'), '--format', f'{f}'])
    assert gather_all_options_in_list(table) == sorted([('load_dump', f'{f}', 'file')])


@parametrize(
    *tuple((f.value for f in Format.Format if Format.format_is_cbor(f.value)))
)
def test_read_cbor_format(f):
    table = execution_with_command(['read', generate_test_path('cbor/sample'), '--format', f'{f}'])
    assert gather_all_options_in_list(table) == sorted([('load_dump', f'{f}', 'file')])


@parametrize(
    *tuple((io.value for io in Io_type.Io_type))
)
def test_write_io_type(f):
    table = execution_with_command(
        ['write', generate_test_path('integers.ion'), '--io-type', f'{f}', '--format', 'json'])
    assert gather_all_options_in_list(table) == sorted([('load_dump', 'json', f'{f}')])


@parametrize(
    *tuple((io.value for io in Io_type.Io_type))
)
def test_read_io_type(f):
    table = execution_with_command(
        ['read', generate_test_path('integers.ion'), '--io-type', f'{f}', '--format', 'json', '--format', 'ion_binary'])
    assert gather_all_options_in_list(table) == sorted(
        [('load_dump', 'json', f'{f}'), ('load_dump', 'ion_binary', f'{f}')])


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
