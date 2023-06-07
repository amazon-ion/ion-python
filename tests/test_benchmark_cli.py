import json
import os
import time
from itertools import chain
from os.path import abspath, join, dirname

import cbor2
from docopt import docopt

from amazon.ion import simpleion
from amazon.ion.equivalence import ion_equals
from amazon.ionbenchmark import ion_benchmark_cli, Format, Io_type
from amazon.ionbenchmark.Format import format_is_ion, format_is_cbor, format_is_json, rewrite_file_to_format
from amazon.ionbenchmark.ion_benchmark_cli import generate_read_test_code, \
    generate_write_test_code, ion_python_benchmark_cli, output_result_table, REGRESSION_THRESHOLD
from amazon.ionbenchmark.util import str_to_bool, TOOL_VERSION
from tests import parametrize
from tests.test_simpleion import generate_scalars_text
from tests.writer_util import SIMPLE_SCALARS_MAP_TEXT

doc = ion_benchmark_cli.__doc__
result_table_option_idx = 3


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
        rtn += [table[count][result_table_option_idx]]
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
        ['read', generate_test_path('integers.ion'), '--io-type', f'{f}', '--format', 'ion_text', '--format', 'ion_binary'])
    assert gather_all_options_in_list(table) == sorted(
        [('load_dump', 'ion_text', f'{f}'), ('load_dump', 'ion_binary', f'{f}')])


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


def test_output_result_table_none():
    test_table = [['field1', 'field2'], [1, 2], [3, 4]]
    results_output = None
    res = output_result_table(results_output, test_table)
    # Should print as expected and return None
    assert res is None


def test_output_result_table_output():
    test_table = [['field1', 'field2'], [1, 2], [3, 4]]
    results_output = 'test'
    res = output_result_table(results_output, test_table)
    # Cleans up the output file generated
    if os.path.exists(results_output):
        os.remove(results_output)
    act = simpleion.dumps(res, binary=False)
    exp = "[{field1:1,field2:2},{field1:3,field2:4}]"
    assert_ion_string_equals(act, exp)


def test_compare():
    # Generates test files and result report
    test_file_list = ['f1', 'f2', 'f3']
    execution_with_command(['read', generate_test_path('integers.ion'), '-o', test_file_list[0]])
    execution_with_command(['read', generate_test_path('integers.ion'), '-o', test_file_list[1]])
    execution_with_command(['compare', '--benchmark-result-previous', test_file_list[0], '--benchmark-result-new',
                            test_file_list[1], test_file_list[2]])
    # Collects results and clean up resources
    with open(test_file_list[2], 'br') as f3:
        res = simpleion.load(f3)
    # Cleans up resources
    for f in test_file_list:
        if os.path.exists(f):
            os.remove(f)
    # Makes sure the result includes required fields
    assert res[0].get('relative_difference_score') is not None
    assert res[0].get('command') is not None
    assert res[0].get('input') is not None
    assert res[0].get('options') is not None


def test_compare_without_regression():
    test_file_list = [generate_test_path('compare/f1'), generate_test_path('compare/f2'), generate_test_path('compare_output')]
    reg_f = execution_with_command(
        ['compare', '--benchmark-result-previous', test_file_list[1], '--benchmark-result-new',
         test_file_list[0], test_file_list[2]])
    # Only clean up output results
    if os.path.exists(generate_test_path('compare_output')):
        os.remove(generate_test_path('compare_output'))
    assert reg_f is None


def test_compare_with_regression():
    test_file_list = [generate_test_path('compare/f1'), generate_test_path('compare/f2'), generate_test_path('compare_output')]
    reg_f = execution_with_command(
        ['compare', '--benchmark-result-previous', test_file_list[0], '--benchmark-result-new',
         test_file_list[1], test_file_list[2]])
    with open(test_file_list[2], 'br') as f3:
        res = simpleion.load(f3)
    # Only clean up output results
    if os.path.exists(generate_test_path('compare_output')):
        os.remove(generate_test_path('compare_output'))
    assert res[0].get('relative_difference_score').get('total_time (s)') > REGRESSION_THRESHOLD
    assert reg_f == 'integers.ion'


def test_compare_big_gap_with_regression():
    test_file_list = [generate_test_path('compare/f1'), generate_test_path('compare/f3'), generate_test_path('compare_output')]
    reg_f = execution_with_command(
        ['compare', '--benchmark-result-previous', test_file_list[0], '--benchmark-result-new',
         test_file_list[1], test_file_list[2]])
    with open(test_file_list[2], 'br') as f3:
        res = simpleion.load(f3)
    # Only clean up output results
    if os.path.exists(generate_test_path('compare_output')):
        os.remove(generate_test_path('compare_output'))
    assert res[0].get('relative_difference_score').get('total_time (s)') > REGRESSION_THRESHOLD
    assert reg_f == 'integers.ion'


def test_format_conversion_ion_binary_to_ion_text():
    rewrite_file_to_format(generate_test_path('integers.ion'), Format.Format.ION_BINARY.value)
    assert os.path.exists('temp_integers.10n')
    # if os.path.exists('temp_integers.10n'):
    #     os.remove('temp_integers.10n')


def test_format_conversion_ion_text_to_ion_binary():
    rewrite_file_to_format(generate_test_path('integers.10n'), Format.Format.ION_TEXT.value)
    assert os.path.exists('temp_integers.ion')
    if os.path.exists('temp_integers.ion'):
        os.remove('temp_integers.ion')
