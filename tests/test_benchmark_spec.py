import json
import os
from os.path import abspath, join, dirname
from pathlib import Path

import cbor2

from amazon.ion import simpleion
from amazon.ion.equivalence import ion_equals
from amazon.ionbenchmark.Format import format_is_binary
from amazon.ionbenchmark.benchmark_runner import _create_test_fun
from amazon.ion.simpleion import IonPyValueModel
from amazon.ion.symbols import SymbolToken
from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec
from tests import parametrize


def _generate_test_path(p):
    return join(dirname(abspath(__file__)), 'benchmark_sample_data', p)


_minimal_params = {'format': "ion_text", 'input_file': "cat.ion"}
_minimal_spec = BenchmarkSpec(_minimal_params, working_directory=_generate_test_path("sample_spec"))

_multiple_top_level_json_obj_params = {'format': "json", 'input_file': "multiple_top_level_object.json"}
_multiple_top_level_json_obj_spec = BenchmarkSpec(_multiple_top_level_json_obj_params,
                                                  working_directory=_generate_test_path("sample_spec"))

_multiple_top_level_cbor_obj_params = {'format': "cbor2", 'input_file': "multiple_top_level_object.cbor"}
_multiple_top_level_cbor_obj_spec = BenchmarkSpec(_multiple_top_level_cbor_obj_params,
                                                  working_directory=_generate_test_path("sample_spec"))


# We use JSON to generate native Python objects within `multiple_top_level_object` and then store them in a list.
# [{"name":"John", "age":30, "car":null}, {"name":"Mike", "age":33, "car":null}, {"name":"Jack", "age":24, "car":null}]
def _generate_multiple_top_level_python_obj():
    rtn = []
    with open(join(_generate_test_path("sample_spec"), 'multiple_top_level_object.json'), 'r') as fp:
        while True:
            jsonl = fp.readline()
            if jsonl == '':
                break
            rtn.append(json.loads(jsonl))
        return rtn


# Make sure all generated read benchmarking functions works correctly, especially for the multiple top level objects
# use case. It uses ion_equals to validate Ion objects.
@parametrize(
    ('read', 'cbor2', 'multiple_top_level_object.cbor'),
    ('read', 'json', 'multiple_top_level_object.json'),
    ('read', 'ion_binary', 'multiple_top_level_object.ion'),
)
def test_create_read_test_fun(args):
    (command, format_option, file) = args
    params = {'command': command, 'format': format_option, 'input_file': file, 'io_type': 'file'}
    spec = BenchmarkSpec(params, working_directory=_generate_test_path("sample_spec"))
    test_fun = _create_test_fun(spec, return_obj=True)

    _exp_obj_list = _generate_multiple_top_level_python_obj()
    if format_option == 'ion_binary':
        assert ion_equals(test_fun(), _exp_obj_list)
    else:
        assert test_fun() == _exp_obj_list


# Make sure all generated write benchmarking functions works correctly, especially for the multiple top level objects
# use case.
@parametrize(
    ('write', 'cbor2', 'multiple_top_level_object.cbor'),
    ('write', 'json', 'multiple_top_level_object.json'),
    ('write', 'ion_binary', 'multiple_top_level_object.ion'),
)
def test_create_write_test_fun(args):
    (command, format_option, file) = args
    test_file = 'test_create_write_test_fun'
    params = {'command': command, 'format': format_option, 'input_file': file, 'io_type': 'file'}
    spec = BenchmarkSpec(params, working_directory=_generate_test_path("sample_spec"))
    test_fun = _create_test_fun(spec, custom_file=test_file)
    test_fun()

    # Validation.
    # This is the generated test_file
    with open(test_file, 'br' if format_is_binary(format_option) else 'r') as fp:
        # This is the original file used for benchmarking
        with open(join(_generate_test_path("sample_spec"), file),
                  'br' if format_is_binary(format_option) else 'r') as fp2:
            # Compare two JSON files, we remove all newline and whitespace and compare the text.
            if format_option == 'json':
                assert fp.read().replace('\n', '').replace(' ', '') == fp2.read().replace('\n', '').replace(
                    ' ', '')
            # Compare two Ion files, we load Ion values, and use ion_equals since Ion has both text and binary formats.
            elif format_option == 'ion':
                ion_equals(simpleion.load(fp, single_value=False), simpleion.load(fp2, single_value=False))
            # Compare two CBOR files, compare bytes
            elif format_option == 'cbor2':
                assert fp.read() == fp2.read()

    if os.path.exists(test_file):
        os.remove(test_file)


# make sure all top level JSON objects are generated
def test_write_generate_multiple_top_level_json_values():
    data_obj = _multiple_top_level_json_obj_spec.get_data_object()
    data_obj = list(data_obj)
    obj_count = len(data_obj)
    load_count = 0
    with open(join(_generate_test_path("sample_spec"), 'multiple_top_level_object.json'), 'r') as f:
        # iterate each top level object
        while True:
            jsonl = f.readline()
            if jsonl == '':
                break
            # make sure the json object are equivalent
            assert data_obj[load_count] == json.loads(jsonl)
            load_count += 1
    # make sure they have the same size
    assert obj_count == load_count


# make sure all top level CBOR objects are generated
def test_write_generate_multiple_top_level_cbor_values():
    data_obj = _multiple_top_level_cbor_obj_spec.get_data_object()
    data_obj = list(data_obj)
    obj_count = len(data_obj)
    load_count = 0
    with open(join(_generate_test_path("sample_spec"), 'multiple_top_level_object.cbor'), 'br') as f:
        # iterate each top level object
        while True:
            try:
                o = cbor2.load(f)
                # make sure the CBOR object are equivalent
                assert data_obj[load_count] == o
            except EOFError:
                break
            load_count += 1
    # make sure they have the same size
    assert obj_count == load_count


def test_get_input_file_size():
    real_size = _minimal_spec.get_input_file_size()
    exp_size = Path(join(_generate_test_path("sample_spec"), _minimal_params['input_file'])).stat().st_size
    assert real_size == exp_size


def test_get_format():
    assert _minimal_spec.get_format() == 'ion_text'


def test_get_command():
    assert _minimal_spec.get_command() == 'read'


def test_get_api():
    assert _minimal_spec.get_api() == 'load_dump'


def test_get_name():
    assert _minimal_spec.get_name() == '(ion_text,loads,cat.ion)'


def test_defaults_and_overrides_applied_in_correct_order():
    a = {'c': 1, 'b': 10, 'a': 100}
    b = {'c': 2, 'b': 20}
    c = {'c': 3, **_minimal_params}

    spec = BenchmarkSpec(user_defaults=a, params=b, user_overrides=c)

    # From tool default
    assert spec['api'] == 'load_dump'
    # From user default
    assert spec['a'] == 100
    # From params
    assert spec['b'] == 20
    # From user override
    assert spec['c'] == 3


def test_model_flags():
    spec = BenchmarkSpec({**_minimal_params})
    ion_loader = spec.get_loader_dumper()
    assert ion_loader.value_model is IonPyValueModel.ION_PY

    spec = BenchmarkSpec({**_minimal_params, 'model_flags': ["MAY_BE_BARE", SymbolToken("SYMBOL_AS_TEXT", None, None)]})
    ion_loader = spec.get_loader_dumper()
    assert ion_loader.value_model is IonPyValueModel.MAY_BE_BARE | IonPyValueModel.SYMBOL_AS_TEXT
