import json
from os.path import abspath, join, dirname
from pathlib import Path

import cbor2

from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec


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


# make sure all top level JSON objects are generated
def test_write_generate_multiple_top_level_json_values():
    data_obj = _multiple_top_level_json_obj_spec.get_data_object()
    obj_count = len(data_obj)
    load_count = 0
    with open(join(_generate_test_path("sample_spec"), 'multiple_top_level_object.json'), 'r') as f:
        # iterate each top level object
        while True:
            jsonl = f.readline()
            if jsonl == '':
                break
            # make sure the json object are equivalence
            assert data_obj[load_count] == json.loads(jsonl)
            load_count += 1
    # make sure they have the same size
    assert obj_count == load_count


# make sure all top level CBOR objects are generated
def test_write_generate_multiple_top_level_cbor_values():
    data_obj = _multiple_top_level_cbor_obj_spec.get_data_object()
    obj_count = len(data_obj)
    load_count = 0
    with open(join(_generate_test_path("sample_spec"), 'multiple_top_level_object2.cbor'), 'br') as f:
        # iterate each top level object
        while True:
            try:
                o = cbor2.load(f)
                # make sure the CBOR object are equivalence
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
