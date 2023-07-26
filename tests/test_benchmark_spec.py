from os.path import abspath, join, dirname

from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec


def _generate_test_path(p):
    return join(dirname(abspath(__file__)), 'benchmark_sample_data', p)


_minimal_params = {'format': "ion_text", 'input_file': "cat.ion"}
_minimal_spec = BenchmarkSpec(_minimal_params, working_directory=_generate_test_path("sample_spec"))


def test_get_input_file_size():
    assert _minimal_spec.get_input_file_size() == 161


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
