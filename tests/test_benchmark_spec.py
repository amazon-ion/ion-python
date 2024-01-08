from os.path import abspath, join, dirname
from pathlib import Path

from amazon.ion.simpleion import IonPyValueModel
from amazon.ion.symbols import SymbolToken
from amazon.ionbenchmark.benchmark_spec import BenchmarkSpec


def _generate_test_path(p):
    return join(dirname(abspath(__file__)), 'benchmark_sample_data', p)


_minimal_params = {'format': "ion_text", 'input_file': "cat.ion"}
_minimal_spec = BenchmarkSpec(_minimal_params, working_directory=_generate_test_path("sample_spec"))


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
