# Amazon Ion Python
An implementation of [Amazon Ion](https://amazon-ion.github.io/ion-docs/)
for Python.

[![Build Status](https://travis-ci.org/amazon-ion/ion-python.svg?branch=master)](https://travis-ci.org/amazon-ion/ion-python)
[![Documentation Status](https://readthedocs.org/projects/ion-python/badge/?version=latest)](https://ion-python.readthedocs.io/en/latest/?badge=latest)

This package is designed to work with **Python 3.8+**. Newer language features will be used as deemed valuable. Support may be dropped for versions more than six months past EOL. 

## Getting Started

Install using pip with the following command.

```shell
python3 -m pip install amazon.ion
```


Start with the [simpleion](https://ion-python.readthedocs.io/en/latest/amazon.ion.html#module-amazon.ion.simpleion)
module, which provides four APIs (`dump`, `dumps`, `load`, `loads`) that will be familiar to users of Python's
built-in JSON parsing module. Simpleion module's performance is improved by an optional [C extension](https://github.com/amazon-ion/ion-python/blob/master/C_EXTENSION.md).

For example:

```
>>> import amazon.ion.simpleion as ion
>>> obj = ion.loads('{abc: 123}')
>>> obj['abc']
123
>>> ion.dumps(obj, binary=False)
'$ion_1_0 {abc:123}'
```

For additional examples, consult the [cookbook](https://amazon-ion.github.io/ion-docs/guides/cookbook.html).

## Git Setup
This repository contains two [git submodules](https://git-scm.com/docs/git-submodule).
`ion-tests` holds test data used by `ion-python`'s unit tests and `ion-c` speeds up `ion-python`'s simpleion module.

The easiest way to clone the `ion-python` repository and initialize its `ion-tests`
submodule is to run the following command.

```
$ git clone --recursive https://github.com/amazon-ion/ion-python.git ion-python
```

Alternatively, the submodule may be initialized independently from the clone
by running the following commands.

```
$ git submodule init
$ git submodule update
```

## Development
It is recommended to use `venv` to create a clean environment to build/test Ion Python.

```
$ python3 -m venv ./venv
...
$ . venv/bin/activate
$ python -m pip install build
$ python -m pip install '.[test]'
```

You can run the tests through `py.test` directly:

```
$ py.test --ignore tests/test_benchmark_cli.py --ignore tests/test_benchmark_spec.py
```

This recipe skips the benchmark tests because they require additional dependencies:

When in doubt, look at [`.github/workflows/main.yml`](.github/workflows/main.yml) to see CI setup
and learn how to build/test the project.
See also [ion-python#379](https://github.com/amazon-ion/ion-python/pull/379) for a writeup of our
adoption of pyproject.toml.

### Tox Setup
In order to verify that all platforms we support work with Ion Python, we use a combination
of [tox](http://tox.readthedocs.io/en/latest/) with [pyenv](https://github.com/yyuu/pyenv).

We recommend that you use tox within a virtual environment to isolate from whatever is in the system
installed Python.

Install relevant versions of Python:

```
$ for V in 3.8.10 3.9.5 do pyenv install $V; done
```

Once you have these installations, add them as a local `pyenv` configuration

```
$ pyenv local 3.8.10 3.9.5
```

Assuming you have `pyenv` properly set up (making sure `pyenv init` is evaluated into your shell),
you can now run `tox`:

```
# Run tox for all versions of python which executes py.test.
$ tox

# Run tox for just Python 3.8 and 3.9.
$ tox -e py38,py39

# Run tox for a specific version and run py.test with high verbosity
$ tox -e py39 -- py.test -vv

# Run tox for a specific version and just the virtual env REPL.
$ tox -e py39 -- python
```

## TODO
The following build, deployment, or release tasks are required:

* Add support for [code coverage](http://coverage.readthedocs.io/en/latest/) reporting.
    * Publish coverage to something like [Coverage.io](https://coveralls.io/)
* Consider using something like [PyPy.js](https://github.com/pypyjs/pypyjs) to build an interactive shell for playing
  with Ion python and provide a client-side Ion playground.
  
## Known Issues
[tests/test_vectors.py](https://github.com/amazon-ion/ion-python/blob/master/tests/test_vectors.py#L95) defines skipList variables
referencing test vectors that are not expected to work at this time.
