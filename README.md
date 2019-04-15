# Amazon Ion Python
An implementation of [Amazon Ion](https://amzn.github.io/ion-docs/)
for Python.

[![Build Status](https://travis-ci.org/amzn/ion-python.svg?branch=master)](https://travis-ci.org/amzn/ion-python)
[![Documentation Status](https://readthedocs.org/projects/ion-python/badge/?version=latest)](https://ion-python.readthedocs.io/en/latest/?badge=latest)

This package is designed to work with **Python 2.6+** and **Python 3.3+**

## Getting Started

Start with the [simpleion](https://ion-python.readthedocs.io/en/latest/amazon.ion.html#module-amazon.ion.simpleion)
module, which provides four APIs (`dump`, `dumps`, `load`, `loads`) that will be familiar to users of Python's
built-in JSON parsing module.

For example:

```
>>> import amazon.ion.simpleion as ion
>>> obj = ion.loads('{abc: 123}')
>>> obj['abc']
123
>>> ion.dumps(obj, binary=False)
'$ion_1_0 {abc:123}'
```

## Git Setup
This repository contains a [git submodule](https://git-scm.com/docs/git-submodule)
called `ion-tests`, which holds test data used by `ion-python`'s unit tests.

The easiest way to clone the `ion-python` repository and initialize its `ion-tests`
submodule is to run the following command.

```
$ git clone --recursive https://github.com/amzn/ion-python.git ion-python
```

Alternatively, the submodule may be initialized independently from the clone
by running the following commands.

```
$ git submodule init
$ git submodule update
```

## Development
It is recommended to use `virtualenv` to create a clean environment to build/test Ion Python.

```
$ virtualenv venv
...
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pip install -e .
```

You can also run the tests through `setup.py` or `py.test` directly.

```
$ python setup.py test
```

### Tox Setup
In order to verify that all platforms we support work with Ion Python, we use a combination
of [tox](http://tox.readthedocs.io/en/latest/) with [pyenv](https://github.com/yyuu/pyenv).

Install relevant versions of Python:

```
$ for V in 2.6.9 2.7.15 3.3.7 3.4.8 3.5.5 3.6.5 3.7.2 pypy2.7-6.0.0 pypy3.5-6.0.0; do pyenv install $V; done
```

Note that on Mac OS X, you may need to change the `CFLAGS`:

```
$ for V in 2.6.9 2.7.15 3.3.7 3.4.8 3.5.5 3.6.5 3.7.2 pypy2.7-6.0.0 pypy3.5-6.0.0; do
    CFLAGS="-I$(xcrun --show-sdk-path)/usr/include" pyenv install $V; done
```

Once you have these installations, add them as a local `pyenv` configuration

```
$ pyenv local 2.6.9 2.7.15 3.3.7 3.4.8 3.5.5 3.6.5 3.7.2 pypy2.7-6.0.0 pypy3.5-6.0.0
```

At the time of this writing, on Mac OS X, you may have problems with `pyenv` and `pypy`.
On this platform, it is probably easier to have `pypy` not managed by `pyenv` and install
and use it directly from `brew`.

Assuming you have `pyenv` properly set up (making sure `pyenv init` is evaluated into your shell),
you can now run `tox`:

```
# Run tox for all versions of python which executes py.test.
$ tox

# Run tox for just Python 2.7 and 3.5.
$ tox -e py27,py35

# Run tox for a specific version and run py.test with high verbosity
$ tox -e py27 -- py.test -vv

# Run tox for a specific version and just the virtual env REPL.
$ tox -e py27 -- python
```

## TODO
The following build, deployment, or release tasks are required:

* Add support for [code coverage](http://coverage.readthedocs.io/en/latest/) reporting.
    * Publish coverage to something like [Coverage.io](https://coveralls.io/)
* Improve documentation around setup/development/contribution/getting started.
* Consider using something like [PyPy.js](https://github.com/pypyjs/pypyjs) to build an interactive shell for playing
  with Ion python and provide a client-side Ion playground.
