# Amazon Ion Python
An implementation of [Amazon Ion](https://amznlabs.github.io/ion-docs/)
for Python.

[![Build Status](https://travis-ci.org/amznlabs/ion-python.svg?branch=master)](https://travis-ci.org/amznlabs/ion-python)

This package is designed to work with **Python 2.6+** and **Python 3.3+**

***This package is an early work in progress under active development, and is not (yet)
considered a complete implementation of Ion.***

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
$ for V in 2.6.9 2.7.12 3.3.6 3.4.5 3.5.2 pypy-5.3.1; do pyenv install $V; done
```

Note that on Mac OS X, you may need to change the `CFLAGS`:

```
$ for V in 2.6.9 2.7.12 3.3.6 3.4.5 3.5.2 pypy-5.3.1; do
    CFLAGS="-I$(xcrun --show-sdk-path)/usr/include" pyenv install $V; done
```

Once you have these installations, add them as a local `pyenv` configuration

```
$ pyenv local 2.6.9 2.7.12 3.3.6 3.4.5 3.5.2 pypy-5.3.1
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
Here are some, rather critical, things that need to be implemented:

* Design and implement an event oriented text and binary pull-parser with coroutines.
* Provide a `simplejson` style API for ease-of-use.
    * Provide proper type mappings that retains Ion data model metadata (e.g. Ion type and annotations).

In addition, there are build, deployment, or release tasks that are required:

* Add support for [code coverage](http://coverage.readthedocs.io/en/latest/) reporting.
    * Publish coverage to something like [Coverage.io](https://coveralls.io/)
* Provide proper documentation generation via [Sphinx](http://www.sphinx-doc.org/en/stable/).
    * Add good surrounding documentation around setup/development/contribution/getting started.
    * Publish documentation to [Read the Docs](http://docs.readthedocs.io/en/latest/index.html).
* Follow [Python Packaging Guide](https://python-packaging-user-guide.readthedocs.io/en/latest/) best practices
  as appropriate.
    * Distribute into [PyPI](https://pypi.python.org/pypi)
* Consider using something like [PyPy.js](https://github.com/pypyjs/pypyjs) to build an interactive shell for playing
  with Ion python and provide a client-side Ion playground.
