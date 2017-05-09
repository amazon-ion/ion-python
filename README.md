# Amazon Ion Python
An implementation of [Amazon Ion](https://amznlabs.github.io/ion-docs/)
for Python.

[![Build Status](https://travis-ci.org/amznlabs/ion-python.svg?branch=master)](https://travis-ci.org/amznlabs/ion-python)

This package is designed to work with **Python 2.6+** and **Python 3.3+**

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

### Debugging the C Extension
The library includes a C extension (`ioncmodule`) that provides significant speedups over the
pure-Python code. Unfortunately, most graphical debuggers, such as those commonly found in
Python IDEs, are not capable of debugging CPython extensions in a sane way.

It is tempting to try to write simple C unit tests for the extension module. This won't work
because the CPython modules will not be loaded unless the code is initialized by a CPython
interpreter. In other words, anything in the C extension that relies upon a Python C API will
give different results than if the same code were run by the interpreter.

Because of this, debugging the C extension requires starting in Python and breaking within the
shared library that holds the C extension. This presents challenges for most debuggers because
it requires them to understand both Python and C.

Fortunately, GDB is capable of handling this use case due to its support for Python script
plugins.

#### On Mac
GDB does not ship with XCode any more, having been replaced by the LLDB debugger for LLVM.
There is no evidence that Python C extension debugging is possible in LLDB, so you will
probably have to install GDB manually.

GDB 7 advertises rich Python debugging support. Using scrips provided by a debug build
of the CPython interpreter (located at `$PYTHON_ROOT/python-gdb.py` or
`$PYTHON_ROOT/python.exe-gdb.py`, where `$PYTHON_ROOT` is the location of the debug build
of Python), it adds automatic pretty-printing of locals and adds Python-specific
commands such as `py-bt` (backtrace of Python frames) and `py-list` (show surrounding
Python source). This can be hooked into GDB by putting the following line in `~/.gdbinit`:
`add-auto-load-safe-path $PYTHON_ROOT`. However, the stock release of GDB appears not to
be able to set breakpoints within shared libraries on Mac, so you can't set a breakpoint
within the C extension itself, which is a dealbreaker.

This limitation seems to be a known problem, which has led to the development of a Mac-specific
version of GDB (called `gdb-apple`), available through MacPorts, that supports breakpoints
within shared libraries. The drawback is that `gdb-apple` is based on GDB 6, meaning that it
does not support the rich Python debugging extensions provided by `python-gdb.py`. It does,
however, support an older version of similar extensions, available
[here](https://github.com/python-git/python/blob/master/Misc/gdbinit). The contents of this
file can simply be added to `~/.gdbinit`, and they will be loaded automatically when GDB
starts. This adds several new commands to GDB, the most useful of which is `pyo`, which,
given a `PyObject *` reference, prints its Python type, value, and refcount.

If `gdb-apple` is updated to GDB 7, the newer Python debugging features could be used on Mac.

#### On Linux
On Linux, GDB does not fail to set breakpoints in shared libraries, so GDB 7 may be used. I
haven't had success getting it to work with `python-gdb.py`, though. Fortunately, GDB 7 will
still work with the same older version of the python extensions as must be used on Mac.

## TODO
The following build, deployment, or release tasks are required:

* Add support for [code coverage](http://coverage.readthedocs.io/en/latest/) reporting.
    * Publish coverage to something like [Coverage.io](https://coveralls.io/)
* Provide proper documentation generation via [Sphinx](http://www.sphinx-doc.org/en/stable/).
    * Add good surrounding documentation around setup/development/contribution/getting started.
    * Publish documentation to [Read the Docs](http://docs.readthedocs.io/en/latest/index.html).
* Consider using something like [PyPy.js](https://github.com/pypyjs/pypyjs) to build an interactive shell for playing
  with Ion python and provide a client-side Ion playground.
