# Amazon Ion Python
An implementation of [Amazon Ion](https://amznlabs.github.io/ion-docs/)
for Python.

This package is designed to work with **Python 2.6+** and **Python 3.3+**

***This package is an early work in progress under active development, and is not (yet)
considered a complete implementation of Ion.***

## TODO
Here are some, rather critical, things that need to be implemented:

* Design and implement an event oriented text and binary pull-parser with coroutines.
* Provide a `simplejson` style API for ease-of-use.
    * Provide proper type mappings that retains Ion data model metadata (e.g. Ion type and annotations).

In addition, there are build, deployment, or release tasks that are required:

* Add [Travis CI](https://docs.travis-ci.com/user/languages/python) integration.
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
