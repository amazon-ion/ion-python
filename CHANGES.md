### 0.7.0 (2020-12-02)
* Uses localcontext support decimals of any precision (#132).
* Adds IonToJSONEncoder to allow Ion data to be down-converted to JSON (#107).

### 0.6.0 (2020-04-23)
* Retains the serialized order of struct elements when read via `load/loads`. (#125)
* Makes IonPy* types picklable. (#128)

### 0.5.0 (2019-10-17)
* Adds option to omit the Ion version marker from text output. (#109)
* Adds option to write tuples as Ion s-expressions via simpleion dump/dumps. (#101)
* Adds support for timestamps with arbitrary precision. (#100)

### 0.4.1 (2019-09-05)
* Remove memoization of events in _IonNature. (#97)
* Updates Tox/README Python Versions (#92)
* Adds support for `tuple` in `_FROM_TYPES` mapping. (#91)
* Sets Travis CI dist to Xenial and removes 2.6/2.7 (#90)

### 0.4.0 (2019-04-19)
* Adds support for pretty-printing text Ion.
* Adds support for Python 3.7.
* Adds simpleion support for structs with duplicate field names.
* Includes tests in the source distribution.

### 0.3.1 (2018-05-15)
* Implements support for both binary and text for loads()/dumps().

### 0.2.0 (2017-05-10)
* Added support for reading text Ion
* Fixed bug affecting writes of large binary Ion values

### 0.1.0 (2016-10-20)
* Added support for writing text Ion
* Added support for reading and writing binary Ion
