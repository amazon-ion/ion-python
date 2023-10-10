### 0.11.1 (2023-10-09)
Drops the support for Python versions older than 3.8.

### 0.11.0 (2023-10-09)
* Refactors the benchmark tool
* Uses specific ion-c version to build ion-python C extension. (#250)
* Adds the simpleEnum class back for backward compatibility. (#246)
* Fixes a bug regarding seeking to previous position instead of 0 after reading IVM
* Replaces Usages of record with NamedTuple (#262)
* Defines IonPyBool as Distinct Type (#258) 
* Fixes __repr__ of Empty Struct (#259)
* Adds a CI/CD workflow to detect performance regression. (#264) 
* Improves the deserialization speed by reducing object initialization overhead. (#284)
* Avoids unnecessary method invocations in IonPyDict's add_item method. (#290)

### 0.10.0 (2023-02-13)
* Drops Python 2 support.
* Adds a benchmark tool with essential options. (#228)
* Adds support for multi-options execution for benchmark CLI. (#235)
* Adds two tests to the skip list for pypy compatibility. (#231)
* Adds support for flexible symbol buffer threshold. (#238)

### 0.9.3 (2022-08-18)
* Fixes a timestamp precision check issue. (#211)
* Changes bytes read size to avoid unicode/UTF-8 conversion issue. (#216)
* Adds compile args for C extension setup. (#206)
* Updates C extension document.

### 0.9.2 (2022-05-5)
* Adds required dependency (#197)
* Adds an installation guide in readme (#195)

### 0.9.1 (2022-01-31)
* Fixes distribution issues to enable C extension by default.

### 0.9.0 (2021-12-10)
* Adds a C extension to speed up simpleion module. (#181)

### 0.8.0 (2021-12-07)
* Makes build/CI work with Python 3.9. (#152)
* Sets the default ion_type when an IonPy* is constructed. (#173)
* Adds friendlier debugging for IonPyDict. (#144)
* Adds detailed error messages for missing ion_type. (#138)
* Adds support of empty string field_name. (#141)

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
