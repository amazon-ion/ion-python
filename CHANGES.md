### 0.12.0 (2024-02-16)
* Avoid strcpy of Field Names in C-Ext Read (#312)
* Fix Bugs with emit_bare_values (#313)
* Simplify ION_DECIMAL to Python Decimal Conversion (#314)
* Reorder simpleion and Refactor Pydoc (#317)
* Emit SymbolTokens for Symbol "bare values" (#318)
* Remove PyObject_HasAttrString condition check. (#319)
* Try to read and convert Ion INT as int 64 (#320)
* Enhance ionc_write_big_int method for optimized handling of large integers. (#321)
* Cache the attribute name that is accessed repeatedly. (#323)
* Added and Plumbed IonPyValueModel Flags (#322)
* Implement Symbol as Text in C-extension (#327)
* Create and Build IonPyList instead of wrapping (#328)
* Implement STRUCT_AS_STD_DICT in C-extension load (#330)
* Build Std Dict without Lists (#331)
* Optimize type check while writing IonStruct. (#333)
* Plumb IonPyValueModel Flags from Benchmark Spec (#332)
* Optimize ionc_write_struct (#334)
* Enhance the benchmark runner to support multiple top level objects use case. (#315)
* Use PyDateTime C API to Write Timestamps (#336)
* Updates the release workflow to build wheels for python 3.11 and MacOS arm64. (#338)
* Optimize Timestamp Reads in ion-c extension (#337)

### 0.11.3 (2023-11-30)
* Enables Windows and Linux's build and test workflow (#304) 
* Build dict for IonPyDict in ioncmodule (#309)

### 0.11.2 (2023-11-16)
* Uses the latest ion-c version to build the C extension. (#299)
* Changes IonPyObjects' constructor to match their parent classes. (#298)
* Adds common examples for the simpleion load/dump APIs. (#294)
* Add support for large decimals with >34 digits. (#293) 

### 0.11.1 (2023-10-09)
* Drops the support for Python versions older than 3.8.

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
