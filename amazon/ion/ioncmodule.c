#include <Python.h>
#include "datetime.h"
#include "_ioncmodule.h"

#define cRETURN RETURN(__location_name__, __line__, __count__++, err)

#define YEAR_PRECISION 0
#define MONTH_PRECISION 1
#define DAY_PRECISION 2
#define MINUTE_PRECISION 3
#define SECOND_PRECISION 4

#define MICROSECOND_DIGITS 6

#define ERR_MSG_MAX_LEN 100
#define FIELD_NAME_MAX_LEN 100

static char _err_msg[ERR_MSG_MAX_LEN];

#define _FAILWITHMSG(x, msg) { err = x; snprintf(_err_msg, ERR_MSG_MAX_LEN, msg); goto fail; }

// Python 2/3 compatibility
#if PY_MAJOR_VERSION >= 3
    #define IONC_BYTES_FORMAT "y#"
    #define IONC_READ_ARGS_FORMAT "OOO"
    #define PyInt_AsSsize_t PyLong_AsSsize_t
    #define PyInt_AsLong PyLong_AsLong
    #define PyInt_FromLong PyLong_FromLong
    #define PyString_AsStringAndSize PyBytes_AsStringAndSize
    #define PyString_Check PyUnicode_Check
    #define PyString_FromStringAndSize PyUnicode_FromStringAndSize
    #define PyString_FromString PyUnicode_FromString
    #define PyInt_Check PyLong_Check
#else
    #define IONC_BYTES_FORMAT "s#"
    #define IONC_READ_ARGS_FORMAT "OOO"
#endif

#if PY_VERSION_HEX < 0x02070000
    #define offset_seconds(x) offset_seconds_26(x)
#endif

static PyObject* _math_module;

static PyObject* _decimal_module;
static PyObject* _decimal_constructor;
static PyObject* _py_timestamp_constructor;
static PyObject* _simpletypes_module;
static PyObject* _ionpynull_cls;
static PyObject* _ionpynull_fromvalue;
static PyObject* _ionpybool_cls;
static PyObject* _ionpybool_fromvalue;
static PyObject* _ionpyint_cls;
static PyObject* _ionpyint_fromvalue;
static PyObject* _ionpyfloat_cls;
static PyObject* _ionpyfloat_fromvalue;
static PyObject* _ionpydecimal_cls;
static PyObject* _ionpydecimal_fromvalue;
static PyObject* _ionpytimestamp_cls;
static PyObject* _ionpytimestamp_fromvalue;
static PyObject* _ionpytext_cls;
static PyObject* _ionpytext_fromvalue;
static PyObject* _ionpysymbol_cls;
static PyObject* _ionpysymbol_fromvalue;
static PyObject* _ionpybytes_cls;
static PyObject* _ionpybytes_fromvalue;
static PyObject* _ionpylist_cls;
static PyObject* _ionpylist_fromvalue;
static PyObject* _ionpydict_cls;
static PyObject* _ionpydict_fromvalue;
static PyObject* _ion_core_module;
static PyObject* _py_ion_type;
static PyObject* py_ion_type_table[14];
static int  c_ion_type_table[14];
static PyObject* _py_timestamp_precision;
static PyObject* py_ion_timestamp_precision_table[7];
static PyObject* _ion_symbols_module;
static PyObject* _py_symboltoken_constructor;
static PyObject* _exception_module;
static PyObject* _ion_exception_cls;
static decContext dec_context;


/******************************************************************************
*       helper functions                                                      *
******************************************************************************/

/*
 *  Gets an attribute as an int. NOTE: defaults to 0 if the attribute is None.
 *
 *  Args:
 *      obj: An object whose attribute will be returned
 *      attr_name: An attribute of the object
 *
 *  Returns:
 *      An attribute as an int
 */
static int int_attr_by_name(PyObject* obj, char* attr_name) {
    PyObject* py_int = PyObject_GetAttrString(obj, attr_name);
    int c_int = 0;
    if (py_int != Py_None) {
        c_int = (int)PyInt_AsSsize_t(py_int);
    }
    Py_DECREF(py_int);
    return c_int;
}

// TODO compare performance of these offset_seconds* methods. The _26 version will work with all versions, so if it is
// as fast, should be used for all.
static int offset_seconds_26(PyObject* timedelta) {
    long microseconds = int_attr_by_name(timedelta, "microseconds");
    long seconds_microseconds = (long)int_attr_by_name(timedelta, "seconds") * 1000000;
    long days_microseconds = (long)int_attr_by_name(timedelta, "days") * 24 * 3600 * 1000000;
    return (microseconds + seconds_microseconds + days_microseconds) / 1000000;
}

static int offset_seconds(PyObject* timedelta) {
    PyObject* py_seconds = PyObject_CallMethod(timedelta, "total_seconds", NULL);
    PyObject* py_seconds_int = PyObject_CallMethod(py_seconds, "__int__", NULL);
    int seconds = (int)PyInt_AsSsize_t(py_seconds_int);
    Py_DECREF(py_seconds);
    Py_DECREF(py_seconds_int);
    return seconds;
}

/*
 *  Returns the ion type of an object as an int
 *
 *  Args:
 *      obj: An object whose type will be returned
 *
 *  Returns:
 *      An int in 'c_ion_type_table' representing an ion type
 */
static int ion_type_from_py(PyObject* obj) {
    PyObject* ion_type = NULL;
    if (PyObject_HasAttrString(obj, "ion_type")) {
        ion_type = PyObject_GetAttrString(obj, "ion_type");
    }
    if (ion_type == NULL) return tid_none_INT;
    int c_type = c_ion_type_table[PyInt_AsSsize_t(ion_type)];
    Py_DECREF(ion_type);
    return c_type;
}

/*
 *  Gets a C string from a python string
 *
 *  Args:
 *      str:  A python string that needs to be converted
 *      out:  A C string converted from 'str'
 *      len_out:  Length of 'out'
 */
static iERR c_string_from_py(PyObject* str, char** out, Py_ssize_t* len_out) {
    iENTER;
#if PY_MAJOR_VERSION >= 3
    *out = PyUnicode_AsUTF8AndSize(str, len_out);
#else
    PyObject *utf8_str;
    if (PyUnicode_Check(str)) {
        utf8_str = PyUnicode_AsUTF8String(str);
    }
    else {
        utf8_str = PyString_AsEncodedObject(str, "utf-8", "strict");
    }
    if (!utf8_str) {
        _FAILWITHMSG(IERR_INVALID_ARG, "Python 2 fails to convert python string to utf8 string.");
    }
    PyString_AsStringAndSize(utf8_str, out, len_out);
    Py_DECREF(utf8_str);
#endif
    iRETURN;
}

/*
 *  Gets an ION_STRING from a python string
 *
 *  Args:
 *      str:  A python string that needs to be converted
 *      out:  An ION_STRING converted from 'str'
 */
static iERR ion_string_from_py(PyObject* str, ION_STRING* out) {
    iENTER;
    char* c_str = NULL;
    Py_ssize_t c_str_len;
    IONCHECK(c_string_from_py(str, &c_str, &c_str_len));
    ION_STRING_INIT(out);
    ion_string_assign_cstr(out, c_str, c_str_len);
    iRETURN;
}

/*
 *  Builds a python string using an ION_STRING
 *
 *  Args:
 *      string_value:  An ION_STRING that needs to be converted
 *
 *  Returns:
 *      A python string
 */
static PyObject* ion_build_py_string(ION_STRING* string_value) {
    // TODO Test non-ASCII compatibility.
    // NOTE: this does a copy, which is good.
    if (!string_value->value) return Py_None;
    return PyUnicode_FromStringAndSize((char*)(string_value->value), string_value->length);
}

/*
 *  Adds an element to a List or struct
 *
 *  Args:
 *      pyContainer:  A container that the element is added to
 *      element:  The element to be added to the container
 *      in_struct:  if the current state is in a struct
 *      field_name:  The field name of the element if it is inside a struct
 */
static void ionc_add_to_container(PyObject* pyContainer, PyObject* element, BOOL in_struct, ION_STRING* field_name) {
    if (in_struct) {
        PyObject_CallMethodObjArgs(
            pyContainer,
            PyString_FromString("add_item"),
            ion_build_py_string(field_name),
            (PyObject*)element,
            NULL
        );
    }
    else {
        PyList_Append(pyContainer, (PyObject*)element);
    }
    Py_XDECREF(element);
}

/*
 *  Converts an ion decimal string to a python-decimal-accept string. NOTE: ion spec uses 'd' in a decimal number
 *  while python decimal object accepts 'e'
 *
 *  Args:
 *      dec_str:  A C string representing a decimal number
 *
 */
static void c_decstr_to_py_decstr(char* dec_str) {
    for (int i = 0; i < strlen(dec_str); i++) {
        if (dec_str[i] == 'd' || dec_str[i] == 'D') {
            dec_str[i] = 'e';
        }
    }
}

/*
 *  Returns a python symbol token using an ION_STRING
 *
 *  Args:
 *      string_value:  An ION_STRING that needs to be converted
 *
 *  Returns:
 *      A python symbol token
 */
static PyObject* ion_string_to_py_symboltoken(ION_STRING* string_value) {
    PyObject* py_string_value;
    PyObject* py_sid;
    if (string_value->value) {
        py_string_value = ion_build_py_string(string_value);
        py_sid = Py_None;
    }
    else {
        py_string_value = Py_None;
        py_sid = PyLong_FromLong(0);
    }
    return PyObject_CallFunctionObjArgs(
        _py_symboltoken_constructor,
        py_string_value,
        py_sid,
        NULL
    );
}


/******************************************************************************
*       Write/Dump APIs                                                       *
******************************************************************************/


/*
 *  Writes a symbol token. NOTE: It can be either a value or an annotation
 *
 *  Args:
 *      writer:  An ion writer
 *      symboltoken: A python symbol token
 *      is_value: Writes a symbol token value if is_value is TRUE, otherwise writes an annotation
 *
 */
static iERR ionc_write_symboltoken(hWRITER writer, PyObject* symboltoken, BOOL is_value) {
    iENTER;
    PyObject* symbol_text = PyObject_GetAttrString(symboltoken, "text");
    if (symbol_text == Py_None) {
        PyObject* py_sid = PyObject_GetAttrString(symboltoken, "sid");
        SID sid = PyInt_AsSsize_t(py_sid);
        if (is_value) {
            err = _ion_writer_write_symbol_id_helper(writer, sid);
        }
        else {
            err = _ion_writer_add_annotation_sid_helper(writer, sid);
        }
        Py_DECREF(py_sid);
    }
    else {
        ION_STRING string_value;
        ion_string_from_py(symbol_text, &string_value);
        if (is_value) {
            err = ion_writer_write_symbol(writer, &string_value);
        }
        else {
            err = ion_writer_add_annotation(writer, &string_value);
        }
    }
    Py_DECREF(symbol_text);
    IONCHECK(err);
    iRETURN;
}

/*
 *  Writes annotations
 *
 *  Args:
 *      writer:  An ion writer
 *      obj: A sequence of ion python annotations
 *
 */
static iERR ionc_write_annotations(hWRITER writer, PyObject* obj) {
    iENTER;
    PyObject* annotations = NULL;
    if (PyObject_HasAttrString(obj, "ion_annotations")) {
        annotations = PyObject_GetAttrString(obj, "ion_annotations");
    }

    if (annotations == NULL || PyObject_Not(annotations)) SUCCEED();

    annotations = PySequence_Fast(annotations, "expected sequence");
    Py_ssize_t len = PySequence_Size(annotations);
    Py_ssize_t i;

    for (i = 0; i < len; i++) {
        PyObject* pyAnnotation = PySequence_Fast_GET_ITEM(annotations, i);
        Py_INCREF(pyAnnotation);
        if (PyUnicode_Check(pyAnnotation)) {
            ION_STRING annotation;
            ion_string_from_py(pyAnnotation, &annotation);
            err = ion_writer_add_annotation(writer, &annotation);
        }
        else if (PyObject_TypeCheck(pyAnnotation, (PyTypeObject*)_py_symboltoken_constructor)){
            err = ionc_write_symboltoken(writer, pyAnnotation, /*is_value=*/FALSE);
        }
        Py_DECREF(pyAnnotation);
        if (err) break;
    }
    Py_XDECREF(annotations);
fail:
    Py_XDECREF(annotations);
    cRETURN;
}

/*
 *  Writes a list or a sexp
 *
 *  Args:
 *      writer:  An ion writer
 *      sequence: An ion python list or sexp
 *      tuple_as_sexp: Decides if a tuple is treated as sexp
 *
 */
static iERR ionc_write_sequence(hWRITER writer, PyObject* sequence, PyObject* tuple_as_sexp) {
    iENTER;
    PyObject* child_obj = NULL;
    sequence = PySequence_Fast(sequence, "expected sequence");
    Py_ssize_t len = PySequence_Size(sequence);
    Py_ssize_t i;

    for (i = 0; i < len; i++) {
        child_obj = PySequence_Fast_GET_ITEM(sequence, i);
        Py_INCREF(child_obj);

        IONCHECK(Py_EnterRecursiveCall(" while writing an Ion sequence"));
        err = ionc_write_value(writer, child_obj, tuple_as_sexp);
        Py_LeaveRecursiveCall();
        IONCHECK(err);

        Py_DECREF(child_obj);
        child_obj = NULL;
    }
fail:
    Py_XDECREF(child_obj);
    Py_DECREF(sequence);
    cRETURN;
}

/*
 *  Writes a struct
 *
 *  Args:
 *      writer:  An ion writer
 *      map: An ion python struct
 *      tuple_as_sexp: Decides if a tuple is treated as sexp
 *
 */
static iERR ionc_write_struct(hWRITER writer, PyObject* map, PyObject* tuple_as_sexp) {
    iENTER;
    PyObject * list = PyMapping_Items(map);
    PyObject * seq = PySequence_Fast(list, "expected a sequence within the map.");
    PyObject * key = NULL, *val = NULL, *child_obj = NULL;
    Py_ssize_t len = PySequence_Size(seq);
    Py_ssize_t i;

    for (i = 0; i < len; i++) {
        child_obj = PySequence_Fast_GET_ITEM(seq, i);
        key = PyTuple_GetItem(child_obj, 0);
        val = PyTuple_GetItem(child_obj, 1);
        Py_INCREF(child_obj);
        Py_INCREF(key);
        Py_INCREF(val);

        if (PyUnicode_Check(key)) {
            ION_STRING field_name;
            ion_string_from_py(key, &field_name);
            IONCHECK(ion_writer_write_field_name(writer, &field_name));
        }
        else if (key == Py_None) {
            // if field_name is None, write symbol $0 instead.
            IONCHECK(_ion_writer_write_field_sid_helper(writer, 0));
        }

        IONCHECK(Py_EnterRecursiveCall(" while writing an Ion struct"));
        err = ionc_write_value(writer, val, tuple_as_sexp);
        Py_LeaveRecursiveCall();
        IONCHECK(err);

        Py_DECREF(child_obj);
        Py_DECREF(key);
        Py_DECREF(val);
        child_obj = NULL;
        key = NULL;
        val = NULL;
    }
    Py_XDECREF(list);
    Py_XDECREF(seq);
fail:
    Py_XDECREF(child_obj);
    Py_XDECREF(key);
    Py_XDECREF(val);
    cRETURN;
}

/*
 *  Writes an int
 *
 *  Args:
 *      writer:  An ion writer
 *      obj: An ion python int
 *
 */
static iERR ionc_write_big_int(hWRITER writer, PyObject *obj) {
    iENTER;

    PyObject* ion_int_base = PyLong_FromLong(II_MASK + 1);
    PyObject* temp = Py_BuildValue("O", obj);
    PyObject * pow_value, *size, *res, *py_digit, *py_remainder = NULL;
    PyObject* py_zero = PyLong_FromLong(0);
    PyObject* py_one = PyLong_FromLong(1);

    ION_INT ion_int_value;
    IONCHECK(ion_int_init(&ion_int_value, NULL));

    // Determine sign
    if (PyObject_RichCompareBool(temp, py_zero, Py_LT) == 1) {
        ion_int_value._signum = -1;
        temp = PyNumber_Negative(temp);
    } else if (PyObject_RichCompareBool(temp, py_zero, Py_GT) == 1) {
        ion_int_value._signum = 1;
    }

    // Determine ion_int digits length
    if (PyObject_RichCompareBool(temp, py_zero, Py_EQ) == 1) {
        size = py_one;
    } else {
        size = PyNumber_Add(
                        PyNumber_Long(PyObject_CallMethodObjArgs(
                                        _math_module, PyUnicode_FromString("log"), temp, ion_int_base, NULL)),
                        py_one);
    }

    int c_size = PyLong_AsLong(size);
    IONCHECK(_ion_int_extend_digits(&ion_int_value, c_size, TRUE));

    int base = c_size;
    while(--base > 0) {
        // Python equivalence:  pow_value = int(pow(2^31, base))
        pow_value = PyNumber_Long(PyNumber_Power(ion_int_base, PyLong_FromLong(base), Py_None));

        if (pow_value == Py_None) {
            // pow(2^31, base) should be calculated correctly.
            _FAILWITHMSG(IERR_INTERNAL_ERROR, "Calculation failure: 2^31.");
        }

        // Python equivalence: digit = temp / pow_value, temp = temp % pow_value
        res = PyNumber_Divmod(temp, pow_value);
        py_digit = PyNumber_Long(PyTuple_GetItem(res, 0));
        py_remainder = PyTuple_GetItem(res, 1);

        Py_INCREF(res);
        Py_INCREF(py_digit);
        Py_INCREF(py_remainder);

        II_DIGIT digit = PyLong_AsLong(py_digit);
        temp = Py_BuildValue("O", py_remainder);

        int index = c_size - base - 1;
        *(ion_int_value._digits + index) = digit;

        Py_DECREF(py_digit);
        Py_DECREF(res);
        Py_DECREF(py_remainder);

        pow_value = NULL;
        py_digit = NULL;
        py_remainder = NULL;
        res = NULL;
    }

    *(ion_int_value._digits + c_size - 1) = PyLong_AsLong(temp);
    IONCHECK(ion_writer_write_ion_int(writer, &ion_int_value));
    Py_XDECREF(py_zero);
    Py_XDECREF(py_one);
    Py_XDECREF(ion_int_base);
    Py_XDECREF(size);
    Py_XDECREF(temp);
    Py_XDECREF(pow_value);
fail:
    Py_XDECREF(res);
    Py_XDECREF(py_digit);
    Py_XDECREF(py_remainder);
    cRETURN;
}

/*
 *  Writes a value
 *
 *  Args:
 *      writer:  An ion writer
 *      obj: An ion python value
 *      tuple_as_sexp: Decides if a tuple is treated as sexp
 *
 */
iERR ionc_write_value(hWRITER writer, PyObject* obj, PyObject* tuple_as_sexp) {
    iENTER;

    if (obj == Py_None) {
        IONCHECK(ion_writer_write_null(writer));
        SUCCEED();
    }
    int ion_type = ion_type_from_py(obj);

    IONCHECK(ionc_write_annotations(writer, obj));

    if (PyUnicode_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_STRING_INT;
        }
        ION_STRING string_value;
        ion_string_from_py(obj, &string_value);
        if (tid_STRING_INT == ion_type) {
            IONCHECK(ion_writer_write_string(writer, &string_value));
        }
        else if (tid_SYMBOL_INT == ion_type) {
            IONCHECK(ion_writer_write_symbol(writer, &string_value));
        }
        else {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found text; expected STRING or SYMBOL Ion type.");
        }
    }
    else if (PyBool_Check(obj)) { // NOTE: this must precede the INT block because python bools are ints.
        if (ion_type == tid_none_INT) {
            ion_type = tid_BOOL_INT;
        }
        if (tid_BOOL_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found bool; expected BOOL Ion type.");
        }
        BOOL bool_value;
        if (obj == Py_True) {
            bool_value = TRUE;
        }
        else {
            bool_value = FALSE;
        }
        IONCHECK(ion_writer_write_bool(writer, bool_value));
    }
    else if (PyInt_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_INT_INT;
        }
        if (tid_INT_INT == ion_type) {
            IONCHECK(ionc_write_big_int(writer, obj));
        }
        else if (tid_BOOL_INT == ion_type) {
            IONCHECK(ion_writer_write_bool(writer, PyInt_AsSsize_t(obj)));
        }
        else {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found int; expected INT or BOOL Ion type.");
        }
    }
    else if (PyFloat_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_FLOAT_INT;
        }
        if (tid_FLOAT_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found float; expected FLOAT Ion type.");
        }
        IONCHECK(ion_writer_write_double(writer, PyFloat_AsDouble(obj)));
    }
    else if (PyObject_TypeCheck(obj, (PyTypeObject*)_ionpynull_cls)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_NULL_INT;
        }
        IONCHECK(ion_writer_write_typed_null(writer, (ION_TYPE)ion_type));
    }
    else if (PyObject_TypeCheck(obj, (PyTypeObject*)_decimal_constructor)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_DECIMAL_INT;
        }
        if (tid_DECIMAL_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found Decimal; expected DECIMAL Ion type.");
        }

        ION_DECIMAL decimal_value;
        decQuad decQuad_value;
        decNumber decNumber_value;
        decimal_value.type = ION_DECIMAL_TYPE_QUAD;

        // Get decimal tuple from the python object.
        PyObject* py_decimal_tuple;
        py_decimal_tuple = PyObject_CallMethod(obj, "as_tuple", NULL);

        // Determine exponent.
        PyObject* py_exponent = PyObject_GetAttrString(py_decimal_tuple, "exponent");
        // Ion specification doesn't accept following values: Nan, Inf and -Inf.
        // py_exponent is 'n' for NaN and 'F' for +/- Inf.
        if (!PyLong_Check(py_exponent)) {
            Py_DECREF(py_exponent);
            _FAILWITHMSG(IERR_INVALID_ARG, "Ion decimal doesn't support Nan and Inf.");
        }
        decNumber_value.exponent = PyLong_AsLong(py_exponent);
        Py_DECREF(py_exponent);

        // Determine digits.
        PyObject* py_digits = PyObject_GetAttrString(py_decimal_tuple, "digits");
        int32_t digits_len = PyLong_AsLong(PyObject_CallMethod(py_digits, "__len__", NULL));
        decNumber_value.digits = digits_len;
        if (digits_len > DECNUMDIGITS) {
            Py_DECREF(py_digits);
            _FAILWITHMSG(IERR_NUMERIC_OVERFLOW,
                         "Too much decimal digits, please try again with pure python implementation.");
        }

        // Determine sign. 1=negative, 0=positive or zero.
        PyObject* py_sign = PyObject_GetAttrString(py_decimal_tuple, "sign");
        decNumber_value.bits = 0;
        if (PyLong_AsLong(py_sign) == 1) {
            decNumber_value.bits = DECNEG;
        }

        // Determine lsu.
        int lsu_array[digits_len];
        for (int i=0; i<digits_len; i++) {
            PyObject* digit = PyTuple_GetItem(py_digits, i);
            Py_INCREF(digit);
            lsu_array[i] = PyLong_AsLong(digit);
            Py_DECREF(digit);
        }
        Py_XDECREF(py_digits);

        int index = 0;
        int count = digits_len - 1;
        while (count >= 0) {
            decNumberUnit per_digit = 0;
            int op_count = count + 1 < DECDPUN ? count + 1 : DECDPUN;
            for (int i = 0; i < op_count; i++) {
                per_digit += pow(10, i) * lsu_array[count--];
            }
            decNumber_value.lsu[index++] = per_digit;
        }

        decQuadFromNumber(&decQuad_value, &decNumber_value, &dec_context);
        decimal_value.value.quad_value = decQuad_value;

        Py_DECREF(py_decimal_tuple);
        Py_DECREF(py_sign);

        IONCHECK(ion_writer_write_ion_decimal(writer, &decimal_value));
    }
    else if (PyBytes_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_BLOB_INT;
        }
        char* bytes = NULL;
        Py_ssize_t len;
        IONCHECK(PyString_AsStringAndSize(obj, &bytes, &len));
        if (ion_type == tid_BLOB_INT) {
            IONCHECK(ion_writer_write_blob(writer, (BYTE*)bytes, len));
        }
        else if (ion_type == tid_CLOB_INT) {
            IONCHECK(ion_writer_write_clob(writer, (BYTE*)bytes, len));
        }
        else {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found binary data; expected BLOB or CLOB Ion type.");
        }
    }
    else if (PyDateTime_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_TIMESTAMP_INT;
        }
        if (tid_TIMESTAMP_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found datetime; expected TIMESTAMP Ion type.");
        }
        ION_TIMESTAMP timestamp_value;
        int year, month, day, hour, minute, second;
        short precision, fractional_precision;

        if (PyObject_HasAttrString(obj, "precision")) {
            // This is a Timestamp.
            precision = int_attr_by_name(obj, "precision");
            fractional_precision = int_attr_by_name(obj, "fractional_precision");
        }
        else {
            // This is a naive datetime. It always has maximum precision.
            precision = SECOND_PRECISION;
            fractional_precision = MICROSECOND_DIGITS;
        }

        year = int_attr_by_name(obj, "year");
        if (precision == SECOND_PRECISION) {
            month = int_attr_by_name(obj, "month");
            day = int_attr_by_name(obj, "day");
            hour = int_attr_by_name(obj, "hour");
            minute = int_attr_by_name(obj, "minute");
            second = int_attr_by_name(obj, "second");
            int microsecond = int_attr_by_name(obj, "microsecond");
            if (fractional_precision > 0) {
                decQuad fraction;
                decNumber helper, dec_number_precision;
                decQuadFromInt32(&fraction, (int32_t)microsecond);
                decQuad tmp;
                decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, -MICROSECOND_DIGITS), &dec_context);
                decQuadToNumber(&fraction, &helper);
                decContextClearStatus(&dec_context, DEC_Inexact); // TODO consider saving, clearing, and resetting the status flag
                decNumberRescale(&helper, &helper, decNumberFromInt32(&dec_number_precision, -fractional_precision), &dec_context);
                if (decContextTestStatus(&dec_context, DEC_Inexact)) {
                    // This means the fractional component is not [0, 1) or has more than microsecond precision.
                    decContextClearStatus(&dec_context, DEC_Inexact);
                    _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Requested fractional timestamp precision results in data loss.");
                }
                decQuadFromNumber(&fraction, &helper, &dec_context);
                IONCHECK(ion_timestamp_for_fraction(&timestamp_value, year, month, day, hour, minute, second, &fraction, &dec_context));
            }
            else if (microsecond > 0) {
                _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Not enough fractional precision for timestamp.");
            }
            else {
                IONCHECK(ion_timestamp_for_second(&timestamp_value, year, month, day, hour, minute, second));
            }
        }
        else if (precision == MINUTE_PRECISION) {
            month = int_attr_by_name(obj, "month");
            day = int_attr_by_name(obj, "day");
            hour = int_attr_by_name(obj, "hour");
            minute = int_attr_by_name(obj, "minute");
            IONCHECK(ion_timestamp_for_minute(&timestamp_value, year, month, day, hour, minute));
        }
        else if (precision == DAY_PRECISION) {
            month = int_attr_by_name(obj, "month");
            day = int_attr_by_name(obj, "day");
            IONCHECK(ion_timestamp_for_day(&timestamp_value, year, month, day));
        }
        else if (precision == MONTH_PRECISION) {
            month = int_attr_by_name(obj, "month");
            IONCHECK(ion_timestamp_for_month(&timestamp_value, year, month));
        }
        else if (precision == YEAR_PRECISION) {
            IONCHECK(ion_timestamp_for_year(&timestamp_value, year));
        }
        else {
            _FAILWITHMSG(IERR_INVALID_STATE, "Invalid timestamp precision.");
        }

        if (precision >= MINUTE_PRECISION) {
            PyObject* offset_timedelta = PyObject_CallMethod(obj, "utcoffset", NULL);
            if (offset_timedelta != Py_None) {
                err = ion_timestamp_set_local_offset(&timestamp_value, offset_seconds(offset_timedelta) / 60);
            }
            Py_DECREF(offset_timedelta);
            IONCHECK(err);
        }

        IONCHECK(ion_writer_write_timestamp(writer, &timestamp_value));
    }
    else if (PyDict_Check(obj) || PyObject_IsInstance(obj, _ionpydict_cls)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_STRUCT_INT;
        }
        if (tid_STRUCT_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found dict; expected STRUCT Ion type.");
        }
        IONCHECK(ion_writer_start_container(writer, (ION_TYPE)ion_type));
        IONCHECK(ionc_write_struct(writer, obj, tuple_as_sexp));
        IONCHECK(ion_writer_finish_container(writer));
    }
    else if (PyObject_TypeCheck(obj, (PyTypeObject*)_py_symboltoken_constructor)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_SYMBOL_INT;
        }
        if (tid_SYMBOL_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found SymbolToken; expected SYMBOL Ion type.");
        }
        IONCHECK(ionc_write_symboltoken(writer, obj, /*is_value=*/TRUE));
    }
    else if (PyList_Check(obj) || PyTuple_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_LIST_INT;
        }
        if (tid_LIST_INT != ion_type && tid_SEXP_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found sequence; expected LIST or SEXP Ion type.");
        }

        if (PyTuple_Check(obj) && PyObject_IsTrue(tuple_as_sexp)) {
            IONCHECK(ion_writer_start_container(writer, (ION_TYPE)tid_SEXP_INT));
        }
        else {
            IONCHECK(ion_writer_start_container(writer, (ION_TYPE)ion_type));
        }
        IONCHECK(ionc_write_sequence(writer, obj, tuple_as_sexp));
        IONCHECK(ion_writer_finish_container(writer));
    }
    else {
        _FAILWITHMSG(IERR_INVALID_STATE, "Cannot dump arbitrary object types.");
    }
    iRETURN;
}

/*
 *  A helper function to write a sequence of ion values
 *
 *  Args:
 *      writer:  An ion writer
 *      objs:  A sequence of ion values
 *      tuple_as_sexp: Decides if a tuple is treated as sexp
 *      int i: The i-th value of 'objs' that is going to be written
 *
 */
static iERR _ionc_write(hWRITER writer, PyObject* objs, PyObject* tuple_as_sexp, int i) {
    iENTER;
    PyObject* pyObj = PySequence_Fast_GET_ITEM(objs, i);
    Py_INCREF(pyObj);
    err = ionc_write_value(writer, pyObj, tuple_as_sexp);
    Py_DECREF(pyObj);
    iRETURN;
}

/*
 *  Entry point of write/dump functions
 */
static PyObject* ionc_write(PyObject *self, PyObject *args, PyObject *kwds) {
    iENTER;
    PyObject *obj, *binary, *sequence_as_stream, *tuple_as_sexp;
    ION_STREAM  *ion_stream = NULL;
    BYTE* buf = NULL;
    static char *kwlist[] = {"obj", "binary", "sequence_as_stream", "tuple_as_sexp", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OOOO", kwlist, &obj, &binary, &sequence_as_stream, &tuple_as_sexp)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    Py_INCREF(obj);
    Py_INCREF(binary);
    Py_INCREF(sequence_as_stream);
    Py_INCREF(tuple_as_sexp);
    IONCHECK(ion_stream_open_memory_only(&ion_stream));

    //Create a writer here to avoid re-create writers for each element when sequence_as_stream is True.
    hWRITER writer;
    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.output_as_binary = PyObject_IsTrue(binary);
    IONCHECK(ion_writer_open(&writer, ion_stream, &options));

    if (sequence_as_stream == Py_True && (PyList_Check(obj) || PyTuple_Check(obj))) {
        PyObject* objs = PySequence_Fast(obj, "expected sequence");
        Py_ssize_t len = PySequence_Size(objs);
        Py_ssize_t i;
        BOOL last_element = FALSE;

        for (i = 0; i < len; i++) {
            err = _ionc_write(writer, objs, tuple_as_sexp, i);
            if (err) break;
        }

        Py_DECREF(objs);
        IONCHECK(err);
    }
    else {
        IONCHECK(ionc_write_value(writer, obj, tuple_as_sexp));
    }
    IONCHECK(ion_writer_close(writer));

    POSITION len = ion_stream_get_position(ion_stream);
    IONCHECK(ion_stream_seek(ion_stream, 0));
    // TODO if len > max int32, need to return more than one page...
    buf = (BYTE*)(PyMem_Malloc((size_t)len));
    SIZE bytes_read;
    IONCHECK(ion_stream_read(ion_stream, buf, (SIZE)len, &bytes_read));

    IONCHECK(ion_stream_close(ion_stream));
    if (bytes_read != (SIZE)len) {
        FAILWITH(IERR_EOF);
    }
    // TODO Py_BuildValue copies all bytes... Can a memoryview over the original bytes be returned, avoiding the copy?
    PyObject* written = Py_BuildValue(IONC_BYTES_FORMAT, (char*)buf, bytes_read);
    PyMem_Free(buf);
    Py_DECREF(obj);
    Py_DECREF(binary);
    Py_DECREF(sequence_as_stream);
    Py_DECREF(tuple_as_sexp);
    return written;
fail:
    PyMem_Free(buf);
    Py_DECREF(obj);
    Py_DECREF(binary);
    Py_DECREF(sequence_as_stream);
    Py_DECREF(tuple_as_sexp);
    PyObject* exception = NULL;
    if (err == IERR_INVALID_STATE) {
        exception = PyErr_Format(PyExc_TypeError, "%s", _err_msg);
    }
    else {
        exception = PyErr_Format(_ion_exception_cls, "%s %s", ion_error_to_str(err), _err_msg);
    }

    _err_msg[0] = '\0';
    return exception;
}


/******************************************************************************
*       Read/Load APIs                                                        *
******************************************************************************/


static PyObject* ionc_get_timestamp_precision(int precision) {
    int precision_index = -1;
    while (precision) {
        precision_index++;
        precision = precision >> 1;
    }
    return py_ion_timestamp_precision_table[precision_index];
}

static iERR ionc_read_timestamp(hREADER hreader, PyObject** timestamp_out) {
    iENTER;
    ION_TIMESTAMP timestamp_value;
    PyObject* timestamp_args = NULL;
    IONCHECK(ion_reader_read_timestamp(hreader, &timestamp_value));
    int precision;
    IONCHECK(ion_timestamp_get_precision(&timestamp_value, &precision));
    if (precision < ION_TS_YEAR) {
        _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Found a timestamp with less than year precision.");
    }
    timestamp_args = PyDict_New();
    PyObject* py_precision = ionc_get_timestamp_precision(precision);
    PyDict_SetItemString(timestamp_args, "precision", py_precision);
    BOOL has_local_offset;
    IONCHECK(ion_timestamp_has_local_offset(&timestamp_value, &has_local_offset));

    if (has_local_offset) {
        int off_minutes, off_hours;
        IONCHECK(ion_timestamp_get_local_offset(&timestamp_value, &off_minutes));
        off_hours = off_minutes / 60;
        off_minutes = off_minutes % 60;
        // Bounds checking is performed in python.
        PyDict_SetItemString(timestamp_args, "off_hours", PyInt_FromLong(off_hours));
        PyDict_SetItemString(timestamp_args, "off_minutes", PyInt_FromLong(off_minutes));
    }

    switch (precision) {
        case ION_TS_FRAC:
        {
            decQuad fraction = timestamp_value.fraction;
            int32_t fractional_precision = decQuadGetExponent(&fraction);
            if (fractional_precision > 0) {
                _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Timestamp fractional precision cannot be a positive number.");
            }
            fractional_precision = fractional_precision * -1;

            decQuad tmp;
            decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, MICROSECOND_DIGITS), &dec_context);
            int32_t microsecond = decQuadToInt32Exact(&fraction, &dec_context, DEC_ROUND_DOWN);
            if (fractional_precision > MICROSECOND_DIGITS) {
                // Python only supports up to microsecond precision
                fractional_precision = MICROSECOND_DIGITS;
            }

            if (decContextTestStatus(&dec_context, DEC_Inexact)) {
                // This means the fractional component is not [0, 1) or has more than microsecond precision.
                decContextClearStatus(&dec_context, DEC_Inexact);
            }
            PyDict_SetItemString(timestamp_args, "fractional_precision", PyInt_FromLong(fractional_precision));
            PyDict_SetItemString(timestamp_args, "microsecond", PyInt_FromLong(microsecond));
        }
        case ION_TS_SEC:
            PyDict_SetItemString(timestamp_args, "second", PyInt_FromLong(timestamp_value.seconds));
        case ION_TS_MIN:
            PyDict_SetItemString(timestamp_args, "minute", PyInt_FromLong(timestamp_value.minutes));
            PyDict_SetItemString(timestamp_args, "hour", PyInt_FromLong(timestamp_value.hours));
        case ION_TS_DAY:
            PyDict_SetItemString(timestamp_args, "day", PyInt_FromLong(timestamp_value.day));
        case ION_TS_MONTH:
            PyDict_SetItemString(timestamp_args, "month", PyInt_FromLong(timestamp_value.month));
        case ION_TS_YEAR:
            PyDict_SetItemString(timestamp_args, "year", PyInt_FromLong(timestamp_value.year));
            break;
        }
    *timestamp_out = PyObject_Call(_py_timestamp_constructor, PyTuple_New(0), timestamp_args);

fail:
    Py_XDECREF(timestamp_args);
    cRETURN;
}

/*
 *  Reads values from a container
 *
 *  Args:
 *      hreader:  An ion reader
 *      container:  A container that elements are read from
 *      is_struct:  If the container is an ion struct
 *      emit_bare_values: Decides if the value needs to be wrapped
 *
 */
static iERR ionc_read_into_container(hREADER hreader, PyObject* container, BOOL is_struct, BOOL emit_bare_values) {
    iENTER;
    IONCHECK(ion_reader_step_in(hreader));
    IONCHECK(Py_EnterRecursiveCall(" while reading an Ion container"));
    err = ionc_read_all(hreader, container, is_struct, emit_bare_values);
    Py_LeaveRecursiveCall();
    IONCHECK(err);
    IONCHECK(ion_reader_step_out(hreader));
    iRETURN;
}

/*
 *  Helper function for 'ionc_read_all', reads an ion value
 *
 *  Args:
 *      hreader:  An ion reader
 *      ION_TYPE:  The ion type of the reading value as an int
 *      in_struct:  If the current state is in a struct
 *      emit_bare_values_global: Decides if the value needs to be wrapped
 *
 */
iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container, BOOL in_struct, BOOL emit_bare_values_global) {
    iENTER;

    BOOL        emit_bare_values = emit_bare_values_global;
    BOOL        is_null;
    ION_STRING  field_name;
    SIZE        annotation_count;
    PyObject*   py_annotations = NULL;
    PyObject*   py_value = NULL;
    PyObject*   ion_nature_constructor = NULL;

    char    field_name_value[FIELD_NAME_MAX_LEN];
    int     field_name_len = 0;
    BOOL    None_field_name = TRUE;

    if (in_struct) {
        IONCHECK(ion_reader_get_field_name(hreader, &field_name));
        field_name_len = field_name.length;
        if (field_name.value != NULL) {
            None_field_name = FALSE;
            strcpy(field_name_value, field_name.value);
        }
    }

    IONCHECK(ion_reader_get_annotation_count(hreader, &annotation_count));
    if (annotation_count > 0) {
        emit_bare_values = FALSE;
        ION_STRING* annotations = (ION_STRING*)PyMem_Malloc(annotation_count * sizeof(ION_STRING));
        err = ion_reader_get_annotations(hreader, annotations, annotation_count, &annotation_count);
        if (err) {
            PyMem_Free(annotations);
            IONCHECK(err);
        }
        py_annotations = PyTuple_New(annotation_count);
        int i;
        for (i = 0; i < annotation_count; i++) {
            PyTuple_SetItem(py_annotations, i, ion_string_to_py_symboltoken(&annotations[i]));
        }
        PyMem_Free(annotations);
    }
    ION_TYPE original_t = t;
    IONCHECK(ion_reader_is_null(hreader, &is_null));
    if (is_null) {
        t = tid_NULL;
    }
    int ion_type = ION_TYPE_INT(t);

    switch (ion_type) {
        case tid_EOF_INT:
            SUCCEED();
        case tid_NULL_INT:
        {
            ION_TYPE null_type;
            // Hack for ion-c issue https://github.com/amzn/ion-c/issues/223
            if (original_t != tid_SYMBOL_INT) {
                IONCHECK(ion_reader_read_null(hreader, &null_type));
            }
            else {
                null_type = tid_SYMBOL_INT;
            }

            ion_type = ION_TYPE_INT(null_type);
            py_value = Py_BuildValue(""); // INCREFs and returns Python None.
            emit_bare_values = emit_bare_values && (ion_type == tid_NULL_INT);
            ion_nature_constructor = _ionpynull_fromvalue;
            break;
        }
        case tid_BOOL_INT:
        {
            BOOL bool_value;
            IONCHECK(ion_reader_read_bool(hreader, &bool_value));
            py_value = PyBool_FromLong(bool_value);
            ion_nature_constructor = _ionpybool_fromvalue;
            break;
        }
        case tid_INT_INT:
        {
            // TODO add ion-c API to return an int64 if possible, or an ION_INT if necessary
            ION_INT ion_int_value;
            IONCHECK(ion_int_init(&ion_int_value, hreader));
            IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));

            PyObject* ion_int_base = PyLong_FromLong(II_MASK + 1);
            int c_size = ion_int_value._len;
            py_value = PyLong_FromLong(0);

            int i = 0;
            for (i; i < c_size; i++) {
                int base = c_size - 1 - i;
                // Python equivalence:  pow_value = int(pow(2^31, base))
                PyObject* pow_value = PyNumber_Long(PyNumber_Power(ion_int_base, PyLong_FromLong(base), Py_None));

                // Python equivalence: py_value += pow_value * _digits[i]
                py_value = PyNumber_Add(py_value, PyNumber_Multiply(pow_value, PyLong_FromLong(*(ion_int_value._digits + i))));

                Py_DECREF(pow_value);
            }

            if (ion_int_value._signum < 0) {
                py_value = PyNumber_Negative(py_value);
            }

            ion_nature_constructor = _ionpyint_fromvalue;
            Py_DECREF(ion_int_base);
            break;
        }
        case tid_FLOAT_INT:
        {
            double double_value;
            IONCHECK(ion_reader_read_double(hreader, &double_value));
            py_value = Py_BuildValue("d", double_value);
            ion_nature_constructor = _ionpyfloat_fromvalue;
            break;
        }
        case tid_DECIMAL_INT:
        {
            ION_DECIMAL decimal_value;
            IONCHECK(ion_reader_read_ion_decimal(hreader, &decimal_value));
            decNumber read_number;
            decQuad read_quad;

            // Determine ion decimal type.
            if (decimal_value.type == ION_DECIMAL_TYPE_QUAD) {
                read_quad = decimal_value.value.quad_value;
                decQuadToNumber(&read_quad, &read_number);
            } else if (decimal_value.type == ION_DECIMAL_TYPE_NUMBER
                        || decimal_value.type == ION_DECIMAL_TYPE_NUMBER_OWNED) {
                read_number = *(decimal_value.value.num_value);
            } else {
                _FAILWITHMSG(IERR_INVALID_ARG, "Unknown type of Ion Decimal.")
            }

            int read_number_digits = read_number.digits;
            int read_number_bits =  read_number.bits;
            int read_number_exponent = read_number.exponent;
            int sign = ((DECNEG & read_number.bits) == DECNEG) ? 1 : 0;
            // No need to release below PyObject* since PyTuple "steals" its reference.
            PyObject* digits_tuple = PyTuple_New(read_number_digits);

            // Returns a decimal tuple to avoid losing precision.
            // Decimal tuple format: (sign, (digits tuple), exponent).
            py_value = PyTuple_New(3);
            PyTuple_SetItem(py_value, 0, PyLong_FromLong(sign));
            PyTuple_SetItem(py_value, 1, digits_tuple);
            PyTuple_SetItem(py_value, 2, PyLong_FromLong(read_number_exponent));

            int count = (read_number_digits + DECDPUN - 1) / DECDPUN;
            int index = 0;
            int remainder = read_number_digits % DECDPUN;

            // "i" represents the index of a decNumberUnit in lsu array.
            for (int i = count - 1; i >= 0; i--) {
                int cur_digits = read_number.lsu[i];
                int end_index = (i == count - 1 && remainder > 0) ? remainder : DECDPUN;

                // "j" represents the j-th digit of a decNumberUnit we are going to convert.
                for (int j = 0; j < end_index; j++) {
                    int cur_digit = cur_digits % 10;
                    cur_digits = cur_digits / 10;
                    int write_index = (i == count - 1 && remainder > 0)
                                        ? remainder - index - 1 : index + DECDPUN - 2 * j - 1;
                    PyTuple_SetItem(digits_tuple, write_index, PyLong_FromLong(cur_digit));
                    index++;
                }
            }

            ion_nature_constructor = _ionpydecimal_fromvalue;
            break;
        }
        case tid_TIMESTAMP_INT:
        {
            IONCHECK(ionc_read_timestamp(hreader, &py_value));
            ion_nature_constructor = _ionpytimestamp_fromvalue;
            break;
        }
        case tid_SYMBOL_INT:
        {
            emit_bare_values = FALSE; // Symbol values must always be emitted as IonNature because of ambiguity with string.
            ION_STRING string_value;
            IONCHECK(ion_reader_read_string(hreader, &string_value));
            ion_nature_constructor = _ionpysymbol_fromvalue;
            py_value = ion_string_to_py_symboltoken(&string_value);
            break;
        }
        case tid_STRING_INT:
        {
            ION_STRING string_value;
            IONCHECK(ion_reader_read_string(hreader, &string_value));
            py_value = ion_build_py_string(&string_value);
            ion_nature_constructor = _ionpytext_fromvalue;
            break;
        }
        case tid_CLOB_INT:
        {
            emit_bare_values = FALSE; // Clob values must always be emitted as IonNature because of ambiguity with blob.
            // intentional fall-through
        }
        case tid_BLOB_INT:
        {
            SIZE length, bytes_read;
            char *buf = NULL;
            IONCHECK(ion_reader_get_lob_size(hreader, &length));
            if (length) {
                buf = (char*)PyMem_Malloc((size_t)length);
                err = ion_reader_read_lob_bytes(hreader, (BYTE *)buf, length, &bytes_read);
                if (err) {
                    PyMem_Free(buf);
                    IONCHECK(err);
                }
                if (length != bytes_read) {
                    PyMem_Free(buf);
                    FAILWITH(IERR_EOF);
                }
            }
            else {
                buf = "";
            }
            py_value = Py_BuildValue(IONC_BYTES_FORMAT, buf, length);
            if (length) {
                PyMem_Free(buf);
            }
            ion_nature_constructor = _ionpybytes_fromvalue;
            break;
        }
        case tid_STRUCT_INT:
            ion_nature_constructor = _ionpydict_fromvalue;
            //Init a IonPyDict
            py_value = PyObject_CallFunctionObjArgs(
                ion_nature_constructor,
                py_ion_type_table[ion_type >> 8],
                PyDict_New(),
                py_annotations,
                NULL
            );

            IONCHECK(ionc_read_into_container(hreader, py_value, /*is_struct=*/TRUE, emit_bare_values));
            emit_bare_values = TRUE;
            break;
        case tid_SEXP_INT:
        {
            emit_bare_values = FALSE; // Sexp values must always be emitted as IonNature because of ambiguity with list.
            // intentional fall-through
        }
        case tid_LIST_INT:
            py_value = PyList_New(0);
            IONCHECK(ionc_read_into_container(hreader, py_value, /*is_struct=*/FALSE, emit_bare_values));
            ion_nature_constructor = _ionpylist_fromvalue;
            break;
        case tid_DATAGRAM_INT:
        default:
            FAILWITH(IERR_INVALID_STATE);
        }
    if (!emit_bare_values) {
        py_value = PyObject_CallFunctionObjArgs(
            ion_nature_constructor,
            py_ion_type_table[ion_type >> 8],
            py_value,
            py_annotations,
            NULL
        );
    }

    if (in_struct && !None_field_name) {
        ION_STRING_INIT(&field_name);
        ion_string_assign_cstr(&field_name, field_name_value, field_name_len);
    }
    ionc_add_to_container(container, py_value, in_struct, &field_name);

fail:
    if (err) {
        Py_XDECREF(py_annotations);
        Py_XDECREF(py_value);
    }
    cRETURN;
}

/*
 *  Reads ion values
 *
 *  Args:
 *      hreader:  An ion reader
 *      container:  A container that elements are read from
 *      in_struct:  If the current state is in a struct
 *      emit_bare_values: Decides if the value needs to be wrapped
 *
 */
iERR ionc_read_all(hREADER hreader, PyObject* container, BOOL in_struct, BOOL emit_bare_values) {
    iENTER;
    ION_TYPE t;
    for (;;) {
        IONCHECK(ion_reader_next(hreader, &t));
        if (t == tid_EOF) {
            assert(t == tid_EOF && "next() at end");
            break;
        }
        IONCHECK(ionc_read_value(hreader, t, container, in_struct, emit_bare_values));
    }
    iRETURN;
}

/*
 *  Entry point of read/load functions
 */
PyObject* ionc_read(PyObject* self, PyObject *args, PyObject *kwds) {
    iENTER;
    hREADER      reader;
    long         size;
    char     *buffer = NULL;
    PyObject *py_buffer = NULL;
    PyObject *top_level_container = NULL;
    PyObject *single_value, *emit_bare_values;
    static char *kwlist[] = {"data", "single_value", "emit_bare_values", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, IONC_READ_ARGS_FORMAT, kwlist, &py_buffer, &single_value, &emit_bare_values)) {
        FAILWITH(IERR_INVALID_ARG);
    }

    PyString_AsStringAndSize(py_buffer, &buffer, &size);
    // TODO what if size is larger than SIZE ?
    ION_READER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.decimal_context = &dec_context;
    IONCHECK(ion_reader_open_buffer(&reader, (BYTE*)buffer, (SIZE)size, &options)); // NULL represents default reader options

    top_level_container = PyList_New(0);
    IONCHECK(ionc_read_all(reader, top_level_container, FALSE, emit_bare_values == Py_True));
    IONCHECK(ion_reader_close(reader));
    if (single_value == Py_True) {
        Py_ssize_t len = PyList_Size(top_level_container);
        if (len != 1) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Single_value option specified; expected a single value.")
        }
        PyObject* value = PyList_GetItem(top_level_container, 0);
        Py_XINCREF(value);
        Py_DECREF(top_level_container);
        return value;
    }

    return top_level_container;
fail:
    Py_XDECREF(top_level_container);
    PyObject* exception = PyErr_Format(_ion_exception_cls, "%s %s", ion_error_to_str(err), _err_msg);
    _err_msg[0] = '\0';
    return exception;
}


/******************************************************************************
*       Initial module                                                        *
******************************************************************************/


static char ioncmodule_docs[] =
    "C extension module for ion-c.\n";

static PyMethodDef ioncmodule_funcs[] = {
    {"ionc_write", (PyCFunction)ionc_write, METH_VARARGS | METH_KEYWORDS, ioncmodule_docs},
    {"ionc_read", (PyCFunction)ionc_read, METH_VARARGS | METH_KEYWORDS, ioncmodule_docs},
    {NULL}
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "ionc",             /* m_name */
    ioncmodule_docs,    /* m_doc */
    -1,                 /* m_size */
    ioncmodule_funcs,   /* m_methods */
    NULL,               /* m_reload */
    NULL,               /* m_traverse */
    NULL,               /* m_clear*/
    NULL,               /* m_free */
};
#endif

PyObject* ionc_init_module(void) {
    PyDateTime_IMPORT;
    PyObject* m;

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("ionc", ioncmodule_funcs,"Extension module example!");
#endif

    // TODO is there a destructor for modules? These should be decreffed there
     _math_module               = PyImport_ImportModule("math");

    _decimal_module             = PyImport_ImportModule("decimal");
    _decimal_constructor        = PyObject_GetAttrString(_decimal_module, "Decimal");
    _simpletypes_module         = PyImport_ImportModule("amazon.ion.simple_types");

    _ionpynull_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyNull");
    _ionpynull_fromvalue        = PyObject_GetAttrString(_ionpynull_cls, "from_value");
    _ionpybool_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyBool");
    _ionpybool_fromvalue        = PyObject_GetAttrString(_ionpybool_cls, "from_value");
    _ionpyint_cls               = PyObject_GetAttrString(_simpletypes_module, "IonPyInt");
    _ionpyint_fromvalue         = PyObject_GetAttrString(_ionpyint_cls, "from_value");
    _ionpyfloat_cls             = PyObject_GetAttrString(_simpletypes_module, "IonPyFloat");
    _ionpyfloat_fromvalue       = PyObject_GetAttrString(_ionpyfloat_cls, "from_value");
    _ionpydecimal_cls           = PyObject_GetAttrString(_simpletypes_module, "IonPyDecimal");
    _ionpydecimal_fromvalue     = PyObject_GetAttrString(_ionpydecimal_cls, "from_value");
    _ionpytimestamp_cls         = PyObject_GetAttrString(_simpletypes_module, "IonPyTimestamp");
    _ionpytimestamp_fromvalue   = PyObject_GetAttrString(_ionpytimestamp_cls, "from_value");
    _ionpybytes_cls             = PyObject_GetAttrString(_simpletypes_module, "IonPyBytes");
    _ionpybytes_fromvalue       = PyObject_GetAttrString(_ionpybytes_cls, "from_value");
    _ionpytext_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyText");
    _ionpytext_fromvalue        = PyObject_GetAttrString(_ionpytext_cls, "from_value");
    _ionpysymbol_cls            = PyObject_GetAttrString(_simpletypes_module, "IonPySymbol");
    _ionpysymbol_fromvalue      = PyObject_GetAttrString(_ionpysymbol_cls, "from_value");
    _ionpylist_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyList");
    _ionpylist_fromvalue        = PyObject_GetAttrString(_ionpylist_cls, "from_value");
    _ionpydict_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyDict");
    _ionpydict_fromvalue        = PyObject_GetAttrString(_ionpydict_cls, "from_value");

    _ion_core_module            = PyImport_ImportModule("amazon.ion.core");
    _py_timestamp_precision     = PyObject_GetAttrString(_ion_core_module, "TimestampPrecision");
    _py_timestamp_constructor   = PyObject_GetAttrString(_ion_core_module, "timestamp");
    _py_ion_type                = PyObject_GetAttrString(_ion_core_module, "IonType");

    _ion_symbols_module         = PyImport_ImportModule("amazon.ion.symbols");
    _py_symboltoken_constructor = PyObject_GetAttrString(_ion_symbols_module, "SymbolToken");

    py_ion_type_table[0x0] = PyObject_GetAttrString(_py_ion_type, "NULL");
    py_ion_type_table[0x1] = PyObject_GetAttrString(_py_ion_type, "BOOL");
    py_ion_type_table[0x2] = PyObject_GetAttrString(_py_ion_type, "INT");
    py_ion_type_table[0x3] = PyObject_GetAttrString(_py_ion_type, "INT");
    py_ion_type_table[0x4] = PyObject_GetAttrString(_py_ion_type, "FLOAT");
    py_ion_type_table[0x5] = PyObject_GetAttrString(_py_ion_type, "DECIMAL");
    py_ion_type_table[0x6] = PyObject_GetAttrString(_py_ion_type, "TIMESTAMP");
    py_ion_type_table[0x7] = PyObject_GetAttrString(_py_ion_type, "SYMBOL");
    py_ion_type_table[0x8] = PyObject_GetAttrString(_py_ion_type, "STRING");
    py_ion_type_table[0x9] = PyObject_GetAttrString(_py_ion_type, "CLOB");
    py_ion_type_table[0xA] = PyObject_GetAttrString(_py_ion_type, "BLOB");
    py_ion_type_table[0xB] = PyObject_GetAttrString(_py_ion_type, "LIST");
    py_ion_type_table[0xC] = PyObject_GetAttrString(_py_ion_type, "SEXP");
    py_ion_type_table[0xD] = PyObject_GetAttrString(_py_ion_type, "STRUCT");

    c_ion_type_table[0x0] = tid_NULL_INT;
    c_ion_type_table[0x1] = tid_BOOL_INT;
    c_ion_type_table[0x2] = tid_INT_INT;
    c_ion_type_table[0x3] = tid_FLOAT_INT;
    c_ion_type_table[0x4] = tid_DECIMAL_INT;
    c_ion_type_table[0x5] = tid_TIMESTAMP_INT;
    c_ion_type_table[0x6] = tid_SYMBOL_INT;
    c_ion_type_table[0x7] = tid_STRING_INT;
    c_ion_type_table[0x8] = tid_CLOB_INT;
    c_ion_type_table[0x9] = tid_BLOB_INT;
    c_ion_type_table[0xA] = tid_LIST_INT;
    c_ion_type_table[0xB] = tid_SEXP_INT;
    c_ion_type_table[0xC] = tid_STRUCT_INT;

    py_ion_timestamp_precision_table[0] = PyObject_GetAttrString(_py_timestamp_precision, "YEAR");
    py_ion_timestamp_precision_table[1] = PyObject_GetAttrString(_py_timestamp_precision, "MONTH");
    py_ion_timestamp_precision_table[2] = PyObject_GetAttrString(_py_timestamp_precision, "DAY");
    py_ion_timestamp_precision_table[3] = NULL; // Impossible; there is no hour precision.
    py_ion_timestamp_precision_table[4] = PyObject_GetAttrString(_py_timestamp_precision, "MINUTE");
    py_ion_timestamp_precision_table[5] = PyObject_GetAttrString(_py_timestamp_precision, "SECOND");
    py_ion_timestamp_precision_table[6] = PyObject_GetAttrString(_py_timestamp_precision, "SECOND");

    _exception_module   = PyImport_ImportModule("amazon.ion.exceptions");
    _ion_exception_cls  = PyObject_GetAttrString(_exception_module, "IonException");

    decContextDefault(&dec_context, DEC_INIT_DECQUAD);  //The writer already had one of these, but it's private.
    return m;
}

static PyObject* init_module(void) {
    return ionc_init_module();
}

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit_ionc(void)
{
    return init_module();
}
#else
void
initionc(void)
{
    init_module();
}
#endif
