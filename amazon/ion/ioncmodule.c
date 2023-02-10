// See https://python.readthedocs.io/en/stable/c-api/arg.html#strings-and-buffers
#define PY_SSIZE_T_CLEAN

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

#define MAX_TIMESTAMP_PRECISION 9

#define ERR_MSG_MAX_LEN 100
#define FIELD_NAME_MAX_LEN 1000
#define ANNOTATION_MAX_LEN 50

#define IONC_STREAM_READ_BUFFER_SIZE 1024*32
#define IONC_STREAM_BYTES_READ_SIZE PyLong_FromLong(IONC_STREAM_READ_BUFFER_SIZE/4)

static char _err_msg[ERR_MSG_MAX_LEN];

#define _FAILWITHMSG(x, msg) { err = x; snprintf(_err_msg, ERR_MSG_MAX_LEN, msg); goto fail; }

#define IONC_BYTES_FORMAT "y#"
#define IONC_READ_ARGS_FORMAT "OOO"

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

typedef struct {
    PyObject *py_file; // a TextIOWrapper-like object
    BYTE buffer[IONC_STREAM_READ_BUFFER_SIZE];
} _ION_READ_STREAM_HANDLE;

typedef struct {
    PyObject_HEAD
    hREADER reader;
    ION_READER_OPTIONS _reader_options;
    BOOL closed;
    BOOL emit_bare_values;
    _ION_READ_STREAM_HANDLE file_handler_state;
} ionc_read_Iterator;

PyObject* ionc_read_iter(PyObject *self);
PyObject* ionc_read_iter_next(PyObject *self);
void ionc_read_iter_dealloc(PyObject *self);

static PyTypeObject ionc_read_IteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "ionc_read.Iterator",
    .tp_basicsize = sizeof(ionc_read_Iterator),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Internal ION iterator object.",
    .tp_iter = ionc_read_iter,
    .tp_iternext = ionc_read_iter_next,
    .tp_dealloc = ionc_read_iter_dealloc
};

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
        c_int = (int)PyLong_AsSsize_t(py_int);
    }
    Py_DECREF(py_int);
    return c_int;
}

// an Alternative to calculate timedelta, see https://github.com/amazon-ion/ion-python/issues/225
static int offset_seconds(PyObject* timedelta) {
    PyObject* py_seconds = PyObject_CallMethod(timedelta, "total_seconds", NULL);
    PyObject* py_seconds_int = PyObject_CallMethod(py_seconds, "__int__", NULL);
    int seconds = (int)PyLong_AsSsize_t(py_seconds_int);
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
    int c_type = c_ion_type_table[PyLong_AsSsize_t(ion_type)];
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
    *out = PyUnicode_AsUTF8AndSize(str, len_out);
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
        PyObject* py_attr = PyUnicode_FromString("add_item");
        PyObject* py_field_name = ion_build_py_string(field_name);
        PyObject_CallMethodObjArgs(
            pyContainer,
            py_attr,
            py_field_name,
            (PyObject*)element,
            NULL
        );
        Py_DECREF(py_attr);
        Py_DECREF(py_field_name);
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
    PyObject* py_string_value, *py_sid, *return_value;
    if (string_value->value) {
        py_string_value = ion_build_py_string(string_value);
        py_sid = Py_None;
    }
    else {
        py_string_value = Py_None;
        py_sid = PyLong_FromLong(0);
    }
    return_value = PyObject_CallFunctionObjArgs(
        _py_symboltoken_constructor,
        py_string_value,
        py_sid,
        NULL
    );
    if (py_sid != Py_None) Py_DECREF(py_sid);
    if (py_string_value != Py_None) Py_DECREF(py_string_value);
    return return_value;
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
        SID sid = PyLong_AsSsize_t(py_sid);
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
    PyObject* int_str = PyObject_CallMethod(obj, "__str__", NULL);
    ION_STRING string_value;
    ion_string_from_py(int_str, &string_value);
    ION_INT ion_int_value;

    IONCHECK(ion_int_init(&ion_int_value, NULL));
    IONCHECK(ion_int_from_string(&ion_int_value, &string_value));
    IONCHECK(ion_writer_write_ion_int(writer, &ion_int_value));
fail:
    Py_XDECREF(int_str);
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
    else if (PyLong_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_INT_INT;
        }
        if (tid_INT_INT == ion_type) {
            IONCHECK(ionc_write_big_int(writer, obj));
        }
        else if (tid_BOOL_INT == ion_type) {
            IONCHECK(ion_writer_write_bool(writer, PyLong_AsSsize_t(obj)));
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

        PyObject* decimal_str = PyObject_CallMethod(obj, "__str__", NULL);
        char* decimal_c_str = NULL;
        Py_ssize_t decimal_c_str_len;
        c_string_from_py(decimal_str, &decimal_c_str, &decimal_c_str_len);

        ION_DECIMAL decimal_value;
        IONCHECK(ion_decimal_from_string(&decimal_value, decimal_c_str, &dec_context));
        Py_DECREF(decimal_str);

        IONCHECK(ion_writer_write_ion_decimal(writer, &decimal_value));
    }
    else if (PyBytes_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_BLOB_INT;
        }
        char* bytes = NULL;
        Py_ssize_t len;
        IONCHECK(PyBytes_AsStringAndSize(obj, &bytes, &len));
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
        PyObject *fractional_seconds, *fractional_decimal_tuple, *py_exponent, *py_digits;
        int year, month, day, hour, minute, second;
        short precision, fractional_precision;
        int final_fractional_precision, final_fractional_seconds;
        if (PyObject_HasAttrString(obj, "precision") && PyObject_GetAttrString(obj, "precision") != Py_None) {
            // This is a Timestamp.
            precision = int_attr_by_name(obj, "precision");
            fractional_precision = int_attr_by_name(obj, "fractional_precision");
            if (PyObject_HasAttrString(obj, "fractional_seconds")) {
                fractional_seconds = PyObject_GetAttrString(obj, "fractional_seconds");
                fractional_decimal_tuple = PyObject_CallMethod(fractional_seconds, "as_tuple", NULL);
                py_exponent = PyObject_GetAttrString(fractional_decimal_tuple, "exponent");
                py_digits = PyObject_GetAttrString(fractional_decimal_tuple, "digits");
                int exp = PyLong_AsLong(py_exponent) * -1;
                if (exp > MAX_TIMESTAMP_PRECISION) {
                    final_fractional_precision = MAX_TIMESTAMP_PRECISION;
                } else {
                    final_fractional_precision = exp;
                }

                int keep = exp - final_fractional_precision;
                int digits_len = PyLong_AsLong(PyObject_CallMethod(py_digits, "__len__", NULL));
                final_fractional_seconds = 0;
                for (int i = 0; i < digits_len - keep; i++) {
                    PyObject* digit = PyTuple_GetItem(py_digits, i);
                    Py_INCREF(digit);
                    final_fractional_seconds = final_fractional_seconds * 10 + PyLong_AsLong(digit);
                    Py_DECREF(digit);
                }

                Py_DECREF(fractional_seconds);
                Py_DECREF(fractional_decimal_tuple);
                Py_DECREF(py_exponent);
                Py_DECREF(py_digits);

            } else {
                final_fractional_precision = fractional_precision;
                final_fractional_seconds = int_attr_by_name(obj, "microsecond");
            }
        }
        else {
            // This is a naive datetime. It always has maximum precision.
            precision = SECOND_PRECISION;
            final_fractional_precision = MICROSECOND_DIGITS;
            final_fractional_seconds = int_attr_by_name(obj, "microsecond");
        }

        year = int_attr_by_name(obj, "year");
        if (precision == SECOND_PRECISION) {
            month = int_attr_by_name(obj, "month");
            day = int_attr_by_name(obj, "day");
            hour = int_attr_by_name(obj, "hour");
            minute = int_attr_by_name(obj, "minute");
            second = int_attr_by_name(obj, "second");
            int microsecond = int_attr_by_name(obj, "microsecond");
            if (final_fractional_precision > 0) {
                decQuad fraction;
                decNumber helper, dec_number_precision;
                decQuadFromInt32(&fraction, (int32_t)final_fractional_seconds);
                decQuad tmp;
                decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, -final_fractional_precision), &dec_context);
                decQuadToNumber(&fraction, &helper);
                decContextClearStatus(&dec_context, DEC_Inexact); // TODO consider saving, clearing, and resetting the status flag
                decNumberRescale(&helper, &helper, decNumberFromInt32(&dec_number_precision, -final_fractional_precision), &dec_context);
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
    options.max_annotation_count = ANNOTATION_MAX_LEN;
    IONCHECK(ion_writer_open(&writer, ion_stream, &options));

    if (Py_TYPE(obj) == &ionc_read_IteratorType) {
        PyObject *item;
        while (item = PyIter_Next(obj)) {
            err = ionc_write_value(writer, item, tuple_as_sexp);
            Py_DECREF(item);
            if (err) break;
        }
        IONCHECK(err);
        if (PyErr_Occurred()) {
            _FAILWITHMSG(IERR_INTERNAL_ERROR, "unexpected error occurred while iterating the input");
        }
    }
    else if (sequence_as_stream == Py_True && (PyList_Check(obj) || PyTuple_Check(obj))) {
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
    writer = 0;

    POSITION len = ion_stream_get_position(ion_stream);
    IONCHECK(ion_stream_seek(ion_stream, 0));
    // TODO if len > max int32, need to return more than one page...
    buf = (BYTE*)(PyMem_Malloc((size_t)len));
    SIZE bytes_read;
    IONCHECK(ion_stream_read(ion_stream, buf, (SIZE)len, &bytes_read));

    IONCHECK(ion_stream_close(ion_stream));
    ion_stream = NULL;
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
    if (writer) {
        ion_writer_close(writer);
    }
    if (ion_stream != NULL) {
        ion_stream_close(ion_stream);
    }
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
        PyObject* py_off_hours = PyLong_FromLong(off_hours);
        PyObject* py_off_minutes = PyLong_FromLong(off_minutes);
        // Bounds checking is performed in python.
        PyDict_SetItemString(timestamp_args, "off_hours", py_off_hours);
        PyDict_SetItemString(timestamp_args, "off_minutes", py_off_minutes);
        Py_DECREF(py_off_hours);
        Py_DECREF(py_off_minutes);
    }

    switch (precision) {
        case ION_TS_FRAC:
        {
            decQuad fraction = timestamp_value.fraction;
            decQuad tmp;

            int32_t fractional_precision = decQuadGetExponent(&fraction);
            if (fractional_precision > 0) {
                _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Timestamp fractional precision cannot be a positive number.");
            }
            fractional_precision = fractional_precision * -1;

            if (fractional_precision > MICROSECOND_DIGITS) {
                decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, fractional_precision), &dec_context);
                int dec = decQuadToInt32Exact(&fraction, &dec_context, DEC_ROUND_DOWN);
                if (fractional_precision > MAX_TIMESTAMP_PRECISION) fractional_precision = MAX_TIMESTAMP_PRECISION;
                if (decContextTestStatus(&dec_context, DEC_Inexact)) {
                    // This means the fractional component is not [0, 1) or has more than microsecond precision.
                    decContextClearStatus(&dec_context, DEC_Inexact);
                }

                char dec_num[DECQUAD_String];
                decQuad d;
                decQuadFromInt32(&d, dec);
                decQuadScaleB(&d, &d, decQuadFromInt32(&tmp, -fractional_precision), &dec_context);
                decQuadToString(&d, dec_num);

                PyObject* py_dec_str = PyUnicode_FromString(dec_num);
                PyObject* py_fractional_seconds = PyObject_CallFunctionObjArgs(_decimal_constructor, py_dec_str, NULL);
                PyDict_SetItemString(timestamp_args, "fractional_seconds", py_fractional_seconds);
                Py_DECREF(py_fractional_seconds);
                Py_DECREF(py_dec_str);
            } else {
                decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, MICROSECOND_DIGITS), &dec_context);
                int32_t microsecond = decQuadToInt32Exact(&fraction, &dec_context, DEC_ROUND_DOWN);

                if (decContextTestStatus(&dec_context, DEC_Inexact)) {
                    // This means the fractional component is not [0, 1) or has more than microsecond precision.
                    decContextClearStatus(&dec_context, DEC_Inexact);
                }

                PyObject* py_microsecond = PyLong_FromLong(microsecond);
                PyObject* py_fractional_precision = PyLong_FromLong(fractional_precision);
                PyDict_SetItemString(timestamp_args, "microsecond", py_microsecond);
                PyDict_SetItemString(timestamp_args, "fractional_precision", py_fractional_precision);
                Py_DECREF(py_microsecond);
                Py_DECREF(py_fractional_precision);
            }
        }
        case ION_TS_SEC:
        {
            PyObject* temp_seconds = PyLong_FromLong(timestamp_value.seconds);
            PyDict_SetItemString(timestamp_args, "second", temp_seconds);
            Py_DECREF(temp_seconds);
        }
        case ION_TS_MIN:
        {
            PyObject* temp_minutes = PyLong_FromLong(timestamp_value.minutes);
            PyObject* temp_hours = PyLong_FromLong(timestamp_value.hours);

            PyDict_SetItemString(timestamp_args, "minute", temp_minutes);
            PyDict_SetItemString(timestamp_args, "hour",  temp_hours);

            Py_DECREF(temp_minutes);
            Py_DECREF(temp_hours);
        }
        case ION_TS_DAY:
        {
            PyObject* temp_day = PyLong_FromLong(timestamp_value.day);
            PyDict_SetItemString(timestamp_args, "day", temp_day);
            Py_DECREF(temp_day);
        }
        case ION_TS_MONTH:
        {   PyObject* temp_month = PyLong_FromLong(timestamp_value.month);
            PyDict_SetItemString(timestamp_args, "month", temp_month);
            Py_DECREF(temp_month);
        }
        case ION_TS_YEAR:
        {
            PyObject* temp_year = PyLong_FromLong(timestamp_value.year);
            PyDict_SetItemString(timestamp_args, "year", temp_year);
            Py_DECREF(temp_year);
            break;
        }
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
        if (field_name_len > FIELD_NAME_MAX_LEN) {
            _FAILWITHMSG(IERR_INVALID_ARG,
                "Filed name overflow, please try again with pure python.");
        }
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
            // Hack for ion-c issue https://github.com/amazon-ion/ion-c/issues/223
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
            ION_INT ion_int_value;
            IONCHECK(ion_int_init(&ion_int_value, hreader));
            IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));
            SIZE int_char_len, int_char_written;
            IONCHECK(ion_int_char_length(&ion_int_value, &int_char_len));
            char* ion_int_str = (char*)PyMem_Malloc(int_char_len + 1); // Leave room for \0
            err = ion_int_to_char(&ion_int_value, (BYTE*)ion_int_str, int_char_len, &int_char_written);
            if (err) {
                PyMem_Free(ion_int_str);
                IONCHECK(err);
            }
            if (int_char_len < int_char_written) {
                PyMem_Free(ion_int_str);
                _FAILWITHMSG(IERR_BUFFER_TOO_SMALL, "Not enough space given to represent int as string.");
            }
            py_value = PyLong_FromString(ion_int_str, NULL, 10);
            PyMem_Free(ion_int_str);

            ion_nature_constructor = _ionpyint_fromvalue;
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
        {
            ion_nature_constructor = _ionpydict_fromvalue;
            //Init a IonPyDict
            PyObject* new_dict = PyDict_New();
            py_value = PyObject_CallFunctionObjArgs(
                ion_nature_constructor,
                py_ion_type_table[ion_type >> 8],
                new_dict,
                py_annotations,
                NULL
            );
            Py_XDECREF(new_dict);

            IONCHECK(ionc_read_into_container(hreader, py_value, /*is_struct=*/TRUE, emit_bare_values));
            emit_bare_values = TRUE;
            break;
        }
        case tid_SEXP_INT:
        {
            emit_bare_values = FALSE; // Sexp values must always be emitted as IonNature because of ambiguity with list.
            // intentional fall-through
        }
        case tid_LIST_INT:
        {
            py_value = PyList_New(0);
            IONCHECK(ionc_read_into_container(hreader, py_value, /*is_struct=*/FALSE, emit_bare_values));
            ion_nature_constructor = _ionpylist_fromvalue;
            break;
        }
        case tid_DATAGRAM_INT:
        default:
            FAILWITH(IERR_INVALID_STATE);
        }

    PyObject* final_py_value = py_value;
    if (!emit_bare_values) {
        final_py_value = PyObject_CallFunctionObjArgs(
            ion_nature_constructor,
            py_ion_type_table[ion_type >> 8],
            py_value,
            py_annotations,
            NULL
        );
        if (py_value != Py_None) Py_XDECREF(py_value);
    }
    Py_XDECREF(py_annotations);

    if (in_struct && !None_field_name) {
        ION_STRING_INIT(&field_name);
        ion_string_assign_cstr(&field_name, field_name_value, field_name_len);
    }
    ionc_add_to_container(container, final_py_value, in_struct, &field_name);

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

iERR ion_read_file_stream_handler(struct _ion_user_stream *pstream) {
    iENTER;
    char *char_buffer = NULL;
    Py_ssize_t size;
    _ION_READ_STREAM_HANDLE *stream_handle = (_ION_READ_STREAM_HANDLE *) pstream->handler_state;
    PyObject *py_buffer_as_bytes = NULL;
    PyObject *py_buffer = PyObject_CallMethod(stream_handle->py_file, "read", "O", IONC_STREAM_BYTES_READ_SIZE);

    if (py_buffer == NULL) {
        pstream->limit = NULL;
        FAILWITH(IERR_READ_ERROR);
    }

    if (PyBytes_Check(py_buffer)) {
        // stream is binary
        if (PyBytes_AsStringAndSize(py_buffer, &char_buffer, &size) < 0) {
            pstream->limit = NULL;
            FAILWITH(IERR_READ_ERROR);
        }
    } else {
        // convert str to unicode
        py_buffer_as_bytes = PyUnicode_AsUTF8String(py_buffer);
        if (py_buffer_as_bytes == NULL || py_buffer_as_bytes == Py_None) {
            pstream->limit = NULL;
            FAILWITH(IERR_READ_ERROR);
        }
        if (PyBytes_AsStringAndSize(py_buffer_as_bytes, &char_buffer, &size) < 0) {
            pstream->limit = NULL;
            FAILWITH(IERR_READ_ERROR);
        }
    }

    // safe-guarding the size variable to protect memcpy bounds
    if (size < 0  || size > IONC_STREAM_READ_BUFFER_SIZE) {
        FAILWITH(IERR_READ_ERROR);
    }
    memcpy(stream_handle->buffer, char_buffer, size);

    pstream->curr = stream_handle->buffer;
    if (size < 1) {
        pstream->limit = NULL;
        DONTFAILWITH(IERR_EOF);
    }
    pstream->limit = pstream->curr + size;

fail:
    Py_XDECREF(py_buffer_as_bytes);
    Py_XDECREF(py_buffer);
    cRETURN;
}

PyObject* ionc_read_iter_next(PyObject *self) {
    iENTER;
    ION_TYPE t;
    ionc_read_Iterator *iterator = (ionc_read_Iterator*) self;
    PyObject* container = NULL;
    hREADER reader = iterator->reader;
    BOOL emit_bare_values = iterator->emit_bare_values;

    if (iterator->closed) {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
    IONCHECK(ion_reader_next(reader, &t));

    if (t == tid_EOF) {
        assert(t == tid_EOF && "next() at end");

        IONCHECK(ion_reader_close(reader));
        PyErr_SetNone(PyExc_StopIteration);
        iterator->closed = TRUE;
        return NULL;
    }

    container = PyList_New(0);
    IONCHECK(ionc_read_value(reader, t, container, FALSE, emit_bare_values));
    Py_ssize_t len = PyList_Size(container);
    if (len != 1) {
        _FAILWITHMSG(IERR_INVALID_ARG, "assertion failed: len == 1");
    }

    PyObject* value = PyList_GetItem(container, 0);
    Py_XINCREF(value);
    Py_DECREF(container);

    return value;

fail:
    Py_XDECREF(container);
    PyObject* exception = PyErr_Format(_ion_exception_cls, "%s %s", ion_error_to_str(err), _err_msg);
    _err_msg[0] = '\0';
    return exception;
}

PyObject* ionc_read_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}

void ionc_read_iter_dealloc(PyObject *self) {
    ionc_read_Iterator *iterator = (ionc_read_Iterator*) self;
    if (!iterator->closed) {
        ion_reader_close(iterator->reader);
        iterator->closed = TRUE;
    }
    Py_DECREF(iterator->file_handler_state.py_file);
    PyObject_Del(self);
}

/*
 *  Entry point of read/load functions
 */
PyObject* ionc_read(PyObject* self, PyObject *args, PyObject *kwds) {
    iENTER;
    PyObject *py_file = NULL; // TextIOWrapper
    PyObject *emit_bare_values;
    PyObject *text_buffer_size_limit;
    ionc_read_Iterator *iterator = NULL;
    static char *kwlist[] = {"file", "emit_bare_values", "text_buffer_size_limit", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, IONC_READ_ARGS_FORMAT, kwlist, &py_file,
                                        &emit_bare_values, &text_buffer_size_limit)) {
        FAILWITH(IERR_INVALID_ARG);
    }

    iterator = PyObject_New(ionc_read_Iterator, &ionc_read_IteratorType);
    if (!iterator) {
        FAILWITH(IERR_INTERNAL_ERROR);
    }
    Py_INCREF(py_file);

    if (!PyObject_Init((PyObject*) iterator, &ionc_read_IteratorType)) {
        FAILWITH(IERR_INTERNAL_ERROR);
    }

    iterator->closed = FALSE;
    iterator->file_handler_state.py_file = py_file;
    iterator->emit_bare_values = emit_bare_values == Py_True;
    memset(&iterator->reader, 0, sizeof(iterator->reader));
    memset(&iterator->_reader_options, 0, sizeof(iterator->_reader_options));
    iterator->_reader_options.decimal_context = &dec_context;
    if (text_buffer_size_limit != Py_None) {
        int symbol_threshold = PyLong_AsLong(text_buffer_size_limit);
        iterator->_reader_options.symbol_threshold = symbol_threshold;
        Py_XDECREF(text_buffer_size_limit);
    }

    IONCHECK(ion_reader_open_stream(
        &iterator->reader,
        &iterator->file_handler_state,
        ion_read_file_stream_handler,
        &iterator->_reader_options)); // NULL represents default reader options
    return iterator;

fail:
    if (iterator != NULL) {
        Py_DECREF(py_file);
    }
    Py_XDECREF(iterator);
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

PyObject* ionc_init_module(void) {
    PyDateTime_IMPORT;
    PyObject* m;

    m = PyModule_Create(&moduledef);

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

PyMODINIT_FUNC
PyInit_ionc(void)
{
    return init_module();
}

