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

static char _err_msg[ERR_MSG_MAX_LEN];

#define _FAILWITHMSG(x, msg) { err = x; snprintf(_err_msg, ERR_MSG_MAX_LEN, msg); goto fail; }

#define IONC_BYTES_FORMAT "y#"
#define IONC_READ_ARGS_FORMAT "ObO"

static PyObject* IONC_STREAM_BYTES_READ_SIZE;

static PyObject* _decimal_module;
static PyObject* _decimal_constructor;
static PyObject* _decimal_zero;
static PyObject* _py_timestamp_cls;
static PyObject* _py_timestamp__new__;
static PyObject* _simpletypes_module;
static PyObject* _ionpynull_cls;
static PyObject* _ionpybool_cls;
static PyObject* _ionpyint_cls;
static PyObject* _ionpyfloat_cls;
static PyObject* _ionpydecimal_cls;
static PyObject* _ionpytimestamp_cls;
static PyObject* _ionpytext_cls;
static PyObject* _ionpysymbol_cls;
static PyObject* _ionpybytes_cls;
static PyObject* _ionpylist_cls;
static PyObject* _ionpydict_cls;
static PyObject* _ionpystddict_cls;

static PyObject* _ionpynull_fromvalue;
static PyObject* _ionpybool_fromvalue;
static PyObject* _ionpyint_fromvalue;
static PyObject* _ionpyfloat_fromvalue;
static PyObject* _ionpydecimal_fromvalue;
static PyObject* _ionpytimestamp_fromvalue;
static PyObject* _ionpytext_fromvalue;
static PyObject* _ionpysymbol_fromvalue;
static PyObject* _ionpybytes_fromvalue;
static PyObject* _ionpylist_fromvalue;
static PyObject* _ionpydict_factory;
static PyObject* _ionpylist_factory;

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
static PyObject* ion_type_str;
static PyObject* ion_annotations_str;
static PyObject* text_str;
static PyObject* sid_str;
static PyObject* precision_str;
static PyObject* fractional_seconds_str;
static PyObject* exponent_str;
static PyObject* digits_str;
static PyObject* fractional_precision_str;
static PyObject* store_str;

// Note: We cast x (assumed to be an int) to a long so that we can cast to a pointer without warning.
#define INT_TO_ION_TYPE(x) ( (ION_TYPE)(long)(x) )

typedef struct {
    PyObject *py_file; // a TextIOWrapper-like object
    BYTE buffer[IONC_STREAM_READ_BUFFER_SIZE];
} _ION_READ_STREAM_HANDLE;

typedef struct {
    PyObject_HEAD
    hREADER reader;
    ION_READER_OPTIONS _reader_options;
    BOOL closed;
    uint8_t value_model;
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
static int int_attr_by_name(PyObject* obj, PyObject* attr_name) {
    PyObject* py_int = PyObject_GetAttr(obj, attr_name);
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
    ion_type = PyObject_GetAttr(obj, ion_type_str);
    if (ion_type == NULL) {
        PyErr_Clear();
        return tid_none_INT;
    }
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
static iERR c_string_from_py(PyObject* str, const char** out, Py_ssize_t* len_out) {
    *out = PyUnicode_AsUTF8AndSize(str, len_out);
    return IERR_OK;
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
    const char* c_str = NULL;
    Py_ssize_t c_str_len;
    IONCHECK(c_string_from_py(str, &c_str, &c_str_len));
    ION_STRING_INIT(out);
    ion_string_assign_cstr(out, (char *)c_str, c_str_len);
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
    PyObject* symbol_text = PyObject_GetAttr(symboltoken, text_str);
    if (symbol_text == Py_None) {
        PyObject* py_sid = PyObject_GetAttr(symboltoken, sid_str);
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
    annotations = PyObject_GetAttr(obj, ion_annotations_str);
    if (annotations == NULL || PyObject_Not(annotations)) {
        PyErr_Clear();
        // Proceed as if the attribute is not there.
        SUCCEED();
    }

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
 * Process and write the key-value pair.
 *  *  Args:
 *      writer:  An ion writer
 *      key: The key of IonStruct item
 *      val: The value of IonStruct item
 *      tuple_as_sexp: Decides if a tuple is treated as sexp
 */

static iERR write_struct_field(hWRITER writer, PyObject* key, PyObject* val, PyObject* tuple_as_sexp) {
    iERR err;
    if (PyUnicode_Check(key)) {
        ION_STRING field_name;
        ion_string_from_py(key, &field_name);
        IONCHECK(ion_writer_write_field_name(writer, &field_name));
    } else if (key == Py_None) {
        IONCHECK(_ion_writer_write_field_sid_helper(writer, 0));
    }
    IONCHECK(Py_EnterRecursiveCall(" while writing an Ion struct"));
    err = ionc_write_value(writer, val, tuple_as_sexp);
    Py_LeaveRecursiveCall();
    IONCHECK(err);

    iRETURN;
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
    PyObject *store = NULL, *key = NULL, *val_list = NULL, *val = NULL;
    Py_ssize_t pos = 0, i, list_len;
    if (PyDict_Check(map)) {
        while (PyDict_Next(map, &pos, &key, &val)) {
            IONCHECK(write_struct_field(writer, key, val, tuple_as_sexp));
        }
    } else {
        store = PyObject_GetAttr(map, store_str);
        if (store == NULL || !PyDict_Check(store)) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Failed to retrieve 'store': Object is either NULL or not a Python dictionary.");
        }
        pos = 0;
        while (PyDict_Next(store, &pos, &key, &val_list)) {
            if (!PyList_Check(val_list)) {
                _FAILWITHMSG(IERR_INVALID_ARG, "Invalid value type for the key: Expected a list, but found a different type.");
            }
            list_len = PyList_Size(val_list);
            for (i = 0; i < list_len; i++) {
                val = PyList_GetItem(val_list, i); // Borrowed reference
                IONCHECK(write_struct_field(writer, key, val, tuple_as_sexp));
            }
        }
        Py_DECREF(store);
    }

fail:
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
    PyObject* int_str = NULL;
    int overflow;
    long long int_value = PyLong_AsLongLongAndOverflow(obj, &overflow);

    if (!overflow && PyErr_Occurred() == NULL) {
        // Value fits within int64, write it as int64
        IONCHECK(ion_writer_write_int64(writer, int_value));
    } else {
        PyErr_Clear();
        int_str = PyObject_Str(obj);
        ION_STRING string_value;
        ion_string_from_py(int_str, &string_value);
        ION_INT ion_int_value;
        IONCHECK(ion_int_init(&ion_int_value, NULL));
        IONCHECK(ion_int_from_string(&ion_int_value, &string_value));
        IONCHECK(ion_writer_write_ion_int(writer, &ion_int_value));
    }
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
        IONCHECK(ion_writer_write_typed_null(writer, INT_TO_ION_TYPE(ion_type)));
    }
    else if (PyObject_TypeCheck(obj, (PyTypeObject*)_decimal_constructor)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_DECIMAL_INT;
        }
        if (tid_DECIMAL_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found Decimal; expected DECIMAL Ion type.");
        }

        PyObject* decimal_str = PyObject_CallMethod(obj, "__str__", NULL);
        const char* decimal_c_str = NULL;
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
        PyObject *fractional_seconds, *fractional_decimal_tuple, *py_exponent, *py_digits, *precision_attr;
        int year, month, day, hour, minute, second;
        short precision, fractional_precision;
        int final_fractional_precision, final_fractional_seconds;
        precision_attr = PyObject_GetAttr(obj, precision_str);
        if (precision_attr != NULL && precision_attr != Py_None) {
            // This is a Timestamp.
            precision = int_attr_by_name(obj, precision_str);
            fractional_precision = int_attr_by_name(obj, fractional_precision_str);
            fractional_seconds = PyObject_GetAttr(obj, fractional_seconds_str);
            if (fractional_seconds != NULL) {
                fractional_decimal_tuple = PyObject_CallMethod(fractional_seconds, "as_tuple", NULL);
                py_exponent = PyObject_GetAttr(fractional_decimal_tuple, exponent_str);
                py_digits = PyObject_GetAttr(fractional_decimal_tuple, digits_str);
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
                Py_DECREF(precision_attr);
            } else {
                PyErr_Clear();
                final_fractional_precision = fractional_precision;
                final_fractional_seconds = PyDateTime_DATE_GET_MICROSECOND(obj);
            }
        }
        else {
            PyErr_Clear();
            // This is a naive datetime. It always has maximum precision.
            precision = SECOND_PRECISION;
            final_fractional_precision = MICROSECOND_DIGITS;
            final_fractional_seconds = PyDateTime_DATE_GET_MICROSECOND(obj);
        }

        year = PyDateTime_GET_YEAR(obj);
        if (precision == SECOND_PRECISION) {
            month = PyDateTime_GET_MONTH(obj);
            day = PyDateTime_GET_DAY(obj);
            hour = PyDateTime_DATE_GET_HOUR(obj);
            minute = PyDateTime_DATE_GET_MINUTE(obj);
            second = PyDateTime_DATE_GET_SECOND(obj);
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
            else if (final_fractional_seconds > 0) {
                _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Not enough fractional precision for timestamp.");
            }
            else {
                IONCHECK(ion_timestamp_for_second(&timestamp_value, year, month, day, hour, minute, second));
            }
        }
        else if (precision == MINUTE_PRECISION) {
            month = PyDateTime_GET_MONTH(obj);
            day = PyDateTime_GET_DAY(obj);
            hour = PyDateTime_DATE_GET_HOUR(obj);
            minute = PyDateTime_DATE_GET_MINUTE(obj);
            IONCHECK(ion_timestamp_for_minute(&timestamp_value, year, month, day, hour, minute));
        }
        else if (precision == DAY_PRECISION) {
            month = PyDateTime_GET_MONTH(obj);
            day = PyDateTime_GET_DAY(obj);
            IONCHECK(ion_timestamp_for_day(&timestamp_value, year, month, day));
        }
        else if (precision == MONTH_PRECISION) {
            month = PyDateTime_GET_MONTH(obj);
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
    else if (PyDict_Check(obj) || PyObject_TypeCheck(obj, (PyTypeObject *)_ionpydict_cls)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_STRUCT_INT;
        }
        if (tid_STRUCT_INT != ion_type) {
            _FAILWITHMSG(IERR_INVALID_ARG, "Found dict; expected STRUCT Ion type.");
        }
        IONCHECK(ion_writer_start_container(writer, INT_TO_ION_TYPE(ion_type)));
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
            IONCHECK(ion_writer_start_container(writer, tid_SEXP));
        }
        else {
            IONCHECK(ion_writer_start_container(writer, INT_TO_ION_TYPE(ion_type)));
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
    iERR err = IERR_OK;

    PyObject* pyObj = PySequence_Fast_GET_ITEM(objs, i);
    Py_INCREF(pyObj);
    err = ionc_write_value(writer, pyObj, tuple_as_sexp);
    Py_DECREF(pyObj);

    return err;
}

/*
 *  Entry point of write/dump functions
 */
static PyObject* ionc_write(PyObject *self, PyObject *args, PyObject *kwds) {
    iENTER;
    PyObject *obj=NULL, *binary=NULL, *sequence_as_stream=NULL, *tuple_as_sexp=NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE* buf = NULL;
    hWRITER writer = NULL;
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
    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.output_as_binary = PyObject_IsTrue(binary);
    options.max_annotation_count = ANNOTATION_MAX_LEN;
    IONCHECK(ion_writer_open(&writer, ion_stream, &options));

    if (Py_TYPE(obj) == &ionc_read_IteratorType) {
        PyObject *item;
        while ((item = PyIter_Next(obj)) != NULL) {
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
    Py_XDECREF(obj);
    Py_XDECREF(binary);
    Py_XDECREF(sequence_as_stream);
    Py_XDECREF(tuple_as_sexp);

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
/*
 *  Converts an ion decimal string to a python-decimal-accept string.
 *
 *  Args:
 *      dec_str:  A C string representing a decimal number
 *
 */
static void c_decstr_to_py_decstr(char* dec_str, int dec_len) {
    for (int i = 0; i < dec_len; i++) {
        if (dec_str[i] == 'd' || dec_str[i] == 'D') {
            dec_str[i] = 'e';
        }
    }
}

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
    PyObject* py_fractional_seconds = _decimal_zero;
    PyObject* tzinfo = Py_None;

    IONCHECK(ion_reader_read_timestamp(hreader, &timestamp_value));
    int year, month = 1, day = 1, hours = 0, minutes = 0, seconds = 0, precision, fractional_precision = 0;
    IONCHECK(ion_timestamp_get_precision(&timestamp_value, &precision));
    if (precision < ION_TS_YEAR) {
        _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Found a timestamp with less than year precision.");
    }
    PyObject* py_precision = ionc_get_timestamp_precision(precision);

    BOOL has_local_offset;
    IONCHECK(ion_timestamp_has_local_offset(&timestamp_value, &has_local_offset));
    if (has_local_offset) {
        int off_minutes;
        IONCHECK(ion_timestamp_get_local_offset(&timestamp_value, &off_minutes));
        PyObject *offset = PyDelta_FromDSU(0, off_minutes * 60, 0);
        tzinfo = PyTimeZone_FromOffset(offset);
        Py_DECREF(offset);
    }

    switch (precision) {
        case ION_TS_FRAC:
        {
            decQuad fraction = timestamp_value.fraction;
            decQuad tmp;

            fractional_precision = decQuadGetExponent(&fraction);
            if (fractional_precision > 0) {
                _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Timestamp fractional precision cannot be a positive number.");
            }
            fractional_precision = fractional_precision * -1;

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

            if (fractional_precision > MICROSECOND_DIGITS) fractional_precision = MICROSECOND_DIGITS;
            py_fractional_seconds = PyObject_CallFunction(_decimal_constructor, "s", dec_num, NULL);
        }
        case ION_TS_SEC:
        {
            seconds = timestamp_value.seconds;
        }
        case ION_TS_MIN:
        {
            minutes = timestamp_value.minutes;
            hours = timestamp_value.hours;
        }
        case ION_TS_DAY:
        {
            day = timestamp_value.day;
        }
        case ION_TS_MONTH:
        {
            month = timestamp_value.month;
        }
        case ION_TS_YEAR:
        {
            year = timestamp_value.year;
            break;
        }
        default:
            _FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Illegal Timestamp Precision!")
    }
    *timestamp_out = PyObject_CallFunction(_py_timestamp__new__, "OiiiiiiOOOiO",
        _py_timestamp_cls, year, month, day, hours, minutes, seconds, Py_None, // we assume that microseconds will be assigned in __new__
        tzinfo, py_precision, fractional_precision, py_fractional_seconds, NULL);

fail:
    if (py_fractional_seconds != _decimal_zero) Py_DECREF(py_fractional_seconds);
    if (tzinfo != Py_None) Py_DECREF(tzinfo);

    cRETURN;
}

/*
 *  Reads values from a container
 *
 *  Args:
 *      hreader:  An ion reader
 *      container: A container that elements are read into
 *      parent_type: Type of container to add to.
 *      value_model: Flags to control how Ion values map to Python types
 *
 */
static iERR ionc_read_into_container(hREADER hreader, PyObject* container, enum ContainerType parent_type, uint8_t value_model) {
    iENTER;
    IONCHECK(ion_reader_step_in(hreader));
    IONCHECK(Py_EnterRecursiveCall(" while reading an Ion container"));
    err = ionc_read_all(hreader, container, parent_type, value_model);
    Py_LeaveRecursiveCall();
    IONCHECK(err);
    IONCHECK(ion_reader_step_out(hreader));
    iRETURN;
}

/*
 *  Adds an element to a List or struct
 *
 *  Args:
 *      pyContainer:  A container that the element is added to
 *      element:  The element to be added to the container
 *      container_type: Type of container to add to.
 *      field_name:  The field name of the element if it is inside a struct
 */
static void ionc_add_to_container(PyObject* pyContainer, PyObject* element, enum ContainerType container_type, PyObject* field_name) {
    switch (container_type) {
        case MULTIMAP:
        {
            // this builds the "hash-map of lists" structure that the IonPyDict object
            // expects for its __store
            PyObject* empty = PyList_New(0);
            // SetDefault performs get|set with a single hash of the key
            PyObject* found = PyDict_SetDefault(pyContainer, field_name, empty);
            PyList_Append(found, element);

            Py_DECREF(empty);
            break;
        }
        case STD_DICT:
        {
            PyDict_SetItem(pyContainer, field_name, element);
            break;
        }
        case LIST:
        {
            PyList_Append(pyContainer, (PyObject*)element);
            break;
        }
    }
    Py_XDECREF(element);
}

/*
 *  Helper function for 'ionc_read_all', reads an ion value
 *
 *  Args:
 *      hreader:  An ion reader
 *      ION_TYPE:  The ion type of the reading value as an int
 *      parent_type: Type of the parent container.
 *      value_model: Flags to control how Ion values map to Python types
 */
iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container, enum ContainerType parent_type, uint8_t value_model) {
    iENTER;

    BOOL        wrap_py_value = !(value_model & 1);
    BOOL        symbol_as_text = value_model & 2;
    BOOL        use_std_dict   = value_model & 4;

    BOOL        is_null;
    ION_STRING  field_name;
    SIZE        annotation_count;
    PyObject*   py_annotations = NULL;
    PyObject*   py_value = NULL;
    PyObject*   ion_nature_constructor = NULL;
    PyObject*   py_field_name = NULL;

    if (parent_type > LIST) {
        IONCHECK(ion_reader_get_field_name(hreader, &field_name));
        py_field_name = ion_build_py_string(&field_name);
    }

    IONCHECK(ion_reader_get_annotation_count(hreader, &annotation_count));
    if (annotation_count > 0) {
        wrap_py_value = TRUE;
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
            // TODO double check the real null type, now it's initialized to IonType.NULL by default
            ION_TYPE null_type;
            // Hack for ion-c issue https://github.com/amazon-ion/ion-c/issues/223
            if (original_t != tid_SYMBOL) {
                IONCHECK(ion_reader_read_null(hreader, &null_type));
            }
            else {
                null_type = tid_SYMBOL;
            }

            ion_type = ION_TYPE_INT(null_type);
            py_value = Py_None;
            // you wouldn't think you need to incref Py_None, and in
            // newer C API versions you won't, but for now you do.
            // see https://github.com/python/cpython/issues/103906 for more
            Py_INCREF(py_value);
            wrap_py_value = wrap_py_value || (ion_type != tid_NULL_INT);
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
            int64_t int64_value;
            err = ion_reader_read_int64(hreader, &int64_value);
            if (err == IERR_OK) {
                py_value = PyLong_FromLongLong(int64_value);
            } else if (err == IERR_NUMERIC_OVERFLOW) {
                ION_INT ion_int_value;
                IONCHECK(ion_int_init(&ion_int_value, hreader));
                IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));
                SIZE int_char_len, int_char_written;
                // ion_int_char_length includes 1 char for \0
                // which ion_int_to_char sets at end.
                IONCHECK(ion_int_char_length(&ion_int_value, &int_char_len));
                char* ion_int_str = (char*)PyMem_Malloc(int_char_len);
                err = ion_int_to_char(&ion_int_value, (BYTE*)ion_int_str, int_char_len, &int_char_written);
                if (err) {
                    PyMem_Free(ion_int_str);
                    IONCHECK(err);
                }
                py_value = PyLong_FromString(ion_int_str, NULL, 10);
                PyMem_Free(ion_int_str);
            } else {
                FAILWITH(err)
            }

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
            SIZE dec_len = ION_DECIMAL_STRLEN(&decimal_value);
            char* dec_str = (char*)PyMem_Malloc(dec_len + 1);
            // returns iERR but only error condition that would cause that is null decimal value
            iERR e = ion_decimal_to_string(&decimal_value, dec_str);
            if (e) {
                ion_decimal_free(&decimal_value);
                PyMem_Free(dec_str);
                FAILWITH(e);
            }
            dec_str[dec_len] = '\0';
            c_decstr_to_py_decstr(dec_str, dec_len);

            if (wrap_py_value) {
                py_value = Py_BuildValue("s", dec_str);
            } else {
                py_value = PyObject_CallFunction(_decimal_constructor, "s", dec_str, NULL);
            }
            ion_decimal_free(&decimal_value);
            PyMem_Free(dec_str);

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
            ION_STRING string_value;
            IONCHECK(ion_reader_read_string(hreader, &string_value));
            if (!symbol_as_text) {
                py_value = ion_string_to_py_symboltoken(&string_value);
                ion_nature_constructor = _ionpysymbol_fromvalue;
            } else if (ion_string_is_null(&string_value)) {
                _FAILWITHMSG(IERR_INVALID_STATE, "Cannot emit symbol with undefined text when SYMBOL_AS_TEXT is set.");
            } else {
                py_value = ion_build_py_string(&string_value);
                ion_nature_constructor = _ionpytext_fromvalue;
            }
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
            // Clob values must always be emitted as IonPyBytes, to avoid ambiguity with blob.
            wrap_py_value = TRUE;
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
            enum ContainerType container_type;

            if (use_std_dict) {
                if (wrap_py_value) {
                    // we construct an empty IonPyStdDict and don't wrap later to avoid
                    // copying the values when wrapping or needing to delegate in the impl
                    py_value = PyObject_CallFunctionObjArgs(
                            _ionpystddict_cls,
                            py_annotations,
                            NULL);
                    wrap_py_value = FALSE;
                } else {
                    py_value = PyDict_New();
                }
                container_type = STD_DICT;
            } else {
                py_value = PyDict_New();
                ion_nature_constructor = _ionpydict_factory;
                // there is no non-IonPy multimap so we always wrap
                wrap_py_value = TRUE;
                container_type = MULTIMAP;
            }

            IONCHECK(ionc_read_into_container(hreader, py_value, container_type, value_model));
            break;
        }
        case tid_SEXP_INT:
        {
            // Sexp values must always be emitted as IonPyList to avoid ambiguity with list.
            wrap_py_value = TRUE;
            // intentional fall-through
        }
        case tid_LIST_INT:
        {
            // instead of creating a std Python list and "wrapping" it
            // which would copy the elements, create the IonPyList now
            if (wrap_py_value) {
                py_value = PyObject_CallFunctionObjArgs(
                        _ionpylist_factory,
                        py_ion_type_table[ion_type >> 8],
                        py_annotations,
                        NULL);
                wrap_py_value = FALSE;
            } else {
                py_value = PyList_New(0);
            }
            IONCHECK(ionc_read_into_container(hreader, py_value, LIST, value_model));
            ion_nature_constructor = _ionpylist_fromvalue;
            break;
        }
        case tid_DATAGRAM_INT:
        default:
            FAILWITH(IERR_INVALID_STATE);
        }

    PyObject* final_py_value = py_value;
    if (wrap_py_value) {
        final_py_value = PyObject_CallFunctionObjArgs(
            ion_nature_constructor,
            py_ion_type_table[ion_type >> 8],
            py_value,
            py_annotations,
            NULL
        );
        Py_XDECREF(py_value);
    }

    ionc_add_to_container(container, final_py_value, parent_type, py_field_name);

fail:
    Py_XDECREF(py_annotations);
    // note: we're not actually increffing None when we have a field name that has
    // no text, which we technically _should_ be doing.
    // todo: consider increffing Py_None in ion_build_py_string
    if (py_field_name && py_field_name != Py_None) Py_DECREF(py_field_name);
    if (err) {
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
 *      parent_type: the type of the container to add to.
 *      value_model: Flags to control how Ion values map to Python types
 *
 */
iERR ionc_read_all(hREADER hreader, PyObject* container, enum ContainerType parent_type, uint8_t value_model) {
    iENTER;
    ION_TYPE t;
    for (;;) {
        IONCHECK(ion_reader_next(hreader, &t));
        if (t == tid_EOF) {
            assert(t == tid_EOF && "next() at end");
            break;
        }
        IONCHECK(ionc_read_value(hreader, t, container, parent_type, value_model));
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
    IONCHECK(ionc_read_value(reader, t, container, FALSE, iterator->value_model));
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
    uint8_t value_model = 0;
    PyObject *text_buffer_size_limit = NULL;
    ionc_read_Iterator *iterator = NULL;
    static char *kwlist[] = {"file", "value_model", "text_buffer_size_limit", NULL};
    // todo: this could be simpler and likely faster by converting to c types here.
    if (!PyArg_ParseTupleAndKeywords(args, kwds, IONC_READ_ARGS_FORMAT, kwlist, &py_file,
                                     &value_model, &text_buffer_size_limit)) {
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
    iterator->value_model = value_model;
    iterator->reader = NULL;
    memset(&iterator->_reader_options, 0, sizeof(iterator->_reader_options));
    iterator->_reader_options.decimal_context = &dec_context;
    if (text_buffer_size_limit != Py_None) {
        int symbol_threshold = PyLong_AsLong(text_buffer_size_limit);
        iterator->_reader_options.symbol_threshold = symbol_threshold;
    }

    IONCHECK(ion_reader_open_stream(
        &iterator->reader,
        &iterator->file_handler_state,
        ion_read_file_stream_handler,
        &iterator->_reader_options)); // NULL represents default reader options
    return (PyObject *)iterator;


fail:
    if (iterator != NULL) {
        // Since we've created an iterator, that means we have INCREF'd py_file, so correct that.
        Py_DECREF(py_file);
        Py_DECREF(iterator);
    }
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

    IONC_STREAM_BYTES_READ_SIZE = PyLong_FromLong(IONC_STREAM_READ_BUFFER_SIZE/4);

    // TODO is there a destructor for modules? these should be decreffed there
    _decimal_module             = PyImport_ImportModule("decimal");
    _decimal_constructor        = PyObject_GetAttrString(_decimal_module, "Decimal");
    _decimal_zero               = PyObject_CallFunction(_decimal_constructor, "i", 0, NULL);
    _simpletypes_module         = PyImport_ImportModule("amazon.ion.simple_types");

    _ionpynull_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyNull");
    _ionpybool_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyBool");
    _ionpyint_cls               = PyObject_GetAttrString(_simpletypes_module, "IonPyInt");
    _ionpyfloat_cls             = PyObject_GetAttrString(_simpletypes_module, "IonPyFloat");
    _ionpydecimal_cls           = PyObject_GetAttrString(_simpletypes_module, "IonPyDecimal");
    _ionpytimestamp_cls         = PyObject_GetAttrString(_simpletypes_module, "IonPyTimestamp");
    _ionpybytes_cls             = PyObject_GetAttrString(_simpletypes_module, "IonPyBytes");
    _ionpytext_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyText");
    _ionpysymbol_cls            = PyObject_GetAttrString(_simpletypes_module, "IonPySymbol");
    _ionpylist_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyList");
    _ionpydict_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyDict");
    _ionpystddict_cls           = PyObject_GetAttrString(_simpletypes_module, "IonPyStdDict");

    _ionpynull_fromvalue        = PyObject_GetAttrString(_ionpynull_cls, "from_value");
    _ionpybool_fromvalue        = PyObject_GetAttrString(_ionpybool_cls, "from_value");
    _ionpyint_fromvalue         = PyObject_GetAttrString(_ionpyint_cls, "from_value");
    _ionpyfloat_fromvalue       = PyObject_GetAttrString(_ionpyfloat_cls, "from_value");
    _ionpydecimal_fromvalue     = PyObject_GetAttrString(_ionpydecimal_cls, "from_value");
    _ionpytimestamp_fromvalue   = PyObject_GetAttrString(_ionpytimestamp_cls, "from_value");
    _ionpybytes_fromvalue       = PyObject_GetAttrString(_ionpybytes_cls, "from_value");
    _ionpytext_fromvalue        = PyObject_GetAttrString(_ionpytext_cls, "from_value");
    _ionpysymbol_fromvalue      = PyObject_GetAttrString(_ionpysymbol_cls, "from_value");
    _ionpylist_fromvalue        = PyObject_GetAttrString(_ionpylist_cls, "from_value");
    _ionpylist_factory          = PyObject_GetAttrString(_ionpylist_cls, "_factory");
    _ionpydict_factory          = PyObject_GetAttrString(_ionpydict_cls, "_factory");

    _ion_core_module            = PyImport_ImportModule("amazon.ion.core");
    _py_timestamp_precision     = PyObject_GetAttrString(_ion_core_module, "TimestampPrecision");
    _py_timestamp_cls           = PyObject_GetAttrString(_ion_core_module, "Timestamp");
    _py_timestamp__new__        = PyObject_GetAttrString(_py_timestamp_cls, "__new__");
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
    dec_context.digits = 10000;
    dec_context.emax = DEC_MAX_MATH;
    dec_context.emin = -DEC_MAX_MATH;
    ion_type_str = PyUnicode_FromString("ion_type");
    ion_annotations_str = PyUnicode_FromString("ion_annotations");
    text_str = PyUnicode_FromString("text");
    sid_str = PyUnicode_FromString("sid");
    precision_str = PyUnicode_FromString("precision");
    fractional_seconds_str = PyUnicode_FromString("fractional_seconds");
    exponent_str = PyUnicode_FromString("exponent");
    digits_str = PyUnicode_FromString("digits");
    fractional_precision_str = PyUnicode_FromString("fractional_precision");
    store_str = PyUnicode_FromString("_IonPyDict__store");

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
