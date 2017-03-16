#include "Python.h"
#include "datetime.h"
#include "_ioncmodule.h"

#define cRETURN RETURN(__location_name__, __line__, __count__++, err)

#define YEAR_PRECISION 0
#define MONTH_PRECISION 1
#define DAY_PRECISION 2
#define MINUTE_PRECISION 3
#define SECOND_PRECISION 4

#define MICROSECOND_DIGITS 6

#if PY_MAJOR_VERSION >= 3
    #define IONC_BYTES_FORMAT "y#"
    #define PyInt_AsSsize_t PyLong_AsSsize_t
    #define PyInt_AsLong PyLong_AsLong
#else
    #define IONC_BYTES_FORMAT "s#"
#endif

static int int_attr_by_name(PyObject* obj, char* attr_name) {
    // Gets an attribute as an int. NOTE: defaults to 0 if the attribute is None.
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

#if PY_VERSION_HEX < 0x02070000
    #define offset_seconds(x) offset_seconds_26(x)
#endif

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
static PyObject* _six_module; // TODO remove all these six dependencies once verified they're not needed.
static PyObject* _six_binary_type;
static PyObject* _six_text_type;
static PyObject* _exception_module;
static PyObject* _ion_exception_cls;
static decContext dec_context;  // TODO verify it's fine to share this for the lifetime of the module.

PyObject* helloworld(PyObject* self)
{
    return Py_BuildValue("s", "python extensions");
}

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

static void c_string_from_py(PyObject* str, char** out, Py_ssize_t* len_out) {
#if PY_MAJOR_VERSION >= 3
    // TODO does this need to work for binary types?
    *out = PyUnicode_AsUTF8AndSize(str, len_out);
#else
    // NOTE: This returns a string in the default encoding, which should be UTF8.
    PyString_AsStringAndSize(str, out, len_out);
#endif
}

static void ion_string_from_py(PyObject* str, ION_STRING* out) {
    char* c_str = NULL;
    Py_ssize_t c_str_len;
    c_string_from_py(str, &c_str, &c_str_len);
    ION_STRING_INIT(out);
    ion_string_assign_cstr(out, c_str, c_str_len);
}

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
        // TODO handle SymbolTokens as well as text
        PyObject* pyAnnotation = PySequence_Fast_GET_ITEM(annotations, i);
        Py_INCREF(pyAnnotation);
        ION_STRING annotation;
        ion_string_from_py(pyAnnotation, &annotation);
        IONCHECK(ion_writer_add_annotation(writer, &annotation));
        Py_DECREF(pyAnnotation);
    }
fail:
    Py_XDECREF(annotations);
    cRETURN;
}

static iERR ionc_write_value(hWRITER writer, PyObject* obj);

static iERR ionc_write_sequence(hWRITER writer, PyObject* sequence) {
    iENTER;
    PyObject* child_obj = NULL;
    sequence = PySequence_Fast(sequence, "expected sequence");
    Py_ssize_t len = PySequence_Size(sequence);
    Py_ssize_t i;
    for (i = 0; i < len; i++) {
        child_obj = PySequence_Fast_GET_ITEM(sequence, i);
        Py_INCREF(child_obj);
        IONCHECK(Py_EnterRecursiveCall(" while writing an Ion sequence"));
        err = ionc_write_value(writer, child_obj);
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

static iERR ionc_write_struct(hWRITER writer, PyObject* dict) {
    iENTER;
    PyObject *key = NULL, *child_obj = NULL;
    Py_ssize_t pos = 0;
    while (PyDict_Next(dict, &pos, &key, &child_obj)) {
        Py_INCREF(key);
        Py_INCREF(child_obj);
        ION_STRING field_name;
        ion_string_from_py(key, &field_name);
        // NOTE: the writer does NOT make a copy of the field name until it is flushed (which happens upon writing its
        // following value), making it important to INCREF the PyObject that holds the underlying buffer. This ensures
        // the object is not deallocated before it is flushed.
        IONCHECK(ion_writer_write_field_name(writer, &field_name));
        IONCHECK(Py_EnterRecursiveCall(" while writing an Ion struct"));
        err = ionc_write_value(writer, child_obj);
        Py_LeaveRecursiveCall();
        IONCHECK(err);
        Py_DECREF(key);
        key = NULL;
        Py_DECREF(child_obj);
        child_obj = NULL;
    }
fail:
    Py_XDECREF(key);
    Py_XDECREF(child_obj);
    cRETURN;
}

static iERR ionc_write_value(hWRITER writer, PyObject* obj) {
    iENTER;
    int ion_type = ion_type_from_py(obj);
    IONCHECK(ionc_write_annotations(writer, obj));
    if (PyList_Check(obj) || PyTuple_Check(obj)) {
        if (ion_type == tid_none_INT) {
            // TODO should tuple implicitly be SEXP for visual match?
            ion_type = tid_LIST_INT;
        }
        if (tid_LIST_INT != ion_type && tid_SEXP_INT != ion_type) {
            FAILWITHMSG(IERR_INVALID_ARG, "Found sequence; expected LIST or SEXP Ion type.");
        }

        IONCHECK(ion_writer_start_container(writer, (ION_TYPE)ion_type));
        IONCHECK(ionc_write_sequence(writer, obj));
        IONCHECK(ion_writer_finish_container(writer));
    }
    else if (PyDict_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_STRUCT_INT;
        }
        if (tid_STRUCT_INT != ion_type) {
            FAILWITHMSG(IERR_INVALID_ARG, "Found dict; expected STRUCT Ion type.");
        }

        IONCHECK(ion_writer_start_container(writer, (ION_TYPE)ion_type));
        IONCHECK(ionc_write_struct(writer, obj));
        IONCHECK(ion_writer_finish_container(writer));
    }
    //else if (PyObject_TypeCheck(obj, (PyTypeObject*)_six_text_type)) {
    else if (PyUnicode_Check(obj)) {
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
            FAILWITHMSG(IERR_INVALID_ARG, "Found text; expected STRING or SYMBOL Ion type.");
        }
    }
    else if (PyBool_Check(obj)) { // NOTE: this must precede the INT block because python bools are ints.
        if (ion_type == tid_none_INT) {
            ion_type = tid_BOOL_INT;
        }
        if (tid_BOOL_INT != ion_type) {
            FAILWITHMSG(IERR_INVALID_ARG, "Found bool; expected BOOL Ion type.");
        }
        BOOL bool_value;
        if (obj == Py_True)
            bool_value = TRUE;
        else
            bool_value = FALSE;
        IONCHECK(ion_writer_write_bool(writer, bool_value));
    }
    else if (
        #if PY_MAJOR_VERSION < 3 // TODO need to verify this works/is necessary for Python 2. Will PyLong_*() work with PyInt_Type?
            PyInt_Check(obj) ||
        #endif
            PyLong_Check(obj)
    ) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_INT_INT;
        }
        if (tid_INT_INT != ion_type) {
            FAILWITHMSG(IERR_INVALID_ARG, "Found int; expected INT Ion type.");
        }
        // TODO obviously only gets 64 bits... document as limitation. There are no APIs to write arbitrary-length ints.
        IONCHECK(ion_writer_write_long(writer, PyLong_AsLong(obj)));
    }
    else if (PyFloat_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_FLOAT_INT;
        }
        if (tid_FLOAT_INT != ion_type) {
            FAILWITHMSG(IERR_INVALID_ARG, "Found float; expected FLOAT Ion type.");
        }
        // TODO verify this works for nan/inf
        IONCHECK(ion_writer_write_double(writer, PyFloat_AsDouble(obj)));
    }
    else if (PyObject_TypeCheck(obj, (PyTypeObject*)_decimal_constructor)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_DECIMAL_INT;
        }
        if (tid_DECIMAL_INT != ion_type) {
            FAILWITHMSG(IERR_INVALID_ARG, "Found Decimal; expected DECIMAL Ion type.");
        }
        PyObject* decimal_str = PyObject_CallMethod(obj, "__str__", NULL); // TODO converting every decimal to string is slow.
        char* decimal_c_str = NULL;
        Py_ssize_t decimal_c_str_len;
        c_string_from_py(decimal_str, &decimal_c_str, &decimal_c_str_len);
        Py_DECREF(decimal_str);
        decQuad decimal_value;
        decQuadFromString(&decimal_value, decimal_c_str, &dec_context);
        IONCHECK(ion_writer_write_decimal(writer, &decimal_value));
    }
    //else if (PyObject_TypeCheck(obj, (PyTypeObject*)_six_binary_type)) {
    else if (PyBytes_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_BLOB_INT;
        }
        char* bytes = NULL;
        Py_ssize_t len;
        IONCHECK(PyBytes_AsStringAndSize(obj, &bytes, &len));  // TODO verify this works on Py 2. If not, use PyString_*.
        if (ion_type == tid_BLOB_INT) {
            IONCHECK(ion_writer_write_blob(writer, (BYTE*)bytes, len));
        }
        else if (ion_type == tid_CLOB_INT) {
            IONCHECK(ion_writer_write_clob(writer, (BYTE*)bytes, len));
        }
        else {
            FAILWITHMSG(IERR_INVALID_ARG, "Found binary data; expected BLOB or CLOB Ion type.");
        }
    }
    else if (PyDateTime_Check(obj)) {
        if (ion_type == tid_none_INT) {
            ion_type = tid_TIMESTAMP_INT;
        }
        if (tid_TIMESTAMP_INT != ion_type) {
            FAILWITHMSG(IERR_INVALID_ARG, "Found datetime; expected TIMESTAMP Ion type.");
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
                decNumberRescale(&helper, &helper, decNumberFromInt32(&dec_number_precision, -fractional_precision), &dec_context);
                if (decContextTestStatus(&dec_context, DEC_Inexact)) {
                    // This means the fractional component is not [0, 1) or has more than microsecond precision.
                    decContextClearStatus(&dec_context, DEC_Inexact);
                    FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Requested fractional timestamp precision results in data loss.");
                }
                decQuadFromNumber(&fraction, &helper, &dec_context);
                IONCHECK(ion_timestamp_for_fraction(&timestamp_value, year, month, day, hour, minute, second, &fraction, &dec_context));
            }
            else if (microsecond > 0) {
                FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Not enough fractional precision for timestamp.");
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
            FAILWITHMSG(IERR_INVALID_STATE, "Invalid timestamp precision.");
        }

        PyObject* offset_timedelta = PyObject_CallMethod(obj, "utcoffset", NULL);
        if (offset_timedelta != Py_None) {
            err = ion_timestamp_set_local_offset(&timestamp_value, offset_seconds(offset_timedelta) / 60);
        }
        Py_DECREF(offset_timedelta);
        IONCHECK(err);

        IONCHECK(ion_writer_write_timestamp(writer, &timestamp_value));

    }
    else {
        FAILWITH(IERR_INVALID_STATE);
    }
    // TODO all other types, else error
    iRETURN;
}

int _ionc_write(PyObject* obj, PyObject* binary, ION_STREAM* ion_stream) {
    iENTER;
    hWRITER writer;
    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.output_as_binary = PyObject_IsTrue(binary);

    IONCHECK(ion_writer_open(&writer, ion_stream, &options));
    IONCHECK(ionc_write_value(writer, obj));
    IONCHECK(ion_writer_close(writer));
    //IONCHECK(ion_stream_close(ion_stream)); // callers must close stream themselves
    iRETURN;
}

static PyObject *
ionc_write(PyObject *self, PyObject *args, PyObject *kwds)
{
    iENTER;

    PyObject *obj, *binary;
    ION_STREAM  *ion_stream = NULL;
    BYTE* buf = NULL;

    // TODO support sequence_as_stream
    static char *kwlist[] = {"obj", "binary", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", kwlist, &obj, &binary)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    Py_INCREF(obj);
    Py_INCREF(binary);
    IONCHECK(ion_stream_open_memory_only(&ion_stream));
    IONCHECK(_ionc_write(obj, binary, ion_stream));
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
    return written;
fail:
    PyMem_Free(buf);
    Py_DECREF(obj);
    Py_DECREF(binary);
    return PyErr_Format(_ion_exception_cls, "%s", ion_error_to_str(err));
}

static iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container, BOOL in_struct);

static iERR ionc_read_all(hREADER hreader, PyObject* container, BOOL in_struct) {
    iENTER;
    ION_TYPE t;
    for (;;) {
        IONCHECK(ion_reader_next(hreader, &t));
        if (t == tid_EOF) {
            // TODO IONC-4 does next() return tid_EOF or tid_none at end of stream?
            // See ion_parser_next where it returns tid_none
            assert(t == tid_EOF && "next() at end");
            break;
        }
        IONCHECK(ionc_read_value(hreader, t, container, in_struct));
    }
    iRETURN;
}

static PyObject* ion_build_py_string(ION_STRING* string_value) {
    // TODO Test non-ASCII compatibility.
    return PyUnicode_FromStringAndSize((char*)(string_value->value), string_value->length);
}

static void ionc_add_to_container(PyObject* pyContainer, PyObject* element, BOOL in_struct, ION_STRING* field_name) {
    if (in_struct) {
        // TODO assert field_name is not NULL
        PyDict_SetItem(pyContainer, ion_build_py_string(field_name), (PyObject*)element);
    }
    else {
        PyList_Append(pyContainer, (PyObject*)element);
    }
    Py_DECREF(element);  // TODO Not sure about this DECREF. Check
}

PyObject* ionc_read(PyObject* self, PyObject *args, PyObject *kwds) {
    iENTER;
    hREADER      reader;
    long         size;
    char        *buffer = NULL;
    PyObject* top_level_container = NULL;

    static char *kwlist[] = {"data", NULL};
    // TODO y# on Py3 won't work with unicode-type input, only bytes
    if (!PyArg_ParseTupleAndKeywords(args, kwds, IONC_BYTES_FORMAT, kwlist, &buffer, &size)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    // TODO what if size is larger than SIZE ?
    IONCHECK(ion_reader_open_buffer(&reader, (BYTE*)buffer, (SIZE)size, NULL)); // NULL represents default reader options
    top_level_container = PyList_New(0);
    IONCHECK(ionc_read_all(reader, top_level_container, FALSE));
    IONCHECK(ion_reader_close(reader));
    return top_level_container;
fail:
    Py_XDECREF(top_level_container); // TODO need to DECREF all of its children too?
    return PyErr_Format(_ion_exception_cls, "%s", ion_error_to_str(err));
}

static PyObject* ionc_get_timestamp_precision(int precision) {
    int precision_index = -1;
    while (precision) {
        precision_index++;
        precision = precision >> 1;
    }
    return py_ion_timestamp_precision_table[precision_index];
}

static iERR ionc_read_into_container(hREADER hreader, PyObject* container, BOOL is_struct) {
    iENTER;
    IONCHECK(ion_reader_step_in(hreader));
    IONCHECK(Py_EnterRecursiveCall(" while reading an Ion container"));
    err = ionc_read_all(hreader, container, is_struct);
    Py_LeaveRecursiveCall();
    IONCHECK(err);
    IONCHECK(ion_reader_step_out(hreader));
    iRETURN;
}

static iERR ionc_read_timestamp(hREADER hreader, PyObject** timestamp_out) {
    iENTER;
    ION_TIMESTAMP timestamp_value;
    PyObject* timestamp_args = NULL;
    IONCHECK(ion_reader_read_timestamp(hreader, &timestamp_value));
    int precision;
    IONCHECK(ion_timestamp_get_precision(&timestamp_value, &precision));
    if (precision < ION_TS_YEAR) {
        FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Found a timestamp with less than year precision."); //TODO have a FAILWITHMSG that actually surfaces the message.
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
        PyDict_SetItemString(timestamp_args, "off_hours", PyLong_FromLong(off_hours));
        PyDict_SetItemString(timestamp_args, "off_minutes", PyLong_FromLong(off_minutes));
    }
    switch (precision) {
    case ION_TS_FRAC:
    {
        decQuad fraction = timestamp_value.fraction;
        int32_t fractional_precision = decQuadGetExponent(&fraction);
        // TODO assert fractional_precision < 0
        fractional_precision = fractional_precision * -1;
        if (fractional_precision > MICROSECOND_DIGITS) {
            // Python only supports up to microsecond precision
            FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Timestamp fractional seconds cannot exceed microsecond precision.");
        }
        decQuad tmp;
        decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, MICROSECOND_DIGITS), &dec_context);
        int32_t microsecond = decQuadToInt32Exact(&fraction, &dec_context, DEC_ROUND_HALF_EVEN);
        if (decContextTestStatus(&dec_context, DEC_Inexact)) {
            // This means the fractional component is not [0, 1) or has more than microsecond precision.
            decContextClearStatus(&dec_context, DEC_Inexact);
            FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Timestamp fractional seconds must be in [0,1).");
        }
        PyDict_SetItemString(timestamp_args, "fractional_precision", PyLong_FromLong(fractional_precision));
        PyDict_SetItemString(timestamp_args, "microsecond", PyLong_FromLong(microsecond));
    }
    case ION_TS_SEC:
        PyDict_SetItemString(timestamp_args, "second", PyLong_FromLong(timestamp_value.seconds));
    case ION_TS_MIN:
        PyDict_SetItemString(timestamp_args, "minute", PyLong_FromLong(timestamp_value.minutes));
        PyDict_SetItemString(timestamp_args, "hour", PyLong_FromLong(timestamp_value.hours));
    case ION_TS_DAY:
        PyDict_SetItemString(timestamp_args, "day", PyLong_FromLong(timestamp_value.day));
    case ION_TS_MONTH:
        PyDict_SetItemString(timestamp_args, "month", PyLong_FromLong(timestamp_value.month));
    case ION_TS_YEAR:
        PyDict_SetItemString(timestamp_args, "year", PyLong_FromLong(timestamp_value.year));
        break;
    }
    *timestamp_out = PyObject_Call(_py_timestamp_constructor, PyTuple_New(0), timestamp_args);

fail:
    Py_XDECREF(timestamp_args);
    cRETURN;
}

static iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container, BOOL in_struct) {
    iENTER;

    BOOL        emit_bare_values = TRUE; // TODO allow this config option to be passed in, initialize here. This allows pure python values to be emitted when they type is unambiguous and the ion value is unannotated.

    BOOL        is_null;
    ION_STRING  field_name;
    SIZE        annotation_count;
    PyObject*   py_annotations = NULL;
    PyObject*   py_value = NULL;
    PyObject*   ion_nature_constructor = NULL;

    if (in_struct) {
        IONCHECK(ion_reader_get_field_name(hreader, &field_name));
    }

    IONCHECK(ion_reader_get_annotation_count(hreader, &annotation_count));
    if (annotation_count > 0) {
        emit_bare_values = FALSE;
        // TODO for speed, could have a max number of annotations allowed, then reuse a static array.
        ION_STRING* annotations = (ION_STRING*)PyMem_Malloc(annotation_count * sizeof(ION_STRING));
        err = ion_reader_get_annotations(hreader, annotations, annotation_count, &annotation_count);
        if (err) {
            PyMem_Free(annotations);
            IONCHECK(err);
        }
        py_annotations = PyTuple_New(annotation_count);
        int i;
        for (i = 0; i < annotation_count; i++) {
            PyTuple_SetItem(py_annotations, i, ion_build_py_string(&annotations[i]));
        }
        PyMem_Free(annotations);
    }

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
        IONCHECK(ion_reader_read_null(hreader, &null_type));
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
        int64_t ion_int64;
        err = ion_reader_read_int64(hreader, &ion_int64);
        if (err == IERR_NUMERIC_OVERFLOW) {
            err = 0;
            ION_INT ion_int_value;
            IONCHECK(ion_int_init(&ion_int_value, hreader));
            IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));
            SIZE int_char_len, int_char_written;
            IONCHECK(ion_int_char_length(&ion_int_value, &int_char_len));
            char* ion_int_str = (char*)PyMem_Malloc(int_char_len);
            err = ion_int_to_char(&ion_int_value, (BYTE*)ion_int_str, int_char_len, &int_char_written);
            if (err) {
                PyMem_Free(ion_int_str);
                IONCHECK(err);
            }
            if (int_char_len != int_char_written) {
                PyMem_Free(ion_int_str);
                FAILWITHMSG(IERR_BUFFER_TOO_SMALL, "Not enough space given to represent int as string.");
            }
            py_value = PyLong_FromString(ion_int_str, NULL, 10);
            PyMem_Free(ion_int_str);
        }
        else {
            IONCHECK(err);
            py_value = Py_BuildValue("i", ion_int64);
        }
        ion_nature_constructor = _ionpyint_fromvalue;
        break;
    }
    case tid_FLOAT_INT:
    {
        // TODO verify nans
        double double_value;
        IONCHECK(ion_reader_read_double(hreader, &double_value));
        py_value = Py_BuildValue("d", double_value);
        ion_nature_constructor = _ionpyfloat_fromvalue;
        break;
    }
    case tid_DECIMAL_INT:
    {
        decQuad decimal_value;
        IONCHECK(ion_reader_read_decimal(hreader, &decimal_value));
        // TODO the max length must be retrieved from somewhere authoritative, or a different technique must be used.
        char dec_str[41];
        py_value = PyObject_CallFunction(_decimal_constructor, "s", decQuadToString(&decimal_value, dec_str));
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
        // intentional fall-through
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
        IONCHECK(ion_reader_get_lob_size(hreader, &length));
        BYTE *buf = (BYTE*)PyMem_Malloc((size_t)length);
        err = ion_reader_read_lob_bytes(hreader, buf, length, &bytes_read);
        if (err) {
            PyMem_Free(buf);
            IONCHECK(err);
        }
        if (length != bytes_read) {
            PyMem_Free(buf);
            FAILWITH(IERR_EOF);
        }
        py_value = Py_BuildValue(IONC_BYTES_FORMAT, (char*)buf, length);
        PyMem_Free(buf);
        ion_nature_constructor = _ionpybytes_fromvalue;
        break;
    }
    case tid_STRUCT_INT:
        py_value = PyDict_New();
        IONCHECK(ionc_read_into_container(hreader, py_value, /*is_struct=*/TRUE));
        ion_nature_constructor = _ionpydict_fromvalue;
        break;
    case tid_SEXP_INT:
    {
        emit_bare_values = FALSE; // Sexp values must always be emitted as IonNature because of ambiguity with list.
        // intentional fall-through
    }
    case tid_LIST_INT:
        py_value = PyList_New(0);
        IONCHECK(ionc_read_into_container(hreader, py_value, /*is_struct=*/FALSE));
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
    ionc_add_to_container(container, py_value, in_struct, &field_name);

fail:
    if (err) {
        Py_XDECREF(py_annotations);
        Py_XDECREF(py_value);
    }
    cRETURN;
}

static char ioncmodule_docs[] =
    "C extension module for ion-c.\n";

static PyMethodDef ioncmodule_funcs[] = {
    {"helloworld", (PyCFunction)helloworld, METH_NOARGS, ioncmodule_docs},
    {"ionc_write", (PyCFunction)ionc_write, METH_VARARGS | METH_KEYWORDS, ioncmodule_docs}, // TODO still think this should be PyCFunctionWithKeywords...
    {"ionc_read", (PyCFunction)ionc_read, METH_VARARGS | METH_KEYWORDS, ioncmodule_docs},
    {NULL}
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "ionc",       /* m_name */
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
    m = Py_InitModule3("ionc", ioncmodule_funcs,
                   "Extension module example!");
#endif
    // TODO is there a destructor for modules? These should be decreffed there
    _decimal_module             = PyImport_ImportModule("decimal");
    _decimal_constructor        = PyObject_GetAttrString(_decimal_module, "Decimal");  // TODO or use PyInstance_New?
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
    _ionpylist_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyList");
    _ionpylist_fromvalue        = PyObject_GetAttrString(_ionpylist_cls, "from_value");
    _ionpydict_cls              = PyObject_GetAttrString(_simpletypes_module, "IonPyDict");
    _ionpydict_fromvalue        = PyObject_GetAttrString(_ionpydict_cls, "from_value");

    _ion_core_module            = PyImport_ImportModule("amazon.ion.core");
    _py_timestamp_precision     = PyObject_GetAttrString(_ion_core_module, "TimestampPrecision");
    _py_timestamp_constructor   = PyObject_GetAttrString(_ion_core_module, "timestamp");
    _py_ion_type                = PyObject_GetAttrString(_ion_core_module, "IonType");

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
    py_ion_type_table[0xB] = PyObject_GetAttrString(_py_ion_type, "STRUCT");
    py_ion_type_table[0xC] = PyObject_GetAttrString(_py_ion_type, "LIST");
    py_ion_type_table[0xD] = PyObject_GetAttrString(_py_ion_type, "SEXP");

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
    c_ion_type_table[0xA] = tid_SEXP_INT;
    c_ion_type_table[0xB] = tid_LIST_INT;
    c_ion_type_table[0xC] = tid_STRUCT_INT;

    py_ion_timestamp_precision_table[0] = PyObject_GetAttrString(_py_timestamp_precision, "YEAR");
    py_ion_timestamp_precision_table[1] = PyObject_GetAttrString(_py_timestamp_precision, "MONTH");
    py_ion_timestamp_precision_table[2] = PyObject_GetAttrString(_py_timestamp_precision, "DAY");
    py_ion_timestamp_precision_table[3] = NULL; // Impossible; there is no hour precision.
    py_ion_timestamp_precision_table[4] = PyObject_GetAttrString(_py_timestamp_precision, "MINUTE");
    py_ion_timestamp_precision_table[5] = PyObject_GetAttrString(_py_timestamp_precision, "SECOND");
    py_ion_timestamp_precision_table[6] = PyObject_GetAttrString(_py_timestamp_precision, "SECOND");

    _six_module = PyImport_ImportModule("six");
    _six_binary_type = PyObject_GetAttrString(_six_module, "binary_type");
    _six_text_type = PyObject_GetAttrString(_six_module, "text_type");

    _exception_module   = PyImport_ImportModule("amazon.ion.exceptions");
    _ion_exception_cls  = PyObject_GetAttrString(_exception_module, "IonException");

    decContextDefault(&dec_context, DEC_INIT_DECQUAD);  // TODO The writer already has one of these, but it's private...
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