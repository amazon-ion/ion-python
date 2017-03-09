#include "Python.h"
#include "_ioncmodule.h"

static PyObject* _decimal_module;
static PyObject* _decimal_constructor;
static PyObject* _py_timestamp_constructor;
static PyObject* _simpletypes_module;
static PyObject* _ion_nature_cls;
static PyObject* _ionstring_fromvalue;
static PyObject* _ion_core_module;
static PyObject* _py_ion_type;
static PyObject* py_ion_type_table[14];
static PyObject* _py_timestamp_precision;
static PyObject* py_ion_timestamp_precision_table[7];
static PyObject* _exception_module;
static PyObject* _ion_exception_cls;
static decContext dec_context;  // TODO verify it's fine to share this for the lifetime of the module.

PyObject* helloworld(PyObject* self)
{
    return Py_BuildValue("s", "python extensions");
}

static void ion_type_from_py(PyObject* obj, ION_TYPE* out) {
    PyObject* ion_type = NULL;
    if (PyObject_HasAttrString(obj, "ion_type")) {
        ion_type = PyObject_GetAttrString(obj, "ion_type");
    }
    if (ion_type == NULL) return;
    *out = (ION_TYPE)(PyLong_AsSsize_t(ion_type) << 8);
}

static void c_string_from_py(PyObject* str, char** out, Py_ssize_t* len_out) {
#if PY_MAJOR_VERSION >= 3
    // TODO does this need to work for binary types?
    *out = PyUnicode_AsUTF8AndSize(str, len_out);
#else
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

static iERR write_annotations(hWRITER* writer, PyObject* obj) {
    iENTER;
    PyObject* annotations = NULL;
    if (PyObject_HasAttrString(obj, "ion_annotations")) {
        annotations = PyObject_GetAttrString(obj, "ion_annotations");
    }
    if (annotations == NULL) SUCCEED();
    annotations = PySequence_Fast(annotations, "expected sequence");
    Py_ssize_t len = PySequence_Size(annotations);
    Py_ssize_t i;
    for (i = 0; i < len; i++) {
        // TODO handle SymbolTokens as well as text
        PyObject* pyAnnotation = PySequence_Fast_GET_ITEM(annotations, i);
        ION_STRING annotation;
        ion_string_from_py(pyAnnotation, &annotation);
        IONCHECK(ion_writer_add_annotation(*writer, &annotation));
    }
    iRETURN;
}

// TODO no need to pass around an hWRITER pointer... it's already an opaque handle (void *)
static iERR ionc_write_value(hWRITER* writer, PyObject* obj) {
    iENTER;
    ION_TYPE* ion_type = NULL;
    ion_type_from_py(obj, ion_type);
    IONCHECK(write_annotations(writer, obj));
    if (PyObject_TypeCheck(obj, &PyList_Type) || PyObject_TypeCheck(obj, &PyTuple_Type)) {
        if (ion_type == NULL) {
            ION_TYPE type = tid_LIST; // TODO should tuple implicitly be SEXP for visual match?
            ion_type = &type;
        }

        IONCHECK(ion_writer_start_container(*writer, *ion_type));
        obj = PySequence_Fast(obj, "expected sequence");
        Py_ssize_t len = PySequence_Size(obj);
        Py_ssize_t i;
        for (i = 0; i < len; i++) {
            PyObject* child_obj = PySequence_Fast_GET_ITEM(obj, i);
            IONCHECK(Py_EnterRecursiveCall(" while writing an Ion sequence"));
            IONCHECK(ionc_write_value(writer, child_obj));
            Py_LeaveRecursiveCall();
        }
        IONCHECK(ion_writer_finish_container(*writer));
    }
    else if (PyObject_TypeCheck(obj, &PyDict_Type)) {
        if (ion_type == NULL) {
            ION_TYPE type = tid_STRUCT;
            ion_type = &type;
        }
        else {
            // TODO if ion_type is present, assert it is STRUCT
        }

        IONCHECK(ion_writer_start_container(*writer, *ion_type));
        PyObject *key, *child_obj;
        Py_ssize_t pos = 0;
        while (PyDict_Next(obj, &pos, &key, &child_obj)) {
            ION_STRING field_name;
            ion_string_from_py(key, &field_name);
            IONCHECK(ion_writer_write_field_name(*writer, &field_name));
            IONCHECK(Py_EnterRecursiveCall(" while writing an Ion struct"));
            IONCHECK(ionc_write_value(writer, child_obj));
            Py_LeaveRecursiveCall();
        }
        IONCHECK(ion_writer_finish_container(*writer));
    }
    else if (PyUnicode_Check(obj) || PyObject_TypeCheck(obj, &PyBytes_Type)) {
        if (ion_type == NULL) {
            ION_TYPE type = tid_STRING;
            ion_type = &type;
        }
        ION_STRING string_value;
        ion_string_from_py(obj, &string_value);
        IONCHECK(ion_writer_write_string(*writer, &string_value));
    }
    else if (PyBool_Check(obj)) { // NOTE: this must precede the INT block because python bools are ints.
        if (ion_type == NULL) {
            ION_TYPE type = tid_BOOL;
            ion_type = &type;
        }
        BOOL bool_value;
        if (obj == Py_True)
            bool_value = TRUE;
        else
            bool_value = FALSE;
        IONCHECK(ion_writer_write_bool(*writer, bool_value));
    }
    else if (
        #if PY_MAJOR_VERSION < 3 // TODO need to verify this works/is necessary for Python 2. Will PyLong_*() work with PyInt_Type?
            PyObject_TypeCheck(obj, &PyInt_Type) ||
        #endif
            PyObject_TypeCheck(obj, &PyLong_Type) // TODO or just use PyLong_Check ?
    ) {
        if (ion_type == NULL) {
            ION_TYPE type = tid_INT;
            ion_type = &type;
        }
        // TODO obviously only gets 64 bits... document as limitation. There are no APIs to write arbitrary-length ints.
        IONCHECK(ion_writer_write_long(*writer, PyLong_AsLong(obj)));
    }
    else if (PyFloat_Check(obj)) {
        if (ion_type == NULL) {
            ION_TYPE type = tid_FLOAT;
            ion_type = &type;
        }
        // TODO verify this works for nan/inf
        IONCHECK(ion_writer_write_double(*writer, PyFloat_AsDouble(obj)));
    }
    else if (PyObject_TypeCheck(obj, (PyTypeObject*)_decimal_constructor)) {
         if (ion_type == NULL) {
            ION_TYPE type = tid_DECIMAL;
            ion_type = &type;
        }
        PyObject* decimal_str = PyObject_CallMethod(obj, "__str__", NULL); // TODO converting every decimal to string is slow.
        char* decimal_c_str = NULL;
        Py_ssize_t decimal_c_str_len;
        c_string_from_py(decimal_str, &decimal_c_str, &decimal_c_str_len);
        decQuad decimal_value;
        decQuadFromString(&decimal_value, decimal_c_str, &dec_context);
        IONCHECK(ion_writer_write_decimal(*writer, &decimal_value));
    }
    // TODO all other types, else error
    iRETURN;
}

int _ionc_write(PyObject* obj, PyObject* binary, ION_STREAM* f_ion_stream) {
    iENTER;
    hWRITER writer;
    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.output_as_binary = PyObject_IsTrue(binary);

    IONCHECK(ion_writer_open(&writer, f_ion_stream, &options));
    IONCHECK(ionc_write_value(&writer, obj));
    // TODO is manual flush needed?
    IONCHECK(ion_writer_close(writer));
    //IONCHECK(ion_stream_close(f_ion_stream)); // callers must close stream themselves
    iRETURN;
}

static PyObject *
ionc_write(PyObject *self, PyObject *args, PyObject *kwds)
{
    iENTER;

    PyObject *obj, *binary;
    ION_STREAM  *f_ion_stream = NULL;
    FILE        *fstream = NULL;
    BYTE* buf = NULL;

    // TODO support sequence_as_stream
    static char *kwlist[] = {"obj", "binary", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", kwlist, &obj, &binary)) {
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(ion_stream_open_memory_only(&f_ion_stream));
    IONCHECK(_ionc_write(obj, binary, f_ion_stream));
    POSITION len = ion_stream_get_position(f_ion_stream);
    IONCHECK(ion_stream_seek(f_ion_stream, 0));
    // TODO if len > max int32, need to return more than one page...
    buf = (BYTE*)(PyMem_Malloc((size_t)len));
    SIZE bytes_read;
    IONCHECK(ion_stream_read(f_ion_stream, buf, (SIZE)len, &bytes_read));

    IONCHECK(ion_stream_close(f_ion_stream));
    if (bytes_read != (SIZE)len) {
        FAILWITH(IERR_EOF);
    }
    // TODO Py_BuildValue copies all bytes... Can a memoryview over the original bytes be returned, avoiding the copy?
    PyObject* written = Py_BuildValue("y#", (char*)buf, bytes_read);
    PyMem_Free(buf);
    return written;
    fail:
        if (buf) {
            PyMem_Free(buf);
        }
        return PyErr_Format(_ion_exception_cls, "%s", ion_error_to_str(err));
}

static iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container, BOOL in_struct);

static iERR ionc_read_all(hREADER hreader, PyObject* container, BOOL in_struct) {
    iENTER;
    ION_TYPE t, t2;
    BOOL     more;
    for (;;) {
        IONCHECK(ion_reader_next(hreader, &t));
        if (t == tid_EOF) {
            // TODO IONC-4 does next() return tid_EOF or tid_none at end of stream?
            // See ion_parser_next where it returns tid_none
            assert(t == tid_EOF && "next() at end");
            more = FALSE;
        }
        else {
            more = TRUE;
        }

        IONCHECK(ion_reader_get_type(hreader, &t2));

        if (!more) break;


        IONCHECK(ionc_read_value(hreader, t, container, in_struct));
    }
    iRETURN;
}

static PyObject* ion_build_py_string(ION_STRING* string_value) {
    // TODO PyUnicode_FromKindAndData is new in 3.3. Find alternative for 2.x. Also check for non-ASCII compatibility.
    return PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, string_value->value, string_value->length);
}

static void ionc_add_to_container(PyObject* pyContainer, PyObject* element, BOOL in_struct, ION_STRING* field_name) {
    if (in_struct) {
        // TODO assert field_name is not NULL
        PyDict_SetItem(pyContainer, ion_build_py_string(field_name), (PyObject*)element);
    }
    else {
        PyList_Append(pyContainer, (PyObject*)element);
    }
    Py_DECREF(element);
}

PyObject* ionc_read(PyObject* self, PyObject *args, PyObject *kwds) {
    iENTER;
    FILE        *fstream = NULL;
    ION_STREAM  *f_ion_stream = NULL;
    hREADER      reader;
    long         size;
    char        *buffer = NULL;
    long         result;

    static char *kwlist[] = {"data", NULL};
    // TODO y# won't work with unicode-type input, only bytes
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#", kwlist, &buffer, &size)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    // TODO what if size is larger than SIZE ?
    IONCHECK(ion_reader_open_buffer(&reader, (BYTE*)buffer, (SIZE)size, NULL)); // NULL represents default reader options
    PyObject* top_level_container = PyList_New(0);
    IONCHECK(ionc_read_all(reader, top_level_container, FALSE));
    IONCHECK(ion_reader_close(reader));
    //IONCHECK(ion_stream_close(f_ion_stream));
    return top_level_container;
fail:
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

#define MICROSECOND_PRECISION = 6

static iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container, BOOL in_struct) {
    iENTER;

    ION_TYPE    ion_type;
    BOOL        is_null;
    BOOL        bool_value;
    ION_INT     ion_int_value;
    double      double_value;
    decQuad     decimal_value;
    ION_TIMESTAMP timestamp_value;
    SID         sid;
    ION_STRING  string_value, field_name, *indirect_string_value = NULL;
    SIZE        length, remaining;
    BYTE        *buf = NULL;
    hSYMTAB     hsymtab = 0;
    PyObject*   child_container = NULL;
    BOOL        child_is_struct = FALSE;
    BOOL        has_annotations;
    PyObject*   py_annotations = NULL;

    if (in_struct) {
        IONCHECK(ion_reader_get_field_name(hreader, &field_name));
    }

    IONCHECK(ion_reader_has_any_annotations(hreader, &has_annotations));
    if (has_annotations) {
        // TODO this is untested because I don't think ion_reader_get_annotations is correct. No testing of it in ionc.
        // TODO max number of annotations should be something less arbitrary than "100"
        ION_STRING* annotations = NULL;
        SIZE        annotations_len = 0;
        IONCHECK(ion_reader_get_annotations(hreader, annotations, 100, &annotations_len));
        py_annotations = PyTuple_New(annotations_len);
        int i;
        for (i = 0; i < annotations_len; i++) {
            PyTuple_SetItem(py_annotations, i, ion_build_py_string(&annotations[i]));
        }
    }

    IONCHECK(ion_reader_is_null(hreader, &is_null));
    if (is_null) {
        t = tid_NULL;
    }

    switch (ION_TYPE_INT(t)) {
    case tid_EOF_INT:
        // do nothing
        break;
    case tid_NULL_INT:
        IONCHECK(ion_reader_read_null(hreader, &ion_type));
        break;
    case tid_BOOL_INT:
        IONCHECK(ion_reader_read_bool(hreader, &bool_value));
        ionc_add_to_container(container, PyBool_FromLong(bool_value), in_struct, &field_name);
        break;
    case tid_INT_INT:
    {
        PyObject* py_int = NULL;
        int64_t ion_int64;
        err = ion_reader_read_int64(hreader, &ion_int64);
        if (err == IERR_NUMERIC_OVERFLOW) {
            err = 0;
            IONCHECK(ion_int_init(&ion_int_value, hreader));
            IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));
            SIZE int_char_len, int_char_written;
            IONCHECK(ion_int_char_length(&ion_int_value, &int_char_len));
            char* ion_int_str = (char*)PyMem_Malloc(int_char_len);
            IONCHECK(ion_int_to_char(&ion_int_value, (BYTE*)ion_int_str, int_char_len, &int_char_written));
            if (int_char_len != int_char_written) {
                FAILWITHMSG(IERR_BUFFER_TOO_SMALL, "Not enough space given to represent int as string.");
            }
            py_int = PyLong_FromString(ion_int_str, NULL, 10);
            PyMem_Free(ion_int_str);
        }
        else {
            IONCHECK(err);
            py_int = Py_BuildValue("i", ion_int64);
        }
        ionc_add_to_container(container, py_int, in_struct, &field_name);
        break;
    }
    case tid_FLOAT_INT:
        // TODO verify nans
        IONCHECK(ion_reader_read_double(hreader, &double_value));
        ionc_add_to_container(container, Py_BuildValue("d", double_value), in_struct, &field_name);
        break;
    case tid_DECIMAL_INT:
        IONCHECK(ion_reader_read_decimal(hreader, &decimal_value));
        // TODO the max length must be retrieved from somewhere authoritative, or a different technique must be used.
        char dec_str[41];
        PyObject* pyDecimal = PyObject_CallFunction(_decimal_constructor, "s", decQuadToString(&decimal_value, dec_str));
        ionc_add_to_container(container, pyDecimal, in_struct, &field_name);
        break;
    case tid_TIMESTAMP_INT:
        IONCHECK(ion_reader_read_timestamp(hreader, &timestamp_value));
        int precision;
        IONCHECK(ion_timestamp_get_precision(&timestamp_value, &precision));
        if (precision < ION_TS_YEAR) {
            FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Found a timestamp with less than year precision.");
        }
        PyObject* timestamp_args = PyDict_New();
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
            if (fractional_precision > 6) {  // TODO why can't I use MICROSECOND_PRECISION macro here?
                // Python only supports up to microsecond precision
                FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Timestamp fractional seconds cannot exceed microsecond precision.");
            }
            decQuad tmp;
            decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, 6), &dec_context);
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
        PyObject* py_timestamp = PyObject_Call(_py_timestamp_constructor, PyTuple_New(0), timestamp_args);
        ionc_add_to_container(container, py_timestamp, in_struct, &field_name);
        break;
    case tid_STRING_INT:
        IONCHECK(ion_reader_read_string(hreader, &string_value));
        //PyObject* string_ionnature = PyObject_CallFunction(_ionstring_fromvalue, "OO", py_ion_type_table[tid_STRING_INT >> 8], ion_build_py_string(&string_value));
        PyObject* py_string;
        if (has_annotations) {
            ionc_add_to_container(container, Py_BuildValue("s", "FOUND ANNOTATION"), in_struct, &field_name); // TODO REMOVE; DEBUGGING
            py_string = PyObject_CallFunctionObjArgs(_ionstring_fromvalue, py_ion_type_table[tid_STRING_INT >> 8], ion_build_py_string(&string_value), py_annotations, NULL);
        }
        else {
            // TODO this is an optimization, avoiding creating IonNature unless annotations. This should be explored as a
            // potential configurable option. It seems to save quite a lot of time.
            py_string = ion_build_py_string(&string_value);
        }
        ionc_add_to_container(container, py_string, in_struct, &field_name);
        break;
    case tid_SYMBOL_INT:
        //IONCHECK(ion_reader_read_symbol_sid(hreader, &sid));
        //IONCHECK(ion_reader_get_symbol_table(hreader, &hsymtab));
        //IONCHECK(ion_symbol_table_find_by_sid(hsymtab, sid, &indirect_string_value));

        IONCHECK(ion_reader_read_string(hreader, &string_value));
        ionc_add_to_container(container, ion_build_py_string(&string_value), in_struct, &field_name);
        break;
    case tid_CLOB_INT:
    case tid_BLOB_INT:
        IONCHECK(ion_reader_get_lob_size(hreader, &length));
        buf = (BYTE*)PyMem_Malloc((size_t)length);
        SIZE bytes_read;
        IONCHECK(ion_reader_read_lob_bytes(hreader, buf, length, &bytes_read));
        if (length != bytes_read) {
            PyMem_Free(buf);
            FAILWITH(IERR_EOF);
        }
        ionc_add_to_container(container, Py_BuildValue("y#", (char*)buf, length), in_struct, &field_name);
        PyMem_Free(buf);
        break;
    case tid_STRUCT_INT:
        child_is_struct = TRUE;
        child_container = PyDict_New();
    case tid_LIST_INT:
    case tid_SEXP_INT:
        if (!child_is_struct) {
            child_container = PyList_New(0);
        }
        IONCHECK(ion_reader_step_in(hreader));
        IONCHECK(Py_EnterRecursiveCall(" while reading an Ion container"));
        IONCHECK(ionc_read_all(hreader, child_container, child_is_struct));
        Py_LeaveRecursiveCall();
        IONCHECK(ion_reader_step_out(hreader));
        ionc_add_to_container(container, child_container, in_struct, &field_name);
        break;

    case tid_DATAGRAM_INT:
    default:
        break;
    }
    iRETURN;
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
    PyObject* m;
#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("ionc", ioncmodule_funcs,
                   "Extension module example!");
#endif
    // TODO is there a destructor for modules? These should be decreffed there
    _decimal_module = PyImport_ImportModule("decimal");
    _decimal_constructor = PyObject_GetAttrString(_decimal_module, "Decimal");  // TODO or use PyInstance_New?
    _simpletypes_module = PyImport_ImportModule("amazon.ion.simple_types");
    _ion_nature_cls = PyObject_GetAttrString(_simpletypes_module, "IonPyText");  // TODO one of these for each type...
    _ionstring_fromvalue = PyObject_GetAttrString(_ion_nature_cls, "from_value");  // TODO one of these for each type...
    _ion_core_module = PyImport_ImportModule("amazon.ion.core");
    _py_ion_type = PyObject_GetAttrString(_ion_core_module, "IonType");
    _py_timestamp_precision = PyObject_GetAttrString(_ion_core_module, "TimestampPrecision");
    _py_timestamp_constructor = PyObject_GetAttrString(_ion_core_module, "timestamp");
    _exception_module = PyImport_ImportModule("amazon.ion.exceptions");
    _ion_exception_cls = PyObject_GetAttrString(_exception_module, "IonException");

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

    py_ion_timestamp_precision_table[0] = PyObject_GetAttrString(_py_timestamp_precision, "YEAR");
    py_ion_timestamp_precision_table[1] = PyObject_GetAttrString(_py_timestamp_precision, "MONTH");
    py_ion_timestamp_precision_table[2] = PyObject_GetAttrString(_py_timestamp_precision, "DAY");
    py_ion_timestamp_precision_table[3] = NULL; // Impossible; there is no hour precision.
    py_ion_timestamp_precision_table[4] = PyObject_GetAttrString(_py_timestamp_precision, "MINUTE");
    py_ion_timestamp_precision_table[5] = PyObject_GetAttrString(_py_timestamp_precision, "SECOND");
    py_ion_timestamp_precision_table[6] = PyObject_GetAttrString(_py_timestamp_precision, "SECOND");


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