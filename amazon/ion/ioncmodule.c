#include "Python.h"
#include "_ioncmodule.h"

static PyObject* _decimal_module;
static PyObject* _decimal_constructor;
static PyObject* _simpletypes_module;
static PyObject* _ion_nature_cls;
static PyObject* _ionstring_fromvalue;
static PyObject* _ion_core_module;
static PyObject* _py_ion_type;
static PyObject* py_ion_type_table[14];

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

static void ion_string_from_py(PyObject* str, ION_STRING* out) {
    char* c_str = NULL;
    Py_ssize_t c_str_len;
#if PY_MAJOR_VERSION >= 3
    // TODO does this need to work for binary types?
    c_str = PyUnicode_AsUTF8AndSize(str, &c_str_len);
#else
    PyString_AsStringAndSize(str, &c_str, &c_str_len);
#endif
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
            IONCHECK(ionc_write_value(writer, child_obj));
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
            IONCHECK(ionc_write_value(writer, child_obj));
        }
        IONCHECK(ion_writer_finish_container(*writer));
    }
    else if (PyObject_TypeCheck(obj, &PyUnicode_Type) || PyObject_TypeCheck(obj, &PyBytes_Type)){
        if (ion_type == NULL) {
            ION_TYPE type = tid_STRING;
            ion_type = &type;
        }
        ION_STRING string_value;
        // TODO uncomment
        ion_string_from_py(obj, &string_value);
        // TODO REMOVE - TESTING
        //ION_STRING_INIT(&string_value);
        //ion_string_assign_cstr(&string_value, "abc", 3);
        // TODO END REMOVE
        IONCHECK(ion_writer_write_string(*writer, &string_value));
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
    IONCHECK(ion_stream_close(f_ion_stream));
    iRETURN;
}

static PyObject *
ionc_write(PyObject *self, PyObject *args, PyObject *kwds)
{
    iENTER;

    PyObject *obj, *binary;
    ION_STREAM  *f_ion_stream = NULL;
    FILE        *fstream = NULL;

    // TODO support sequence_as_stream
    static char *kwlist[] = {"obj", "binary", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", kwlist, &obj, &binary)) {
        err = -1;
        goto fail;
    }

    char *pathname = "/Users/greggt/Desktop/ionc_out.ion";

    fstream = fopen(pathname, "wb");
    if (!fstream) {
        printf("\nERROR: can't open file %s\n", pathname);
        goto fail;
    }
    IONCHECK(ion_stream_open_file_out(fstream, &f_ion_stream));
    IONCHECK(_ionc_write(obj, binary, f_ion_stream));

    return Py_BuildValue("s", NULL);
    fail:
        // TODO raise IonException.
        return Py_BuildValue("s", "ERROR");
}

#define TEMP_BUF_SIZE 0x10000
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


        ionc_read_value(hreader, t, container, in_struct);
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

static PyObject* ionc_read(PyObject* self) {
    iENTER;
    FILE        *fstream = NULL;
    ION_STREAM  *f_ion_stream = NULL;
    hREADER      reader;
    long         size;
    char        *buffer;
    long         result;
    char        *pathname = "/Users/greggt/Desktop/generated_short.10n";

    fstream = fopen(pathname, "rb");
    if (!fstream) {
        printf("\nERROR: can't open file %s\n", pathname);
        goto fail;
    }

    IONCHECK(ion_stream_open_file_in(fstream, &f_ion_stream));
    IONCHECK(ion_reader_open(&reader, f_ion_stream, NULL));
    PyObject* top_level_container = PyList_New(0);
    IONCHECK(ionc_read_all(reader, top_level_container, FALSE));
    IONCHECK(ion_reader_close(reader));
    IONCHECK(ion_stream_close(f_ion_stream));
    return top_level_container;
fail:
    // TODO raise IonException.
    return Py_BuildValue("s", NULL);
}

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
    BYTE        buf[TEMP_BUF_SIZE];
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
        break;
    case tid_INT_INT:
        IONCHECK(ion_int_init(&ion_int_value, hreader));
        IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));
        // IONCHECK(ion_reader_read_int64(hreader, &long_value));
        // TODO this caps ints at 64 bits... is there a way to preserve precision?
        int64_t ion_int64;
        ion_int_to_int64(&ion_int_value, &ion_int64);
        ionc_add_to_container(container, Py_BuildValue("i", ion_int64), in_struct, &field_name);
        break;
    case tid_FLOAT_INT:
        IONCHECK(ion_reader_read_double(hreader, &double_value));
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
        IONCHECK(ion_reader_read_symbol_sid(hreader, &sid));
        // you can only read a value once! IONCHECK(ion_reader_read_string(hreader, &string_value));
        // so we look it up
        IONCHECK(ion_reader_get_symbol_table(hreader, &hsymtab));
        IONCHECK(ion_symbol_table_find_by_sid(hsymtab, sid, &indirect_string_value));
        break;
    case tid_CLOB_INT:
    case tid_BLOB_INT:
        IONCHECK(ion_reader_get_lob_size(hreader, &length));
        // just to cover both API's
        if (length < TEMP_BUF_SIZE) {
            IONCHECK(ion_reader_read_lob_bytes(hreader, buf, TEMP_BUF_SIZE, &length));
        }
        else {
            for (remaining = length; remaining > 0; remaining -= length) {
                IONCHECK(ion_reader_read_lob_bytes(hreader, buf, TEMP_BUF_SIZE, &length));
                // IONCHECK(ion_reader_read_chunk(hreader, buf, TEMP_BUF_SIZE, &length));
            }
        }
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
        IONCHECK(ionc_read_all(hreader, child_container, child_is_struct));
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
    {"ionc_read", (PyCFunction)ionc_read, METH_NOARGS, ioncmodule_docs},
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

static PyObject* init_module(void) {
    PyObject* m;
#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("ionc", ioncmodule_funcs,
                   "Extension module example!");
#endif
    // TODO is there a destructor for modules? These should be decreffed there
    _decimal_module = PyImport_ImportModule("decimal");
    _decimal_constructor = PyObject_GetAttrString(_decimal_module, "Decimal");
    _simpletypes_module = PyImport_ImportModule("amazon.ion.simple_types");
    _ion_nature_cls = PyObject_GetAttrString(_simpletypes_module, "IonPyText");
    _ionstring_fromvalue = PyObject_GetAttrString(_ion_nature_cls, "from_value");
    _ion_core_module = PyImport_ImportModule("amazon.ion.core");
    _py_ion_type = PyObject_GetAttrString(_ion_core_module, "IonType");

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
    return m;
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