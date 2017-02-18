#include "Python.h"
#include "structmember.h"
#include "ion.h"

#define PyCIonEvent_Check(op) PyObject_TypeCheck(op, &PyIonEventType)
#define PyCIonEvent_CheckExact(op) (Py_TYPE(op) == &PyIonEventType)

static PyObject *
ion_event_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
static int
ion_event_init(PyObject *self, PyObject *args, PyObject *kwds);
static void
ion_event_dealloc(PyObject *self);
static int
ion_event_clear(PyObject *self);

static PyTypeObject PyIonEventType;

static PyObject* helloworld(PyObject* self)
{
    return Py_BuildValue("s", "python extensions");
}

typedef struct _PyCIonEventObject {
    PyObject_HEAD
    PyObject* event_type;
    PyObject* ion_type;
    PyObject* value;
    PyObject* annotations;
    PyObject* field_name;
} PyCIonEventObject;


static PyMemberDef ion_event_members[] = {
    {"event_type", T_OBJECT, offsetof(PyCIonEventObject, event_type), READONLY, "event_type"},
    {"ion_type", T_OBJECT, offsetof(PyCIonEventObject, ion_type), READONLY, "ion_type"},
    {"value", T_OBJECT, offsetof(PyCIonEventObject, value), READONLY, "value"},
    {"annotations", T_OBJECT, offsetof(PyCIonEventObject, annotations), READONLY, "annotations"},
    {"field_name", T_OBJECT, offsetof(PyCIonEventObject, field_name), READONLY, "field_name"},
    {NULL}
};

static int
ion_event_clear(PyObject *self)
{
    PyCIonEventObject *event;
    assert(PyCIonEvent_Check(self));
    event = (PyCIonEventObject *)self;
    Py_CLEAR(event->event_type);
    Py_CLEAR(event->ion_type);
    Py_CLEAR(event->value);
    Py_CLEAR(event->annotations);
    Py_CLEAR(event->field_name);
    return 0;
}

static void
ion_event_dealloc(PyObject *self)
{
    /* Deallocate ion event object */
    ion_event_clear(self);
    Py_TYPE(self)->tp_free(self);
}

static int
ion_event_init(PyObject *self, PyObject *args, PyObject *kwds)
{
    /* Initialize IonEvent object */
    PyObject *py_ion_event;
    static char *kwlist[] = {"event", NULL};
    PyCIonEventObject *event;

    assert(PyCIonEvent_Check(self));
    event = (PyCIonEventObject *)self;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O:init_cion_event", kwlist, &py_ion_event))
        return -1;

    event->event_type = PyObject_GetAttrString(py_ion_event, "event_type");
    if (event->event_type == NULL)
        goto fail;
    event->ion_type = PyObject_GetAttrString(py_ion_event, "ion_type");
    //if (event->ion_type == NULL)
    //    goto fail;
    event->value = PyObject_GetAttrString(py_ion_event, "value");
    //if (event->value == NULL)
    //    goto fail;
    event->annotations = PyObject_GetAttrString(py_ion_event, "annotations");
    if (event->annotations == NULL)
        goto fail;
    event->field_name = PyObject_GetAttrString(py_ion_event, "field_name");
    //if (event->field_name == NULL)
    //    goto fail;

    return 0;

fail:
    Py_CLEAR(event->event_type);
    Py_CLEAR(event->ion_type);
    Py_CLEAR(event->value);
    Py_CLEAR(event->annotations);
    Py_CLEAR(event->field_name);
    return -1;
}

static PyObject *
ion_event_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyCIonEventObject *event;
    event = (PyCIonEventObject *)type->tp_alloc(type, 0);
    if (event != NULL) {
        event->event_type = NULL;
        event->ion_type = NULL;
        event->value = NULL;
        event->annotations = NULL;
        event->field_name = NULL;
    }
    return (PyObject *)event;
}

PyDoc_STRVAR(ion_event_doc, "C IonEvent object");

static
PyTypeObject PyIonEventType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "amazon.ion.ionc.CIonEvent",       /* tp_name */
    sizeof(PyCIonEventObject), /* tp_basicsize */
    0,                    /* tp_itemsize */
    ion_event_dealloc, /* tp_dealloc */
    0,                    /* tp_print */
    0,                    /* tp_getattr */
    0,                    /* tp_setattr */
    0,                    /* tp_compare */
    0,                    /* tp_repr */
    0,                    /* tp_as_number */
    0,                    /* tp_as_sequence */
    0,                    /* tp_as_mapping */
    0,                    /* tp_hash */
    0,         /* tp_call */
    0,                    /* tp_str */
    0,/* PyObject_GenericGetAttr, */                    /* tp_getattro */
    0,/* PyObject_GenericSetAttr, */                    /* tp_setattro */
    0,                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,   /* tp_flags */
    ion_event_doc,          /* tp_doc */
    0,                    /* tp_traverse */
    ion_event_clear,                    /* tp_clear */
    0,                    /* tp_richcompare */
    0,                    /* tp_weaklistoffset */
    0,                    /* tp_iter */
    0,                    /* tp_iternext */
    0,                    /* tp_methods */
    ion_event_members,                    /* tp_members */
    0,                    /* tp_getset */
    0,                    /* tp_base */
    0,                    /* tp_dict */
    0,                    /* tp_descr_get */
    0,                    /* tp_descr_set */
    0,                    /* tp_dictoffset */
    ion_event_init,                    /* tp_init */
    0,/* PyType_GenericAlloc, */        /* tp_alloc */
    ion_event_new,          /* tp_new */
    0,/* PyObject_GC_Del, */              /* tp_free */
};

static PyObject* next(PyObject* self) {
    PyTypeObject* ion_event_cls = (PyTypeObject*)Py_BuildValue("O", (PyObject*)&PyIonEventType);
    PyCIonEventObject* ion_event = (PyCIonEventObject*)ion_event_new(ion_event_cls, NULL, NULL);
    PyObject* events = PyList_New(0);
    ion_event->event_type = Py_BuildValue("i", 1);
    ion_event->ion_type = Py_BuildValue("i", 2);
    ion_event->value = Py_BuildValue("i", 142);
    //static char* annotations[] = {NULL};
    ion_event->annotations = PyTuple_New(0);
    ion_event->field_name = Py_BuildValue("s", NULL);
    PyList_Append(events, (PyObject*)ion_event);
    Py_DECREF(ion_event);
    PyObject* tst = Py_BuildValue("s", NULL);
    PyList_Append(events, tst);
    Py_DECREF(tst);
    //PyCIonEventObject* events[1];
    //events[0] = ion_event;
    //return (PyObject*) ion_event;
    //return (PyObject*)Py_BuildValue("(O)", events);
    return events;
}

#define TEMP_BUF_SIZE 0x10000
static iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container);

static iERR ionc_read_all(hREADER hreader, PyObject* container) {
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


        ionc_read_value(hreader, t, container);
    }
    iRETURN;
}

static void ionc_add_list(PyObject* pyList, PyObject* element) {
    PyList_Append(pyList, (PyObject*)element);
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
    char        *pathname = "/Users/greggt/Desktop/generated_short.json";

    fstream = fopen(pathname, "rb");
    if (!fstream) {
        printf("\nERROR: can't open file %s\n", pathname);
        goto fail;
    }

    IONCHECK(ion_stream_open_file_in(fstream, &f_ion_stream));
    IONCHECK(ion_reader_open(&reader, f_ion_stream, NULL));
    PyObject* top_level_container = PyList_New(0);
    IONCHECK(ionc_read_all(reader, top_level_container));
    IONCHECK(ion_reader_close(reader));
    IONCHECK(ion_stream_close(f_ion_stream));
    return top_level_container;
fail:
    // TODO raise IonException.
    return Py_BuildValue("s", NULL);
}

static iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container) {
    iENTER;

    ION_TYPE    ion_type;
    BOOL        is_null;
    BOOL        bool_value;
    ION_INT     ion_int_value;
    double      double_value;
    decQuad     decimal_value;
    ION_TIMESTAMP timestamp_value;
    SID         sid;
    ION_STRING  string_value, *indirect_string_value = NULL;
    SIZE        length, remaining;
    BYTE        buf[TEMP_BUF_SIZE];
    hSYMTAB     hsymtab = 0;

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
        break;
    case tid_FLOAT_INT:
        IONCHECK(ion_reader_read_double(hreader, &double_value));
        break;
    case tid_DECIMAL_INT:
        IONCHECK(ion_reader_read_decimal(hreader, &decimal_value));
        break;
    case tid_TIMESTAMP_INT:
        IONCHECK(ion_reader_read_timestamp(hreader, &timestamp_value));
        break;
    case tid_STRING_INT:
        IONCHECK(ion_reader_read_string(hreader, &string_value));
        // TODO PyUnicode_FromKindAndData is new in 3.3. Find alternative for 2.x. Also check for non-ASCII compatibility.
        ionc_add_list(container, PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, string_value.value, string_value.length));
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
    case tid_LIST_INT:
    case tid_SEXP_INT:
        IONCHECK(ion_reader_step_in(hreader));
        // TODO mint a new container of the appropriate type
        IONCHECK(ionc_read_all(hreader, container));
        IONCHECK(ion_reader_step_out(hreader));
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
    {"next", (PyCFunction)next, METH_NOARGS, ioncmodule_docs},
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
    PyIonEventType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&PyIonEventType) < 0)
        return NULL;
#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("ionc", ioncmodule_funcs,
                   "Extension module example!");
#endif
    Py_INCREF((PyObject*)&PyIonEventType);
    PyModule_AddObject(m, "init_cion_event", (PyObject*)&PyIonEventType);
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