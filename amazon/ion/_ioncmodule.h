#ifndef _IONCMODULE_H_
#define _IONCMODULE_H_

#include "structmember.h"
#include "ion.h"
#include "decimal128.h"

PyObject* ionc_init_module(void);
PyObject* helloworld(PyObject* self);
int _ionc_write(PyObject* obj, PyObject* binary, ION_STREAM* f_ion_stream);
PyObject* ionc_read(PyObject* self, PyObject *args, PyObject *kwds);
iERR ionc_read_all(hREADER hreader, PyObject* container, BOOL in_struct, BOOL emit_bare_values);

#endif