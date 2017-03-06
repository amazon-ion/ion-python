#ifndef _IONCMODULE_H_
#define _IONCMODULE_H_

#include "structmember.h"
#include "ion.h"

PyObject* helloworld(PyObject* self);
int _ionc_write(PyObject* obj, PyObject* binary, ION_STREAM* f_ion_stream);

#endif