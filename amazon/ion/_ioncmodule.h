#ifndef _IONCMODULE_H_
#define _IONCMODULE_H_

#include "structmember.h"
#include "decimal128.h"
#include "ion.h"

PyObject* ionc_init_module(void);
iERR ionc_write_value(hWRITER writer, PyObject* obj, PyObject* tuple_as_sexp);
PyObject* ionc_read(PyObject* self, PyObject *args, PyObject *kwds);

enum ContainerType { LIST, MULTIMAP, STD_DICT };

iERR ionc_read_all(hREADER hreader, PyObject* container, enum ContainerType parent_type, uint8_t value_model);
iERR ionc_read_value(hREADER hreader, ION_TYPE t, PyObject* container, enum ContainerType parent_type, uint8_t value_model);

iERR _ion_writer_write_symbol_id_helper(ION_WRITER *pwriter, SID value);
iERR _ion_writer_add_annotation_sid_helper(ION_WRITER *pwriter, SID sid);
iERR _ion_writer_write_field_sid_helper(ION_WRITER *pwriter, SID sid);
ION_API_EXPORT void ion_helper_breakpoint(void);

#endif
