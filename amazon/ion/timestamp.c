#include "_ioncmodule.h"
#include "decNumber/decContext.h"
#include <datetime.h>

#define YEAR_PRECISION 0
#define MONTH_PRECISION 1
#define DAY_PRECISION 2
#define MINUTE_PRECISION 3
#define SECOND_PRECISION 4
#define MICROSECOND_DIGITS 6
#define MAX_TIMESTAMP_PRECISION 9

typedef struct {
   PyDateTime_DateTime datetime; // Keep this first, it is our base class.
   int precision;
   PyObject *microseconds; // Microseconds (as Decimal) including sub-microseconds that are not handled by DateTime
} IonTimestamp;

static void IonTimestamp_dealloc(IonTimestamp *self) {
   Py_XDECREF(self->microseconds);
   Py_TYPE(self)->tp_base->tp_free(self);
}

static PyObject *IonTimestamp_new(PyTypeObject *tpe, PyObject *args, PyObject *kwds) {
   PyObject *year = NULL;
   PyObject *month = Py_None;
   PyObject *day = Py_None;
   PyObject *hour = Py_None;
   PyObject *minutes = Py_None;
   PyObject *seconds = Py_None;
   PyObject *useconds = Py_None;
   PyObject *off_hours = Py_None;
   PyObject *off_minutes = Py_None;
   PyObject *useconds_whole = Py_None;
   static char *keywords[] = {"year", "month", "day", "hour", "minutes", "seconds", "microseconds", "off_hours", "off_minutes",  NULL};

   if (!PyArg_ParseTupleAndKeywords(args, kwds, "OOOOOO|OOO", keywords, &year, &month, &day, &hour, &minutes, &seconds, &useconds, &off_hours, &off_minutes)) {
      printf("ERROR on parse tuple...\n");
      PyErr_Print();
      return NULL;
   }

   int precision = -1;
   if (year != Py_None) {
      precision++;
   } else {
      PyErr_Format(PyExc_ValueError, "Cannot create a timestamp with lower precision than Year.");
      return NULL;
   }
   if (month != Py_None) { precision++; }
   if (day != Py_None) { precision++; }
   if (minutes != Py_None) { precision++; }
   if (seconds != Py_None) { precision++; }
   if (useconds != Py_None) {
      Py_INCREF(useconds); // Incref because we're going to store it in our timestamp object.
      precision++;
      if (PyObject_IsInstance(useconds, _decimal_constructor)) {

         // If our microseconds contain fractional component, we need to remove it for our parent datetime.
         PyObject *tuple_func = PyUnicode_FromString("as_tuple");
         PyObject *tuple = PyObject_CallMethodNoArgs(useconds, tuple_func);
         if (PyErr_Occurred()) {
            if (tuple_func != Py_None)
               Py_XDECREF(tuple_func);
            if (tuple != Py_None)
               Py_XDECREF(tuple);
            goto fail;
         }

         PyObject *exp = PyObject_GetAttrString(tuple, "exponent"); // Will be negative; New Ref
         PyObject *digits = PyObject_GetAttrString(tuple, "digits"); // New Ref
         int exponent = PyLong_AsLong(exp);
         Py_ssize_t end = PyTuple_Size(digits) + exponent;

         uint32_t usec_int = 0;
         for (int i=0; i < end+exponent; i++) {
            PyObject *digit = PyTuple_GetItem(digits, i); // Borrowed
            uint32_t digit_int = PyLong_AsLong(digit);
            usec_int += digit_int * pow(10, end - i - 1);
         }
         useconds_whole = PyLong_FromLong(usec_int);

         Py_DECREF(exp);
         Py_DECREF(digits);
         Py_DECREF(tuple_func);
         Py_DECREF(tuple);
      } else {
         useconds_whole = useconds;
         Py_INCREF(useconds_whole); // Increment this, so that we can decrement later for both paths.
      }
   }

   PyObject *datetime_args = PyTuple_Pack(7, year, month, day, hour, minutes, seconds, useconds_whole);
   IonTimestamp *self = (IonTimestamp *) (tpe->tp_base->tp_new(tpe, datetime_args, NULL)); // DateTime __new__
   if (self == NULL || PyErr_Occurred()) {
      Py_XDECREF(self);
      goto fail;
   }

   self->precision = precision;
   self->microseconds = useconds;
fail:
   if (useconds_whole != Py_None)
      Py_XDECREF(useconds_whole);
   Py_XDECREF(datetime_args);
   return (PyObject *)self;
}

static PyObject *IonTimestamp_FromComponents(PyObject *year, PyObject *month, PyObject *day, PyObject *hour, PyObject *min, PyObject *sec, PyObject *usec, PyObject *off_hour, PyObject *off_min) {
   PyObject *args = PyTuple_Pack(9, year, month, day, hour, min, sec, usec, off_hour, off_min);
   PyObject *ts = IonTimestamp_new(&IonTimestamp_Type, args, NULL);

   Py_DECREF(args);
   return ts;
}

PyObject *IonTimestamp_FromTimestamp(ION_TIMESTAMP *ts) {
   iENTER;
   int precision;
   PyObject *py_year = NULL;
   PyObject *py_month = Py_None;
   PyObject *py_day = Py_None;
   PyObject *py_hours = Py_None;
   PyObject *py_minutes = Py_None;
   PyObject *py_secs = Py_None;
   PyObject *py_usecs = Py_None;
   PyObject *py_off_hours = Py_None;
   PyObject *py_off_minutes = Py_None;
   PyObject *ret = NULL;

   IONCHECK(ion_timestamp_get_precision(ts, &precision));
   if (precision < ION_TS_YEAR) {
      PyErr_Format(PyExc_ValueError, "Cannot create Timestamp with less than year precision");
      Py_RETURN_NONE;
   }

   BOOL has_local_offset;
   IONCHECK(ion_timestamp_has_local_offset(ts, &has_local_offset));
   if (has_local_offset) {
      int off_minutes, off_hours;
      IONCHECK(ion_timestamp_get_local_offset(ts, &off_minutes));
      off_hours = off_minutes / 60;
      off_minutes = off_minutes % 60;
      py_off_hours = PyLong_FromLong(off_hours);
      py_off_minutes = PyLong_FromLong(off_minutes);
   }

   // We need to extract the time components for our timestamp. Once we have those, we can create
   // our timestamp.
   switch (precision) {
      case ION_TS_FRAC: {
         decQuad fraction = ts->fraction;
         decQuad tmp;
         int32_t frac_prec = decQuadGetExponent(&fraction);

         if (frac_prec > 1) {
            PyErr_Format(PyExc_ValueError, "Timestamp fractional precision cannot be a positive number.");
            return NULL;
         }

         frac_prec *= -1;
         if (frac_prec > MAX_TIMESTAMP_PRECISION) frac_prec = MAX_TIMESTAMP_PRECISION;

         decQuadScaleB(&fraction, &fraction, decQuadFromInt32(&tmp, MICROSECOND_DIGITS), &dec_context);
         int32_t microseconds = decQuadToInt32Exact(&fraction, &dec_context, DEC_ROUND_DOWN);

         if (decContextTestStatus(&dec_context, DEC_Inexact)) {
            // This means the fractional component is not [0, 1) or has more than microsecond precision.
            decContextClearStatus(&dec_context, DEC_Inexact);
         }

         // If we have more precision than is needed for microseconds, then we render the fraction to a
         // string so that we can capture all of the sub-microsecond precision.
         if (frac_prec > MICROSECOND_DIGITS) {
            // Render our microseconds to a string..
            char tmp_str[DECQUAD_String];
            decQuadToString(&fraction, tmp_str);

            // TODO: This can be made more efficient with the decimal C API that is available in 3.10+(?), but we
            //       need to support 3.8+, so we'd need to use it conditionally, based on the Python API version.
            // Then convert it to a python decimal..
            PyObject *py_usec_str = PyUnicode_FromString(tmp_str);
            py_usecs = PyObject_CallFunctionObjArgs(_decimal_constructor, py_usec_str, NULL);
            Py_DECREF(py_usec_str);
         } else {
            py_usecs = PyLong_FromLong(microseconds);
         }
      }
      case ION_TS_SEC:
         py_secs = PyLong_FromLong(ts->seconds);
      case ION_TS_MIN:
         py_minutes = PyLong_FromLong(ts->minutes);
         py_hours = PyLong_FromLong(ts->hours);
      case ION_TS_DAY:
         py_day = PyLong_FromLong(ts->day);
      case ION_TS_MONTH:
         py_month = PyLong_FromLong(ts->month);
      case ION_TS_YEAR:
         py_year = PyLong_FromLong(ts->year);
         break;
   }
   ret = IonTimestamp_FromComponents(py_year, py_month, py_day, py_hours, py_minutes, py_secs, py_usecs, py_off_hours, py_off_minutes);

fail:
   Py_XDECREF(py_year);
   Py_XDECREF(py_month);
   Py_XDECREF(py_day);
   Py_XDECREF(py_hours);
   Py_XDECREF(py_minutes);
   Py_XDECREF(py_secs);
   Py_XDECREF(py_usecs);
   Py_XDECREF(py_off_hours);
   Py_XDECREF(py_off_minutes);
   if (err != IERR_OK) {
      PyErr_Format(PyExc_IOError, "An internal ion error has occured: %s", ion_error_to_str(err));
   }
   return ret;
}

static PyObject *IonTimestamp_str(PyObject *self) {
   IonTimestamp *self_ts = (IonTimestamp *)self;

   // Our DateTime base will render everything up to the microsecond. We just need to append the fractional
   // microseconds.
   PyObject *func_name = PyUnicode_FromString("strftime");
   PyObject *date_fmt = PyUnicode_FromString("%Y-%m-%dT%H:%M:%S");
   PyObject *base_str = PyObject_CallMethodOneArg(self, func_name, date_fmt);

   Py_DECREF(func_name);
   Py_DECREF(date_fmt);

   PyObject *new_str = NULL;
   if (self_ts->microseconds != Py_None) {
      // Get our microseconds exponent..
      if (PyObject_IsInstance(self_ts->microseconds, (PyObject *)(_decimal_constructor))) {
         PyObject *tuple_func = PyUnicode_FromString("as_tuple");
         PyObject *digits_tuple = PyObject_CallMethodNoArgs(self_ts->microseconds, tuple_func);
         Py_DECREF(tuple_func);

         PyObject *exponent = PyObject_GetAttrString(digits_tuple, "exponent");
         exponent = PyNumber_Absolute(exponent);
         Py_DECREF(digits_tuple);
         
         PyObject *exp_str = exponent->ob_type->tp_str(exponent);

         //scale..
         PyObject *scaler = PyObject_CallFunctionObjArgs(_decimal_constructor, exp_str, NULL);
         PyObject *scaleb = PyUnicode_FromString("scaleb");
         PyObject *scaled = PyObject_CallMethodObjArgs(self_ts->microseconds, scaleb, scaler, NULL);
         new_str = PyUnicode_FromFormat("%U.%S", base_str, scaled);

         Py_DECREF(exponent);
         Py_DECREF(exp_str);
         Py_DECREF(scaler);
         Py_DECREF(scaleb);
         Py_DECREF(scaled);
      } else if (PyObject_IsInstance(self_ts->microseconds, (PyObject*)&PyLong_Type)) {
         new_str = PyUnicode_FromFormat("%U.%S", base_str, self_ts->microseconds);
      } else {
         PyErr_Format(PyExc_ValueError, "Unrecognized type for microseconds");
         Py_DECREF(base_str);
         Py_RETURN_NONE;
         return Py_None;
      }
      Py_DECREF(base_str);
      return new_str;
   }

   return base_str;
}


static PyMethodDef IonTimestamp_methods[] = {
   { NULL, NULL, 0, NULL },
};

PyTypeObject IonTimestamp_Type = {
   PyVarObject_HEAD_INIT(NULL, 0)
   .tp_name = "Timestamp",
   .tp_doc = "Ion Timestamp",
   .tp_basicsize = sizeof(IonTimestamp),
   .tp_itemsize = 0,
   .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DISALLOW_INSTANTIATION,
   .tp_init = NULL,
   .tp_new = IonTimestamp_new,
   .tp_methods = IonTimestamp_methods,
   .tp_alloc = PyType_GenericAlloc,
   .tp_dealloc = (destructor)IonTimestamp_dealloc,
   .tp_str = (reprfunc)IonTimestamp_str,
};
