# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the
# License.

"""Provides common functionality for Ion binary and text readers."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ctypes as _ctypes
import sys as _sys
import decimal as _decimal
import datetime as _datetime

#
# ctypes utils
#
from amazon.ion.core import IonType
from amazon.ion.exceptions import IonException

_byref = _ctypes.byref
_cast = _ctypes.cast
_string_at = _ctypes.string_at

#
# ionc definitions
#

#
# constants
#

ION_TYPE_NONE = -0x200
ION_TYPE_EOF = -0x100
ION_TYPE_NULL = 0x000
ION_TYPE_BOOL = 0x100
ION_TYPE_INT = 0x200
ION_TYPE_FLOAT = 0x400
ION_TYPE_DECIMAL = 0x500
ION_TYPE_TIMESTAMP = 0x600
ION_TYPE_SYMBOL = 0x700
ION_TYPE_STRING = 0x800
ION_TYPE_CLOB = 0x900
ION_TYPE_BLOB = 0xA00
ION_TYPE_STRUCT = 0xB00
ION_TYPE_LIST = 0xC00
ION_TYPE_SEXP = 0xD00
ION_TYPE_DATAGRAM = 0xF00

ION_TYPE_FROM_TID = {
    ION_TYPE_NULL: IonType.NULL,
    ION_TYPE_BOOL: IonType.BOOL,
    ION_TYPE_INT: IonType.INT,
    ION_TYPE_FLOAT: IonType.FLOAT,
    ION_TYPE_DECIMAL: IonType.DECIMAL,
    ION_TYPE_TIMESTAMP: IonType.TIMESTAMP,
    ION_TYPE_SYMBOL: IonType.SYMBOL,
    ION_TYPE_STRING: IonType.STRING,
    ION_TYPE_CLOB: IonType.CLOB,
    ION_TYPE_BLOB: IonType.BLOB,
    ION_TYPE_STRUCT: IonType.STRUCT,
    ION_TYPE_LIST: IonType.LIST,
    ION_TYPE_SEXP: IonType.SEXP,
}

#
# types
#

_C_STRUCT = _ctypes.Structure

_C_BOOL = _ctypes.c_bool
_C_BYTE = _ctypes.c_int8
_C_INT = _ctypes.c_int
_C_SIZE = _ctypes.c_int32

_C_PTR = _ctypes.POINTER


#
# DecNum interop
#

class _DEC_CONTEXT(_C_STRUCT):
    _fields_ = [
        ('digits', _ctypes.c_int32),
        ('emax', _ctypes.c_int32),
        ('emin', _ctypes.c_int32),
        ('round', _ctypes.c_int),  # enum rounding
        ('traps', _ctypes.c_uint32),
        ('status', _ctypes.c_uint32),
        ('clamp', _ctypes.c_uint8),
        ('extended', _ctypes.c_uint8),  # decnum may not be compiled with this field
    ]


_DEC_QUAD_LEN = 16
_DEC_QUAD_PACKED_LEN = 18  # 34 digits (1 per nibble) + prefix pad nibble + suffix sign nibble
_DEC_QUAD_PACKED_ARRAY = _ctypes.c_int8 * _DEC_QUAD_PACKED_LEN

_DEC_FLOAT_NAN = 0x7c000000
_DEC_FLOAT_SNAN = 0x7e000000
_DEC_FLOAT_INF = 0x78000000


class _DEC_QUAD(_ctypes.Union):
    _fields_ = [
        ('bytes', _ctypes.c_uint8 * _DEC_QUAD_LEN),
        ('shorts', _ctypes.c_uint16 * (_DEC_QUAD_LEN // 2)),
        ('words', _ctypes.c_uint32 * (_DEC_QUAD_LEN // 4)),
        ('longs', _ctypes.c_uint32 * (_DEC_QUAD_LEN // 8)),  # decnum may not be compiled with this field
    ]


_DECIMAL_POS_INF = _decimal.Decimal('Inf')
_DECIMAL_NEG_INF = _decimal.Decimal('-Inf')
_DECIMAL_NAN = _decimal.Decimal('NaN')


# forward declarations

class _ION_SYMBOL_TABLE(_C_STRUCT): pass


class _ION_CATALOG(_C_STRUCT): pass


class _ION_READER(_C_STRUCT): pass


_hREADER = _C_PTR(_ION_READER)


class _ION_TYPE_STRUCT(_C_STRUCT): pass


_ION_TYPE = _C_PTR(_ION_TYPE_STRUCT)


# public structs

class _ION_STRING(_C_STRUCT):
    _fields_ = [
        ('length', _C_SIZE),
        ('value', _C_PTR(_C_BYTE)),
    ]


class _ION_TIMESTAMP(_C_STRUCT):
    _fields_ = [
        ('precision', _ctypes.c_uint8),

        ('tz_offset', _ctypes.c_int16),
        ('year', _ctypes.c_uint16),
        ('month', _ctypes.c_uint16),
        ('day', _ctypes.c_uint16),

        ('hours', _ctypes.c_uint16),
        ('minutes', _ctypes.c_uint16),
        ('seconds', _ctypes.c_uint16),

        ('fraction', _DEC_QUAD),
    ]


class _ION_READER_OPTIONS(_C_STRUCT):
    _fields_ = [
        ('return_system_values', _C_BOOL),
        ('return_shared_symbol_tables', _C_BOOL),
        ('new_line_char', _C_INT),
        ('max_container_depth', _C_SIZE),
        ('max_annotation_count', _C_SIZE),
        ('max_annotation_buffered', _C_SIZE),
        ('symbol_threshold', _C_SIZE),
        ('user_value_threshold', _C_SIZE),
        ('chunk_threshold', _C_SIZE),
        ('allocation_page_size', _C_SIZE),
        ('skip_character_validation', _C_BOOL),
        ('pcatalog', _C_PTR(_ION_CATALOG)),
    ]


class _ION_WRITER_OPTIONS(_C_STRUCT):
    _fields_ = [
        ('output_as_binary', _C_BOOL),
        ('escape_all_non_ascii', _C_BOOL),
        ('pretty_print', _C_BOOL),
        ('indent_with_tabs', _C_BOOL),
        ('indent_size', _C_SIZE),
        ('small_containers_in_line', _C_BOOL),
        ('supress_system_values', _C_BOOL),
        ('flush_every_value', _C_BOOL),
        ('max_container_depth', _C_SIZE),
        ('max_annotation_count', _C_SIZE),
        ('temp_buffer_size', _C_SIZE),
        ('allocation_page_size', _C_SIZE),
        ('pcatalog', _C_PTR(_ION_CATALOG)),
        ('encoding_psymbol_table', _C_PTR(_ION_SYMBOL_TABLE)),
    ]


#
# Utility
#

def _ionc_load(path=None):
    '''Load the C Library'''
    # platform specific calling conventions
    if _sys.platform == 'win32':
        dll = _ctypes.windll
    else:
        dll = _ctypes.cdll
    # default name either in loader path
    if path is None:
        if _sys.platform == 'win32':
            ionc = dll.LoadLibrary('IonC')
        else:
            ionc = dll.LoadLibrary('libionc.so')
    else:
        ionc = dll.LoadLibrary(path)
    return ionc


def _check_call(func, *args):
    '''Invokes a C-function using the Ion C error convention'''
    err = func(*args)


def _ionc_type_value(ion_type):
    '''Converts a pointer-as-enum to its ordinal int value, this only supports such enums with less than int16_t domain'''
    # XXX we have no intptr_t in ctypes, but since ion type is a constrained int
    #     however, we can cheat and truncate it safely if the pointer size is at least 2
    return _cast(_byref(ion_type), _C_PTR(_ctypes.c_int16)).contents.value


def _ionc_create_string(ionc):
    '''Creates an ion string struct'''
    val = _ION_STRING()
    ionc.ion_string_init(_byref(val))
    return val


def _ionc_convert_string(sval):
    '''Converts an Ion C string struct into a Python string'''
    if sval.length == 0:
        return ''
    return _string_at(sval.value, sval.length)


def _ionc_convert_decimal(ionc, quad):
    '''Converts an Ion C decQuad to native Python Decimal without loss of precision.'''
    exponent = _ctypes.c_int32()
    packed = _DEC_QUAD_PACKED_ARRAY()
    ionc.ion_quad_get_packed_and_exponent_from_quad(_byref(quad), packed, _byref(exponent))
    if (packed[-1] & 0xF) == 0xC:
        sign = 0
    else:
        sign = 1
    exponent = exponent.value
    if exponent == _DEC_FLOAT_NAN or exponent == _DEC_FLOAT_SNAN:
        return _DECIMAL_NAN
    if exponent == _DEC_FLOAT_INF:
        return _DECIMAL_NEG_INF if sign else _DECIMAL_POS_INF

    def iterate_nibbles():
        for index, octet in enumerate(packed):
            hn = (octet >> 4) & 0xF
            ln = octet & 0xF
            yield index * 2, hn
            yield index * 2 + 1, ln

    def iterate_digits():
        started = False
        for index, digit in iterate_nibbles():
            # 34 digits + prefix pad zero
            if index >= 35:
                return
            if digit == 0 and not started:
                continue
            started = True
            yield digit

    return _decimal.Decimal((sign, tuple(iterate_digits()), exponent))


class _OffsetTZInfo(_datetime.tzinfo):
    '''Simple offset only TZInfo'''

    def __init__(self, mins):
        self.__offset = _datetime.timedelta(minutes=mins)

    def utcoffset(self, dt):
        return self.__offset

    def dst(self, dt):
        return _datetime.timedelta(0)


_MICRO_SECONDS_PER_SECONDS = _decimal.Decimal(1000000)


def _ionc_convert_timestamp(ionc, ts):
    '''Converts an Ion C timestamp struct into a Python datetime.  Python only maintains microsecond precision.'''
    # XXX is it required to use the accessors in Ion C?
    # TODO honor precision
    frac_seconds = _ionc_convert_decimal(ionc, ts.fraction)
    microseconds = (frac_seconds * _MICRO_SECONDS_PER_SECONDS).to_integral_value()

    dt = _datetime.datetime(
        year=ts.year,
        month=ts.month,
        day=ts.day,

        hour=ts.hours,
        minute=ts.minutes,
        second=ts.seconds,
        microsecond=microseconds,

        tzinfo=_OffsetTZInfo(ts.tz_offset)
    )
    return dt


#
# Main Interface
#

class _CIonException(IonException):
    def __init__(self, msg, bytes=-1, line=-1, offset=-1):
        self.bytes = bytes
        self.line = line
        self.offset = offset
        if bytes == -1 and line == -1 and offset == -1:
            real_msg = msg
        else:
            real_msg = '%s (line: %d, offset: %d, byte position: %d)' % (msg, line, offset, bytes)
        super(IonException, self).__init__(real_msg)


class IonC(object):
    '''Low-level interface into the Ion C library'''

    def __init__(self, path=None):
        self.clib = _ionc_load(path)

        # fixup return types - win32 C function returns don't persist in DLLs
        # TODO this is pretty incomplete.
        self.clib.ion_error_to_str.restype = _ctypes.c_char_p
        self.clib.ion_string_init.restype = None
        self.clib.ion_quad_get_packed_and_exponent_from_quad.restype = None

    def _check_call(self, func, *args):
        '''Invokes an IonC function with the iERR convention'''
        err = func(*args)
        if err != 0:
            msg = self.clib.ion_error_to_str(err)
            raise _CIonException(msg)

    def __getattr__(self, name):
        '''Resolves a name against the underlying ionc library and wraps it if the return type is integer'''
        func = getattr(self.clib, name)
        if func.restype == _ctypes.c_int:
            def wrapper(*args):
                return self._check_call(func, *args)

            # fixup wrapper name
            wrapper.__name__ = name
            wrapper.func_name = name
            return wrapper
        return func

class TimeHolder:
    def __init__(self):
        self.elapsed = _datetime.timedelta()

    def add(self, delta):
        self.elapsed += delta

C_ELAPSED_TIME = TimeHolder()

class Timer:
    def __enter__(self):
        self.start = _datetime.datetime.now()
        return self

    def __exit__(self, *args):
        self.end = _datetime.datetime.now()
        self.elapsed = self.end - self.start

def _ion_reader_handler(func):
    def wrapper(*args, **kwargs):
        try:
            with Timer() as t:
                res = func(*args, **kwargs)
            C_ELAPSED_TIME.add(t.elapsed)
            return res
        except _CIonException as exc_info:
            # capture the exception information
            #exc_info = _sys.exc_info()

            reader = args[0]
            c_bytes = _ctypes.c_int64(-1)
            c_line = _ctypes.c_int32(-1)
            c_off = _ctypes.c_int32(-1)
            # escape down to clib to avoid exception wrapping
            reader._IonReader__ionc.clib.ion_reader_get_position(
                reader._IonReader__reader,
                _byref(c_bytes), _byref(c_line), _byref(c_off)
            )
            raise IonException('%s (bytes:%ld, line:%d, off:%d)' % (exc_info, c_bytes.value, c_line.value, c_off.value))

    wrapper.__name__ = func.__name__
    #wrapper.func_name = func.func_name
    return wrapper


class IonReader(object):
    '''
    Abstraction over the Ion C reader
    Usage looks like:

      text = 'a b c d [1] {a:5}'
      ionc = IonC()
      with IonCReader(ionc, text) as reader :
        for tid in reader :
          if reader.is_container :
            reader.step_in()
            continue
          if tid == ION_TYPE_EOF :
            reader.step_out()
            continue
          print reader.annotations, reader.field_name, reader.value

    '''

    def __init__(self, ionc, data, options=None):
        '''Constructs a reader over the given bytes'''
        # TODO support streams (files and like objects)
        self.__ionc = ionc

        # XXX keep a live reference to make sure that GC doesn't trash the buffer
        self.__data = data

        # jump table to resolve TIDs
        self.__value_resolvers = (
            # FIXME add in the resolvers
            self.__resolve_none,
            self.__resolve_bool,
            self.__resolve_int,  # pos-int (returned for all integers from ionc)
            self.__resolve_int,  # neg-int (should never be returned from ionc)
            self.__resolve_float,
            self.__resolve_decimal,
            self.__resolve_timestamp,
            self.__resolve_text,  # symbol
            self.__resolve_text,  # string
            self.__resolve_lob,  # clob
            self.__resolve_lob,  # blob
            self.__resolve_none,  # struct
            self.__resolve_none,  # list
            self.__resolve_none,  # sexp
        )

        # allocate memory for handle and open
        self.__reader = _hREADER()
        self.__type = _ION_TYPE()
        self.__type_code = ION_TYPE_NONE
        self.__ionc.ion_reader_open_buffer(_byref(self.__reader), data, len(data), options)

    def __resolve_none(self):
        return None

    def __resolve_bool(self):
        val = _C_BOOL()
        self.__ionc.ion_reader_read_bool(self.__reader, _byref(val))
        return val.value

    def __resolve_int(self):
        # TODO support arbitrary big int
        val = _ctypes.c_int64()
        self.__ionc.ion_reader_read_int64(self.__reader, _byref(val))
        return val.value

    def __resolve_float(self):
        val = _ctypes.c_double()
        self.__ionc.ion_reader_read_double(self.__reader, _byref(val))
        return val.value

    def __resolve_decimal(self):
        quad = _DEC_QUAD()
        self.__ionc.ion_reader_read_decimal(self.__reader, _byref(quad))
        return _ionc_convert_decimal(self.__ionc, quad)

    def __resolve_timestamp(self):
        ts = _ION_TIMESTAMP()
        self.__ionc.ion_reader_read_timestamp(self.__reader, _byref(ts))
        return _ionc_convert_timestamp(self.__ionc, ts)

    def __resolve_text(self):
        # TODO support partial string reads
        val = _ionc_create_string(self.__ionc)
        self.__ionc.ion_reader_read_string(self.__reader, _byref(val))
        return _ionc_convert_string(val)

    def __resolve_lob(self):
        # TODO support parial LOB reads
        size = _C_SIZE()
        self.__ionc.ion_reader_get_lob_size(self.__reader, _byref(size))
        if size.value < 0:
            raise _CIonException('Bad size from LOB: %d' % size)
        if size.value == 0:
            return ''
        read_size = _C_SIZE()
        buf = _ctypes.create_string_buffer(size.value)
        self.__ionc.ion_reader_read_lob_bytes(self.__reader, buf, len(buf), _byref(read_size))
        assert read_size.value == size.value
        return _string_at(buf, len(buf))

    def __enter__(self):
        '''Context manager enter'''
        return self

    def __exit__(self, e_type, e_val, tb):
        '''Context manager exit'''
        self.close()

    @_ion_reader_handler
    def close(self):
        self.__ionc.ion_reader_close(self.__reader)

    def __iter__(self):
        '''Iterator support'''
        while True:
            yield self.next()

    @_ion_reader_handler
    def next(self):
        '''Advances the reader'''
        self.__ionc.ion_reader_next(self.__reader, _byref(self.__type))
        self.__type_code = _ionc_type_value(self.__type)
        if self.__type_code == ION_TYPE_EOF and self.depth == 0:
            raise StopIteration()
        return self.__type_code

    @_ion_reader_handler
    def step_in(self):
        '''Steps into a container'''
        self.__ionc.ion_reader_step_in(self.__reader)

    @_ion_reader_handler
    def step_out(self):
        '''Steps out of a container'''
        self.__ionc.ion_reader_step_out(self.__reader)

    @property
    @_ion_reader_handler
    def depth(self):
        '''The current depth of containers'''
        val = _C_SIZE()
        self.__ionc.ion_reader_get_depth(self.__reader, _byref(val))
        return val.value

    @property
    @_ion_reader_handler
    def type(self):
        '''Returns the current TID'''
        return self.__type_code

    @property
    @_ion_reader_handler
    def is_container(self):
        '''Returns whether the current value is a container'''
        return self.__type_code in (ION_TYPE_STRUCT, ION_TYPE_LIST, ION_TYPE_SEXP)

    @property
    @_ion_reader_handler
    def annotations(self):
        count = _C_SIZE()
        self.__ionc.ion_reader_get_annotation_count(self.__reader, _byref(count))
        if count.value == 0:
            return []
        vals = (_ION_STRING * count.value)()
        # FIXME we should probably do a proper API init here for the array
        fetch_count = _C_SIZE()
        self.__ionc.ion_reader_get_annotations(self.__reader, vals, count, _byref(fetch_count))
        assert count.value == fetch_count.value
        return list(_ionc_convert_string(x) for x in vals)

    @property
    @_ion_reader_handler
    def field_name(self):
        '''Returns the field name for the current value, or None if it doesn't exist'''
        val = _ionc_create_string(self.__ionc)
        self.__ionc.ion_reader_get_field_name(self.__reader, _byref(val))
        # FIXME we technically cannot distinguish no field name from empty here without more tracking state
        if val.length == 0:
            return None
        return _ionc_convert_string(val)

    @property
    @_ion_reader_handler
    def is_null(self):
        val = _C_BOOL()
        self.__ionc.ion_reader_is_null(self.__reader, _byref(val))
        return val.value

    @property
    @_ion_reader_handler
    def value(self):
        '''Returns the current value that the reader is on, for non-scalars, this will return None'''
        if self.__type_code < 0:
            return None
        # get the tid from the padded value
        tid_actual = self.__type_code >> 8
        if tid_actual >= len(self.__value_resolvers):
            raise _CIonException('Unknown TID: %d' % tid_actual)

        # dispatch resolve based on tid
        value_resolver = self.__value_resolvers[tid_actual]
        return value_resolver()


if __name__ == '__main__':
    def main():
        import sys
        path = None
        if len(sys.argv) > 1:
            path = sys.argv[1]
        data = b'a b c d [1] {a:5}'  #sys.stdin.read()
        with IonReader(IonC(path), data) as r:
            for t in r:
                print('%sTID: %s' % (' ' * r.depth, hex(t)))
                if t == ION_TYPE_EOF:
                    r.step_out()
                    continue
                print('%sANNOTATIONS: %s FIELD: %s' % (' ' * r.depth , r.annotations, r.field_name))
                if r.is_container:
                    r.step_in()
                    continue
                print('%s%s' % (' ' * r.depth, r.value))


    main()


