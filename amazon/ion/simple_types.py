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

"""The type mappings for the ``simplejson``-like API.

In particular, this module provides the extension to native Python data types with
particulars of the Ion data model.
"""

from decimal import Decimal

# in Python 3.10, abstract collections have moved into their own module
# for compatibility with 3.10+, first try imports from the new location
# if that fails, try from the pre-3.10 location
try:
    from collections.abc import MutableMapping
except:
    from collections import MutableMapping

from collections import OrderedDict
from decimal import Decimal

from amazon.ion.core import IonType, IonEvent, Timestamp, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, TIMESTAMP_PRECISION_FIELD, \
    TimestampPrecision
from amazon.ion.symbols import SymbolToken


class IonPyNull(object):
    __name__ = 'IonPyNull'
    __qualname__ = 'IonPyNull'

    def __init__(self, ion_type=IonType.NULL, value=None, annotations=()):
        self.ion_type = ion_type
        self.ion_annotations = annotations

    def __bool__(self):
        return False

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyDecimal(Decimal):
    __name__ = 'IonPyDecimal'
    __qualname__ = 'IonPyDecimal'
    ion_type = IonType.DECIMAL

    def __new__(cls, ion_type=IonType.DECIMAL, value=None, annotations=()):
        v = super().__new__(cls, value)
        v.ion_annotations = annotations
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyBytes(bytes):
    __name__ = 'IonPyBytes'
    __qualname__ = 'IonPyBytes'

    def __new__(cls, ion_type=IonType.BLOB, value=None, annotations=()):
        v = super().__new__(cls, value)
        v.ion_annotations = annotations
        v.ion_type = ion_type
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyInt(int):
    __name__ = 'IonPyInt'
    __qualname__ = 'IonPyInt'
    ion_type = IonType.INT

    def __new__(cls, ion_type=IonType.INT, value=None, annotations=()):
        v = super().__new__(cls, value)
        v.ion_annotations = annotations
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyBool(int):
    __name__ = 'IonPyBool'
    __qualname__ = 'IonPyBool'
    ion_type = IonType.BOOL

    def __repr__(self):
        return str(bool(self))

    def __new__(cls, ion_type=IonType.BOOL, value=None, annotations=()):
        v = super().__new__(cls, value)
        v.ion_annotations = annotations
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyFloat(float):
    __name__ = 'IonPyFloat'
    __qualname__ = 'IonPyFloat'
    ion_type = IonType.FLOAT

    def __new__(cls, ion_type=IonType.FLOAT, value=None, annotations=()):
        v = super().__new__(cls, value)
        v.ion_annotations = annotations
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyText(str):
    __name__ = 'IonPyText'
    __qualname__ = 'IonPyText'
    ion_type = IonType.STRING

    def __new__(cls, ion_type=IonType.STRING, value=None, annotations=()):
        v = super().__new__(cls, value)
        v.ion_annotations = annotations
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyTimestamp(Timestamp):
    __name__ = 'IonPyTimestamp'
    __qualname__ = 'IonPyTimestamp'
    ion_type = IonType.TIMESTAMP

    def __new__(cls, *args, **kwargs):
        # The from_value like signature
        if isinstance(args[0], IonType):
            value = args[1]
            annotations = ()
            if len(args) > 2 and args[2] is not None:
                annotations = args[2]
            if value is not None:
                args_new, kwargs = cls._to_constructor_args(value)
            else:
                args_new, kwargs = (None, None, ()), {}
            v = super().__new__(cls, *args_new, **kwargs)
            v.ion_type = args[0]
            v.ion_annotations = annotations
        # Regular timestamp constructor
        else:
            v = super().__new__(cls, *args, **kwargs)
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(ts):
        if isinstance(ts, Timestamp):
            args = (ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, None, ts.tzinfo)
            fractional_seconds = getattr(ts, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, None)
            precision = getattr(ts, TIMESTAMP_PRECISION_FIELD, TimestampPrecision.SECOND)
            kwargs = {TIMESTAMP_PRECISION_FIELD: precision, TIMESTAMP_FRACTIONAL_SECONDS_FIELD: fractional_seconds}
        else:
            args = (ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, ts.microsecond, ts.tzinfo)
            kwargs = {TIMESTAMP_PRECISION_FIELD: TimestampPrecision.SECOND}
        return args, kwargs

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPySymbol(SymbolToken):
    __name__ = 'IonPySymbol'
    __qualname__ = 'IonPySymbol'
    ion_type = IonType.SYMBOL

    # a good signature: IonPySymbol(ion_type, symbol_token, annotation)
    def __new__(cls, ion_type=IonType.SYMBOL, value=None, annotations=()):
        v = super().__new__(cls, *value)
        v.ion_annotations = annotations
        return v

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(st):
        try:
            args = (None, (st.text, st.sid, st.location), ())
        except AttributeError:
            args = (None, (st, None, None), ())
        kwargs = {}
        return args, kwargs

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = self
        if isinstance(self, IonPyNull):
            value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyList(list):
    __name__ = 'IonPyList'
    __qualname__ = 'IonPyList'

    def __init__(self, ion_type=IonType.LIST, value=None, annotations=()):
        if value is None:
            value = []
        super().__init__(value)
        self.ion_annotations = annotations
        # it's possible to be Sexp
        self.ion_type = ion_type

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, [], ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


class IonPyDict(MutableMapping):
    """
    Dictionary that can hold multiple values for the same key

    In order not to break existing customers, getting and inserting elements with ``[]`` keeps the same behaviour
    as the built-in dict. If multiple elements are already mapped to the key, ``[]`  will return
    the newest one.

    To map multiple elements to a key, use the ``add_item`` operation.
    To retrieve all the values map to a key, use ``get_all_values``.
    """
    __name__ = 'IonPyDict'
    __qualname__ = 'IonPyDict'
    ion_type = IonType.STRUCT

    def __init__(self, ion_type=IonType.STRUCT, value=None, annotations=()):
        super().__init__()
        self.ion_annotations = annotations
        self.__store = OrderedDict()
        if value is not None:
            for key, value in iter(value.items()):
                if key in self.__store.keys():
                    self.__store[key].append(value)
                else:
                    self.__store[key] = [value]

    def __getitem__(self, key):
        """
        Return the newest value for the given key. To retrieve all the values map to the key, use ``get_all_values``.
        """
        return self.__store[key][len(self.__store[key]) - 1]  # Return only one in order not to break clients

    def __delitem__(self, key):
        """
        Delete all values for the given key.
        """
        del self.__store[key]

    def __setitem__(self, key, value):
        """
        Set the desired value to the given key.
        """
        self.__store[key] = [value]

    def __len__(self):
        return sum([len(values) for values in iter(self.__store.values())])

    def __iter__(self):
        for key in iter(self.__store.keys()):
            yield key

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return '{%s}' % ', '.join(['%r: %r' % (k, v) for k, v in self.items()])

    def add_item(self, key, value):
        """
        Add a value for the given key. This operation appends the value to the end of the value list instead of
        overwriting the existing value.
        """
        if key in self.__store:
            self.__store[key].append(value)
        else:
            self.__store[key] = [value]

    def get_all_values(self, key):
        """
        Retrieve all the values mapped to the given key
        """
        return self.__store[key]

    def iteritems(self):
        """
        Return an iterator over (key, value) tuple pairs.
        """
        for key in self.__store:
            for value in self.__store[key]:
                yield (key, value)

    def items(self):
        """
        Return a list of the IonPyDict's (key, value) tuple pairs.
        """
        output = []
        for k, v in self.iteritems():
            output.append((k, v))
        return output

    def __copy__(self):
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (None, value,), {}

    @classmethod
    def from_event(cls, ion_event):
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            args, kwargs = (None, None, ()), {}
        value = cls(*args, **kwargs)
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, in_struct=False, depth=None):
        value = None

        if in_struct:
            if not isinstance(field_name, SymbolToken):
                field_name = SymbolToken(field_name, 0 if field_name is None else None)
        else:
            field_name = None

        return IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                        annotations=self.ion_annotations, depth=depth)


def is_null(value):
    """A mechanism to determine if a value is ``None`` or an Ion ``null``."""
    return value is None or isinstance(value, IonPyNull)
