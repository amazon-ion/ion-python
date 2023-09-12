from collections import OrderedDict
from decimal import Decimal
from collections.abc import MutableMapping

from amazon.ion.core import IonType, IonEvent
from amazon.ion.symbols import SymbolToken


class IonPyNull(object):
    __name__ = 'IonPyNull_new'
    __qualname__ = 'IonPyNull_new'

    def __init__(self, ion_type=IonType.NULL, value=None, annotations=()):
        self.ion_type = ion_type  # TODO initialized to NULL type first, what's the real type?
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
    __name__ = 'IonPyDecimal_new'
    __qualname__ = 'IonPyDecimal_new'
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


class IonPyInt(int):
    __name__ = 'IonPyInt_new'
    __qualname__ = 'IonPyInt_new'
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
    __name__ = 'IonPyBool_new'
    __qualname__ = 'IonPyBool_new'
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
    __name__ = 'IonPyFloat_new'
    __qualname__ = 'IonPyFloat_new'
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
    __name__ = 'IonPyText_new'
    __qualname__ = 'IonPyText_new'
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


class IonPySymbol(SymbolToken):
    __name__ = 'IonPySymbol_new'
    __qualname__ = 'IonPySymbol_new'
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
    __name__ = 'IonPyList_new'
    __qualname__ = 'IonPyList_new'

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
    __name__ = 'IonPyDict_new'
    __qualname__ = 'IonPyDict_new'
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
            self.__setitem__(key, value)

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
