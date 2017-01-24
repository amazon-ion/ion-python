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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from decimal import Decimal

import six

from amazon.ion.symbols import SymbolToken
from .core import TIMESTAMP_PRECISION_FIELD
from .core import Timestamp, IonEvent, IonType, TIMESTAMP_FRACTION_PRECISION_FIELD, TimestampPrecision, \
    MICROSECOND_PRECISION


class _IonNature(object):
    """Mix-in for Ion related properties.

    Attributes:
        ion_event (Optional[IonEvent]): The event, if any associated with the value.
        ion_type (IonType): The Ion type for the value.
        ion_annotations (Sequence[unicode]): The annotations associated with the value.

    Notes:
        There is no ``field_name`` attribute as that is generally modeled as a property of the
        container.

        The ``ion_event`` field is only provided if the value was derived from a low-level event.
        User constructed values will generally not set this field.
    """
    def __init__(self, *args, **kwargs):
        self.ion_event = None
        self.ion_type = None
        self.ion_annotations = ()

    def _copy(self):
        """Copies this instance. Its IonEvent (if any) is not preserved.

        Keeping this protected until/unless we decide there's use for it publicly.
        """
        args, kwargs = self._to_constructor_args(self)
        value = self.__class__(*args, **kwargs)
        value.ion_event = None
        value.ion_type = self.ion_type
        value.ion_annotations = self.ion_annotations
        return value

    @staticmethod
    def _to_constructor_args(value):
        return (value, ), {}

    @classmethod
    def from_event(cls, ion_event):
        """Constructs the given native extension from the properties of an event.

        Args:
            ion_event (IonEvent): The event to construct the native value from.
        """
        if ion_event.value is not None:
            args, kwargs = cls._to_constructor_args(ion_event.value)
        else:
            # if value is None (i.e. this is a container event), args must be empty or initialization of the
            # underlying container will fail.
            args, kwargs = (), {}
        value = cls(*args, **kwargs)
        value.ion_event = ion_event
        value.ion_type = ion_event.ion_type
        value.ion_annotations = ion_event.annotations
        return value

    @classmethod
    def from_value(cls, ion_type, value, annotations=()):
        """Constructs a value as a copy with an associated Ion type and annotations.

        Args:
            ion_type (IonType): The associated Ion type.
            value (Any): The value to construct from, generally of type ``cls``.
            annotations (Sequence[unicode]):  The sequence Unicode strings decorating this value.
        """
        if value is None:
            value = IonPyNull()
        else:
            args, kwargs = cls._to_constructor_args(value)
            value = cls(*args, **kwargs)
        value.ion_event = None
        value.ion_type = ion_type
        value.ion_annotations = annotations
        return value

    def to_event(self, event_type, field_name=None, depth=None):
        """Constructs an IonEvent from this _IonNature value.

        Args:
            event_type (IonEventType): The type of the resulting event.
            field_name (Optional[text]): The field name associated with this value, if any.
            depth (Optional[int]): The depth of this value.

        Returns:
            An IonEvent with the properties from this value.
        """
        if self.ion_event is None:
            value = self
            if isinstance(self, IonPyNull):
                value = None
            self.ion_event = IonEvent(event_type, ion_type=self.ion_type, value=value, field_name=field_name,
                                      annotations=self.ion_annotations, depth=depth)
        return self.ion_event


def _ion_type_for(name, base_cls):
    class IonPyValueType(base_cls, _IonNature):
        def __init__(self, *args, **kwargs):
            super(IonPyValueType, self).__init__(*args, **kwargs)

    IonPyValueType.__name__ = name
    return IonPyValueType


if six.PY2:
    IonPyInt = _ion_type_for('IonPyInt', long)
else:
    IonPyInt = _ion_type_for('IonPyInt', int)


IonPyBool = IonPyInt
IonPyFloat = _ion_type_for('IonPyFloat', float)
IonPyDecimal = _ion_type_for('IonPyDecimal', Decimal)
IonPyText = _ion_type_for('IonPyText', six.text_type)
IonPyBytes = _ion_type_for('IonPyBytes', six.binary_type)


class IonPySymbol(SymbolToken, _IonNature):
    def __init__(self, *args, **kwargs):
        super(IonPySymbol, self).__init__(*args, **kwargs)

    @staticmethod
    def _to_constructor_args(st):
        try:
            args = (st.text, st.sid, st.location)
        except AttributeError:
            args = (st, None, None)
        kwargs = {}
        return args, kwargs


class IonPyTimestamp(Timestamp, _IonNature):
    def __init__(self, *args, **kwargs):
        super(IonPyTimestamp, self).__init__(*args, **kwargs)

    @staticmethod
    def _to_constructor_args(ts):
        args = (ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, ts.microsecond, ts.tzinfo)
        kwargs = {}
        precision = getattr(ts, TIMESTAMP_PRECISION_FIELD, None)
        if precision is None:
            precision = TimestampPrecision.SECOND
        kwargs[TIMESTAMP_PRECISION_FIELD] = precision
        try:
            fractional_precision = getattr(ts, TIMESTAMP_FRACTION_PRECISION_FIELD)
        except AttributeError:
            fractional_precision = MICROSECOND_PRECISION
        kwargs[TIMESTAMP_FRACTION_PRECISION_FIELD] = fractional_precision
        return args, kwargs


class IonPyNull(_IonNature):
    """Representation of ``null``.

    Notes:
        ``None`` is a singleton and cannot be sub-classed, so we have our
         own value type for it.  The function ``is_null`` is the best way
         to test for ``null``-ness or ``None``-ness.
    """
    def __init__(self, *args, **kwargs):
        super(IonPyNull, self).__init__(*args, **kwargs)

    def __nonzero__(self):
        return False

    def __bool__(self):
        return False

    @staticmethod
    def _to_constructor_args(value):
        return (), {}


def is_null(value):
    """A mechanism to determine if a value is ``None`` or an Ion ``null``."""
    return value is None or isinstance(value, IonPyNull)


IonPyList = _ion_type_for('IonPyList', list)
IonPyDict = _ion_type_for('IonPyDict', dict)  # TODO support multiple mappings for same field name (iteration only).
