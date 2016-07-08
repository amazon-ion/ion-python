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

"""Ion core types."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime, timedelta, tzinfo

from .util import record
from .util import Enum


class IonType(Enum):
    """Enumeration of the Ion data types."""
    NULL = 0
    BOOL = 1
    INT = 2
    FLOAT = 3
    DECIMAL = 4
    TIMESTAMP = 5
    SYMBOL = 6
    STRING = 7
    CLOB = 8
    BLOB = 9
    LIST = 10
    SEXP = 11
    STRUCT = 12

    @property
    def is_text(self):
        """Returns whether the type is a Unicode textual type."""
        return self is IonType.SYMBOL or self is IonType.STRING

    @property
    def is_lob(self):
        """Returns whether the type is a LOB."""
        return self is IonType.CLOB or self is IonType.BLOB

    @property
    def is_container(self):
        """Returns whether the type is a container."""
        return self >= IonType.LIST


# TODO At some point we can add SCALAR_START/SCALAR_END for streaming large values.
class IonEventType(Enum):
    """Enumeration of Ion parser or serializer events.

    These types do not correspond directly to the Ion type system, but they are related.
    In particular, ``null.*`` will surface as a ``SCALAR`` even though they are containers.

    Attributes:
        INCOMPLETE: Indicates that parsing cannot be completed due to lack of input.
        STREAM_END: Indicates that the logical stream has terminated.
        VERSION_MARKER: Indicates that the **Ion Version Marker** has been encountered.
        SCALAR: Indicates an *atomic* value has been encountered.
        CONTAINER_START: Indicates that the start of a container has been reached.
        CONTAINER_END: Indicates that the end of a container has been reached.
    """
    INCOMPLETE = -2
    STREAM_END = -1
    VERSION_MARKER = 0
    SCALAR = 1
    CONTAINER_START = 2
    CONTAINER_END = 3

    @property
    def begins_value(self):
        """Indicates if the event type is a start of a value."""
        return self is IonEventType.SCALAR or self is IonEventType.CONTAINER_START

    @property
    def ends_container(self):
        """Indicates if the event type terminates a container or stream."""
        return self is IonEventType.STREAM_END or self is IonEventType.CONTAINER_END

    @property
    def is_stream_signal(self):
        """Indicates that the event type corresponds to a stream signal."""
        return self < 0


class IonEvent(record(
        'event_type',
        ('ion_type', None),
        ('value', None),
        ('field_name', None),
        ('annotations', ()),
        ('depth', None)
    )):
    """An parse or serialization event.

    Args:
        event_type (IonEventType): The type of event.
        ion_type (Optional(amazon.ion.core.IonType)): The Ion data model type
            associated with the event.
        value (Optional[any]): The data value associated with the event.
        field_name (Optional[Union[amazon.ion.symbols.SymbolToken, unicode]]): The field name
            associated with the event.
        annotations (Sequence[Union[amazon.ion.symbols.SymbolToken, unicode]]): The annotations
            associated with the event.
        depth (Optional[int]): The tree depth of the event if applicable.
    """
    def derive_field_name(self, field_name):
        """Derives a new event from this one setting the ``field_name`` attribute.

        Args:
            field_name (Union[amazon.ion.symbols.SymbolToken, unicode]): The field name to set.
        Returns:
            IonEvent: The newly generated non-thunk event.
        """
        cls = type(self)
        return cls(
            self.event_type,
            self.ion_type,
            self.value,
            field_name,
            self.annotations,
            self.depth
        )

    def derive_annotations(self, annotations):
        """Derives a new event from this one setting the ``annotations`` attribute.

        Args:
            annotations: (Sequence[Union[amazon.ion.symbols.SymbolToken, unicode]]):
                The annotations associated with the derived event.

        Returns:
            IonEvent: The newly generated non-thunk event.
        """
        cls = type(self)
        return cls(
            self.event_type,
            self.ion_type,
            self.value,
            self.field_name,
            annotations,
            self.depth
        )

    def derive_value(self, value):
        """Derives a new event from this one setting the ``value`` attribute.

        Args:
            value: (any):
                The value associated with the derived event.

        Returns:
            IonEvent: The newly generated non-thunk event.
        """
        return IonEvent(
            self.event_type,
            self.ion_type,
            value,
            self.field_name,
            self.annotations,
            self.depth
        )


class IonThunkEvent(IonEvent):
    """An :class:`IonEvent` whose ``value`` attribute is a thunk (descriptor)."""
    @property
    def value(self):
        # TODO memoize the materialized value.
        # We're masking the value field, this gets around that.
        return self[2]()

# Singletons for structural events
ION_STREAM_END_EVENT = IonEvent(IonEventType.STREAM_END)
ION_STREAM_INCOMPLETE_EVENT = IonEvent(IonEventType.INCOMPLETE)
ION_VERSION_MARKER_EVENT = IonEvent(
    IonEventType.VERSION_MARKER, ion_type=None, value=(1, 0), depth=0
)


class DataEvent(record('type', 'data')):
    """Event generated as a result of the writer or as input into the reader.

    Args:
        type (Enum): The type of event.
        data (bytes):  The serialized data returned.  If no data is to be serialized,
            this should be the empty byte string.
    """


class Transition(record('event', 'delegate')):
    """A pair of event and co-routine delegate.

    This is generally used as a result of a state-machine.

    Args:
        event (Union[DataEvent]): The event associated with the transition.
        delegate (Coroutine): The co-routine delegate which can be the same routine from
            whence this transition came.
    """

_MIN_OFFSET = timedelta(hours=-12)
_MAX_OFFSET = timedelta(hours=12)


class OffsetTZInfo(tzinfo):
    """A trivial UTC offset :class:`tzinfo`."""
    def __init__(self, delta=timedelta()):
        if delta < _MIN_OFFSET or delta > _MAX_OFFSET:
            raise ValueError('Invalid UTC offset: %s' % delta)
        self.delta = delta

    def dst(self, date_time):
        return timedelta()

    def tzname(self, date_time):
        return None

    def utcoffset(self, date_time):
        return self.delta


class TimestampPrecision(Enum):
    """The different levels of precision supported in an Ion timestamp."""
    YEAR = 0
    MONTH = 1
    DAY = 2
    MINUTE = 3
    SECOND = 4

    @property
    def includes_month(self):
        """Precision has at least the ``month`` field."""
        return self >= TimestampPrecision.MONTH

    @property
    def includes_day(self):
        """Precision has at least the ``day`` field."""
        return self >= TimestampPrecision.DAY

    @property
    def includes_minute(self):
        """Precision has at least the ``minute`` field."""
        return self >= TimestampPrecision.MINUTE

    @property
    def includes_second(self):
        """Precision has at least the ``second`` field."""
        return self >= TimestampPrecision.SECOND


_TS_PRECISION_FIELD = 'precision'


class Timestamp(datetime):
    """Sub-class of :class:`datetime` that supports a precision field

    Notes:
        The ``precision`` field is passed as a keyword argument of the same name.
    """
    __slots__ = [_TS_PRECISION_FIELD]

    def __new__(cls, *args, **kwargs):
        precision = kwargs.get(_TS_PRECISION_FIELD)
        if precision is not None:
            # Make sure we mask this before we construct the datetime.
            del kwargs[_TS_PRECISION_FIELD]

        instance = super(Timestamp, cls).__new__(cls, *args, **kwargs)
        setattr(instance, _TS_PRECISION_FIELD, precision)

        return instance

    def adjust_from_utc_fields(self, *args, **kwargs):
        """Constructs a timestamp from UTC fields adjusted to the local offset if given."""
        raw_ts = Timestamp(*args, **kwargs)
        offset = raw_ts.utcoffset()
        if offset is None or offset == timedelta():
            return raw_ts

        # XXX This returns a datetime, not a Timestamp (which has our precision if defined)
        adjusted = raw_ts + offset
        if raw_ts.precision is None :
            # No precision means we can just return a regular datetime
            return adjusted

        return Timestamp(
            adjusted.year,
            adjusted.month,
            adjusted.day,
            adjusted.hour,
            adjusted.minute,
            adjusted.second,
            adjusted.microsecond,
            raw_ts.tzinfo,
            precision=raw_ts.precision
        )
