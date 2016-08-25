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

from math import isnan
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
    def __eq__(self, other):
        if not isinstance(other, IonEvent):
            return False

        if isinstance(self.value, float):
            if not isinstance(other.value, float):
                return False

            # Need to deal with NaN appropriately.
            if self.value != other.value and not (isnan(self.value) and isnan(other.value)):
                return False
        else:
            if self.value != other.value:
                return False

            # Timestamp precision has additional requirements.
            if isinstance(self.value, Timestamp) or isinstance(other.value, Timestamp):
                # Special case for timestamps to capture equivalence over precision.
                self_precision = getattr(self.value, TIMESTAMP_PRECISION_FIELD, None)
                other_precision = getattr(other.value, TIMESTAMP_PRECISION_FIELD, None)
                if self_precision != other_precision:
                    return False
            if isinstance(self.value, datetime):
                if self.value.utcoffset() != other.value.utcoffset():
                    return False

        return (self.event_type == other.event_type
            and self.ion_type == other.ion_type
            and self.field_name == other.field_name
            and self.annotations == other.annotations
            and self.depth == other.depth
        )

    def derive_field_name(self, field_name):
        """Derives a new event from this one setting the ``field_name`` attribute.

        Args:
            field_name (Union[amazon.ion.symbols.SymbolToken, unicode]): The field name to set.
        Returns:
            IonEvent: The newly generated event.
        """
        cls = type(self)
        # We use ordinals to avoid thunk materialization.
        return cls(
            self[0],
            self[1],
            self[2],
            field_name,
            self[4],
            self[5]
        )

    def derive_annotations(self, annotations):
        """Derives a new event from this one setting the ``annotations`` attribute.

        Args:
            annotations: (Sequence[Union[amazon.ion.symbols.SymbolToken, unicode]]):
                The annotations associated with the derived event.

        Returns:
            IonEvent: The newly generated event.
        """
        cls = type(self)
        # We use ordinals to avoid thunk materialization.
        return cls(
            self[0],
            self[1],
            self[2],
            self[3],
            annotations,
            self[5]
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

    def derive_depth(self, depth):
        """Derives a new event from this one setting the ``depth`` attribute.

        Args:
            depth: (int):
                The annotations associated with the derived event.

        Returns:
            IonEvent: The newly generated event.
        """
        cls = type(self)
        # We use ordinals to avoid thunk materialization.
        return cls(
            self[0],
            self[1],
            self[2],
            self[3],
            self[4],
            depth
        )


class MemoizingThunk(object):
    """A :class:`callable` that invokes a ``delegate`` and caches and returns the result."""
    def __init__(self, delegate):
        self.delegate = delegate

    def __call__(self):
        if hasattr(self, 'value'):
            return self.value
        self.value = self.delegate()
        return self.value

    def __str__(self):
        return str(self())

    def __repr__(self):
        return repr(self())


class IonThunkEvent(IonEvent):
    """An :class:`IonEvent` whose ``value`` field is a thunk."""
    def __new__(cls, *args, **kwargs):
        if len(args) >= 3:
            args = list(args)
            args[2] = MemoizingThunk(args[2])
        else:
            value = kwargs.get('value')
            if value is not None:
                kwargs['value'] = MemoizingThunk(kwargs['value'])
        return super(IonThunkEvent, cls).__new__(cls, *args, **kwargs)

    @property
    def value(self):
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
_ZERO_DELTA = timedelta()


class OffsetTZInfo(tzinfo):
    """A trivial UTC offset :class:`tzinfo`."""
    def __init__(self, delta=_ZERO_DELTA):
        if delta < _MIN_OFFSET or delta > _MAX_OFFSET:
            raise ValueError('Invalid UTC offset: %s' % delta)
        self.delta = delta

    def dst(self, date_time):
        return timedelta()

    def tzname(self, date_time):
        return None

    def utcoffset(self, date_time):
        return self.delta

    def __repr__(self):
        sign = '+'
        delta = self.delta
        if delta < _ZERO_DELTA:
            sign = '-'
            delta = _ZERO_DELTA - delta
        return 'OffsetTZInfo(%s%s)' % (sign, delta)


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


TIMESTAMP_PRECISION_FIELD = 'precision'


class Timestamp(datetime):
    """Sub-class of :class:`datetime` that supports a precision field

    Notes:
        The ``precision`` field is passed as a keyword argument of the same name.
    """
    __slots__ = [TIMESTAMP_PRECISION_FIELD]

    def __new__(cls, *args, **kwargs):
        precision = None
        if TIMESTAMP_PRECISION_FIELD in kwargs:
            precision = kwargs.get(TIMESTAMP_PRECISION_FIELD)
            # Make sure we mask this before we construct the datetime.
            del kwargs[TIMESTAMP_PRECISION_FIELD]

        instance = super(Timestamp, cls).__new__(cls, *args, **kwargs)
        setattr(instance, TIMESTAMP_PRECISION_FIELD, precision)

        return instance

    def __repr__(self):
        return 'Timestamp(%04d-%02d-%02dT%02d:%02d:%02d.%06d, %r, %r)' % \
               (self.year, self.month, self.day,
                self.hour, self.minute, self.second, self.microsecond,
                self.tzinfo, self.precision)

    @staticmethod
    def adjust_from_utc_fields(*args, **kwargs):
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


def timestamp(year, month=1, day=1,
              hour=0, minute=0, second=0, microsecond=0,
              off_hours=None, off_minutes=None,
              precision=None):
    """Shorthand for the :class:`Timestamp` constructor.

    Specifically, converts ``off_hours`` and ``off_minutes`` parameters to a suitable
    :class:`OffsetTZInfo` instance.
    """
    delta = None
    if off_hours is not None:
        delta = timedelta(hours=off_hours)
    if off_minutes is not None:
        minutes_delta = timedelta(minutes=off_minutes)
        if delta is None:
            delta = minutes_delta
        else:
            delta += minutes_delta

    tz = None
    if delta is not None:
        tz = OffsetTZInfo(delta)

    return Timestamp(
        year, month, day,
        hour, minute, second, microsecond,
        tz, precision=precision
    )
