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

from collections import MutableMapping, MutableSequence, OrderedDict
from datetime import datetime, timedelta, tzinfo
from decimal import Decimal, ROUND_FLOOR, Context, Inexact
from math import isnan

import six

from .util import Enum
from .util import record


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
    def is_numeric(self):
        return IonType.INT <= self <= IonType.TIMESTAMP

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
                if self_precision != other_precision \
                        and not ((self_precision is None and other_precision is TimestampPrecision.SECOND) or
                                 (self_precision is TimestampPrecision.SECOND and other_precision is None)):
                    # The absence of precision indicates a naive datetime, which always has SECOND precision.
                    return False

            if isinstance(self.value, Timestamp) and isinstance(other.value, Timestamp):
                self_fractional_seconds = getattr(self.value, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, None)
                other_fractional_seconds = getattr(other.value, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, None)
                if self_fractional_seconds != other_fractional_seconds:
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

_MIN_OFFSET = timedelta(hours=-24)
_MAX_OFFSET = timedelta(hours=24)
_ZERO_DELTA = timedelta()


class OffsetTZInfo(tzinfo):
    """A trivial UTC offset :class:`tzinfo`."""
    def __init__(self, delta=_ZERO_DELTA):
        if delta <= _MIN_OFFSET or delta >= _MAX_OFFSET:
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
TIMESTAMP_FRACTION_PRECISION_FIELD = 'fractional_precision'
TIMESTAMP_FRACTIONAL_SECONDS_FIELD = 'fractional_seconds'
TIMESTAMP_MICROSECOND_FIELD = 'microsecond'
MICROSECOND_PRECISION = 6
BASE_TEN_MICROSECOND_PRECISION_EXPONENTIATION = 10 ** MICROSECOND_PRECISION
DECIMAL_ZERO = Decimal(0)
PRECISION_LIMIT_LOOKUP = (
    DECIMAL_ZERO,
    Decimal('0.1'),
    Decimal('0.01'),
    Decimal('0.001'),
    Decimal('0.0001'),
    Decimal('0.00001'),
    Decimal('0.000001')
)
DATETIME_CONSTRUCTOR_MICROSECOND_ARGUMENT_INDEX = 6


class Timestamp(datetime):
    """Sub-class of :class:`datetime` that supports a precision field; a ``fractional_precision``
        field that specifies the  precision of the``microseconds`` field in :class:`datetime`;
        and a ``fractional_seconds`` field that is a :class:`Decimal` specifying the fractional
        seconds precisely.

        Notes:
            * The ``precision`` field is passed as a keyword argument of the same name.

            * The ``fractional_precision`` field is passed as a keyword argument of the same name.
              This field only relates to to the ``microseconds`` field and can be thought of
              as the number of decimal digits that are significant.  This is an integer that
              that is in the closed interval ``[0, 6]``.  If ``0``, ``microseconds`` must be
              ``0`` indicating no precision below seconds.  This argument is optional and only valid
              when ``microseconds`` is not ``None``.  If the ``microseconds`` specified has more
              precision than this field indicates, then that is an error.

            * The ``fractional_seconds`` field is passed as a keyword argument of the same name.
              It must be a :class:`Decimal` in the left-closed, right-opened interval of ``[0, 1)``.
              If specified as an argument, ``microseconds`` must be ``None`` **and** ``fractional_precision``
              must not be specified (but can be ``None``).  In addition, if ``microseconds`` is specified
              this argument must not be specified (but can be ``None``). If the specified value has
              ``coefficient==0`` and ``exponent >= 0``, e.g. ``Decimal(0)``, then there is no precision
              beyond seconds.

            * After construction, ``microseconds``, ``fractional_precision``, and ``fractional_seconds``
              will all be present and normalized in the resulting :class:`Timestamp` instance.  If the
              precision of ``fractional_seconds`` is more than is capable of being expressed in
              ``microseconds``, then the ``microseconds`` field is truncated to six digits and
              ``fractional_precision`` is ``6``.

            Consider some examples:

            * `2019-10-01T12:45:01Z` would have the following fields set:
              * ``microseconds == 0``, ``fractional_precision == 0``, ``fractional_seconds == Decimal('0')``
            * `2019-10-01T12:45:01.100Z` would have the following fields set:
              * ``microseconds == 100000``, ``fractional_precision == 3``, ``fractional_seconds == Decimal('0.100')``
            * `2019-10-01T12:45:01.123456789Z` would have the following fields set:
              * ``microseconds == 123456``, ``fractional_precision == 6``, ``fractional_seconds ==
              Decimal('0.123456789')``

        Raises:
            ValueError: If any of the preconditions above are violated.
        """
    __slots__ = [TIMESTAMP_PRECISION_FIELD, TIMESTAMP_FRACTION_PRECISION_FIELD, TIMESTAMP_FRACTIONAL_SECONDS_FIELD]

    def __new__(cls, *args, **kwargs):
        def replace_microsecond(new_value):
            if has_microsecond_argument:
                lst = list(args)
                lst[DATETIME_CONSTRUCTOR_MICROSECOND_ARGUMENT_INDEX] = new_value
                return tuple(lst)
            else:
                kwargs[TIMESTAMP_MICROSECOND_FIELD] = new_value
                return args

        precision = None
        fractional_precision = None
        fractional_seconds = None
        datetime_microseconds = None
        has_microsecond_argument = len(args) > DATETIME_CONSTRUCTOR_MICROSECOND_ARGUMENT_INDEX
        if has_microsecond_argument:
            datetime_microseconds = args[DATETIME_CONSTRUCTOR_MICROSECOND_ARGUMENT_INDEX]
        elif TIMESTAMP_MICROSECOND_FIELD in kwargs:
            datetime_microseconds = kwargs.get(TIMESTAMP_MICROSECOND_FIELD)
        if TIMESTAMP_PRECISION_FIELD in kwargs:
            precision = kwargs.get(TIMESTAMP_PRECISION_FIELD)
            # Make sure we mask this before we construct the datetime.
            del kwargs[TIMESTAMP_PRECISION_FIELD]
        if TIMESTAMP_FRACTION_PRECISION_FIELD in kwargs:
            fractional_precision = kwargs.get(TIMESTAMP_FRACTION_PRECISION_FIELD)
            if fractional_precision is not None and not (0 <= fractional_precision <= MICROSECOND_PRECISION):
                raise ValueError('Cannot construct a Timestamp with fractional precision of %d digits, '
                                 'which is out of the supported range of [0, %d].'
                                 % (fractional_precision, MICROSECOND_PRECISION))
            # Make sure we mask this before we construct the datetime.
            del kwargs[TIMESTAMP_FRACTION_PRECISION_FIELD]
        if TIMESTAMP_FRACTIONAL_SECONDS_FIELD in kwargs:
            fractional_seconds = kwargs.get(TIMESTAMP_FRACTIONAL_SECONDS_FIELD)
            if fractional_seconds is not None:
                if not (0 <= fractional_seconds < 1):
                    raise ValueError('Cannot construct a Timestamp with fractional seconds of %s, '
                                     'which is out of the supported range of [0, 1).'
                                     % str(fractional_seconds))
            # Make sure we mask this before we construct the datetime.
            del kwargs[TIMESTAMP_FRACTIONAL_SECONDS_FIELD]

        if fractional_seconds is not None and (fractional_precision is not None or datetime_microseconds is not None):
            raise ValueError('fractional_seconds cannot be specified '
                             'when fractional_precision or microseconds are not None.')

        if fractional_precision is not None and datetime_microseconds is None:
            raise ValueError('datetime_microseconds cannot be None while fractional_precision is not None.')

        if fractional_precision == 0 and datetime_microseconds != 0:
            raise ValueError('datetime_microseconds cannot be non-zero while fractional_precision is 0.')

        if fractional_seconds is not None:
            fractional_seconds_exponent = fractional_seconds.as_tuple().exponent
            if fractional_seconds == DECIMAL_ZERO and fractional_seconds_exponent > 0:
                # Zero with a positive exponent is just zero. Set the exponent to zero so fractional_precision is
                # calculated correctly.
                fractional_seconds_exponent = 0
                fractional_seconds = DECIMAL_ZERO
            fractional_precision = min(-fractional_seconds_exponent, MICROSECOND_PRECISION)
            # Scale to microseconds and truncate to an integer.
            args = replace_microsecond(int(fractional_seconds * BASE_TEN_MICROSECOND_PRECISION_EXPONENTIATION))
        elif datetime_microseconds is not None:
            if fractional_precision is None:
                fractional_precision = MICROSECOND_PRECISION
            if fractional_precision == 0:
                # As previously verified, datetime_microseconds must be zero in this case.
                fractional_seconds = DECIMAL_ZERO
            else:
                try:
                    fractional_seconds = Decimal(datetime_microseconds).scaleb(-MICROSECOND_PRECISION)\
                        .quantize(PRECISION_LIMIT_LOOKUP[fractional_precision], context=Context(traps=[Inexact]))
                except Inexact:
                    raise ValueError('microsecond value %d cannot be expressed exactly in %d digits.'
                                     % (datetime_microseconds, fractional_precision))
        else:
            assert datetime_microseconds is None
            # The datetime constructor requires a non-None microsecond argument.
            args = replace_microsecond(0)
            fractional_precision = 0
            fractional_seconds = DECIMAL_ZERO

        instance = super(Timestamp, cls).__new__(cls, *args, **kwargs)
        setattr(instance, TIMESTAMP_PRECISION_FIELD, precision)
        setattr(instance, TIMESTAMP_FRACTION_PRECISION_FIELD, fractional_precision)
        setattr(instance, TIMESTAMP_FRACTIONAL_SECONDS_FIELD, fractional_seconds)

        return instance

    def __repr__(self):
        return 'Timestamp(%04d-%02d-%02dT%02d:%02d:%02d.%06d, %r, %r, %s=%s)' % \
               (self.year, self.month, self.day,
                self.hour, self.minute, self.second, self.microsecond,
                self.tzinfo, self.precision,
                TIMESTAMP_FRACTION_PRECISION_FIELD, self.fractional_precision)

    @staticmethod
    def adjust_from_utc_fields(*args, **kwargs):
        """Constructs a timestamp from UTC fields adjusted to the local offset if given."""
        raw_ts = Timestamp(*args, **kwargs)
        offset = raw_ts.utcoffset()
        if offset is None or offset == timedelta():
            return raw_ts

        # XXX This returns a datetime, not a Timestamp (which has our precision if defined)
        adjusted = raw_ts + offset
        if raw_ts.precision is None:
            # No precision means we can just return a regular datetime
            return adjusted

        return Timestamp(
            adjusted.year,
            adjusted.month,
            adjusted.day,
            adjusted.hour,
            adjusted.minute,
            adjusted.second,
            None,
            raw_ts.tzinfo,
            precision=raw_ts.precision,
            fractional_precision=None,
            fractional_seconds=raw_ts.fractional_seconds
        )


def timestamp(year, month=1, day=1,
              hour=0, minute=0, second=0, microsecond=None,
              off_hours=None, off_minutes=None,
              precision=None, fractional_precision=None, fractional_seconds=None):
    """Shorthand for the :class:`Timestamp` constructor.

    Specifically, converts ``off_hours`` and ``off_minutes`` parameters to a suitable
    :class:`OffsetTZInfo` instance.
    """
    delta = None
    if off_hours is not None:
        if off_hours < -23 or off_hours > 23:
            raise ValueError('Hour offset %d is out of required range -23..23.' % (off_hours,))
        delta = timedelta(hours=off_hours)
    if off_minutes is not None:
        if off_minutes < -59 or off_minutes > 59:
            raise ValueError('Minute offset %d is out of required range -59..59.' % (off_minutes,))
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
        tz, precision=precision, fractional_precision=fractional_precision, fractional_seconds=fractional_seconds
    )


class Multimap(MutableMapping):
    """
    Dictionary that can hold multiple values for the same key

    In order not to break existing customers, getting and inserting elements with ``[]`` keeps the same behaviour
    as the built-in dict. If multiple elements are already mapped to the key, ``[]`  will return
    the newest one.

    To map multiple elements to a key, use the ``add_item`` operation.
    To retrieve all the values map to a key, use ``get_all_values``.
    """

    def __init__(self, *args, **kwargs):
        super(Multimap, self).__init__()
        self.__store = OrderedDict()
        if args is not None and len(args) > 0:
            for key, value in six.iteritems(args[0]):
                self.__store[key] = MultimapValue(value)

    def __getitem__(self, key):
        return self.__store[key][len(self.__store[key]) - 1]  # Return only one in order not to break clients

    def __delitem__(self, key):
        del self.__store[key]

    def __setitem__(self, key, value):
        self.__store[key] = MultimapValue(value)

    def __len__(self):
        return sum([len(values) for values in six.itervalues(self.__store)])

    def __iter__(self):
        for key in six.iterkeys(self.__store):
            yield key

    def __str__(self):
        return repr(self)

    def __repr__(self):
        str_repr = '{'
        for key, value in self.items():
            str_repr += '%r: %r, ' % (key, value)
        str_repr = str_repr[:len(str_repr) - 2] + '}'
        return six.ensure_binary(str_repr) if six.PY2 else str_repr

    def add_item(self, key, value):
        if key in self.__store:
            self.__store[key].append(value)
        else:
            self.__setitem__(key, value)

    def get_all_values(self, key):
        return self.__store[key]

    def iteritems(self):
        for key in self.__store:
            for value in self.__store[key]:
                yield (key, value)

    def items(self):
        output = []
        for k, v in self.iteritems():
            output.append((k, v))
        return output


class MultimapValue(MutableSequence):

    def __init__(self, *args):
        if args is not None:
            self.__store = [x for x in args]
        else:
            self.__store = []

    def insert(self, index, value):
        self.__setitem__(index, value)

    def __len__(self):
        return len(self.__store)

    def __getitem__(self, index):
        return self.__store[index]

    def __setitem__(self, index, value):
        self.__store.insert(index, value)

    def __delitem__(self, index):
        del self.__store[index]

    def __iter__(self):
        for x in self.__store:
            yield x
