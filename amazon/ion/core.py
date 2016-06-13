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
            IonEvent: The newly generated event.
        """
        return IonEvent(
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
            IonEvent: The newly generated event.
        """
        return IonEvent(
            self.event_type,
            self.ion_type,
            self.value,
            self.field_name,
            annotations,
            self.depth
        )
