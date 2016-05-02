# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections


def iteritems(dictionary):
    """Utility to provide an iterable view over a dict's items.

    This is specifically to deal with the removal of ``dict.iteritems`` in Python 3.
    """
    iteritems_method = getattr(dictionary, 'iteritems')
    if iteritems_method is not None:
        return iteritems_method()
    return iter(dictionary.items())


class _EnumMetaClass(type):
    """Metaclass for simple enumerations.

    Specifically provides the machinery necessary to emulate simplified Python 3.4 enumerations.
    """
    def __init__(cls, name, bases, attrs):
        members = {}
        # Re-bind any non magic-named method with an instance of the enumeration.
        for attr_name, attr_value in iteritems(attrs):
            if not attr_name.startswith('_') and not callable(attr_value):
                actual_value = cls(attr_name, attr_value)
                setattr(cls, attr_name, actual_value)
                members[attr_value] = actual_value

        # Store the members reverse index.
        cls._enum_members = members

        super(_EnumMetaClass, cls).__init__(name, bases, attrs)

    def __getitem__(cls, name):
        """Looks up an enumeration value field by either name or ordinal."""
        return cls._enum_members[name]

class Enum(object):
    """Simple enumeration type.

    The typical declaration looks like::

        class MyEnum(Enum):
            A = 1
            B = 2
            C = 3

    At this point ``MyEnum.A`` is an instance of ``MyEnum``.

    Note:
        Proper enumerations were added in Python 3.4 (PEP 435), this is a very simplified implementation
        based loosely on that specification.

        In particular, implicit order of the values is not supported.

    Args:
        value (Any): the value associated with the enumeration.

    Attributes:
        name (str): The name of the enum.
        value (Any): The underlying value associated with the enum.
    """
    __metaclass__ = _EnumMetaClass
    _enum_members = {}

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __str__(self):
        return '<%s.%s: %s>' % (type(self).__name__, self.name, self.value)
    __repr__ = __str__


class _RecordMetaClass(type):
    """Metaclass for defining named-tuple based immutable record types."""
    def __new__(cls, name, bases, attrs):
        if attrs.get('_record_sentinel') is None:
            field_declarations = []
            for base_class in bases:
                parent_declarations = getattr(base_class, '_record_fields', [])
                field_declarations.extend(parent_declarations)
            names = []
            defaults = []

            has_defaults = False
            for field in field_declarations:
                if isinstance(field, str):
                    if has_defaults:
                        raise ValueError('Non-defaulted record field must have default: %s' % field)
                    names.append(field)
                elif isinstance(field, tuple) and len(field) == 2:
                    names.append(field[0])
                    defaults.append(field[1])
                    has_defaults = True
                else:
                    raise ValueError('Unable to bind record field: %s' % (field,))

            # Construct actual base type/defaults.
            base_class = collections.namedtuple(name, names)
            base_class.__new__.__defaults__ = tuple(defaults)
            # Eliminate our placeholder(s) in the hierarchy.
            bases = (base_class,)

        return super(_RecordMetaClass, cls).__new__(cls, name, bases, attrs)


def record(*fields):
    """Constructs a type that can be extended to create immutable, value types.

    A typical declaration looks like::

        class MyRecord(record('a', ('b', 1))):
            pass

    The above would make a sub-class of ``collections.namedtuple`` that was named ``MyRecord`` with
    a constructor that had the ``b`` field set to 1 by default.

    Note:
        This uses meta-class machinery to rewrite the inheritance hierarchy.
        This is done in order to make sure that the underlying ``namedtuple`` instance is
        bound to the right type name and to make sure that the synthetic class that is generated
        to enable this machinery is not enabled for sub-classes of a user's record class.

    Args:
        fields: A sequence of str or pairs that
    """
    class RecordType(object):
        _record_sentinel = True
        _record_fields = fields
        __metaclass__ = _RecordMetaClass

    return RecordType
