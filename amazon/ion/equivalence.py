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

"""Provides utilities for determining whether two objects are equivalent under the Ion data model."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import struct
from datetime import datetime
from decimal import Decimal
from math import isnan

import six

from amazon.ion.core import IonType, Timestamp, TimestampPrecision, MICROSECOND_PRECISION, OffsetTZInfo, Multimap
from amazon.ion.simple_types import _IonNature, IonPyList, IonPyDict, IonPyTimestamp, IonPyNull, IonPySymbol, \
    IonPyText, IonPyDecimal, IonPyFloat
from amazon.ion.symbols import SymbolToken


def ion_equals(a, b, timestamps_instants_only=False):
    """Tests two objects for equivalence under the Ion data model.

    There are three important cases:
        * When neither operand specifies its `ion_type` or `annotations`, this method will only return True when the
          values of both operands are equivalent under the Ion data model.
        * When only one of the operands specifies its `ion_type` and `annotations`, this method will only return True
          when that operand has no annotations and has a value equivalent to the other operand under the Ion data model.
        * When both operands specify `ion_type` and `annotations`, this method will only return True when the ion_type
          and annotations of both are the same and their values are equivalent under the Ion data model.

    Note that the order of the operands does not matter.

    Args:
        a (object): The first operand.
        b (object): The second operand.
        timestamps_instants_only (Optional[bool]): False if timestamp objects (datetime and its subclasses) should be
            compared according to the Ion data model (where the instant, precision, and offset must be equal); True
            if these objects should be considered equivalent if they simply represent the same instant.
    """
    if timestamps_instants_only:
        return _ion_equals_timestamps_instants(a, b)
    return _ion_equals_timestamps_data_model(a, b)


def _ion_equals_timestamps_instants(a, b):
    return _ion_equals(a, b, _timestamp_instants_eq, _ion_equals_timestamps_instants)


def _ion_equals_timestamps_data_model(a, b):
    return _ion_equals(a, b, _timestamps_eq, _ion_equals_timestamps_data_model)


def _ion_equals(a, b, timestamp_comparison_func, recursive_comparison_func):
    """Compares a and b according to the description of the ion_equals method."""
    for a, b in ((a, b), (b, a)):  # Ensures that operand order does not matter.
        if isinstance(a, _IonNature):
            if isinstance(b, _IonNature):
                # Both operands have _IonNature. Their IonTypes and annotations must be equivalent.
                eq = a.ion_type is b.ion_type and _annotations_eq(a, b)
            else:
                # Only one operand has _IonNature. It cannot be equivalent to the other operand if it has annotations.
                eq = not a.ion_annotations
            if eq:
                if isinstance(a, IonPyList):
                    return _sequences_eq(a, b, recursive_comparison_func)
                elif isinstance(a, IonPyDict):
                    return _structs_eq(a, b, recursive_comparison_func)
                elif isinstance(a, IonPyTimestamp):
                    return timestamp_comparison_func(a, b)
                elif isinstance(a, IonPyNull):
                    return isinstance(b, IonPyNull) or (b is None and a.ion_type is IonType.NULL)
                elif isinstance(a, IonPySymbol) or (isinstance(a, IonPyText) and a.ion_type is IonType.SYMBOL):
                    return _symbols_eq(a, b)
                elif isinstance(a, IonPyDecimal):
                    return _decimals_eq(a, b)
                elif isinstance(a, IonPyFloat):
                    return _floats_eq(a, b)
                else:
                    return a == b
            return False
    # Reaching this point means that neither operand has _IonNature.
    for a, b in ((a, b), (b, a)):  # Ensures that operand order does not matter.
        if isinstance(a, list):
            return _sequences_eq(a, b, recursive_comparison_func)
        elif isinstance(a, dict):
            return _structs_eq(a, b, recursive_comparison_func)
        elif isinstance(a, datetime):
            return timestamp_comparison_func(a, b)
        elif isinstance(a, SymbolToken):
            return _symbols_eq(a, b)
        elif isinstance(a, Decimal):
            return _decimals_eq(a, b)
        elif isinstance(a, float):
            return _floats_eq(a, b)
    return a == b


def _annotations_eq(a, b):
    return _sequences_eq(a.ion_annotations, b.ion_annotations, _symbols_eq)


def _sequences_eq(a, b, comparison_func):
    assert isinstance(a, (list, tuple))
    if not isinstance(b, (list, tuple)):
        return False
    sequence_len = len(a)
    if sequence_len != len(b):
        return False
    for i in range(sequence_len):
        if not comparison_func(a[i], b[i]):
            return False
    return True


def _structs_eq(a, b, comparison_func):
    assert isinstance(a, (dict, Multimap))
    if not isinstance(b, (dict, Multimap)):
        return False
    dict_len = len(a)
    if dict_len != len(b):
        return False
    for a, b in ((a, b), (b, a)):
        key_iter = six.iterkeys(a)
        while True:
            try:
                key = next(key_iter)
            except StopIteration:
                break
            if key not in b:
                return False
            if isinstance(a, Multimap) and isinstance(b, Multimap):
                values_a = a.get_all_values(key)
                values_b = b.get_all_values(key)
                if len(values_a) != len(values_b):
                    return False
                for value_a in values_a:
                    if not any(comparison_func(value_a, value_b) for value_b in values_b):
                        return False
            else:
                if not comparison_func(a[key], b[key]):
                    return False

    return True


def _timestamps_eq(a, b):
    """Compares two timestamp operands for equivalence under the Ion data model."""
    assert isinstance(a, datetime)
    if not isinstance(b, datetime):
        return False
    # Local offsets must be equivalent.
    if (a.tzinfo is None) ^ (b.tzinfo is None):
        return False
    if a.utcoffset() != b.utcoffset():
        return False
    for a, b in ((a, b), (b, a)):
        if isinstance(a, Timestamp):
            if isinstance(b, Timestamp):
                # Both operands declare their precisions. They are only equivalent if their precisions are the same.
                if a.precision is b.precision and a.fractional_precision is b.fractional_precision \
                        and a.fractional_seconds == b.fractional_seconds:
                    break
                return False
            elif a.precision is not TimestampPrecision.SECOND or a.fractional_precision != MICROSECOND_PRECISION:
                # Only one of the operands declares its precision. It is only equivalent to the other (a naive datetime)
                # if it has full microseconds precision.
                return False
    return a == b


def _timestamp_instants_eq(a, b):
    """Compares two timestamp operands for point-in-time equivalence only."""
    assert isinstance(a, datetime)
    if not isinstance(b, datetime):
        return False
    # datetime's __eq__ can't compare a None offset and a non-None offset. For these equivalence semantics, a None
    # offset (unknown local offset) is treated equivalently to a +00:00.
    if a.tzinfo is None:
        a = a.replace(tzinfo=OffsetTZInfo())
    if b.tzinfo is None:
        b = b.replace(tzinfo=OffsetTZInfo())
    # datetime's __eq__ implementation compares instants; offsets and precision need not be equal.
    return a == b


def _symbols_eq(a, b):
    assert isinstance(a, (six.text_type, SymbolToken))
    if not isinstance(b, (six.text_type, SymbolToken)):
        return False
    a_text = getattr(a, 'text', a)
    b_text = getattr(b, 'text', b)
    if a_text == b_text:
        if a_text is None:
            # Both have unknown text. If they come from a local context, they are equivalent.
            a_location = getattr(a, 'location', None)
            b_location = getattr(b, 'location', None)
            if (a_location is None) ^ (b_location is None):
                return False
            if a_location is not None:
                # Both were imported from shared symbol tables. In this case, they are only equivalent if they were
                # imported from the same position in the same shared symbol table.
                if (a_location.name != b_location.name) or (a_location.position != b_location.position):
                    return False
            a_sid = getattr(a, 'sid', None)
            b_sid = getattr(b, 'sid', None)
            if a_sid is None or b_sid is None:
                raise ValueError('Attempted to compare malformed symbols %s, %s.' % (a, b))
            if (a_sid == 0) ^ (b_sid == 0):
                # SID 0 is only equal to SID 0.
                return False
        return True
    return False


def _decimals_eq(a, b):
    assert isinstance(a, Decimal)
    if not isinstance(b, Decimal):
        return False
    if a.is_zero() and b.is_zero():
        if a.is_signed() ^ b.is_signed():
            # Negative-zero is not equivalent to positive-zero.
            return False
    # This ensures that both have equal precision.
    return a.canonical().compare_total(b.canonical()) == 0


def _is_float_negative_zero(x):
    return struct.pack('>d', x) == b'\x80\x00\x00\x00\x00\x00\x00\x00'


def _floats_eq(a, b):
    assert isinstance(a, float)
    if not isinstance(b, float):
        return False
    if a == 0 and b == 0:
        # Negative-zero is not equivalent to positive-zero.
        return not (_is_float_negative_zero(a) ^ _is_float_negative_zero(b))
    # nan is always equivalent to nan.
    return a == b or (isnan(a) and isnan(b))
