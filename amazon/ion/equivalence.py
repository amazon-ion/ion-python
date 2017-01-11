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

from amazon.ion.core import IonType, _ZERO_DELTA, Timestamp, TimestampPrecision
from amazon.ion.simple_types import _IonNature, IonPyList, IonPyDict, IonPyTimestamp, IonPyNull, IonPySymbol, \
    IonPyText, IonPyDecimal, IonPyFloat
from amazon.ion.symbols import SymbolToken


def ion_equals(a, b):
    for a, b in ((a, b), (b, a)):
        if isinstance(a, _IonNature):
            if isinstance(b, _IonNature):
                eq = a.ion_type is b.ion_type and _annotations_eq(a, b)
            else:
                eq = not a.ion_annotations
            if eq:
                if isinstance(a, IonPyList):
                    return _sequences_eq(a, b)
                elif isinstance(a, IonPyDict):
                    return _structs_eq(a, b)
                elif isinstance(a, IonPyTimestamp):
                    return _timestamps_eq(a, b)
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
    # Reaching this point means that neither a nor b has _IonNature.
    if isinstance(a, list):
        return _sequences_eq(a, b)
    elif isinstance(a, dict):
        return _structs_eq(a, b)
    elif isinstance(a, datetime):
        return _timestamps_eq(a, b)
    elif isinstance(a, SymbolToken):
        return _symbols_eq(a, b)
    elif isinstance(a, Decimal):
        return _decimals_eq(a, b)
    elif isinstance(a, float):
        return _floats_eq(a, b)
    return a == b


def _annotations_eq(a, b):
    return _sequences_eq(a.ion_annotations, b.ion_annotations, _symbols_eq)


def _sequences_eq(a, b, comparison_func=ion_equals):
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


def _structs_eq(a, b):
    assert isinstance(a, dict)
    if not isinstance(b, dict):
        return False
    # TODO support multiple mappings from same field name.
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
            if not ion_equals(a[key], b[key]):
                return False
    return True


def _timestamps_eq(a, b):
    assert isinstance(a, datetime)
    if not isinstance(b, datetime):
        return False
    _a = a - _ZERO_DELTA
    _b = b - _ZERO_DELTA
    if (a.tzinfo is None) ^ (b.tzinfo is None):
        return False
    if a.tzinfo is not None:
        assert b.tzinfo is not None
        _a = a - a.tzinfo.delta
        _a = _a.replace(tzinfo=None)
        _b = b - b.tzinfo.delta
        _b = _b.replace(tzinfo=None)
    for a, b in ((a, b), (b, a)):
        if isinstance(a, Timestamp):
            if isinstance(b, Timestamp):
                if a.precision is b.precision and a.fractional_precision is b.fractional_precision:
                    break
                return False
            elif a.precision is not TimestampPrecision.SECOND:
                return False
    return _a == _b


def _symbols_eq(a, b):
    try:
        a_text = a.text
    except AttributeError:
        a_text = a
    try:
        b_text = b.text
    except AttributeError:
        b_text = b
    return a_text == b_text


def _decimals_eq(a, b):
    assert isinstance(a, Decimal)
    if not isinstance(b, Decimal):
        return False
    if a.is_zero() and b.is_zero():
        if a.is_signed() ^ b.is_signed():
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
        return not (_is_float_negative_zero(a) ^ _is_float_negative_zero(b))
    return a == b or (isnan(a) and isnan(b))
