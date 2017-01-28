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

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tests import parametrize

from amazon.ion.writer_binary_raw_fields import _write_int, _write_int_uncached, _write_varint, \
                                                _write_varint_uncached, _write_uint, \
                                                _write_uint_uncached, _write_varuint, \
                                                _write_varuint_uncached, _CACHE_SIZE


class WriteParameter:
    def __init__(self, value, expected, methods):
        self.value = value
        self.expected = expected
        self.methods = methods

    def __str__(self):
        return "[" + str(self.value) + ", " + str(self.expected) + ", " + str(self.methods) + "]"


int_methods = (_write_int, _write_int_uncached)
varint_methods = (_write_varint, _write_varint_uncached)
uint_methods = (_write_uint, _write_uint_uncached)
varuint_methods = (_write_varuint, _write_varuint_uncached)


def assert_value(p):
    expected = bytearray(p.expected)
    for method in p.methods:
        out = bytearray()
        method(out, p.value)
        assert expected == out


@parametrize(
    WriteParameter(0, [0x00], int_methods),
    WriteParameter(0, [0x00], uint_methods),
    WriteParameter(0, [0x80], varint_methods),
    WriteParameter(0, [0x80], varuint_methods),
    WriteParameter(1, [0x01], int_methods),
    WriteParameter(1, [0x01], uint_methods),
    WriteParameter(1, [0x81], varint_methods),
    WriteParameter(1, [0x81], varuint_methods),
    WriteParameter(-1, [0x81], int_methods),
    WriteParameter(-1, [0xC1], varint_methods),
)
def test_write_basic(p):
    assert_value(p)


@parametrize(
    WriteParameter(126, [0x7E], int_methods),
    WriteParameter(127, [0x7F], int_methods),
    WriteParameter(128, [0x00, 0x80], int_methods),
    WriteParameter(32766, [0x7F, 0xFE], int_methods),
    WriteParameter(32767, [0x7F, 0xFF], int_methods),
    WriteParameter(32768, [0x00, 0x80, 0x00], int_methods),
    WriteParameter(8388606, [0x7F, 0xFF, 0xFE], int_methods),
    WriteParameter(8388607, [0x7F, 0xFF, 0xFF], int_methods),
    WriteParameter(8388608, [0x00, 0x80, 0x00, 0x00], int_methods),
    WriteParameter(-126, [0xFE], int_methods),
    WriteParameter(-127, [0xFF], int_methods),
    WriteParameter(-128, [0x80, 0x80], int_methods),
    WriteParameter(-32766, [0xFF, 0xFE], int_methods),
    WriteParameter(-32767, [0xFF, 0xFF], int_methods),
    WriteParameter(-32768, [0x80, 0x80, 0x00], int_methods),
    WriteParameter(-8388606, [0xFF, 0xFF, 0xFE], int_methods),
    WriteParameter(-8388607, [0xFF, 0xFF, 0xFF], int_methods),
    WriteParameter(-8388608, [0x80, 0x80, 0x00, 0x00], int_methods),
    WriteParameter(62, [0xBE], varint_methods),
    WriteParameter(63, [0xBF], varint_methods),
    WriteParameter(64, [0x00, 0xC0], varint_methods),
    WriteParameter(8190, [0x3F, 0xFE], varint_methods),
    WriteParameter(8191, [0x3F, 0xFF], varint_methods),
    WriteParameter(8192, [0x00, 0x40, 0x80], varint_methods),
    WriteParameter(1048574, [0x3F, 0x7F, 0xFE], varint_methods),
    WriteParameter(1048575, [0x3F, 0x7F, 0xFF], varint_methods),
    WriteParameter(1048576, [0x00, 0x40, 0x00, 0x80], varint_methods),
    WriteParameter(-62, [0xFE], varint_methods),
    WriteParameter(-63, [0xFF], varint_methods),
    WriteParameter(-64, [0x40, 0xC0], varint_methods),
    WriteParameter(-8190, [0x7F, 0xFE], varint_methods),
    WriteParameter(-8191, [0x7F, 0xFF], varint_methods),
    WriteParameter(-8192, [0x40, 0x40, 0x80], varint_methods),
    WriteParameter(-1048574, [0x7F, 0x7F, 0xFE], varint_methods),
    WriteParameter(-1048575, [0x7F, 0x7F, 0xFF], varint_methods),
    WriteParameter(-1048576, [0x40, 0x40, 0x00, 0x80], varint_methods),
    WriteParameter(254, [0xFE], uint_methods),
    WriteParameter(255, [0xFF], uint_methods),
    WriteParameter(256, [0x01, 0x00], uint_methods),
    WriteParameter(65534, [0xFF, 0xFE], uint_methods),
    WriteParameter(65535, [0xFF, 0xFF], uint_methods),
    WriteParameter(65536, [0x01, 0x00, 0x00], uint_methods),
    WriteParameter(16777214, [0xFF, 0xFF, 0xFE], uint_methods),
    WriteParameter(16777215, [0xFF, 0xFF, 0xFF], uint_methods),
    WriteParameter(16777216, [0x01, 0x00, 0x00, 0x00], uint_methods),
    WriteParameter(126, [0xFE], varuint_methods),
    WriteParameter(127, [0xFF], varuint_methods),
    WriteParameter(128, [0x01, 0x80], varuint_methods),
    WriteParameter(16382, [0x7F, 0xFE], varuint_methods),
    WriteParameter(16383, [0x7F, 0xFF], varuint_methods),
    WriteParameter(16384, [0x01, 0x00, 0x80], varuint_methods),
    WriteParameter(2097150, [0x7F, 0x7F, 0xFE], varuint_methods),
    WriteParameter(2097151, [0x7F, 0x7F, 0xFF], varuint_methods),
    WriteParameter(2097152, [0x01, 0x00, 0x00, 0x80], varuint_methods),
)
def test_value_boundaries(p):
    assert_value(p)


@parametrize(
    int_methods,
    varint_methods
)
def test_boundaries_signed(p):
    half = _CACHE_SIZE // 2
    assert_cached_values(p, range(-half - 1, half))  # Go just past the cache boundaries.


@parametrize(
    uint_methods,
    varuint_methods
)
def test_cache_unsigned(p):
    assert_cached_values(p, range(_CACHE_SIZE + 1))


def assert_cached_values(p, value_range):
    for i in value_range:
        cached_value = []
        uncached_value = []
        p[0](cached_value, i)
        p[1](uncached_value, i)
        assert cached_value == uncached_value
