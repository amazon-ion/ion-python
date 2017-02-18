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

"""Methods for writing binary Ion Int, VarInt, UInt, and VarUInt fields.

Whenever the following words are used in variable or method names, their meaning is as
defined below.

- ``signed``: applies only to Int and VarInt - the signed fields (sign bit in first octet).
- ``unsigned``: applies only to UInt and VarUInt - the unsigned fields (no sign bit).
- ``variable``: applies only to VarInt and VarUInt - the variable-length fields (end bit required).
- ``fixed``: applies only to Int and UInt - the fixed-length fields (no end bit).
- ``Int``: applies only to the Int field - the signed, fixed-length field.
- ``varint``: applies only to the VarInt field - the signed, variable-length field.
- ``uint``: applies only to the UInt field - the unsigned, fixed-length field.
- ``varuint``: applies only to the VarUInt field - the unsigned, variable-length field.
"""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .util import bit_length

_VARIABLE_END_BIT_MASK = 0b10000000  # Both VarInt and VarUInt

_INT_SIGN_BIT_MASK = 0b10000000  # fixed Int only
_VARINT_SIGN_BIT_MASK = 0b01000000  # VarInt only

_VARIABLE_BITS_PER_OCTET = 7  # Both VarInt and VarUInt (has end bit) (exclusive of first octet)
_FIXED_BITS_PER_OCTET = 8  # Both fixed Int and fixed UInt (exclusive of first octet)

_OCTET_MASKS = {
    _VARIABLE_BITS_PER_OCTET: 0x7F,
    _FIXED_BITS_PER_OCTET: 0xFF,
}


def _write_varint(buf, value):
    """Writes the given integer value into the given buffer as a binary Ion VarInt field.

    Args:
        buf (Sequence): The buffer into which the VarInt will be written, in the form of
            integer octets.
        value (int): The value to write as a VarInt.

    Returns:
        int: The number of octets written.
    """
    return _write_signed(buf, value, _field_cache.get_varint, _write_varint_uncached)


def _write_varint_uncached(buf, value):
    return _write_signed_uncached(buf, value, _VARINT_SIGN_BIT_MASK, _VARIABLE_BITS_PER_OCTET,
                                  _VARIABLE_END_BIT_MASK)


def _write_int(buf, value):
    """Writes the given integer value into the given buffer as a binary Ion Int field.

    Args:
        buf (Sequence): The buffer into which the Int will be written,
            in the form of integer octets.
        value (int): The value to write as a Int.

    Returns:
        int: The number of octets written.
    """
    return _write_signed(buf, value, _field_cache.get_int, _write_int_uncached)


def _write_int_uncached(buf, value):
    return _write_signed_uncached(buf, value, _INT_SIGN_BIT_MASK, _FIXED_BITS_PER_OCTET)


def _write_signed(buf, value, cached_func, uncached_func):
    if _field_cache.SIGNED_MIN <= value < _field_cache.SIGNED_MAX:
        buf.append(cached_func(value))
        return 1
    return uncached_func(buf, value)


def _write_signed_uncached(buf, value, sign_bit_mask, bits_per_octet, end_bit=0):
    magnitude = value
    sign_bit = 0
    if value < 0:
        magnitude = -magnitude
        sign_bit = sign_bit_mask
    return _write_base(buf, magnitude, bits_per_octet, end_bit, sign_bit, is_signed=True)


def _write_varuint(buf, value):
    """Writes the given integer value into the given buffer as a binary Ion VarUInt.

    Args:
        buf (Sequence): The buffer into which the VarUInt will be written,
            in the form of integer octets.
        value (int): The value to write as a VarUInt.

    Returns:
        int: The number of octets written.
    """
    return _write_unsigned(buf, value, _field_cache.get_varuint, _write_varuint_uncached)


def _write_varuint_uncached(buf, value):
    return _write_base(buf, value, _VARIABLE_BITS_PER_OCTET, _VARIABLE_END_BIT_MASK)


def _write_uint(buf, value):
    """Writes the given integer value into the given buffer as a binary Ion UInt.

    Args:
        buf (Sequence): The buffer into which the UInt will be written,
            in the form of integer octets.
        value (int): The value to write as a UInt.

    Returns:
        int: The number of octets written.
    """
    return _write_unsigned(buf, value, _field_cache.get_uint, _write_uint_uncached)


def _write_uint_uncached(buf, value):
    return _write_base(buf, value, _FIXED_BITS_PER_OCTET)


def _write_unsigned(buf, value, cached_func, uncached_func):
    if value < _CACHE_SIZE:
        buf.append(cached_func(value))
        return 1
    return uncached_func(buf, value)


def _write_base(buf, value, bits_per_octet, end_bit=0, sign_bit=0, is_signed=False):
    """Write a field to the provided buffer.

    Args:
        buf (Sequence): The buffer into which the UInt will be written
            in the form of integer octets.
        value (int): The value to write as a UInt.
        bits_per_octet (int): The number of value bits (i.e. exclusive of the end bit, but
            inclusive of the sign bit, if applicable) per octet.
        end_bit (Optional[int]): The end bit mask.
        sign_bit (Optional[int]): The sign bit mask.

    Returns:
        int: The number of octets written.
    """
    if value == 0:
        buf.append(sign_bit | end_bit)
        return 1
    num_bits = bit_length(value)
    num_octets = num_bits // bits_per_octet
    # 'remainder' is the number of value bits in the first octet.
    remainder = num_bits % bits_per_octet
    if remainder != 0 or is_signed:
        # If signed, the first octet has one fewer bit available, requiring another octet.
        num_octets += 1
    else:
        # This ensures that unsigned values that fit exactly are not shifted too far.
        remainder = bits_per_octet
    for i in range(num_octets):
        octet = 0
        if i == 0:
            octet |= sign_bit
        if i == num_octets - 1:
            octet |= end_bit
        # 'remainder' is used for alignment such that only the first octet
        # may contain insignificant zeros.
        octet |= ((value >> (num_bits - (remainder + bits_per_octet * i))) & _OCTET_MASKS[bits_per_octet])
        buf.append(octet)
    return num_octets


_CACHE_SIZE = 64


class _FieldCache:
    """Contains caches for small Int, UInt, VarInt, and VarUInt values.

    For unsigned fields, values between 0 and 63 will have their representations cached.
    For signed fields, values between -32 and 31 will have their representations cached.
    It is likely that a large proportion of subfields fit in these ranges. For example:
    - All values with lengths less than 64 bytes will retrieve their length
        subfield from the cache.
    - All integer values with magnitudes less than 64 will retrieve their 'magnitude'
        subfields from the cache.
    - Decimal values with exponents and/or coefficients within the signed cache range
        will retrieve those fields from the cache.
    - All timestamps will retrieve their month, day, hour, minute, and second fields
        from the cache (otherwise it would be an invalid datetime). The fractional fields
        may be cached if they meet the decimal value requirement from the previous bullet.
    - The first 64 symbols will have their symbol ID fields retrieved from the cache.
        This applies to symbol values, field names, and annotations.
    - If a value has less than 64 bytes of annotations (hopefully this is true for all
        values), then its annotation wrapper's 'annotation length' field will be cached.
    """
    @classmethod
    def _signed_value(cls, index):
        return index - cls.__HALF

    @classmethod
    def _signed_index(cls, value):
        return cls.__HALF + value

    __HALF = _CACHE_SIZE // 2

    def __init__(self):
        self.SIZE = _CACHE_SIZE
        self.SIGNED_MAX = _FieldCache._signed_value(self.SIZE)
        self.SIGNED_MIN = _FieldCache._signed_value(0)
        self._cached_ints = bytearray()
        self._cached_varints = bytearray()
        self._cached_uints = bytearray()
        self._cached_varuints = bytearray()
        self._fill_cache()

    def _fill_cache(self):
        # Each cached_value is an index, but since the caches are self-resizing sequences,
        # they are not directly used that way here. Each _write* method below appends to
        # the given cache. Since each cache starts empty, the field value ends up at
        # index 'cached_value' in the cache. For unsigned fields, the cached field represents
        # cached_value directly. In other words, the UInt cache contains the UInt field
        # representation of '32' at index '32'. For signed fields, the cached field represents
        # (cached_value - 32) to give a selection of positive and negative values.
        # In other words, the Int cache contains the Int field representation of '0' at index '32'.
        #
        # The size of 64 was chosen because it is the maximum magnitude that fits in a
        # 1-byte VarInt field (the smallest max magnitude of any 1-byte field representation).
        # Therefore, each value of 'cached_value' is guaranteed to fit in one byte for any
        # of the four field types (this is verified by the assertion).
        # Note that different sizes could be used for each of the caches,
        # but for simplicity, 64 is used for all.
        for cached_value in range(self.SIZE):
            signed_value = _FieldCache._signed_value(cached_value)
            assert _write_int_uncached(self._cached_ints, signed_value) == 1
            assert _write_varint_uncached(self._cached_varints, signed_value) == 1
            assert _write_uint_uncached(self._cached_uints, cached_value) == 1
            assert _write_varuint_uncached(self._cached_varuints, cached_value) == 1

    def get_int(self, value):
        return self._cached_ints[_FieldCache._signed_index(value)]

    def get_varint(self, value):
        return self._cached_varints[_FieldCache._signed_index(value)]

    def get_uint(self, value):
        return self._cached_uints[value]

    def get_varuint(self, value):
        return self._cached_varuints[value]

_field_cache = _FieldCache()
