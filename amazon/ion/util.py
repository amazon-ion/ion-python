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

"""General purpose utilities."""

import sys

from collections import namedtuple


class _RecordMetaClass(type):
    """Metaclass for defining named-tuple based immutable record types."""
    def __new__(cls, name, bases, attrs):
        if attrs.get('_record_sentinel') is None:
            field_declarations = []
            has_record_sentinel = False
            for base_class in bases:
                parent_declarations = getattr(base_class, '_record_fields', None)
                if parent_declarations is not None:
                    field_declarations.extend(parent_declarations)
                    has_record_sentinel = True
            if has_record_sentinel:
                # Only mutate the class if we are directly sub-class a record sentinel.
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
                base_class = namedtuple(name, names)
                base_class.__new__.__defaults__ = tuple(defaults)
                # Eliminate our placeholder(s) in the hierarchy.
                bases = (base_class,)

        return super(_RecordMetaClass, cls).__new__(cls, name, bases, attrs)


def record(*fields):
    """Constructs a type that can be extended to create immutable, value types.

    Examples:
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
        fields (list[str | (str, any)]): A sequence of str or pairs that
    """
    class RecordType(object, metaclass=_RecordMetaClass):
        _record_sentinel = True
        _record_fields = fields

    return RecordType


def coroutine(func):
    """Wraps a PEP-342 enhanced generator in a way that avoids boilerplate of the "priming" call to ``next``.

    Args:
        func (Callable): The function constructing a generator to decorate.

    Returns:
        Callable: The decorated generator.
    """
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        val = next(gen)
        if val != None:
            raise TypeError('Unexpected value from start of coroutine')
        return gen
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


_NON_BMP_OFFSET = 0x10000
_UTF_16_MAX_CODE_POINT = 0xFFFF
_HIGH_SURROGATE_START = 0xD800
_HIGH_SURROGATE_END = 0xDBFF
_LOW_SURROGATE_START = 0xDC00
_LOW_SURROGATE_END = 0xDFFF
_SURROGATE_START = _HIGH_SURROGATE_START
_SURROGATE_END = _LOW_SURROGATE_END


def unicode_iter(val):
    """Provides an iterator over the *code points* of the given Unicode sequence.

    Notes:
        Before PEP-393, Python has the potential to support Unicode as UTF-16 or UTF-32.
        This is reified in the property as ``sys.maxunicode``.  As a result, naive iteration
        of Unicode sequences will render non-character code points such as UTF-16 surrogates.

    Args:
        val (unicode): The unicode sequence to iterate over as integer code points in the range
            ``0x0`` to ``0x10FFFF``.
    """
    val_iter = iter(val)
    while True:
        try:
            code_point = next(_next_code_point(val, val_iter, to_int=ord))
        except StopIteration:
            return
        if code_point is None:
            raise ValueError('Unpaired high surrogate at end of Unicode sequence: %r' % val)
        yield code_point


class CodePoint(int):
    """Evaluates as the ordinal of a code point, while also containing the unicode character representation and
    indicating whether the code point was escaped.
    """
    def __init__(self, *args, **kwargs):
        self.char = None
        self.is_escaped = False


def _next_code_point(val, val_iter, yield_char=False, to_int=lambda x: x):
    """Provides the next *code point* in the given Unicode sequence.

    This generator function yields complete character code points, never incomplete surrogates. When a low surrogate is
    found without following a high surrogate, this function raises ``ValueError`` for having encountered an unpaired
    low surrogate. When the provided iterator ends on a high surrogate, this function yields ``None``. This is the
    **only** case in which this function yields ``None``. When this occurs, the user may append additional data to the
    input unicode sequence and resume iterating through another ``next`` on this generator. When this function receives
    ``next`` after yielding ``None``, it *reinitializes the unicode iterator*. This means that this feature can only
    be used for values that contain an ``__iter__`` implementation that remains at the current position in the data
    when called (e.g. :class:`BufferQueue`). At this point, there are only two possible outcomes:
        * If next code point is a valid low surrogate, this function yields the combined code point represented by the
          surrogate pair.
        * Otherwise, this function raises ``ValueError`` for having encountered an unpaired high surrogate.

    Args:
        val (unicode|BufferQueue): A unicode sequence or unicode BufferQueue over which to iterate.
        val_iter (Iterator[unicode|BufferQueue]): The unicode sequence iterator over ``val`` from which to generate the
            next integer code point in the range ``0x0`` to ``0x10FFFF``.
        yield_char (Optional[bool]): If True **and** the character code point resulted from a surrogate pair, this
            function will yield a :class:`CodePoint` representing the character code point and containing the original
            unicode character. This is useful when the original unicode character will be needed again because UCS2
            Python builds will error when trying to convert code points greater than 0xFFFF back into their
            unicode character representations. This avoids requiring the user to mathematically re-derive the
            surrogate pair in order to successfully convert the code point back to a unicode character.
        to_int (Optional[callable]): A function to call on each element of val_iter to convert that element to an int.
    """
    try:
        high = next(val_iter)
    except StopIteration:
        return
    low = None
    code_point = to_int(high)
    if _LOW_SURROGATE_START <= code_point <= _LOW_SURROGATE_END:
        raise ValueError('Unpaired low surrogate in Unicode sequence: %d' % code_point)
    elif _HIGH_SURROGATE_START <= code_point <= _HIGH_SURROGATE_END:
        def combine_surrogates():
            low_surrogate = next(val_iter)
            low_code_point = to_int(low_surrogate)
            if low_code_point < _LOW_SURROGATE_START or low_code_point > _LOW_SURROGATE_END:
                raise ValueError('Unpaired high surrogate: %d' % code_point)
            # Decode the surrogates
            real_code_point = _NON_BMP_OFFSET
            real_code_point += (code_point - _HIGH_SURROGATE_START) << 10
            real_code_point += (low_code_point - _LOW_SURROGATE_START)
            return real_code_point, low_surrogate
        try:
            code_point, low = combine_surrogates()
        except StopIteration:
            yield None
            val_iter = iter(val)  # More data has appeared in val.
            code_point, low = combine_surrogates()
    if yield_char and low is not None:
        out = CodePoint(code_point)
        if isinstance(val, str):
            # Iterating over a text type returns text types.
            out.char = high + low
        else:
            out.char = chr(high) + chr(low)
    else:
        out = code_point
    yield out


if sys.version_info < (2, 7):
    def bit_length(value):
        if value == 0:
            return 0
        return len(bin(abs(value))) - 2

    def total_seconds(td):
        return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6
else:
    def bit_length(value):
        return value.bit_length()

    def total_seconds(td):
        return td.total_seconds()


bit_length.__doc__ = 'Returns the bit length of an integer'
total_seconds.__doc__ = 'Timedelta ``total_seconds`` with backported support in Python 2.6'
