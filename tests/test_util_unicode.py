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

from pytest import raises

from tests import parametrize, is_exception, noop_manager

from amazon.ion.util import record, unicode_iter


def _unichr_list(val):
    return list(ord(x) for x in val)


class _P(record('desc', 'input', 'expected')):
    def __str__(self):
        return self.desc


@parametrize(
    _P('ASCII', u'abcd', _unichr_list(u'abcd')),
    _P('BMP SEQUENCE', u'abcd\u3000', _unichr_list(u'abcd\u3000')),
    _P('NON-BMP SEQUENCE', u'abcd\U0001f4a9', _unichr_list(u'abcd') + [0x1F4A9]),
    _P('UNPAIRED LOW', u'\udc00', ValueError),
    _P('UNPAIRED HIGH AT END', u'\ud800', ValueError),
    _P('UNPAIRED HIGH IN MID', u'\ud800a', ValueError),
)
def test_unicode_iter(p):
    ctx = noop_manager()
    if is_exception(p.expected):
        ctx = raises(p.expected)

    with ctx:
        actual = list(unicode_iter(p.input))

        if not is_exception(p.expected):
            assert p.expected == actual
