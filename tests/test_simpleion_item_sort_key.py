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

from tests import parametrize
from amazon.ion.core import record
from amazon.ion import simpleion


class _P(record('text', 'expected', 'item_sort_key')):
    def __str__(self):
        return self.desc


def _sort_by_key(x):
    return x[0]


def _sort_by_value(x):
    return x[1]

_ION_ST = u"$ion_1_0"

_SIMPLE = "{a:3,b:2,c:1}"
_SIMPLE_SORTED_BY_KEY = _SIMPLE
_SIMPLE_SORTED_BY_VALUE = "{c:1,b:2,a:3}"
_LIST = "{a:[3,4],b:[1,2]}"
_LIST_SORTED_BY_KEY = _LIST
_LIST_SORTED_BY_VALUE = "{b:[1,2],a:[3,4]}"
_REPEATED_FIELDS = "{a:4,a:1,b:3,b:2}"
_REPEATED_FIELDS_SORTED_BY_KEY = _REPEATED_FIELDS
_REPEATED_FIELDS_SORTED_BY_VALUE = "{a:1,b:2,b:3,a:4}"


@parametrize(
    _P(
        text=_SIMPLE,
        expected=_SIMPLE_SORTED_BY_KEY,
        item_sort_key=_sort_by_key
    ),
    _P(
        text=_SIMPLE,
        expected=_SIMPLE_SORTED_BY_VALUE,
        item_sort_key=_sort_by_value
    ),
    _P(
        text=_LIST,
        expected=_LIST_SORTED_BY_KEY,
        item_sort_key=_sort_by_key
    ),
    _P(
        text=_LIST,
        expected=_LIST_SORTED_BY_VALUE,
        item_sort_key=_sort_by_value
    )
)
def test_dumps(p):
    obj = simpleion.loads(p.text)
    assert "{0} {1}".format(_ION_ST, p.expected) == simpleion.dumps(obj, item_sort_key=p.item_sort_key, binary=False)

