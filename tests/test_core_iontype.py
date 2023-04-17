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

import itertools
from typing import NamedTuple, Any

import tests

from amazon.ion.core import IonType


class _P(NamedTuple):
    type: str
    expected: Any

    def __str__(self):
        return '{name} - {expected}'.format(name=self.type.name, expected=str(self.expected).upper())


def in_and_out_params(in_types, out_types):
    return tuple(itertools.chain(
        (_P(t, False) for t in out_types),
        (_P(t, True) for t in in_types)
    ))

_IN_TYPES = {IonType.SYMBOL, IonType.STRING}
_OUT_TYPES = set(IonType) - _IN_TYPES


@tests.parametrize(*in_and_out_params(_IN_TYPES, _OUT_TYPES))
def test_is_text(p):
    assert p.expected == p.type.is_text


_IN_TYPES = {IonType.CLOB, IonType.BLOB}
_OUT_TYPES = set(IonType) - _IN_TYPES


@tests.parametrize(*in_and_out_params(_IN_TYPES, _OUT_TYPES))
def test_is_lob(p):
    assert p.expected == p.type.is_lob


_IN_TYPES = {IonType.LIST, IonType.SEXP, IonType.STRUCT}
_OUT_TYPES = set(IonType) - _IN_TYPES


@tests.parametrize(*in_and_out_params(_IN_TYPES, _OUT_TYPES))
def test_is_container(p):
    assert p.expected == p.type.is_container
