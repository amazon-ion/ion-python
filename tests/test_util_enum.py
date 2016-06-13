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

import pytest

from amazon.ion.util import Enum


class SimpleEnum(Enum):
    A = 1
    B = 2


def test_enum_members():
    assert SimpleEnum._enum_members == {1: SimpleEnum.A, 2: SimpleEnum.B}


def test_enum_reverse_lookup():
    assert SimpleEnum[1] == SimpleEnum.A
    assert SimpleEnum[2] == SimpleEnum.B


def test_enum_fields():
    assert SimpleEnum.A.value == 1
    assert SimpleEnum.A.name == 'A'
    assert SimpleEnum.B.value == 2
    assert SimpleEnum.B.name == 'B'

    values = list(SimpleEnum)
    values.sort()
    assert values == [SimpleEnum.A, SimpleEnum.B]


def test_enum_as_int():
    assert isinstance(SimpleEnum.A, int)
    assert SimpleEnum.A == 1
    assert SimpleEnum.A is not 1

def test_malformed_enum():
    with pytest.raises(TypeError):
        class BadEnum(Enum):
            A = 'Allo'
