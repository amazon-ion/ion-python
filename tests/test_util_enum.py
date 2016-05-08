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

import pytest

import amazon.ion.util as util


class TestEnum(util.Enum):
    A = 1
    B = 2


def test_enum_members():
    assert TestEnum._enum_members == {1: TestEnum.A, 2: TestEnum.B}


def test_enum_reverse_lookup():
    assert TestEnum[1] == TestEnum.A
    assert TestEnum[2] == TestEnum.B


def test_enum_fields():
    assert TestEnum.A.value == 1
    assert TestEnum.A.name == 'A'
    assert TestEnum.B.value == 2
    assert TestEnum.B.name == 'B'


def test_enum_as_int():
    assert isinstance(TestEnum.A, int)
    assert TestEnum.A == 1
    assert TestEnum.A is not 1


def test_malformed_enum():
    with pytest.raises(TypeError):
        class BadEnum(util.Enum):
            A = 'Allo'
