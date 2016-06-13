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

from amazon.ion.util import record


def test_default_fields():
    class TestRecord(record('a', 'b', ('c', 5))):
        pass

    a = TestRecord(1, 2)
    assert a.a == 1
    assert a.b == 2
    assert a.c == 5


def test_missing_default():
    with pytest.raises(ValueError):
        class TestRecord(record(('a', 1), 'b')):
            pass


def test_bad_parameter():
    with pytest.raises(ValueError):
        class TestRecord(record(True)):
            pass
