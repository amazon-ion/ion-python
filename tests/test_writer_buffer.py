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

from io import BytesIO

from pytest import raises

from amazon.ion.writer_buffer import BufferTree


def assert_buffer(buf, expected=b'1234'):
    out = BytesIO()
    for partial in buf.drain():
        out.write(partial)
    assert expected == out.getvalue()


def test_scalars():
    buf = BufferTree()
    buf.add_scalar_value(b'1')
    buf.add_scalar_value(b'2')
    buf.add_scalar_value(b'34')
    assert_buffer(buf)


def test_container():
    buf = BufferTree()
    buf.start_container()
    buf.add_scalar_value(b'34')
    buf.end_container(b'12')
    assert_buffer(buf)


def test_nested_containers():
    buf = BufferTree()
    buf.start_container()
    buf.add_scalar_value(b'2')
    buf.start_container()
    buf.add_scalar_value(b'4')
    buf.end_container(b'3')
    buf.end_container(b'1')
    assert_buffer(buf)


def test_nested_containers_at_same_start():
    buf = BufferTree()
    buf.start_container()
    buf.start_container()
    buf.start_container()
    buf.add_scalar_value(b'4')
    buf.end_container(b'3')
    buf.end_container(b'2')
    buf.end_container(b'1')
    assert_buffer(buf)


def test_scalar_after_empty_container():
    buf = BufferTree()
    buf.start_container()
    buf.add_scalar_value(b'2')
    buf.start_container()
    buf.end_container(b'3')
    buf.add_scalar_value(b'4')
    buf.end_container(b'1')
    assert_buffer(buf)


def test_container_length():
    buf = BufferTree()
    buf.start_container()
    buf.add_scalar_value(b'234')
    assert 3 ==  buf.current_container_length
    buf.end_container(b'1')
    assert 4 == buf.current_container_length
    assert_buffer(buf)


def test_reuse_with_scalars():
    buf = BufferTree()
    buf.add_scalar_value(b'1')
    buf.add_scalar_value(b'2')
    buf.add_scalar_value(b'34')
    assert_buffer(buf)  # drain resets the writer buffer
    buf.add_scalar_value(b'5')
    buf.add_scalar_value(b'6')
    buf.add_scalar_value(b'78')
    assert_buffer(buf, b'5678')


def test_reuse_with_containers():
    buf = BufferTree()
    buf.start_container()
    buf.add_scalar_value(b'2')
    buf.start_container()
    buf.add_scalar_value(b'4')
    buf.end_container(b'3')
    buf.end_container(b'1')
    assert_buffer(buf)
    buf.start_container()
    buf.add_scalar_value(b'78')
    buf.end_container(b'56')
    assert_buffer(buf, b'5678')


def test_drain_with_active_container_fails():
    buf = BufferTree()
    buf.start_container()
    buf.add_scalar_value(b'1')
    with raises(ValueError):
        for partial in buf.drain():
            pass


def test_end_container_at_depth_zero_fails():
    buf = BufferTree()
    buf.start_container()
    buf.add_scalar_value(b'2')
    buf.end_container(b'1')
    with raises(ValueError):
        buf.end_container(b'0')
