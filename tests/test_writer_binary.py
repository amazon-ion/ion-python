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

from amazon.ion.core import IonEvent, IonEventType, IonType
from amazon.ion.exceptions import IonException
from amazon.ion.symbols import SymbolTable, LOCAL_TABLE_TYPE
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary import binary_writer


def new_writer(imports=None):
    out = BytesIO()
    return out, blocking_writer(binary_writer(imports), out)


def finish(writer):
    writer.send(IonEvent(IonEventType.STREAM_END))


def test_no_ivm_without_values():
    out, writer = new_writer()
    finish(writer)
    buf = out.getvalue()
    assert len(buf) == 0


def assert_ivm(buf, count=1):
    ivm = BytesIO()
    ivm.write(bytearray([0xE0, 0x01, 0x00, 0xEA]))
    ivm_bytes = ivm.getvalue()
    assert buf.count(ivm_bytes) == count


def test_implicit_ivm():
    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.SCALAR, IonType.NULL))  # This implicitly writes the IVM.
    finish(writer)
    buf = out.getvalue()
    assert_ivm(buf)


def test_one_ivm_per_stream():
    out, writer = new_writer()
    # Explicitly write the IVM.
    writer.send(IonEvent(IonEventType.VERSION_MARKER))
    # If the IVM not explicitly written, this would write one.
    writer.send(IonEvent(IonEventType.SCALAR, IonType.NULL))
    finish(writer)
    buf = out.getvalue()
    assert_ivm(buf)


def test_finish_resets_buffers():
    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.SCALAR, IonType.STRING, u'foo'))
    finish(writer)
    buf = out.getvalue()
    assert len(buf) != 0
    out.truncate(0)
    finish(writer)
    buf = out.getvalue()
    assert len(buf) == 0


def test_import_lst_fails():
    lst = SymbolTable(LOCAL_TABLE_TYPE, [u'foo'])
    out, writer = new_writer([lst])
    with raises(IonException):  # TODO fail earlier?
        writer.send(IonEvent(IonEventType.SCALAR, IonType.STRING, u'bar'))


def test_reuse_after_flush():
    out, writer = new_writer()
    writer.send(IonEvent(IonEventType.SCALAR, IonType.BOOL, True))
    finish(writer)
    length = len(out.getvalue())
    writer.send(IonEvent(IonEventType.SCALAR, IonType.BOOL, False))
    finish(writer)
    buf = out.getvalue()
    assert length != len(buf)
    assert_ivm(buf, 2)
