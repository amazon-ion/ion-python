# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import sys
import textwrap
from decimal import Decimal
from io import BytesIO, StringIO

import pytest

from amazon.ion import simpleion
from amazon.ion.core import IonEventType, IonType, IonEvent, ION_STREAM_END_EVENT
from amazon.ion.reader import read_data_event, NEXT_EVENT, blocking_reader, SKIP_EVENT
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_text import text_reader
from amazon.ion.simple_types import IonPyFloat
from amazon.ion.symbols import shared_symbol_table, SymbolTableCatalog
from amazon.ion.writer import WriteEventType, blocking_writer
from amazon.ion.writer_binary import binary_writer
from amazon.ion.writer_text import text_writer

# Tests for the Python examples in the cookbook (http://amzn.github.io/ion-docs/guides/cookbook.html).
# Changes to these tests should only be made in conjunction with changes to the cookbook examples.

if sys.version_info < (3, 6):
    pytest.skip(u'To avoid polluting the examples with extra compatibility code, they are only written for and tested '
                u'with Python 3.6+.')


def test_reading_simpleion_loads():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = u'{hello: "world"}'
    value = simpleion.loads(data)
    assert u'hello world' == u'hello %s' % value[u'hello']


def test_reading_simpleion_load():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = BytesIO(b'{hello: "world"}')
    value = simpleion.load(data)
    assert u'hello world' == u'hello %s' % value[u'hello']


def test_writing_simpleion_dumps():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = u'{hello: "world"}'
    value = simpleion.loads(data)
    ion = simpleion.dumps(value, binary=False)
    assert u'$ion_1_0 {hello:"world"}' == ion


def test_writing_simpleion_dump():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = u'{hello: "world"}'
    value = simpleion.loads(data)
    ion = BytesIO()
    simpleion.dump(value, ion, binary=True)
    assert b'\xe0\x01\x00\xea\xec\x81\x83\xde\x88\x87\xb6\x85hello\xde\x87\x8a\x85world' == ion.getvalue()


def test_reading_simpleion_loads_multiple_top_level_values():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = u'1 2 3'
    value = simpleion.loads(data, single_value=False)
    assert [1, 2, 3] == value


def test_writing_simpleion_dumps_multiple_top_level_values():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = u'1 2 3'
    value = simpleion.loads(data, single_value=False)
    ion = simpleion.dumps(value, sequence_as_stream=True, binary=False)
    assert u'$ion_1_0 1 2 3' == ion


def test_reading_events_non_blocking():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    reader = managed_reader(text_reader())
    event = reader.send(NEXT_EVENT)
    # No data has been provided, so the reader is at STREAM_END
    # and will wait for data.
    assert event.event_type == IonEventType.STREAM_END
    # Send an incomplete Ion value.
    event = reader.send(read_data_event(b'{hello:'))
    # Enough data was available for the reader to determine that
    # the start of a struct value has been encountered.
    assert event.event_type == IonEventType.CONTAINER_START
    assert event.ion_type == IonType.STRUCT
    # Advancing the reader causes it to step into the struct.
    event = reader.send(NEXT_EVENT)
    # The reader reached the end of the data before completing
    # a value. Therefore, an INCOMPLETE event is returned.
    assert event.event_type == IonEventType.INCOMPLETE
    # Send the rest of the value.
    event = reader.send(read_data_event(b'"world"}'))
    # The reader now finishes parsing the value within the struct.
    assert event.event_type == IonEventType.SCALAR
    assert event.ion_type == IonType.STRING
    hello = event.field_name.text
    world = event.value
    # Advance the reader past the string value.
    event = reader.send(NEXT_EVENT)
    # The reader has reached the end of the struct.
    assert event.event_type == IonEventType.CONTAINER_END
    # Advancing the reader causes it to step out of the struct.
    event = reader.send(NEXT_EVENT)
    # There is no more data and a value has been completed.
    # Therefore, the reader conveys STREAM_END.
    assert event.event_type == IonEventType.STREAM_END
    assert u'hello world' == u'%s %s' % (hello, world)


def test_writing_events_non_blocking():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    def drain_data(incremental_event):
        incremental_data = b''
        while incremental_event.type == WriteEventType.HAS_PENDING:
            incremental_data += incremental_event.data
            incremental_event = writer.send(None)
        return incremental_data

    writer = binary_writer()
    event = writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT))
    data = drain_data(event)
    event = writer.send(IonEvent(IonEventType.SCALAR, IonType.STRING, field_name=u'hello', value=u'world'))
    data += drain_data(event)
    event = writer.send(IonEvent(IonEventType.CONTAINER_END))
    data += drain_data(event)
    event = writer.send(ION_STREAM_END_EVENT)
    data += drain_data(event)
    assert b'\xe0\x01\x00\xea\xec\x81\x83\xde\x88\x87\xb6\x85hello\xde\x87\x8a\x85world' == data


def test_reading_events_blocking():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = BytesIO(b'{hello: "world"}')
    reader = blocking_reader(managed_reader(text_reader()), data)
    event = reader.send(NEXT_EVENT)
    assert event.event_type == IonEventType.CONTAINER_START
    assert event.ion_type == IonType.STRUCT
    # Advancing the reader causes it to step into the struct.
    event = reader.send(NEXT_EVENT)
    assert event.event_type == IonEventType.SCALAR
    assert event.ion_type == IonType.STRING
    hello = event.field_name.text
    world = event.value
    # Advance the reader past the string value.
    event = reader.send(NEXT_EVENT)
    # The reader has reached the end of the struct.
    assert event.event_type == IonEventType.CONTAINER_END
    # Advancing the reader causes it to step out of the struct.
    event = reader.send(NEXT_EVENT)
    # There is no more data and a value has been completed.
    # Therefore, the reader conveys STREAM_END.
    assert event.event_type == IonEventType.STREAM_END
    assert u'hello world' == u'%s %s' % (hello, world)


def test_writing_events_blocking():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-and-writing-ion-data
    data = BytesIO()
    writer = blocking_writer(binary_writer(), data)
    event_type = writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT))
    # The value is not complete, so more events are required.
    assert event_type == WriteEventType.NEEDS_INPUT
    event_type = writer.send(IonEvent(IonEventType.SCALAR, IonType.STRING, field_name=u'hello', value=u'world'))
    # The value is not complete, so more events are required.
    assert event_type == WriteEventType.NEEDS_INPUT
    event_type = writer.send(IonEvent(IonEventType.CONTAINER_END))
    # The value is not complete, so more events are required.
    assert event_type == WriteEventType.NEEDS_INPUT
    event_type = writer.send(ION_STREAM_END_EVENT)
    # The end of the stream was signaled, so the data has been flushed.
    assert event_type == WriteEventType.COMPLETE
    assert b'\xe0\x01\x00\xea\xec\x81\x83\xde\x88\x87\xb6\x85hello\xde\x87\x8a\x85world' == data.getvalue()


def test_pretty_print_simpleion():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#pretty-printing
    unformatted = u'{level1: {level2: {level3: "foo"}, x: 2}, y: [a,b,c]}'
    value = simpleion.loads(unformatted)
    pretty = simpleion.dumps(value, binary=False, indent=u'  ')
    assert pretty == textwrap.dedent(u'''    $ion_1_0
    {
      level1: {
        level2: {
          level3: "foo"
        },
        x: 2
      },
      y: [
        a,
        b,
        c
      ]
    }''')


def test_pretty_print_events():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#pretty-printing
    pretty = BytesIO()
    writer = blocking_writer(text_writer(indent=u'  '), pretty)
    writer.send(ION_STREAM_END_EVENT)


def test_read_numerics_simpleion():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-numeric-types
    data = u'1.23456 1.2345e6 123456 12345678901234567890'
    values = simpleion.loads(data, single_value=False)
    assert isinstance(values[0], Decimal)
    assert isinstance(values[1], float)
    assert isinstance(values[2], int)
    assert isinstance(values[3], int)


def test_write_numeric_with_annotation_simpleion():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-numeric-types
    value = IonPyFloat.from_value(IonType.FLOAT, 123, (u'abc',))
    data = simpleion.dumps(value, binary=False)
    assert u'$ion_1_0 abc::123.0e0' == data


def test_read_numerics_events():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-numeric-types
    data = BytesIO(b'1.23456 1.2345e6 123456 12345678901234567890')
    reader = blocking_reader(managed_reader(text_reader()), data)
    event = reader.send(NEXT_EVENT)
    assert event.ion_type == IonType.DECIMAL
    assert isinstance(event.value, Decimal)
    event = reader.send(NEXT_EVENT)
    assert event.ion_type == IonType.FLOAT
    assert isinstance(event.value, float)
    event = reader.send(NEXT_EVENT)
    assert event.ion_type == IonType.INT
    assert isinstance(event.value, int)
    event = reader.send(NEXT_EVENT)
    assert event.ion_type == IonType.INT
    assert isinstance(event.value, int)


def test_write_numeric_with_annotation_events():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#reading-numeric-types
    event = IonEvent(IonEventType.SCALAR, IonType.FLOAT, annotations=(u'abc',), value=123.0)
    data = BytesIO()
    writer = blocking_writer(text_writer(), data)
    writer.send(event)
    writer.send(ION_STREAM_END_EVENT)
    assert u'abc::123.0e0' == data.getvalue().decode(u'utf-8')


def sparse_reads_data():
    data = u'''
         $ion_1_0
         foo::{
           quantity: 1
         }
         bar::{
           name: "x",
           id: 7
         }
         baz::{
           items:["thing1", "thing2"]
         }
         foo::{
           quantity: 19
         }
         bar::{
           name: "y",
           id: 8
         }'''
    data = simpleion.dumps(simpleion.loads(data, single_value=False), sequence_as_stream=True)
    # This byte literal is included in the examples.
    assert data == b'\xe0\x01\x00\xea' \
        b'\xee\xa5\x81\x83\xde\xa1\x87\xbe\x9e\x83foo\x88quantity\x83' \
        b'bar\x82id\x83baz\x85items\xe7\x81\x8a\xde\x83\x8b!\x01\xea' \
        b'\x81\x8c\xde\x86\x84\x81x\x8d!\x07\xee\x95\x81\x8e\xde\x91' \
        b'\x8f\xbe\x8e\x86thing1\x86thing2\xe7\x81\x8a\xde\x83\x8b!' \
        b'\x13\xea\x81\x8c\xde\x86\x84\x81y\x8d!\x08'
    return data


def test_sparse_reads_simpleion():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#performing-sparse-reads
    data = sparse_reads_data()  # The binary Ion equivalent of the above data.
    values = simpleion.loads(data, single_value=False)
    sum = 0
    for value in values:
        if u'foo' == value.ion_annotations[0].text:
            sum += value[u'quantity']
    assert 20 == sum


def test_sparse_reads_events():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#performing-sparse-reads
    data = BytesIO(sparse_reads_data())
    reader = blocking_reader(managed_reader(binary_reader()), data)
    sum = 0
    event = reader.send(NEXT_EVENT)
    while event != ION_STREAM_END_EVENT:
        assert event.event_type == IonEventType.CONTAINER_START
        assert event.ion_type == IonType.STRUCT
        if u'foo' == event.annotations[0].text:
            # Step into the struct.
            event = reader.send(NEXT_EVENT)
            while event.event_type != IonEventType.CONTAINER_END:
                if u'quantity' == event.field_name.text:
                    sum += event.value
                event = reader.send(NEXT_EVENT)
            # Step out of the struct.
            event = reader.send(NEXT_EVENT)
        else:
            # Skip over the struct without parsing its values.
            event = reader.send(SKIP_EVENT)
            assert event.event_type == IonEventType.CONTAINER_END
            # Position the reader at the start of the next value.
            event = reader.send(NEXT_EVENT)
    assert 20 == sum


def get_csv_structs():
    data = StringIO(
        u'''id,type,state
        1,foo,false
        2,bar,true
        3,baz,true'''
    )
    lines = data.readlines()[1:]

    def split_line(line):
        tokens = line.split(u',')
        mapping = (
            (u'id', int(tokens[0])),
            (u'type', tokens[1]),
            (u'state', u'true' == tokens[2].strip())
        )
        return dict(mapping)

    return [split_line(line) for line in lines]


def test_convert_csv_simpleion():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#converting-non-hierarchical-data-to-ion
    structs = get_csv_structs()
    ion = simpleion.dumps(structs, sequence_as_stream=True)
    assert b'\xe0\x01\x00\xea\xee\x95\x81\x83\xde\x91\x87\xbe\x8e\x82id\x84type\x85state\xde\x8a\x8a!' \
           b'\x01\x8b\x83foo\x8c\x10\xde\x8a\x8a!\x02\x8b\x83bar\x8c\x11\xde\x8a\x8a!\x03\x8b\x83baz\x8c\x11' \
           == ion


def test_convert_csv_events():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#converting-non-hierarchical-data-to-ion
    structs = get_csv_structs()
    ion = BytesIO()
    writer = blocking_writer(binary_writer(), ion)
    for struct in structs:
        writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT))
        writer.send(IonEvent(IonEventType.SCALAR, IonType.INT, field_name=u'id', value=struct[u'id']))
        writer.send(IonEvent(IonEventType.SCALAR, IonType.STRING, field_name=u'type', value=struct[u'type']))
        writer.send(IonEvent(IonEventType.SCALAR, IonType.BOOL, field_name=u'state', value=struct[u'state']))
        writer.send(IonEvent(IonEventType.CONTAINER_END))
    writer.send(ION_STREAM_END_EVENT)
    assert b'\xe0\x01\x00\xea\xee\x95\x81\x83\xde\x91\x87\xbe\x8e\x82id\x84type\x85state\xde\x8a\x8a!' \
           b'\x01\x8b\x83foo\x8c\x10\xde\x8a\x8a!\x02\x8b\x83bar\x8c\x11\xde\x8a\x8a!\x03\x8b\x83baz\x8c\x11' \
           == ion.getvalue()


def write_with_shared_symbol_table_simpleion():
    structs = get_csv_structs()
    table = shared_symbol_table(u'test.csv.columns', 1, (u'id', u'type', u'state'))
    data = simpleion.dumps(structs, imports=(table,), sequence_as_stream=True)
    # This byte literal is included in the examples.
    assert data == b'\xe0\x01\x00\xea' \
        b'\xee\xa4\x81\x83\xde\xa0\x86\xbe\x9b\xde\x99\x84\x8e\x90' \
        b'test.csv.columns\x85!\x01\x88!\x03\x87\xb0\xde\x8a\x8a!' \
        b'\x01\x8b\x83foo\x8c\x10\xde\x8a\x8a!\x02\x8b\x83bar\x8c' \
        b'\x11\xde\x8a\x8a!\x03\x8b\x83baz\x8c\x11'
    return data


def test_write_with_shared_symbol_table_simpleion():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#using-a-shared-symbol-table
    write_with_shared_symbol_table_simpleion()


def write_with_shared_symbol_table_events():
    structs = get_csv_structs()
    table = shared_symbol_table(u'test.csv.columns', 1, (u'id', u'type', u'state'))
    data = BytesIO()
    writer = blocking_writer(binary_writer(imports=(table,)), data)
    for struct in structs:
        writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT))
        writer.send(IonEvent(IonEventType.SCALAR, IonType.INT, field_name=u'id', value=struct[u'id']))
        writer.send(IonEvent(IonEventType.SCALAR, IonType.STRING, field_name=u'type', value=struct[u'type']))
        writer.send(IonEvent(IonEventType.SCALAR, IonType.BOOL, field_name=u'state', value=struct[u'state']))
        writer.send(IonEvent(IonEventType.CONTAINER_END))
    writer.send(ION_STREAM_END_EVENT)
    return data.getvalue()


def test_write_with_shared_symbol_table_events():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#using-a-shared-symbol-table
    write_with_shared_symbol_table_events()


def test_read_with_shared_symbol_table_simpleion():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#using-a-shared-symbol-table
    data = write_with_shared_symbol_table_simpleion()
    table = shared_symbol_table(u'test.csv.columns', 1, (u'id', u'type', u'state'))
    catalog = SymbolTableCatalog()
    catalog.register(table)
    values = simpleion.loads(data, catalog=catalog, single_value=False)
    assert values[2][u'id'] == 3


def test_read_with_shared_symbol_table_events():
    # http://amzn.github.io/ion-docs/guides/cookbook.html#using-a-shared-symbol-table
    table = shared_symbol_table(u'test.csv.columns', 1, (u'id', u'type', u'state'))
    catalog = SymbolTableCatalog()
    catalog.register(table)
    data = BytesIO(write_with_shared_symbol_table_simpleion())
    reader = blocking_reader(managed_reader(binary_reader(), catalog=catalog), data)
    # Position the reader at the first struct.
    reader.send(NEXT_EVENT)
    # Skip over the struct.
    reader.send(SKIP_EVENT)
    # Position the reader at the second struct.
    reader.send(NEXT_EVENT)
    # Skip over the struct.
    reader.send(SKIP_EVENT)
    # Position the reader at the third struct.
    event = reader.send(NEXT_EVENT)
    assert event.ion_type == IonType.STRUCT
    # Step into the struct
    event = reader.send(NEXT_EVENT)
    assert u'id' == event.field_name.text
    assert 3 == event.value
