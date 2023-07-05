#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
A collection of tools to help with protocol buffers for use in the Ion Benchmark CLI.

Usage:
    proto_tools.py <command> [<args>...]
    proto_tools.py --help

Commands:{commands}

Options:
     -h, --help
"""

import types
from docopt import docopt as _docopt
from google.protobuf import descriptor_pb2 as _descriptor_pb2
from self_describing_proto import SelfDescribingProtoSerde


def wrap_command():
    """
    Wrap a protocol buffer in a SelfDescribingMessage protocol buffer.

    Usage:
        proto_tools.py wrap <schema_descriptor_file> <type_name> <input_file> [<output_file>]

    Arguments:
        <schema_descriptor_file>    A Protobuf FileDescriptorSet, generated using `protoc --descriptor_set_out ...`.
        <type_name>                 The type of <input_file>. Must be included in <schema_descriptor_file>.
        <input_file>                A protocol buffer to be wrapped in a self-describing protocol buffer.
        <output_file>               Where the wrapped protocol buffer should be saved. Default: `<input_file>.wrapped`
    """
    arguments = _docopt(wrap_command.__doc__, help=True)
    schema_file = arguments['<schema_descriptor_file>']
    type_name = arguments['<type_name>']
    input_file = arguments['<input_file>']
    output_file = arguments['<output_file>'] or "{}.wrapped".format(input_file)

    descriptor_set = _descriptor_pb2.FileDescriptorSet()

    with open(schema_file, "rb") as schema_fp:
        descriptor_set.ParseFromString(schema_fp.read())

    sd_proto = SelfDescribingProtoSerde()
    inner_obj = sd_proto.generate_class_definition(type_name, descriptor_set)()

    with open(input_file, "rb") as fp:
        inner_obj.ParseFromString(fp.read())
    with open(output_file, "wb") as fp:
        sd_proto.dump(inner_obj, fp)


def unwrap_command():
    """
    Unwrap the inner protocol buffer of a SelfDescribingMessage protocol buffer.

    Usage:
        proto_tools.py unwrap <input_file> [<output_file>]

    Arguments:
        <input_file>     The file to unwrap
        <output_file>    The destination for the unwrapped file. Default: `<input_file>.unwrapped`
    """
    arguments = _docopt(wrap_command.__doc__, help=True)
    input_file = arguments['<input_file>']
    output_file = arguments['<output_file>'] or "{}.unwrapped".format(input_file)

    sd_proto = SelfDescribingProtoSerde()
    with open(input_file, "rb") as fp:
        obj = sd_proto.load(fp)
    with open(output_file, "wb") as fp:
        fp.write(obj.SerializeToString())


def _list_commands():
    """
    Get a formatted list of commands as a string.
    """
    commands = ""
    for name, member in [*globals().items()]:
        if isinstance(member, types.FunctionType) and name.endswith("_command"):
            display_name = name.removesuffix("_command")
            # Get the first line of __doc__ to use as the summary.
            description = next((s.strip() for s in (member.__doc__ or "").split('\n') if s.strip()), "")
            commands += "\n    {: <16}{}".format(display_name, description)
    return commands


if __name__ == '__main__':
    docs = __doc__.format(commands=_list_commands())
    args = _docopt(docs, help=True, options_first=True)
    func_name = "{}_command".format(args['<command>'])
    if func_name in globals().items().mapping:
        globals().items().mapping[func_name]()
    else:
        exit("%r is not a proto_tools.py command. See 'proto_tools.py --help'." % args['<command>'])
