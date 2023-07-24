# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from os import path
from pathlib import Path

from amazon.ion.simple_types import IonPySymbol


# Global defaults for CLI test specs
_tool_defaults = {
    'iterations': 100,
    'warmups': 0,
    'io_type': 'buffer',
    'command': 'read',
    'api': 'dom',
}


class BenchmarkSpec(dict):
    """
    Describes the configuration for a micro benchmark.

    Contains functions for retrieving values that are common to all, and allows dictionary-like access for additional
    parameters.
    """
    _data_object = None
    _loader_dumper = None
    _spec_working_directory = None

    def __init__(self, params: dict, user_overrides: dict = None, user_defaults: dict = None, working_directory=None):
        """
        Construct a new BenchmarkSpec, possibly incorporating user supplied defaults or overrides.

        Between the various dicts of parameters, the fields "format", "input_file", "command", "api", "iterations",
        "warmups", and "io_type" must all have a value.

        :param params: Values for this benchmark spec.
        :param user_overrides: Values that override all other values.
        :param user_defaults: Values that override the tool defaults, but not the `params`.
        :param working_directory: reference point to use if `input_file` is a relative path. Defaults to os.getcwd().
        """
        if user_defaults is None:
            user_defaults = {}
        if user_overrides is None:
            user_overrides = {}

        self._spec_working_directory = working_directory or os.getcwd()

        merged = _tool_defaults | user_defaults | params | user_overrides

        # If not an absolute path, make relative to the working directory.
        input_file = merged['input_file']
        if not path.isabs(input_file):
            input_file = path.join(self._spec_working_directory, input_file)
            merged['input_file'] = input_file

        # Convert symbols to strings
        for k in merged.keys():
            if isinstance(merged[k], IonPySymbol):
                merged[k] = merged[k].text

        super().__init__(merged)

        for k in ["format", "input_file", "command", "api", "iterations", "warmups", "io_type"]:
            if self[k] is None:
                raise ValueError(f"Missing required parameter '{k}'")

        if 'name' not in self:
            self['name'] = f'({self.get_format()},{self.get_operation_name()},{path.basename(self.get_input_file())})'

    def __missing__(self, key):
        # Instead of raising a KeyError like a usual dict, just return None.
        return None

    def get_attribute_as_path(self, key: str):
        """
        Get value from the backing dict, assuming that it is a file path, and appending it to the spec working directory
        if it is a relative path.
        """
        value = self[key]
        if path.isabs(value):
            return value
        else:
            return path.join(self._spec_working_directory, value)

    def get_name(self):
        """
        Get the name of the BenchmarkSpec. If not provided in __init__, one was generated based on the params provided.
        """
        return self["name"]

    def get_format(self):
        return self["format"]

    def get_input_file(self):
        return self["input_file"]

    def get_command(self):
        return self["command"]

    def get_api(self):
        return self["api"]

    def get_io_type(self):
        return self["io_type"]

    def get_iterations(self):
        return self["iterations"]

    def get_warmups(self):
        return self["warmups"]

    def get_operation_name(self):

        match [self.get_io_type(), self.get_command(), self.get_api()]:
            case ['buffer', 'read', 'dom']:
                return 'loads'
            case ['buffer', 'write', 'dom']:
                return 'dumps'
            case ['file', 'read', 'dom']:
                return 'load'
            case ['file', 'write', 'dom']:
                return 'dumps'
            case _:
                raise NotImplementedError("Streaming benchmarks are not supported yet.")

    def get_input_file_size(self):
        return Path(self.get_input_file()).stat().st_size

    def get_data_object(self):
        """
        Get the data object to be used for testing. Used for benchmarks that write data.
        """
        if not self._data_object:
            loader = self.get_loader_dumper()
            with open(self.get_input_file(), "rb") as fp:
                self._data_object = loader.load(fp)
        return self._data_object

    def get_loader_dumper(self):
        """
        :return: an object/class/module that has `dump`, `dumps`, `load`, and `loads` for the given test spec.
        """
        if not self._loader_dumper:
            self._loader_dumper = self._get_loader_dumper()
        return self._loader_dumper

    def _get_loader_dumper(self):
        match self.get_format():
            case 'ion_binary':
                import ion_load_dump
                return ion_load_dump.IonLoadDump(binary=True, c_ext=self['py_c_extension'])
            case 'ion_text':
                import ion_load_dump
                return ion_load_dump.IonLoadDump(binary=False, c_ext=self['py_c_extension'])
            case 'json':
                import json
                return json
            case 'ujson':
                import ujson
                return ujson
            case 'simplejson':
                import simplejson
                return simplejson
            case 'rapidjson':
                import rapidjson
                return rapidjson
            case 'cbor':
                import cbor
                return cbor
            case 'cbor2':
                import cbor2
                return cbor2
            case 'self_describing_protobuf':
                from self_describing_proto import SelfDescribingProtoSerde
                # TODO: Consider making the cache option configurable from the spec file
                return SelfDescribingProtoSerde(cache_type_info=True)
            case 'protobuf':
                import proto
                type_name = self['type']
                if not type_name:
                    raise ValueError("protobuf format requires the type to be specified")
                if self['py_module']:
                    message_type = proto.get_message_type_from_py(type_name, self['py_module'])
                elif self['py_file']:
                    message_type = proto.get_message_type_from_py(type_name, "imported_protobuf_module",
                                                                  self.get_attribute_as_path('py_file'))
                elif self['descriptor_file']:
                    message_type = proto.get_message_type_from_descriptor_set(type_name, self.get_attribute_as_path('descriptor_file'))
                else:
                    raise ValueError("format 'protobuf' spec requires py_module, py_file, or descriptor_file")
                return proto.ProtoSerde(message_type)
            case _:
                return None
