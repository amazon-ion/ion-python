# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from google.protobuf import message as _message
from google.protobuf import message_factory as _message_factory
from google.protobuf import reflection as _reflection

from self_describing_proto_pb2 import SelfDescribingMessage

import six


class SelfDescribingProtoSerde:
    """
    This class provides functions for reading and writing self-describing protocol buffers.

    This class uses the technique described in https://protobuf.dev/programming-guides/techniques/#self-description to
    create a mostly self-describing protocol buffer. (It's not truly self-describing because the reader must know that
    the serialized data uses the Self-describing Messages convention.)

    To create a self-describing protocol buffer, see proto_tools.py.

    This class makes no guarantees of thread-safety because each instance of this class has its own single instance of
    the SelfDescribingMessage wrapper that it reuses for every load(s) call.

    TODO: Add support for importing "well-known types" (https://protobuf.dev/reference/protobuf/google.protobuf/).
    """
    def __init__(self, cache_type_info=True, reuse_inner_object=False):
        """
        :param cache_type_info: Controls whether the type descriptor set and generated classes should be cached. Caching
            the generated classes for load(s) can be significantly faster than not caching, but requires more memory for
            the cache. WARNING: there is no cache eviction implemented yet.
            (Has no effect for dump(s).)
        :param reuse_inner_object: Controls whether loads should create a new instance of the inner message class each
            time load(s) is called.
            (Has no effect if cache_type_info is False. Has no effect for dump(s).)
        """
        self._cache_type_info = cache_type_info
        self._reuse_inner_object = reuse_inner_object
        self._cached_outer_object = SelfDescribingMessage()
        self._cached_inner_definitions = {}
        self._cached_inner_objects = {}

    @staticmethod
    def generate_class_definition(type_name, descriptor_set):
        """
        Generate a Python class for the given type_name using the provided descriptor_set.
        """
        messages_types = _message_factory.GetMessages(descriptor_set.file)
        message_type = messages_types[type_name]()

        class DynamicMessage(six.with_metaclass(_reflection.GeneratedProtocolMessageType, _message.Message)):
            DESCRIPTOR = message_type.DESCRIPTOR

        return DynamicMessage

    def _get_inner_object_instance(self, type_name, descriptor_set):
        """
        Get an uninitialized instance of the inner object for the given message.
        """
        if self._cache_type_info:
            descriptor_set_key = descriptor_set.SerializeToString()

            if descriptor_set_key not in self._cached_inner_definitions:
                self._cached_inner_definitions[descriptor_set_key] = {}
            if type_name not in self._cached_inner_definitions[descriptor_set_key]:
                self._cached_inner_definitions[descriptor_set_key][type_name] = \
                    SelfDescribingProtoSerde.generate_class_definition(type_name, descriptor_set)

            clazz = self._cached_inner_definitions[descriptor_set_key][type_name]

            if self._reuse_inner_object:
                if descriptor_set_key not in self._cached_inner_objects:
                    self._cached_inner_objects[descriptor_set_key] = {}
                if type_name not in self._cached_inner_objects[descriptor_set_key]:
                    self._cached_inner_objects[descriptor_set_key][type_name] = clazz()

                return self._cached_inner_objects[descriptor_set_key][type_name]
            else:
                return clazz()
        else:
            clazz = SelfDescribingProtoSerde.generate_class_definition(type_name, descriptor_set)
            return clazz()

    def loads(self, s):
        """
        Deserialize a self-describing protocol buffer from bytes/string.
        """
        outer_obj = self._cached_outer_object
        outer_obj.ParseFromString(s)
        inner_obj = self._get_inner_object_instance(outer_obj.message.type_url, outer_obj.descriptor_set)
        inner_obj.ParseFromString(outer_obj.message.value)
        return inner_obj

    def load(self, fp):
        """
        Deserialize a self-describing protocol buffer from a file.
        """
        return self.loads(fp.read())

    def dumps(self, obj):
        """
        Serialize a protocol buffer message as self-describing protocol buffer bytes/string.
        """
        # It seems to be faster to create a new SelfDescribingMessage for each call than it is to use the cached object
        # and check and/or clear the descriptor for each call.
        outer_object = SelfDescribingMessage()
        obj.DESCRIPTOR.file.CopyToProto(outer_object.descriptor_set.file.add())

        outer_object.message.type_url = obj.DESCRIPTOR.full_name
        outer_object.message.value = obj.SerializeToString()
        return outer_object.SerializeToString()

    def dump(self, obj, fp):
        """
        Serialize a protocol buffer message as a self-describing protocol buffer file.
        """
        fp.write(self.dumps(obj))
