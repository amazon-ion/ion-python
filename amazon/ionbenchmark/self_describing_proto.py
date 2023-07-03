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

    The generated Protobuf Python code APIs are designed so that you can reuse the same generated protobuf object in a
    single thread. However, caching the type info, and reusing inner objects is default false because if you know that
    you're always using the same file descriptor set, then there isn't as compelling a case for using self-describing
    messages in the first place.

    This class makes no guarantees of thread-safety because each instance of this class has its own single instance of
    the SelfDescribingMessage wrapper that it reuses for every load(s) and dump(s) call.

    TODO: Add support for importing "well-known types" (https://protobuf.dev/reference/protobuf/google.protobuf/).
    """
    def __init__(self, cache_type_info=False, reuse_inner_object=False):
        """
        :param cache_type_info: Controls whether the type descriptor set should be cached. You MAY set this to True only
            if you are sure that all the inner messages to be read by this instance of SelfDescribingProtobufSerde were
            generated using the same descriptor set. If inner messages were generated with different descriptor sets,
            data may be silently corrupted or an error could be raised.
            (Has no effect for dump if cache_outer_object is False.)
        :param reuse_inner_object: Controls whether loads should create a new instance of the inner message class each
            time load(s) is called. (Has no effect if cache_type_info is False. Has no effect for dump(s).)
        """
        self._cache_type_info = cache_type_info
        self._reuse_inner_object = reuse_inner_object
        self._cached_outer_object = SelfDescribingMessage()
        self._cached_inner_definitions = {}
        self._cached_inner_objects = {}

    @staticmethod
    def generate_class_definition(type_name, descriptor_set):
        """
        Generates a Python class for the given type_name using the provided descriptor_set.
        """
        messages_types = _message_factory.GetMessages(descriptor_set.file)
        message_type = messages_types[type_name]()

        class DynamicMessage(six.with_metaclass(_reflection.GeneratedProtocolMessageType, _message.Message)):
            DESCRIPTOR = message_type.DESCRIPTOR

        return DynamicMessage

    def _get_inner_object_instance(self, type_name, descriptor_set):
        """
        Gets an uninitialized instance of the inner object for the given message.
        """
        if self._cache_type_info:
            if type_name not in self._cached_inner_definitions:
                self._cached_inner_definitions[type_name] = SelfDescribingProtoSerde.generate_class_definition(
                    type_name, descriptor_set)
            clazz = self._cached_inner_definitions[type_name]

            if self._reuse_inner_object:
                if type_name not in self._cached_inner_objects:
                    self._cached_inner_objects[type_name] = clazz()
                return self._cached_inner_objects[type_name]
            else:
                return clazz()
        else:
            clazz = SelfDescribingProtoSerde.generate_class_definition(type_name, descriptor_set)
            return clazz()

    def loads(self, s):
        """
        Deserializes a self-describing protocol buffer from bytes/string.
        """
        outer_obj = self._cached_outer_object
        outer_obj.ParseFromString(s)
        inner_obj = self._get_inner_object_instance(outer_obj.message.type_url, outer_obj.descriptor_set)
        inner_obj.ParseFromString(outer_obj.message.value)
        return inner_obj

    def load(self, fp):
        """
        Deserializes a self-describing protocol buffer from a file.
        """
        return self.loads(fp.read())

    def dumps(self, obj):
        """
        Serializes a protocol buffer message as self-describing protocol buffer bytes/string.
        """
        outer_object = self._cached_outer_object

        # If we're not caching the type info, make sure we clear out the descriptor set in the message.
        if not self._cache_type_info:
            outer_object.descriptor_set.ClearField('file')

        # Add the descriptor set, if needed.
        if not outer_object.descriptor_set.file:
            obj.DESCRIPTOR.file.CopyToProto(self._cached_outer_object.descriptor_set.file.add())

        outer_object.message.type_url = obj.DESCRIPTOR.full_name
        outer_object.message.value = obj.SerializeToString()
        return outer_object.SerializeToString()

    def dump(self, obj, fp):
        """
        Serializes a protocol buffer message as a self-describing protocol buffer file.
        """
        fp.write(self.dumps(obj))
