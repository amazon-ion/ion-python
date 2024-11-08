# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


class ProtoSerde:
    """
    This class provides load(s) and dump(s) functions that wrap the protocol buffer APIs for a single message type.

    Example Usage:
    ```
    car_proto = proto.ProtoSerde(proto.get_message_type_from_py("Car", "generated_model.car_pb2"))
    truck_proto = proto.ProtoSerde(proto.get_message_type_from_descriptor_set("Truck", "~/trucks_descriptors.desc"))

    car = car_proto.load("~/my_car.data")
    truck = convert_to_truck(car)
    truck_proto.dump(truck, "~/my_truck.data")
    ```
    """
    def __init__(self, message_type, reuse_message_object=True):
        """
        :param message_type: A protocol buffer message class definition that extends the abstract base class Message
            (https://googleapis.dev/python/protobuf/latest/google/protobuf/message.html#google.protobuf.message.Message).
            A Message implementation can be obtained using `protoc`. To load a message at runtime, see
            `get_message_type_from_descriptor_set` and `get_message_type_from_py`
        :param reuse_message_object: Controls whether to reuse the message object. Python protocol buffer message
            objects are designed to be reused to reduce allocations, but you may not want to reuse the message object if
            you need to hold multiple messages in memory or if you are sharing the message objects or an instance of
            this class across multiple threads.
        """
        self._message_type = message_type
        if reuse_message_object:
            self._message_obj = message_type()

            def get_message_instance():
                return self._message_obj

            self._get_message_instance = get_message_instance
        else:
            self._get_message_instance = message_type

    def loads(self, s):
        """
        Deserialize a protocol buffer message from bytes/string.
        """
        obj = self._get_message_instance()
        obj.ParseFromString(s)
        return obj

    def load(self, fp):
        """
        Deserialize a protocol buffer message from a file.
        """
        return self.loads(fp.read())

    def dumps(self, obj):
        """
        Serialize a protocol buffer message to bytes/string.
        """
        return obj.SerializeToString()

    def dump(self, obj, fp):
        """
        Serialize a protocol buffer message to a file.
        """
        fp.write(obj.SerializeToString())


def get_message_type_from_py(type_name, module_name, module_file=None):
    """
    Import and load a protocol buffer message class from a Python module at runtime.

    A Python protocol buffer module can be created using `protoc --python_out=...`.

    :param type_name: Name of the generated Protocol Buffer Message class to get
    :type type_name: str
    :param module_name: Absolute module path of the python module containing the message class
    :type module_name: str
    :param module_file: Location of the module in the file system. Only required if not in sys.path
    :type module_file: str
    :return: a message type to use for ProtoSerde
    """
    import importlib
    import importlib.util
    import sys

    if module_file:
        spec = importlib.util.spec_from_file_location(module_name, module_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    elif module_name:
        module = importlib.import_module(module_name)
    else:
        raise ValueError("Both module_name and module_file are None; at least one must be set.")

    return getattr(module, type_name)


def get_message_type_from_descriptor_set(type_name, descriptor_set_file):
    """
    Dynamically generate a protocol buffer message class definition from a protocol buffer descriptor file.

    A descriptor file can be created using `protoc --descriptor_set_out=...`.

    :param type_name: Name of the type to use from the descriptor set
    :type type_name: str
    :param descriptor_set_file: The path of the protocol buffer descriptor set file
    :type descriptor_set_file: str
    :return: a message type to use for ProtoSerde
    """
    from google.protobuf import descriptor_pb2 as _descriptor_pb2
    from google.protobuf import message as _message
    from google.protobuf import message_factory as _message_factory
    from google.protobuf import reflection as _reflection
    import six

    descriptor_set = _descriptor_pb2.FileDescriptorSet()

    with open(descriptor_set_file, "rb") as f:
        descriptor_set.ParseFromString(f.read())

    messages_types = _message_factory.GetMessages(descriptor_set.file)
    message_type = messages_types[type_name]()

    class DynamicMessage(six.with_metaclass(_reflection.GeneratedProtocolMessageType, _message.Message)):
        DESCRIPTOR = message_type.DESCRIPTOR

    return DynamicMessage
