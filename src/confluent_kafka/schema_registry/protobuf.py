#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import io
import sys
import base64
import struct
from collections import deque

from google.protobuf.message import DecodeError
from google.protobuf.message_factory import MessageFactory

from . import (_MAGIC_BYTE,
               reference_subject_name_strategy,
               topic_subject_name_strategy,)
from .schema_registry_client import (Schema,
                                     SchemaReference)
from confluent_kafka.serialization import SerializationError

# Converts an int to bytes (opposite of ord)
# Python3.chr() -> Unicode
# Python2.chr() -> str(alias for bytes)
if sys.version > '3':
    def _bytes(b):
        """
        Convert int to bytes

        Args:
            b (int): int to format as bytes.

        """
        return bytes((b,))
else:
    def _bytes(b):
        """
        Convert int to bytes

        Args:
            b (int): int to format as bytes.

        """
        return chr(b)


class _ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.

    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _create_msg_index(msg_desc):
    """
    Maps the location of msg_desc within a FileDescriptor.

    Args:
        msg_desc (MessageDescriptor): Protobuf MessageDescriptor

    Returns:
        [int]: Protobuf MessageDescriptor index

    Raises:
        ValueError: If the message descriptor is malformed.

    """
    msg_idx = deque()
    current = msg_desc
    found = False
    # Traverse tree upwardly it's root
    while current.containing_type is not None:
        previous = current
        current = previous.containing_type
        # find child's position
        for idx, node in enumerate(current.nested_types):
            if node == previous:
                msg_idx.appendleft(idx)
                found = True
                break
        if not found:
            raise ValueError("Nested MessageDescriptor not found")

    found = False
    # find root's position in protofile
    for idx, msg_type_name in enumerate(msg_desc.file.message_types_by_name):
        if msg_type_name == current.name:
            msg_idx.appendleft(idx)
            found = True
            break
    if not found:
        raise ValueError("MessageDescriptor not found in file")

    # The root element at the 0 position does not need a length prefix.
    if len(msg_idx) == 1 and msg_idx[0] == 0:
        return [0]

    msg_idx.appendleft(len(msg_idx))
    return list(msg_idx)


def _schema_to_str(proto_file):
    """
    Base64 encodes a FileDescriptor

    Args:
        proto_file (FileDescriptor): FileDescriptor to encode.

    Returns:
        str: Base64 encoded FileDescriptor

    """
    return base64.standard_b64encode(proto_file.serialized_pb).decode('ascii')


class ProtobufSerializer(object):
    """
    ProtobufSerializer serializes objects in the Confluent Schema Registry
    binary format for Protobuf.

    ProtobufSerializer configuration properties:

    +-------------------------------------+----------+------------------------------------------------------+
    | Property Name                       | Type     | Description                                          |
    +=====================================+==========+======================================================+
    |                                     |          | Registers schemas automatically if not               |
    | ``auto.register.schemas``           | bool     | previously associated with a particular subject.     |
    |                                     |          | Defaults to True.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``subject.name.strategy``           | callable | Instructs the ProtobufSerializer on how to construct |
    |                                     |          | Schema Registry subject names.                       |
    |                                     |          | Defaults to topic_subject_name_strategy.             |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``reference.subject.name.strategy`` | callable | Instructs the ProtobufSerializer on how to construct |
    |                                     |          | Schema Registry subject names for Schema References  |
    |                                     |          | Defaults to reference_subject_name_strategy          |
    +-------------------------------------+----------+------------------------------------------------------+

    Schemas are registered to namespaces known as Subjects which define how a
    schema may evolve over time. By default the subject name is formed by
    concatenating the topic name with the message field separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    ``subject.name.strategy``.

    Supported subject name strategies

    +--------------------------------------+------------------------------+
    | Subject Name Strategy                | Output Format                |
    +======================================+==============================+
    | topic_subject_name_strategy(default) | {topic name}-{message field} |
    +--------------------------------------+------------------------------+
    | topic_record_subject_name_strategy   | {topic name}-{record name}   |
    +--------------------------------------+------------------------------+
    | record_subject_name_strategy         | {record name}                |
    +--------------------------------------+------------------------------+

    See `Subject name strategy <https://docs.confluent.io/current/schema-registry/serializer-formatter.html#subject-name-strategy>`_ for additional details.

    Args:
        msg_type (GeneratedProtocolMessageType): Protobuf Message type.

        schema_registry_client (SchemaRegistryClient): Schema Registry
            client instance.

        conf (dict): ProtobufSerializer configuration.

    See Also:
        `Protobuf API reference <https://googleapis.dev/python/protobuf/latest/google/protobuf.html>`_

    """  # noqa: E501
    __slots__ = ['_auto_register', '_registry', '_known_subjects',
                 '_msg_class', '_msg_index', '_schema', '_schema_id',
                 '_ref_reference_subject_func', '_subject_name_func']
    # default configuration
    _default_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy': topic_subject_name_strategy,
        'reference.subject.name.strategy': reference_subject_name_strategy
    }

    def __init__(self, msg_type, schema_registry_client, conf=None):
        # handle configuration
        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = conf_copy.pop('auto.register.schemas')
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        self._ref_reference_subject_func = conf_copy.pop(
            'reference.subject.name.strategy')
        if not callable(self._ref_reference_subject_func):
            raise ValueError("subject.name.strategy must be callable")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        self._registry = schema_registry_client
        self._schema_id = None
        # Avoid calling registry if schema is known to be registered
        self._known_subjects = set()
        self._msg_class = msg_type

        descriptor = msg_type.DESCRIPTOR
        self._msg_index = _create_msg_index(descriptor)
        self._schema = Schema(_schema_to_str(descriptor.file),
                              schema_type='PROTOBUF')

    @staticmethod
    def _encode_uvarints(buf, ints):
        """
        Encodes each int as a uvarint onto buf

        Args:
            buf (BytesIO): buffer to write to.
            ints ([int]): ints to be encoded.

        """
        for value in ints:
            while (value & ~0x7f) != 0:
                buf.write(_bytes((value & 0x7f) | 0x80))
                value >>= 7
            buf.write(_bytes(value))

    def _resolve_dependencies(self, ctx, file_desc):
        """
        Resolves and optionally registers schema references recursively.

        Args:
            ctx (SerializationContext): Serialization context.

            file_desc (FileDescriptor): file descriptor to traverse.

        """
        schema_refs = []
        for dep in file_desc.dependencies:
            dep_refs = self._resolve_dependencies(ctx, dep)
            subject = self._ref_reference_subject_func(ctx, dep)
            schema = Schema(_schema_to_str(dep),
                            references=dep_refs,
                            schema_type='PROTOBUF')
            if self._auto_register:
                self._registry.register_schema(subject, schema)

            reference = self._registry.lookup_schema(subject, schema)
            # schema_refs are per file descriptor
            schema_refs.append(SchemaReference(dep.name,
                                               subject,
                                               reference.version))
        return schema_refs

    def __call__(self, message_type, ctx):
        """
        Serializes a Protobuf Message to the Confluent Schema Registry
        Protobuf binary format.

        Args:
            message_type (Message): Protobuf message instance.

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if any error occurs serializing obj

        Returns:
            bytes: Confluent Schema Registry formatted Protobuf bytes

        """
        if message_type is None:
            return None

        if not isinstance(message_type, self._msg_class):
            raise ValueError("message must be of type {} not {}"
                             .format(self._msg_class, type(message_type)))

        subject = self._subject_name_func(ctx,
                                          message_type.DESCRIPTOR.full_name)

        if subject not in self._known_subjects:
            self._schema.references = self._resolve_dependencies(
                ctx, message_type.DESCRIPTOR.file)

            if self._auto_register:
                self._schema_id = self._registry.register_schema(subject,
                                                                 self._schema)
            else:
                self._schema_id = self._registry.lookup_schema(
                    subject, self._schema).schema_id

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order
            # (big endian)
            fo.write(struct.pack('>bI', _MAGIC_BYTE, self._schema_id))
            # write the record index to the buffer
            self._encode_uvarints(fo, self._msg_index)
            # write the record itself
            fo.write(message_type.SerializeToString())
            return fo.getvalue()


class ProtobufDeserializer(object):
    """
    ProtobufDeserializer decodes bytes written in the Schema Registry
    Protobuf format to an object.

    Args:
        message_type (GeneratedProtocolMessageType): Protobuf Message type.

    See Also:
    `Protobuf API reference <https://googleapis.dev/python/protobuf/latest/google/protobuf.html>`_

    """
    __slots__ = ['_msg_class', '_msg_index']

    def __init__(self, message_type):
        descriptor = message_type.DESCRIPTOR
        self._msg_index = _create_msg_index(descriptor)
        self._msg_class = MessageFactory().GetPrototype(descriptor)

    @staticmethod
    def _decode_uvarint(buf):
        """
        Decodes a single uvarint from a buffer.

        Args:
            buf (BytesIO): buffer to read from

        Returns:
            int: decoded uvarint

        Raises:
            EOFError: if buffer is empty

        """
        value = 0
        shift = 0
        try:
            while True:
                i = ProtobufDeserializer._read_byte(buf)

                value |= (i & 0x7f) << shift
                shift += 7
                if not (i & 0x80):
                    return value

        except EOFError:
            raise EOFError("Unexpected EOF while reading index")

    @staticmethod
    def _read_byte(buf):
        """
        Returns int representation for a byte.

        Args:
            buf (BytesIO): buffer to read from

        .. _ord:
            https://docs.python.org/2/library/functions.html#ord
        """
        i = buf.read(1)
        if i == b'':
            raise EOFError("Unexpected EOF encountered")
        return ord(i)

    @staticmethod
    def _decode_index(buf):
        """
        Extracts message index from Schema Registry Protobuf formatted bytes.

        Args:
            buf (BytesIO): byte buffer

        Returns:
            int: Protobuf Message index.

        """
        size = ProtobufDeserializer._decode_uvarint(buf)
        msg_index = [size]
        for _ in range(size):
            msg_index.append(ProtobufDeserializer._decode_uvarint(buf))

        return msg_index

    def __call__(self, value, ctx):
        """
        Deserializes Schema Registry formatted Protobuf to Protobuf Message.

        Args:
            value (bytes): Confluent Schema Registry formatted Protobuf bytes.

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Returns:
            Message: Protobuf Message instance.

        Raises:
            SerializerError: If response payload and expected Message type
            differ.

        """
        if value is None:
            return None

        # SR wire protocol + msg_index length
        if len(value) < 6:
            raise SerializationError("Message too small. This message was not"
                                     " produced with a Confluent"
                                     " Schema Registry serializer")

        with _ContextStringIO(value) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unknown magic byte. This message was"
                                         " not produced with a Confluent"
                                         " Schema Registry serializer")

            # Protobuf Messages are self-describing; no need to query schema
            # Move the reader cursor past the index
            _ = ProtobufDeserializer._decode_index(payload)
            msg = self._msg_class()
            try:
                msg.ParseFromString(payload.read())
            except DecodeError as e:
                raise SerializationError(str(e))

            return msg
