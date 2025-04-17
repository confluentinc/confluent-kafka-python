#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020-2022 Confluent Inc.
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

import io
import struct
import warnings
from typing import Set, List, Union, Optional, Tuple

from google.protobuf import json_format, descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.descriptor import Descriptor, FileDescriptor
from google.protobuf.message import DecodeError, Message
from google.protobuf.message_factory import GetMessageClass

from confluent_kafka.schema_registry.common import (_MAGIC_BYTE, _ContextStringIO,
               reference_subject_name_strategy,
               topic_subject_name_strategy)
from confluent_kafka.schema_registry.schema_registry_client import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry.common.protobuf import _bytes, _create_index_array, _init_pool, _is_builtin, _schema_to_str, _str_to_proto, transform
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry import (Schema,
                                     SchemaReference,
                                     RuleMode)
from confluent_kafka.serialization import SerializationError, \
    SerializationContext
from confluent_kafka.schema_registry.common import asyncinit
from confluent_kafka.schema_registry.serde import AsyncBaseSerializer, AsyncBaseDeserializer, ParsedSchemaCache


async def _resolve_named_schema(
    schema: Schema,
    schema_registry_client: AsyncSchemaRegistryClient,
    pool: DescriptorPool,
    visited: Optional[Set[str]] = None
):
    """
    Resolves named schemas referenced by the provided schema recursively.
    :param schema: Schema to resolve named schemas for.
    :param schema_registry_client: AsyncSchemaRegistryClient to use for retrieval.
    :param pool: DescriptorPool to add resolved schemas to.
    :return: DescriptorPool
    """
    if visited is None:
        visited = set()
    if schema.references is not None:
        for ref in schema.references:
            if _is_builtin(ref.name) or ref.name in visited:
                continue
            visited.add(ref.name)
            referenced_schema = await schema_registry_client.get_version(ref.subject, ref.version, True, 'serialized')
            await _resolve_named_schema(referenced_schema.schema, schema_registry_client, pool, visited)
            file_descriptor_proto = _str_to_proto(ref.name, referenced_schema.schema.schema_str)
            pool.Add(file_descriptor_proto)


@asyncinit
class AsyncProtobufSerializer(AsyncBaseSerializer):
    """
    Serializer for Protobuf Message derived classes. Serialization format is Protobuf,
    with Confluent Schema Registry framing.

    Configuration properties:

    +-------------------------------------+----------+------------------------------------------------------+
    | Property Name                       | Type     | Description                                          |
    +=====================================+==========+======================================================+
    |                                     |          | If True, automatically register the configured       |
    | ``auto.register.schemas``           | bool     | schema with Confluent Schema Registry if it has      |
    |                                     |          | not previously been associated with the relevant     |
    |                                     |          | subject (determined via subject.name.strategy).      |
    |                                     |          |                                                      |
    |                                     |          | Defaults to True.                                    |
    |                                     |          |                                                      |
    |                                     |          | Raises SchemaRegistryError if the schema was not     |
    |                                     |          | registered against the subject, or could not be      |
    |                                     |          | successfully registered.                             |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to normalize schemas, which will             |
    | ``normalize.schemas``               | bool     | transform schemas to have a consistent format,       |
    |                                     |          | including ordering properties and references.        |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the given schema ID for               |
    | ``use.schema.id``                   | int      | serialization.                                       |
    |                                     |          |                                                      |
    +-----------------------------------------+----------+--------------------------------------------------+
    |                                     |          | Whether to use the latest subject version for        |
    | ``use.latest.version``              | bool     | serialization.                                       |
    |                                     |          |                                                      |
    |                                     |          | WARNING: There is no check that the latest           |
    |                                     |          | schema is backwards compatible with the object       |
    |                                     |          | being serialized.                                    |
    |                                     |          |                                                      |
    |                                     |          | Defaults to False.                                   |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the latest subject version with       |
    | ``use.latest.with.metadata``        | dict     | the given metadata.                                  |
    |                                     |          |                                                      |
    |                                     |          | WARNING: There is no check that the latest           |
    |                                     |          | schema is backwards compatible with the object       |
    |                                     |          | being serialized.                                    |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether or not to skip known types when resolving    |
    | ``skip.known.types``                | bool     | schema dependencies.                                 |
    |                                     |          |                                                      |
    |                                     |          | Defaults to True.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``subject.name.strategy``           | callable | Defines how Schema Registry subject names are        |
    |                                     |          | constructed. Standard naming strategies are          |
    |                                     |          | defined in the confluent_kafka.schema_registry       |
    |                                     |          | namespace.                                           |
    |                                     |          |                                                      |
    |                                     |          | Defaults to topic_subject_name_strategy.             |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``reference.subject.name.strategy`` | callable | Defines how Schema Registry subject names for schema |
    |                                     |          | references are constructed.                          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to reference_subject_name_strategy          |
    +-------------------------------------+----------+------------------------------------------------------+
    | ``use.deprecated.format``           | bool     | Specifies whether the Protobuf serializer should     |
    |                                     |          | serialize message indexes without zig-zag encoding.  |
    |                                     |          | This option must be explicitly configured as older   |
    |                                     |          | and newer Protobuf producers are incompatible.       |
    |                                     |          | If the consumers of the topic being produced to are  |
    |                                     |          | using confluent-kafka-python <1.8 then this property |
    |                                     |          | must be set to True until all old consumers have     |
    |                                     |          | have been upgraded.                                  |
    |                                     |          |                                                      |
    |                                     |          | Warning: This configuration property will be removed |
    |                                     |          | in a future version of the client.                   |
    +-------------------------------------+----------+------------------------------------------------------+

    Schemas are registered against subject names in Confluent Schema Registry that
    define a scope in which the schemas can be evolved. By default, the subject name
    is formed by concatenating the topic name with the message field (key or value)
    separated by a hyphen.

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
        msg_type (Message): Protobuf Message type.

        schema_registry_client (SchemaRegistryClient): Schema Registry
            client instance.

        conf (dict): ProtobufSerializer configuration.

    See Also:
        `Protobuf API reference <https://googleapis.dev/python/protobuf/latest/google/protobuf.html>`_
    """  # noqa: E501
    __slots__ = ['_skip_known_types', '_known_subjects', '_msg_class', '_index_array',
                 '_schema', '_schema_id', '_ref_reference_subject_func',
                 '_use_deprecated_format', '_parsed_schemas']

    _default_conf = {
        'auto.register.schemas': True,
        'normalize.schemas': False,
        'use.schema.id': None,
        'use.latest.version': False,
        'use.latest.with.metadata': None,
        'skip.known.types': True,
        'subject.name.strategy': topic_subject_name_strategy,
        'reference.subject.name.strategy': reference_subject_name_strategy,
        'use.deprecated.format': False,
    }

    async def __init__(
        self,
        msg_type: Message,
        schema_registry_client: AsyncSchemaRegistryClient,
        conf: Optional[dict] = None,
        rule_conf: Optional[dict] = None,
        rule_registry: Optional[RuleRegistry] = None
    ):
        super().__init__()

        if conf is None or 'use.deprecated.format' not in conf:
            raise RuntimeError(
                "ProtobufSerializer: the 'use.deprecated.format' configuration "
                "property must be explicitly set due to backward incompatibility "
                "with older confluent-kafka-python Protobuf producers and consumers. "
                "See the release notes for more details")

        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = conf_copy.pop('auto.register.schemas')
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._normalize_schemas = conf_copy.pop('normalize.schemas')
        if not isinstance(self._normalize_schemas, bool):
            raise ValueError("normalize.schemas must be a boolean value")

        self._use_schema_id = conf_copy.pop('use.schema.id')
        if (self._use_schema_id is not None and
                not isinstance(self._use_schema_id, int)):
            raise ValueError("use.schema.id must be an int value")

        self._use_latest_version = conf_copy.pop('use.latest.version')
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")
        if self._use_latest_version and self._auto_register:
            raise ValueError("cannot enable both use.latest.version and auto.register.schemas")

        self._use_latest_with_metadata = conf_copy.pop('use.latest.with.metadata')
        if (self._use_latest_with_metadata is not None and
                not isinstance(self._use_latest_with_metadata, dict)):
            raise ValueError("use.latest.with.metadata must be a dict value")

        self._skip_known_types = conf_copy.pop('skip.known.types')
        if not isinstance(self._skip_known_types, bool):
            raise ValueError("skip.known.types must be a boolean value")

        self._use_deprecated_format = conf_copy.pop('use.deprecated.format')
        if not isinstance(self._use_deprecated_format, bool):
            raise ValueError("use.deprecated.format must be a boolean value")
        if self._use_deprecated_format:
            warnings.warn("ProtobufSerializer: the 'use.deprecated.format' "
                          "configuration property, and the ability to use the "
                          "old incorrect Protobuf serializer heading format "
                          "introduced in confluent-kafka-python v1.4.0, "
                          "will be removed in an upcoming release in 2021 Q2. "
                          "Please migrate your Python Protobuf producers and "
                          "consumers to 'use.deprecated.format':False as "
                          "soon as possible")

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
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
        self._schema_id = None
        self._known_subjects = set()
        self._msg_class = msg_type
        self._parsed_schemas = ParsedSchemaCache()

        descriptor = msg_type.DESCRIPTOR
        self._index_array = _create_index_array(descriptor)
        self._schema = Schema(_schema_to_str(descriptor.file),
                              schema_type='PROTOBUF')

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {},
                           rule_conf if rule_conf else {})

    @staticmethod
    def _write_varint(buf: io.BytesIO, val: int, zigzag: bool = True):
        """
        Writes val to buf, either using zigzag or uvarint encoding.

        Args:
            buf (BytesIO): buffer to write to.
            val (int): integer to be encoded.
            zigzag (bool): whether to encode in zigzag or uvarint encoding
        """

        if zigzag:
            val = (val << 1) ^ (val >> 63)

        while (val & ~0x7f) != 0:
            buf.write(_bytes((val & 0x7f) | 0x80))
            val >>= 7
        buf.write(_bytes(val))

    @staticmethod
    def _encode_varints(buf: io.BytesIO, ints: List[int], zigzag: bool = True):
        """
        Encodes each int as a uvarint onto buf

        Args:
            buf (BytesIO): buffer to write to.
            ints ([int]): ints to be encoded.
            zigzag (bool): whether to encode in zigzag or uvarint encoding
        """

        assert len(ints) > 0
        # The root element at the 0 position does not need a length prefix.
        if ints == [0]:
            buf.write(_bytes(0x00))
            return

        AsyncProtobufSerializer._write_varint(buf, len(ints), zigzag=zigzag)

        for value in ints:
            AsyncProtobufSerializer._write_varint(buf, value, zigzag=zigzag)

    async def _resolve_dependencies(
        self, ctx: SerializationContext,
        file_desc: FileDescriptor
    ) -> List[SchemaReference]:
        """
        Resolves and optionally registers schema references recursively.

        Args:
            ctx (SerializationContext): Serialization context.

            file_desc (FileDescriptor): file descriptor to traverse.
        """

        schema_refs = []
        for dep in file_desc.dependencies:
            if self._skip_known_types and _is_builtin(dep.name):
                continue
            dep_refs = await self._resolve_dependencies(ctx, dep)
            subject = self._ref_reference_subject_func(ctx, dep)
            schema = Schema(_schema_to_str(dep),
                            references=dep_refs,
                            schema_type='PROTOBUF')
            if self._auto_register:
                await self._registry.register_schema(subject, schema)

            reference = await self._registry.lookup_schema(subject, schema)
            # schema_refs are per file descriptor
            schema_refs.append(SchemaReference(dep.name,
                                               subject,
                                               reference.version))
        return schema_refs

    def __call__(self, message: Message, ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        return self.__serialize(message, ctx)

    async def __serialize(self, message: Message, ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        """
        Serializes an instance of a class derived from Protobuf Message, and prepends
        it with Confluent Schema Registry framing.

        Args:
            message (Message): An instance of a class derived from Protobuf Message.

            ctx (SerializationContext): Metadata relevant to the serialization.
                operation.

        Raises:
            SerializerError if any error occurs during serialization.

        Returns:
            None if messages is None, else a byte array containing the Protobuf
            serialized message with Confluent Schema Registry framing.
        """

        if message is None:
            return None

        if not isinstance(message, self._msg_class):
            raise ValueError("message must be of type {} not {}"
                             .format(self._msg_class, type(message)))

        subject = self._subject_name_func(ctx,
                                          message.DESCRIPTOR.full_name)
        latest_schema = await self._get_reader_schema(subject, fmt='serialized')
        if latest_schema is not None:
            self._schema_id = latest_schema.schema_id
        elif subject not in self._known_subjects:
            references = await self._resolve_dependencies(
                ctx, message.DESCRIPTOR.file)
            self._schema = Schema(
                self._schema.schema_str,
                self._schema.schema_type,
                references
            )

            if self._auto_register:
                self._schema_id = await self._registry.register_schema(subject,
                                                                 self._schema,
                                                                 self._normalize_schemas)
            else:
                self._schema_id = await self._registry.lookup_schema(
                    subject, self._schema, self._normalize_schemas).schema_id

            self._known_subjects.add(subject)

        if latest_schema is not None:
            fd_proto, pool = await self._get_parsed_schema(latest_schema.schema)
            fd = pool.FindFileByName(fd_proto.name)
            desc = fd.message_types_by_name[message.DESCRIPTOR.name]
            field_transformer = lambda rule_ctx, field_transform, msg: (  # noqa: E731
                transform(rule_ctx, desc, msg, field_transform))
            message = self._execute_rules(ctx, subject, RuleMode.WRITE, None,
                                          latest_schema.schema, message, None,
                                          field_transformer)

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order
            # (big endian)
            fo.write(struct.pack('>bI', _MAGIC_BYTE, self._schema_id))
            # write the index array that specifies the message descriptor
            # of the serialized data.
            self._encode_varints(fo, self._index_array,
                                 zigzag=not self._use_deprecated_format)
            # write the serialized data itself
            fo.write(message.SerializeToString())
            return fo.getvalue()

    async def _get_parsed_schema(self, schema: Schema) -> Tuple[descriptor_pb2.FileDescriptorProto, DescriptorPool]:
        result = self._parsed_schemas.get_parsed_schema(schema)
        if result is not None:
            return result

        pool = DescriptorPool()
        _init_pool(pool)
        await _resolve_named_schema(schema, self._registry, pool)
        fd_proto = _str_to_proto("default", schema.schema_str)
        pool.Add(fd_proto)
        self._parsed_schemas.set(schema, (fd_proto, pool))
        return fd_proto, pool


@asyncinit
class AsyncProtobufDeserializer(AsyncBaseDeserializer):
    """
    Deserializer for Protobuf serialized data with Confluent Schema Registry framing.

    Args:
        message_type (Message derived type): Protobuf Message type.
        conf (dict): Configuration dictionary.

    ProtobufDeserializer configuration properties:

    +-------------------------------------+----------+------------------------------------------------------+
    | Property Name                       | Type     | Description                                          |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the latest subject version for        |
    | ``use.latest.version``              | bool     | deserialization.                                     |
    |                                     |          |                                                      |
    |                                     |          | Defaults to False.                                   |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the latest subject version with       |
    | ``use.latest.with.metadata``        | dict     | the given metadata.                                  |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``subject.name.strategy``           | callable | Defines how Schema Registry subject names are        |
    |                                     |          | constructed. Standard naming strategies     are      |
    |                                     |          | defined in the confluent_kafka.    schema_registry   |
    |                                     |          | namespace    .                                       |
    |                                     |          |                                                      |
    |                                     |          | Defaults to topic_subject_name_strategy.             |
    +-------------------------------------+----------+------------------------------------------------------+
    | ``use.deprecated.format``           | bool     | Specifies whether the Protobuf deserializer should   |
    |                                     |          | deserialize message indexes without zig-zag encoding.|
    |                                     |          | This option must be explicitly configured as older   |
    |                                     |          | and newer Protobuf producers are incompatible.       |
    |                                     |          | If Protobuf messages in the topic to consume were    |
    |                                     |          | produced with confluent-kafka-python <1.8 then this  |
    |                                     |          | property must be set to True until all old messages  |
    |                                     |          | have been processed and producers have been upgraded.|
    |                                     |          | Warning: This configuration property will be removed |
    |                                     |          | in a future version of the client.                   |
    +-------------------------------------+----------+------------------------------------------------------+


    See Also:
    `Protobuf API reference <https://googleapis.dev/python/protobuf/latest/google/protobuf.html>`_
    """

    __slots__ = ['_msg_class', '_use_deprecated_format', '_parsed_schemas']

    _default_conf = {
        'use.latest.version': False,
        'use.latest.with.metadata': None,
        'subject.name.strategy': topic_subject_name_strategy,
        'use.deprecated.format': False,
    }

    async def __init__(
        self,
        message_type: Message,
        conf: Optional[dict] = None,
        schema_registry_client: Optional[AsyncSchemaRegistryClient] = None,
        rule_conf: Optional[dict] = None,
        rule_registry: Optional[RuleRegistry] = None
    ):
        super().__init__()

        self._registry = schema_registry_client
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
        self._parsed_schemas = ParsedSchemaCache()
        self._use_schema_id = None

        # Require use.deprecated.format to be explicitly configured
        # during a transitionary period since old/new format are
        # incompatible.
        if conf is None or 'use.deprecated.format' not in conf:
            raise RuntimeError(
                "ProtobufDeserializer: the 'use.deprecated.format' configuration "
                "property must be explicitly set due to backward incompatibility "
                "with older confluent-kafka-python Protobuf producers and consumers. "
                "See the release notes for more details")

        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._use_latest_version = conf_copy.pop('use.latest.version')
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")

        self._use_latest_with_metadata = conf_copy.pop('use.latest.with.metadata')
        if (self._use_latest_with_metadata is not None and
                not isinstance(self._use_latest_with_metadata, dict)):
            raise ValueError("use.latest.with.metadata must be a dict value")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        self._use_deprecated_format = conf_copy.pop('use.deprecated.format')
        if not isinstance(self._use_deprecated_format, bool):
            raise ValueError("use.deprecated.format must be a boolean value")
        if self._use_deprecated_format:
            warnings.warn("ProtobufDeserializer: the 'use.deprecated.format' "
                          "configuration property, and the ability to use the "
                          "old incorrect Protobuf serializer heading format "
                          "introduced in confluent-kafka-python v1.4.0, "
                          "will be removed in an upcoming release in 2022 Q2. "
                          "Please migrate your Python Protobuf producers and "
                          "consumers to 'use.deprecated.format':False as "
                          "soon as possible")

        descriptor = message_type.DESCRIPTOR
        self._msg_class = GetMessageClass(descriptor)

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {},
                           rule_conf if rule_conf else {})

    @staticmethod
    def _decode_varint(buf: io.BytesIO, zigzag: bool = True) -> int:
        """
        Decodes a single varint from a buffer.

        Args:
            buf (BytesIO): buffer to read from
            zigzag (bool): decode as zigzag or uvarint

        Returns:
            int: decoded varint

        Raises:
            EOFError: if buffer is empty
        """

        value = 0
        shift = 0
        try:
            while True:
                i = AsyncProtobufDeserializer._read_byte(buf)

                value |= (i & 0x7f) << shift
                shift += 7
                if not (i & 0x80):
                    break

            if zigzag:
                value = (value >> 1) ^ -(value & 1)

            return value

        except EOFError:
            raise EOFError("Unexpected EOF while reading index")

    @staticmethod
    def _read_byte(buf: io.BytesIO) -> int:
        """
        Read one byte from buf as an int.

        Args:
            buf (BytesIO): The buffer to read from.

        .. _ord:
            https://docs.python.org/2/library/functions.html#ord
        """

        i = buf.read(1)
        if i == b'':
            raise EOFError("Unexpected EOF encountered")
        return ord(i)

    @staticmethod
    def _read_index_array(buf: io.BytesIO, zigzag: bool = True) -> List[int]:
        """
        Read an index array from buf that specifies the message
        descriptor of interest in the file descriptor.

        Args:
            buf (BytesIO): The buffer to read from.

        Returns:
            list of int: The index array.
        """

        size = AsyncProtobufDeserializer._decode_varint(buf, zigzag=zigzag)
        if size < 0 or size > 100000:
            raise DecodeError("Invalid Protobuf msgidx array length")

        if size == 0:
            return [0]

        msg_index = []
        for _ in range(size):
            msg_index.append(AsyncProtobufDeserializer._decode_varint(buf,
                                                                 zigzag=zigzag))

        return msg_index

    def __call__(self, data: bytes, ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        return self.__serialize(data, ctx)

    async def __serialize(self, data: bytes, ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        """
        Deserialize a serialized protobuf message with Confluent Schema Registry
        framing.

        Args:
            data (bytes): Serialized protobuf message with Confluent Schema
                           Registry framing.

            ctx (SerializationContext): Metadata relevant to the serialization
                operation.

        Returns:
            Message: Protobuf Message instance.

        Raises:
            SerializerError: If there was an error reading the Confluent framing
                data, or parsing the protobuf serialized message.
        """

        if data is None:
            return None

        # SR wire protocol + msg_index length
        if len(data) < 6:
            raise SerializationError("Expecting data framing of length 6 bytes or "
                                     "more but total data size is {} bytes. This "
                                     "message was not produced with a Confluent "
                                     "Schema Registry serializer".format(len(data)))

        subject = self._subject_name_func(ctx, None)
        latest_schema = None
        if subject is not None and self._registry is not None:
            latest_schema = await self._get_reader_schema(subject, fmt='serialized')

        with _ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unknown magic byte. This message was "
                                         "not produced with a Confluent "
                                         "Schema Registry serializer")

            msg_index = self._read_index_array(payload, zigzag=not self._use_deprecated_format)

            if self._registry is not None:
                writer_schema_raw = await self._registry.get_schema(schema_id, fmt='serialized')
                fd_proto, pool = await self._get_parsed_schema(writer_schema_raw)
                writer_schema = pool.FindFileByName(fd_proto.name)
                writer_desc = self._get_message_desc(pool, writer_schema, msg_index)
                if subject is None:
                    subject = self._subject_name_func(ctx, writer_desc.full_name)
                    if subject is not None:
                        latest_schema = await self._get_reader_schema(subject, fmt='serialized')
            else:
                writer_schema_raw = None
                writer_schema = None

            if latest_schema is not None:
                migrations = self._get_migrations(subject, writer_schema_raw, latest_schema, None)
                reader_schema_raw = latest_schema.schema
                fd_proto, pool = await self._get_parsed_schema(latest_schema.schema)
                reader_schema = pool.FindFileByName(fd_proto.name)
            else:
                migrations = None
                reader_schema_raw = writer_schema_raw
                reader_schema = writer_schema

            if reader_schema is not None:
                # Initialize reader desc to first message in file
                reader_desc = self._get_message_desc(pool, reader_schema, [0])
                # Attempt to find a reader desc with the same name as the writer
                reader_desc = reader_schema.message_types_by_name.get(writer_desc.name, reader_desc)

            if migrations:
                msg = GetMessageClass(writer_desc)()
                try:
                    msg.ParseFromString(payload.read())
                except DecodeError as e:
                    raise SerializationError(str(e))

                obj_dict = json_format.MessageToDict(msg, True)
                obj_dict = self._execute_migrations(ctx, subject, migrations, obj_dict)
                msg = GetMessageClass(reader_desc)()
                msg = json_format.ParseDict(obj_dict, msg)
            else:
                # Protobuf Messages are self-describing; no need to query schema
                msg = self._msg_class()
                try:
                    msg.ParseFromString(payload.read())
                except DecodeError as e:
                    raise SerializationError(str(e))

            field_transformer = lambda rule_ctx, field_transform, message: (  # noqa: E731
                transform(rule_ctx, reader_desc, message, field_transform))
            msg = self._execute_rules(ctx, subject, RuleMode.READ, None,
                                      reader_schema_raw, msg, None,
                                      field_transformer)

            return msg

    async def _get_parsed_schema(self, schema: Schema) -> Tuple[descriptor_pb2.FileDescriptorProto, DescriptorPool]:
        result = self._parsed_schemas.get_parsed_schema(schema)
        if result is not None:
            return result

        pool = DescriptorPool()
        _init_pool(pool)
        await _resolve_named_schema(schema, self._registry, pool)
        fd_proto = _str_to_proto("default", schema.schema_str)
        pool.Add(fd_proto)
        self._parsed_schemas.set(schema, (fd_proto, pool))
        return fd_proto, pool

    def _get_message_desc(
        self, pool: DescriptorPool, fd: FileDescriptor,
        msg_index: List[int]
    ) -> Descriptor:
        file_desc_proto = descriptor_pb2.FileDescriptorProto()
        fd.CopyToProto(file_desc_proto)
        (full_name, desc_proto) = self._get_message_desc_proto("", file_desc_proto, msg_index)
        package = file_desc_proto.package
        qualified_name = package + "." + full_name if package else full_name
        return pool.FindMessageTypeByName(qualified_name)

    def _get_message_desc_proto(
        self,
        path: str,
        desc: Union[descriptor_pb2.FileDescriptorProto, descriptor_pb2.DescriptorProto],
        msg_index: List[int]
    ) -> Tuple[str, descriptor_pb2.DescriptorProto]:
        index = msg_index[0]
        if isinstance(desc, descriptor_pb2.FileDescriptorProto):
            msg = desc.message_type[index]
            path = path + "." + msg.name if path else msg.name
            if len(msg_index) == 1:
                return path, msg
            return self._get_message_desc_proto(path, msg, msg_index[1:])
        else:
            msg = desc.nested_type[index]
            path = path + "." + msg.name if path else msg.name
            if len(msg_index) == 1:
                return path, msg
            return self._get_message_desc_proto(path, msg, msg_index[1:])
