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
import struct

from fastavro import schemaless_writer, schemaless_reader

from .schema_registry_client import SchemaRegistryClient
from confluent_kafka.serialization.error import SerializationError
from confluent_kafka.serialization.kafka_serdes import Serializer, Deserializer

_MAGIC_BYTE = 0


class SubjectNameStrategy(object):
    """
    Instructs the Serializer on how to construct subject names when registering
    schemas. See the Schema Registryy documentation for additional details.

    .. _SchemaRegistry Documentation:
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#group-by-topic-or-other-relationships

    """

    # Configuration enum-like structure
    TopicNameStrategy = 0
    TopicRecordNameStrategy = 1
    RecordNameStrategy = 2

    _str = ['TopicNameStrategy', 'TopicRecordNameStrategy', 'RecordNameStrategy']

    def __call__(self, schema, ctx):
        raise NotImplementedError

    @classmethod
    def __str__(cls, strategy):
        return cls._str[strategy]


class TopicNameStrategy(SubjectNameStrategy):
    """
    Constructs a subject name in the form of {topic}={key|value}.

    Args:
        schema (Schema): Schema associated with a subject
        ctx (SerializationContext): Serialization context.

    """
    def __call__(self, schema, ctx):
        return ctx.topic + "-" + str(ctx.field)


class TopicRecordNameStrategy(SubjectNameStrategy):
    """
    Constructs a subject name in the form of {topic}={record.name}.

    Args:
        schema (Schema): Schema associated with a subject
        ctx (SerializationContext): Serialization context.

    """
    def __call__(self, schema, ctx):
        return ctx.topic + "-" + schema.name


class RecordNameStrategy(SubjectNameStrategy):
    """
    Constructs a subject name in the form of {record.name}.

    Args:
        schema (Schema): Schema associated with a subject
        ctx (SerializationContext): Serialization context.

    """
    def __call__(self, schema, ctx):
        return schema.name


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.

    """
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class AvroSerializer(Serializer):
    """
    AvroSerializer encodes objects into the Avro binary format in accordance
    with the schema provided at instantiation.

    Unless configured otherwise the AvroSerializer will automatically register
    its schema with the provided registry instance.

    Arguments:
        conf (AvroSerializerConfig): Avro Serializer configuration instance
        schema (Schema): Parsed Avro Schema object

    .. _Schema Registry specification
        https://avro.apache.org/docs/current/spec.html for more details.

    .. _Schema Resolution
        https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution

    """
    def __init__(self, conf, schema):
        self.registry = SchemaRegistryClient(conf.SchemaRegistryConfig)
        self.schema = schema
        self.id_func = self.registry.register_schema if conf.SchemaIDStrategy else \
            self.registry.get_registration
        self.name_func = conf.SubjectNameStrategy

    def __hash__(self):
        return hash(self.schema)

    def __call__(self, datum, ctx):
        """
        Encode datum to Avro binary format.

        Arguments:
            datum (object): Avro object to encode
            ctx (SerializationContext): Serialization context object.

        Raises:
            SerializerError if any error occurs encoding datum with the writer
            schema.

        Returns:
            bytes: Encoded Record|Primitive

        """
        if datum is None:
            return None

        subject = self.name_func(self.schema, ctx)
        version = self.id_func(subject, self.schema)

        with ContextStringIO() as fo:
            self._encode(fo, version, datum)

            return fo.getvalue()

    def _encode(self, fo, version, datum):
        """
        Encode datum on buffer.

        Arguments:
            fo (IOBytes): buffer encode datum on.
            version (Version): Schema registration information
            datum (object): Record|Primitive to encode.

        Raises:
            SerializerError if an error occurs when encoding datum.

        """

        # Write the magic byte and schema ID in network byte order (big endian)
        fo.write(struct.pack('>bI', _MAGIC_BYTE, version.schema_id))
        # write the record to the rest of the buffer
        schemaless_writer(fo, self.schema.schema, datum)

        return


class AvroDeserializer(Deserializer):
    """
    AvroDeserializer decodes objects from their Avro binary format in accordance
    with the schema provided at instantiation.

    Unless configured otherwise the AvroSerializer will automatically register
    its schema with the provided registry instance.

    Arguments:
        - registry (Registryclient): Confluent Schema Registry client
        - schema (Schema): Parsed Avro Schema object

    Keyword Arguments:
        reader_schema (Schema, optional): Optional parsed schema to project
            decoded record on. See Schema Resolution for more details. Defaults
            to None.

    .. _Schema Registry specification
        https://avro.apache.org/docs/current/spec.html for more details.

    .. _Schema Resolution
        https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution

    """
    def __init__(self, conf, schema,
                 reader_schema=None):

        self.registry = SchemaRegistryClient(conf.SchemaRegistryConfig)
        self.schema = schema
        self.reader_schema = reader_schema

    def __call__(self, data, ctx):
        """
        Decode Avro binary to object.

        Arguments:
            data (bytes): Avro binary encoded bytes
            ctx (SerializationContext): Serialization context object.

        Raises:
            SerializerError if an error occurs ready data.

        Returns:
            object: Decoded object

        """
        if data is None:
            return None

        record = self._decode(data, self.reader_schema)

        return record

    def _decode(self, data, reader_schema=None):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        Arguments:
            data (bytes | str): Avro binary to be decoded.
            reader_schema (Schema, optional): Schema to project on.

        Returns:
            object: Decoded object

        """
        if len(data) <= 5:
            raise SerializationError("message is too small to decode {}".format(data))

        with ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("message does not start with magic byte")

            schema = self.registry.get_schema(schema_id)

            if reader_schema:
                return schemaless_reader(payload, schema.schema, reader_schema.schema)
            return schemaless_reader(payload, schema.schema)
