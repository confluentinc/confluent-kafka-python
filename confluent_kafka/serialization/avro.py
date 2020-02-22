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

from .error import SerializerError
from confluent_kafka.schema_registry.cached_schema_registry_client import TopicNameStrategy

MAGIC_BYTE = 0


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class Converter(object):
    """
    Converts class instance objects to and from dict instances to enable more sophisticated
    Avro record serialization/deserialization.
    """
    def to_dict(self, record_obj):
        """
        Converts class instance objects to dict for serialization.

        :param record_obj:
        :returns: custom object
        :rtype: object
        """
        return record_obj

    def from_dict(self, record_dict):
        """
        Converts dict to class instance object.

        :param record_dict:
        :returns: custom object
        :rtype: object
        """
        return record_dict


class AvroSerializer(object):
    __slots__ = ['registry_client', 'namer', 'schema', 'reader_schema',
                 'converter']

    def __init__(self, registry, schema=None, reader_schema=None,
                 name_strategy=TopicNameStrategy, converter=None):
        """
        Apache Avro conformant serializer.

        By default all Avro records must be converted from their instance representation
        to a dict prior to calling serialize. Likewise the deserialize function will
        always returns records as type dict. A custom Converter implementation may
        be provided to handle this conversion within the serializer.

        :param CachedSchemaRegistryClient registry: Confluent Schema Registry client
        :param AvroSchema schema: Parsed AvroSchema object
        :param AvroSchema reader_schema: Optional parsed schema to project decoded
            record on.
        :param callable(Schema, SerializationContext) name_strategy: derives Schema
            registration namespace
        :param Converter converter: Converter instance to handle class instance
            dict conversions.
        """
        self.registry_client = registry
        self.schema = schema
        self.reader_schema = reader_schema
        self.namer = name_strategy
        self.converter = converter

    def __hash__(self):
        return hash(self.schema)

    def serialize(self, datum, ctx):
        """
        Encode datum to Avro binary format

        :param object datum: Avro object to encode
        :param SerializationContext ctx:

        :raises: SerializerError

        :returns: encoded Record|Primitive
        :rtype: bytes
        """
        if datum is None:
            return None

        if self.converter:
            datum = self.converter.to_dict(datum)

        with ContextStringIO() as fo:
            self._encode(fo, self.schema, datum, ctx)

            return fo.getvalue()

    def write(self, fo, datum, ctx):
        """
        Write serialized datum to a buffer.

        :param BytesIO fo: buffer to write encoded datum to.
        :param object datum: Record|Primitive to encode
        :param SerializationContext ctx:

        :raises: SerializerError

        :returns: None
        :rtype: None
        """
        if datum is None:
            return None

        self._encode(fo, datum, ctx)

    def _encode(self, fo, schema, datum, ctx):
        """
        Given a parsed Schema, encode a record for the given topic.  The
        record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'

        :param SerializationContext ctx: Topic name
        :param object datum: Avro object to serialize
        :returns: Encoded record with schema ID as bytes
        :rtype: bytes

        :raises: SerializerError
        """

        if schema is None:
            raise SerializerError("Missing schema.")

        subject = self.namer(schema, ctx)
        self.registry_client.register(subject, schema)

        # Write the magic byte and schema ID in network byte order (big endian)
        fo.write(struct.pack('>bI', MAGIC_BYTE, schema.id))
        # write the record to the rest of the buffer
        schemaless_writer(fo, schema.schema, datum)

        return

    def deserialize(self, data, ctx):
        """
        Decode Avro Binary to Avro object

        :param bytes data: Avro binary
        :param SerializationContext ctx: Optional schema to project object on.

        :raises SerializerError

        :returns: decoded Avro object
        :rtype: object
        """
        if data is None:
            return None

        return self._decode(data, self.reader_schema)

    def _decode(self, data, reader_schema=None):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        :param str|bytes or None data: message key or value to be decoded
        :param Schema reader_schema: Optional compatible Schema to project value on.

        :returns: Decoded value.
        :rtype object:
        """

        if len(data) <= 5:
            raise SerializerError("message is too small to decode {}".format(data))

        with ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")

            schema = self.registry_client.get_by_id(schema_id)

            if reader_schema:
                return schemaless_reader(payload, schema.schema, reader_schema.schema, True)
            return schemaless_reader(payload, schema.schema, None, True)
