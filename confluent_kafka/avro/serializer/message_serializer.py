#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Confluent Inc.
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
import logging
import struct
import warnings

from fastavro import schemaless_writer, schemaless_reader

from . import KeySerializerError, ValueSerializerError
from confluent_kafka.avro.serializer import SerializerError

log = logging.getLogger(__name__)
MAGIC_BYTE = 0

# TODO: Add generic serde example for avro
warnings.warn(
    "MessageSerializer has been deprecated."
    "Use confluent_kafka.serialization.AvroSerializer instead."
    "have been repackaged under confluent_kafka.serialization. "
    "See confluent_kafka.avro.AvroProducer and confluent_kafka.avro.AvroCosnumer "
    "for example usage.",
    category=DeprecationWarning, stacklevel=2)


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class MessageSerializer(object):
    """
    A helper class that can serialize and deserialize messages
    that need to be encoded or decoded using the schema registry.

    All encode_* methods return a buffer that can be sent to kafka.
    All decode_* methods accept a buffer received from kafka.
    """

    # TODO: Deprecate reader_[key|value]_schema args move into Schema object
    def __init__(self, registry_client, reader_key_schema=None, reader_value_schema=None):
        self.registry_client = registry_client
        self.reader_key_schema = reader_key_schema
        self.reader_value_schema = reader_value_schema
        warnings.warn(
            "MessageSerializer is being deprecated and will be removed in a future release."
            "Use AvroSerializer instead. ",
            category=DeprecationWarning, stacklevel=2)

    # TODO: Deprecate, rename serialize()
    def encode_record_with_schema(self, topic, schema, record, is_key=False):
        """
        Given a parsed Schema, encode a record for the given topic.  The
        record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'

        :param str topic: Topic name
        :param Schema schema: Parsed schema
        :param object record: An object to serialize
        :param bool is_key: If the record is a key

        :raises: SerializationError

        :returns: Encoded record with schema ID as bytes
        :rtype: bytes
        """

        subject_suffix = ('-key' if is_key else '-value')
        subject = topic + subject_suffix

        self.registry_client.register(subject, schema)

        serialize_err = KeySerializerError if is_key else ValueSerializerError

        try:
            with ContextStringIO() as outf:
                # Write the magic byte and schema ID in network byte order (big endian)
                outf.write(struct.pack('>bI', MAGIC_BYTE, schema.id))

                # write the record to the rest of the buffer
                schemaless_writer(outf, schema.schema, record)

                return outf.getvalue()
        except Exception as e:
            raise serialize_err(e)

    def encode_record_with_schema_id(self, schema_id, record, is_key=False):
        """
        Encode Avro object with a given schema id.

        :param int schema_id:
        :param object record:
        :param bool is_key:

        :raises: SerializationError

        :returns: encoded Avro object
        :rtype: bytes
        """
        schema = self.registry_client.get_by_id(schema_id)

        serialize_err = KeySerializerError if is_key else ValueSerializerError

        try:
            with ContextStringIO() as outf:
                # Write the magic byte and schema ID in network byte order (big endian)
                outf.write(struct.pack('>bI', MAGIC_BYTE, schema_id))

                # write the record to the rest of the buffer
                schemaless_writer(outf, schema.schema, record)

                return outf.getvalue()
        except Exception as e:
            raise serialize_err(e)

    def _deserialize(self, schema_id, data, is_key=False):
        """
        Deserializes bytes.

        :param int schema_id: Schema Registry Id
        :param BytesIO data: bytes to be decoded
        :param bool is_key: True if Message Key

        :raises: SerializationError

        :returns: Decoded value
        :rtype: object
        """

        schema = self.registry_client.get_by_id(schema_id)
        reader_schema = self.reader_key_schema if is_key else self.reader_value_schema

        if reader_schema:
            return schemaless_reader(data, schema.schema, reader_schema.schema)
        return schemaless_reader(data, schema.schema)

    def decode_message(self, data, is_key=False):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        :param str|bytes or None data: message key or value to be decoded
        :param bool is_key: True if data is Message Key

        :raises: SerializationError

        :returns: Decoded value.
        :rtype object:
        """

        if data is None:
            return None

        if len(data) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")

            try:
                return self._deserialize(schema_id, payload, is_key)
            except Exception as e:
                raise SerializerError(e)
