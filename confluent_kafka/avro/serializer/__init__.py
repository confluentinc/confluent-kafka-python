#!/usr/bin/env python
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

import avro
import avro.io
import io
import logging
import struct
import sys
import traceback

from confluent_kafka.avro.schema import GenericAvroRecord, get_schema
from confluent_kafka.avro.error import ClientError

log = logging.getLogger(__name__)

MAGIC_BYTE = 0

HAS_FAST = False
try:
    from fastavro import schemaless_reader, schemaless_writer

    HAS_FAST = True
except ImportError:
    pass


class SerializerError(Exception):
    """Generic error from serializer package"""

    def __new__(cls, message, is_key=False):
        if is_key:
            return super(SerializerError, cls).__new__(KeySerializerError, message)
        return super(SerializerError, cls).__new__(ValueSerializerError, message)

    def __init__(self, message, is_key=False):
        self.message = message

        def __repr__(self):
            return '{klass}(error={error})'.format(
                klass=self.__class__.__name__,
                error=self.message
            )

        def __str__(self):
            return self.message


class KeySerializerError(SerializerError):
    pass


class ValueSerializerError(SerializerError):
    pass


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def TopicNameStrategy(topic=None, is_key=False, schema=None):
    """
    Constructs the subject name under which a schema is registered with the Confluent Schema Registry.

    TopicNameStrategy returns the schema's subject in the form of <topic>-key or <topic>-value.

    :param str topic: Topic name.
    :param is_key: True if subject is being registered for a message key.
    :param schema schema: Parsed Avro schema. *Note* Not used by TopicNameStrategy
    :raises ValueError: If topic is unset.
    :returns: The subject name with which to register the schema.
    :rtype: str
    """
    if topic is None:
        raise ValueError("Topic must be set when using TopicNameStrategy")

    return "-".join([topic, '-key' if is_key else '-value'])


def RecordNameStrategy(topic=None, is_key=False, schema=None):
    """
    Constructs the subject name under which a schema is registered with the Confluent Schema Registry.

    RecordNameStrategy returns the fully-qualified record name regardless of the topic.

    Compatibility checks of the same record name across all topics.
    This strategy allows a topic to contain a mixture of different record types.

    :param str topic: Topic name.  *Note* Not used by RecordNameStrategy
    :param is_key: True if subject is being registered for a message key. *Note* Not used by RecordNameStrategy.
    :param schema schema: Parsed Avro schema.
    :raises ValueError: If schema is not set.
    :returns: The subject name with which to register the schema.
    :rtype: str
    """
    if schema is None:
        raise ValueError("Schema must be set when using RecordNameStategy")

    return schema.fullname


def TopicRecordNameStrategy(topic=None, is_key=False, schema=None):
    """
    Constructs the subject name under which a schema is registered with the Confluent Schema Registry.

    TopicRecordNameStrategy returns the topic name appended by the fully-qualified record name.

    Compatibility checks are performed against all records of the same name within the same topic.
    Like the RecordNameStrategy mixed record types are allowed within a topic.
    This strategy is more flexible in that records needn't be complaint across the cluster.

    :param str topic: Topic name.
    :param schema schema: Parsed Avro schema.
    :param is_key: True if used by a key_serializer.
    :raises ValueError: If topic and schema are not set.
    :returns: The subject name with which to register the schema.
    :rtype: str
    """
    if not any([topic, schema]):
        raise ValueError("Both Topic and Schema must be set when using TopicRecordNameStrategy")
    return "-".join([topic, schema.fullname])


class AvroSerializer(object):
    """
    Encodes kafka messages as Avro; registering the schema with the Confluent Schema Registry.

    :param registry_client CachedSchemaRegistryClient: Instance of CachedSchemaRegistryClient.
    :param bool is_key: True if configured as a key_serializer.
    :param func(str, bool, schema): Returns the subject name used when registering schemas.
    """

    __slots__ = ["registry_client", "codec_cache", "is_key", "subject_strategy"]

    def __init__(self, registry_client, is_key=False, subject_strategy=TopicNameStrategy):
        self.registry_client = registry_client
        self.codec_cache = {}
        self.is_key = is_key
        self.subject_strategy = subject_strategy

    def __call__(self, topic, record):
        """
        Given a parsed avro schema, encode a record for the given topic.

        The schema is registered with the subject of 'topic-value'.
        :param str topic: Topic name.
        :param GenericAvroRecord record: An object to serialize.
        :returns: Encoded record with schema ID as bytes.
        :rtype: bytes
        """

        if record is None:
            return None

        subject = self.subject_strategy(topic, self.is_key, get_schema(record))

        schema_id = self.registry_client.register(subject, get_schema(record))
        if not schema_id:
            message = "Failed to register schema with subject {}.".format(subject)
            raise SerializerError(message, is_key=self.is_key)

        if schema_id not in self.codec_cache:
            self.codec_cache[schema_id] = self._get_encoder_func(get_schema(record))

        return self._encode(schema_id, record)

    def _get_encoder_func(self, writer_schema):
        if HAS_FAST:
            return lambda record, fp: schemaless_writer(fp, writer_schema.to_json(), record)
        writer = avro.io.DatumWriter(writer_schema)
        return lambda record, fp: writer.write(record, avro.io.BinaryEncoder(fp))

    def _encode(self, schema_id, datum):
        """
        Encode a datum with a given schema id.
        :param int schema_id: integer ID
        :param object datum: An object to serialize
        :param bool is_key: If the record is a key
        :param SerializerErr err_type: Error type to raise on serialization exception
        :returns: decoder function
        :rtype: func
        """

        if schema_id not in self.codec_cache:
            try:
                schema = self.registry_client.get_by_id(schema_id)
                if not schema:
                    raise SerializerError("Schema does not exist", self.is_key)
                self.codec_cache[schema_id] = self._get_encoder_func(schema)
            except ClientError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise SerializerError(
                    repr(traceback.format_exception(exc_type, exc_value, exc_traceback)),
                    self.is_key)

        # get the writer
        writer = self.codec_cache[schema_id]
        with ContextStringIO() as outf:
            # Write the magic byte and schema ID in network byte order (big endian)
            outf.write(struct.pack('>bI', MAGIC_BYTE, schema_id))

            # write the record to the rest of the buffer
            writer(datum, outf)
            return outf.getvalue()


class AvroDeserializer(object):
    """
    Decodes Kafka messages encoded by Confluent Schema Registry compliant Avro Serializers.

    :param registry_client CachedSchemaRegistryClient: Instance of CachedSchemaRegistryClient.
    :param bool is_key: True if configured as a key_serializer.
    :param schema reader_schema: Optional reader schema to be used during deserialization.
    """
    __slots__ = ["registry_client", "codec_cache", "is_key", "reader_schema"]

    def __init__(self, registry_client, is_key=False, reader_schema=None):
        self.registry_client = registry_client
        self.codec_cache = {}
        self.is_key = is_key
        self.reader_schema = reader_schema

    def __call__(self, topic, datum):
        """
        Decode a datum from kafka that has been encoded for use with the Confluent Schema Registry.
        :param str|bytes or None datum: message key or value to be decoded.
        :returns: Decoded message key or value contents.
        :rtype GenericAvroRecord:
        """

        if datum is None:
            return None

        if len(datum) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(datum) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte", self.is_key)

            decoder_func = self._get_decoder_func(schema_id, payload)
            return decoder_func(payload)

    def _get_decoder_func(self, schema_id, payload):
        if schema_id in self.codec_cache:
            return self.codec_cache[schema_id]

        try:
            writer_schema = self.registry_client.get_by_id(schema_id)
        except ClientError as e:
            raise SerializerError("unable to fetch schema with id %d: %s" % (schema_id, str(e)))

        if writer_schema is None:
            raise SerializerError("unable to fetch schema with id %d" % (schema_id))

        reader_schema = writer_schema if self.reader_schema is None else self.reader_schema

        curr_pos = payload.tell()

        if HAS_FAST:
            # try to use fast avro
            try:
                schemaless_reader(payload, writer_schema.to_json())

                # If we reach this point, this means we have fastavro and it can
                # do this deserialization. Rewind since this method just determines
                # the reader function and we need to deserialize again along the
                # normal path.
                payload.seek(curr_pos)

                def fast_decoder(p):
                    return schemaless_reader(p, writer_schema.to_json(), reader_schema.to_json())

                def fast_record_decoder(p):
                    return GenericAvroRecord(reader_schema,
                                             schemaless_reader(p, writer_schema.to_json(), reader_schema.to_json()))

                if writer_schema.type is 'record':
                    self.codec_cache[schema_id] = fast_record_decoder
                else:
                    self.codec_cache[schema_id] = fast_decoder

                return self.codec_cache[schema_id]
            except Exception:
                # Fast avro failed, fall thru to standard avro below.
                pass

        payload.seek(curr_pos)

        avro_reader = avro.io.DatumReader(writer_schema, reader_schema)

        def record_decoder(p):
            bin_decoder = avro.io.BinaryDecoder(p)
            return GenericAvroRecord(reader_schema, avro_reader.read(bin_decoder))

        def decoder(p):
            bin_decoder = avro.io.BinaryDecoder(p)
            return avro_reader.read(bin_decoder)

        if writer_schema.type is 'record':
            self.codec_cache[schema_id] = record_decoder
        else:
            self.codec_cache[schema_id] = decoder

        return self.codec_cache[schema_id]
