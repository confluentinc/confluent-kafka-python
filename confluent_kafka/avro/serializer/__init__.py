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
from confluent_kafka.avro import ClientError

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

    def __init__(self, message):
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


class AvroSerializer(object):

    __slots__ = ["registry_client", "codec_cache", "is_key"]

    def __init__(self, registry_client, is_key=False):
        self.registry_client = registry_client
        self.codec_cache = {}
        self.is_key = is_key

    def __call__(self, topic, record):
        """
        Given a parsed avro schema, encode a record for the given topic.

        The schema is registered with the subject of 'topic-value'
        :param str topic: Topic name
        :param GenericAvroRecord record: An object to serialize
        :returns: Encoded record with schema ID as bytes
        :rtype: bytes
        """

        if record is None:
            return None

        subject_suffix = '-key' if self.is_key else '-value'
        subject = topic + subject_suffix

        schema_id = self.registry_client.register(subject, get_schema(record))
        if not schema_id:
            message = "Failed to register schema with subject {}.".format(subject)
            raise SerializerError(message, is_key=self.is_key)

        if schema_id not in self.codec_cache:
            self.codec_cache[schema_id] = self._get_encoder_func(get_schema(record))

        return self._encode_record_with_schema_id(schema_id, record)

    def _get_encoder_func(self, writer_schema):
        if HAS_FAST:
            return lambda record, fp: schemaless_writer(fp, writer_schema.to_json(), record)
        writer = avro.io.DatumWriter(writer_schema)
        return lambda record, fp: writer.write(record, avro.io.BinaryEncoder(fp))

    def _encode_record_with_schema_id(self, schema_id, record):
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.
        :param int schema_id: integer ID
        :param dict record: An object to serialize
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
            writer(record, outf)
            return outf.getvalue()


class AvroDeserializer(object):

    __slots__ = ["registry_client", "codec_cache", "is_key", "reader_schema"]

    def __init__(self, registry_client, is_key=False, reader_schema=None):
        self.registry_client = registry_client
        self.codec_cache = {}
        self.is_key = is_key
        self.reader_schema = reader_schema

    def __call__(self, topic, message):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        :param str|bytes or None message: message key or value to be decoded
        :returns: Decoded message contents.
        :rtype GenericAvroRecord:
        """

        if message is None:
            return None

        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(message) as payload:
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

        if writer_schema.get_prop('type') is 'record':
            self.codec_cache[schema_id] = record_decoder
        else:
            self.codec_cache[schema_id] = decoder

        return self.codec_cache[schema_id]
