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


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#
import io
import logging
import struct
import sys
import traceback

import avro
import avro.io

from confluent_kafka.avro import ClientError
from confluent_kafka.avro.serializer import (SerializerError,
                                             KeySerializerError,
                                             ValueSerializerError)

log = logging.getLogger(__name__)

MAGIC_BYTE = 0

HAS_FAST = False
try:
    from fastavro import schemaless_reader

    HAS_FAST = True
except ImportError:
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


class MessageSerializer(object):
    """
    A helper class that can serialize and deserialize messages
    that need to be encoded or decoded using the schema registry.

    All encode_* methods return a buffer that can be sent to kafka.
    All decode_* methods expect a buffer received from kafka.
    """

    def __init__(self, registry_client):
        self.registry_client = registry_client
        self.id_to_decoder_func = {}
        self.id_to_writers = {}

    '''

    '''

    def encode_record_with_schema(self, topic, schema, record, is_key=False):
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.

        The schema is registered with the subject of 'topic-value'
        @:param topic : Topic name
        @:param schema : Avro Schema
        @:param record : An object to serialize
        @:param is_key : If the record is a key
        @:returns : Encoded record with schema ID as bytes
        """
        serialize_err = KeySerializerError if is_key else ValueSerializerError

        subject_suffix = ('-key' if is_key else '-value')
        # get the latest schema for the subject
        subject = topic + subject_suffix
        # register it
        schema_id = self.registry_client.register(subject, schema)
        if not schema_id:
            message = "Unable to retrieve schema id for subject %s" % (subject)
            raise serialize_err(message)

        # cache writer
        self.id_to_writers[schema_id] = avro.io.DatumWriter(schema)

        return self.encode_record_with_schema_id(schema_id, record, is_key=is_key)

    def encode_record_with_schema_id(self, schema_id, record, is_key=False):
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.
        @:param: schema_id : integer ID
        @:param: record : An object to serialize
        @:param is_key : If the record is a key
        @:returns: decoder function
        """
        serialize_err = KeySerializerError if is_key else ValueSerializerError

        # use slow avro
        if schema_id not in self.id_to_writers:
            # get the writer + schema

            try:
                schema = self.registry_client.get_by_id(schema_id)
                if not schema:
                    raise serialize_err("Schema does not exist")
                self.id_to_writers[schema_id] = avro.io.DatumWriter(schema)
            except ClientError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise serialize_err(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        # get the writer
        writer = self.id_to_writers[schema_id]
        with ContextStringIO() as outf:
            # write the header
            # magic byte

            outf.write(struct.pack('b', MAGIC_BYTE))

            # write the schema ID in network byte order (big end)

            outf.write(struct.pack('>I', schema_id))

            # write the record to the rest of it
            # Create an encoder that we'll write to
            encoder = avro.io.BinaryEncoder(outf)
            # write the magic byte
            # write the object in 'obj' as Avro to the fake file...
            writer.write(record, encoder)

            return outf.getvalue()

    # Decoder support
    def _get_decoder_func(self, schema_id, payload):
        if schema_id in self.id_to_decoder_func:
            return self.id_to_decoder_func[schema_id]

        # fetch from schema reg
        try:
            schema = self.registry_client.get_by_id(schema_id)
        except ClientError as e:
            raise SerializerError("unable to fetch schema with id %d: %s" % (schema_id, str(e)))

        if schema is None:
            raise SerializerError("unable to fetch schema with id %d" % (schema_id))

        curr_pos = payload.tell()
        if HAS_FAST:
            # try to use fast avro
            try:
                schema_dict = schema.to_json()
                schemaless_reader(payload, schema_dict)

                # If we reach this point, this means we have fastavro and it can
                # do this deserialization. Rewind since this method just determines
                # the reader function and we need to deserialize again along the
                # normal path.
                payload.seek(curr_pos)

                self.id_to_decoder_func[schema_id] = lambda p: schemaless_reader(p, schema_dict)
                return self.id_to_decoder_func[schema_id]
            except Exception:
                # Fast avro failed, fall thru to standard avro below.
                pass

        # here means we should just delegate to slow avro
        # rewind
        payload.seek(curr_pos)
        avro_reader = avro.io.DatumReader(schema)

        def decoder(p):
            bin_decoder = avro.io.BinaryDecoder(p)
            return avro_reader.read(bin_decoder)

        self.id_to_decoder_func[schema_id] = decoder
        return self.id_to_decoder_func[schema_id]

    def decode_message(self, message):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        @:param: message
        """

        if message is None:
            return None

        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(message) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")
            decoder_func = self._get_decoder_func(schema_id, payload)
            return decoder_func(payload)
