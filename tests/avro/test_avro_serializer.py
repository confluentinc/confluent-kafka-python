#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
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

import struct

import unittest

from tests.avro import data_gen
from confluent_kafka.avro.serializer import AvroSerializer, AvroDeserializer
from confluent_kafka.avro.schema import GenericAvroRecord
from tests.avro.mock_schema_registry_client import MockSchemaRegistryClient
from confluent_kafka import avro


class TestAvroSerializer(unittest.TestCase):
    def setUp(self):
        # need to set up the serializer
        self.client = MockSchemaRegistryClient()
        self.serializer = AvroSerializer(self.client)
        self.deserializer = AvroDeserializer(self.client)

    def assertMessageIsSame(self, message, expected, schema_id):
        self.assertTrue(message)
        self.assertTrue(len(message) > 5)
        magic, sid = struct.unpack('>bI', message[0:5])
        self.assertEqual(magic, 0)
        self.assertEqual(sid, schema_id)
        decoded = self.deserializer(None, message)
        self.assertTrue(decoded)
        self.assertEqual(decoded, expected)

    def test_encode_with_schema_id(self):
        adv = avro.loads(data_gen.ADVANCED_SCHEMA)
        basic = avro.loads(data_gen.BASIC_SCHEMA)
        subject = 'test'
        schema_id = self.client.register(subject, basic)

        records = data_gen.BASIC_ITEMS
        for record in records:
            message = self.serializer._encode(schema_id, record)
            self.assertMessageIsSame(message, record, schema_id)

        subject = 'test_adv'
        adv_schema_id = self.client.register(subject, adv)
        self.assertNotEqual(adv_schema_id, schema_id)
        records = data_gen.ADVANCED_ITEMS
        for record in records:
            message = self.serializer._encode(adv_schema_id, record)
            self.assertMessageIsSame(message, record, adv_schema_id)

    def test_encode_record_with_schema(self):
        topic = 'test'
        basic = avro.loads(data_gen.BASIC_SCHEMA)
        subject = 'test-value'
        schema_id = self.client.register(subject, basic)
        records = data_gen.BASIC_ITEMS
        for record in records:
            message = self.serializer(topic, GenericAvroRecord(basic, record))
            self.assertMessageIsSame(message, record, schema_id)

    def test_decode_none(self):
        """"null/None messages should decode to None"""

        self.assertIsNone(self.deserializer(None, None))
