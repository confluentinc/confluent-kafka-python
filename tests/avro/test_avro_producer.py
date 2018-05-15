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
import os

from confluent_kafka import avro

from requests.exceptions import ConnectionError

import unittest
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import (KeySerializerError,
                                             ValueSerializerError)

from tests.avro.mock_schema_registry_client import MockSchemaRegistryClient


avsc_dir = os.path.dirname(os.path.realpath(__file__))


class TestAvroProducer(unittest.TestCase):

    def test_instantiation(self):
        obj = AvroProducer({'schema.registry.url': 'http://127.0.0.1:0'})
        self.assertTrue(isinstance(obj, AvroProducer))
        self.assertNotEqual(obj, None)

    def test_produce_no_key(self):
        value_schema = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, default_value_schema=value_schema)
        with self.assertRaises(ConnectionError):  # Unexistent schema-registry
            producer.produce(topic='test', value={"name": 'abc"'})

    def test_produce_no_value(self):
        key_schema = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, default_key_schema=key_schema)
        with self.assertRaises(ConnectionError):  # Unexistent schema-registry
            producer.produce(topic='test', key={"name": 'abc"'})

    def test_produce_no_value_schema(self):
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
        with self.assertRaises(ValueSerializerError):
            # Producer should not accept a value with no schema
            producer.produce(topic='test', value={"name": 'abc"'})

    def test_produce_no_key_schema(self):
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
        with self.assertRaises(KeySerializerError):
            # If the key is provided as a dict an avro schema must also be provided
            producer.produce(topic='test', key={"name": 'abc"'})

    def test_produce_value_and_key_schemas(self):
        value_schema = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, default_value_schema=value_schema,
                                default_key_schema=value_schema)
        with self.assertRaises(ConnectionError):  # Unexistent schema-registry
            producer.produce(topic='test', value={"name": 'abc"'}, key={"name": 'abc"'})

    def test_produce_primitive_string_key(self):
        value_schema = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
        key_schema = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
        with self.assertRaises(ConnectionError):  # Unexistent schema-registry
            producer.produce(topic='test', value={"name": 'abc"'}, value_schema=value_schema, key='mykey',
                             key_schema=key_schema)

    def test_produce_primitive_key_and_value(self):
        value_schema = avro.load(os.path.join(avsc_dir, "primitive_float.avsc"))
        key_schema = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
        with self.assertRaises(ConnectionError):  # Unexistent schema-registry
            producer.produce(topic='test', value=32., value_schema=value_schema, key='mykey', key_schema=key_schema)

    def test_produce_with_custom_registry(self):
        schema_registry = MockSchemaRegistryClient()
        value_schema = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
        key_schema = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))
        producer = AvroProducer({}, schema_registry=schema_registry)
        producer.produce(topic='test', value={"name": 'abc"'}, value_schema=value_schema, key='mykey',
                         key_schema=key_schema)

    def test_produce_with_custom_registry_and_registry_url(self):
        schema_registry = MockSchemaRegistryClient()
        with self.assertRaises(ValueError):
            AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, schema_registry=schema_registry)

    def test_produce_with_empty_value_no_schema(self):
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer({}, schema_registry=schema_registry)
        with self.assertRaises(ValueSerializerError):
            producer.produce(topic='test', value='', key='not empty')

    def test_produce_with_empty_key_no_schema(self):
        value_schema = avro.load(os.path.join(avsc_dir, "primitive_float.avsc"))
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer({}, schema_registry=schema_registry,
                                default_value_schema=value_schema)
        with self.assertRaises(KeySerializerError):
            producer.produce(topic='test', value=0.0, key='')

    def test_produce_with_empty_key_value_with_schema(self):
        key_schema = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))
        value_schema = avro.load(os.path.join(avsc_dir, "primitive_float.avsc"))
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer({}, schema_registry=schema_registry,
                                default_key_schema=key_schema,
                                default_value_schema=value_schema)
        producer.produce(topic='test', value=0.0, key='')
