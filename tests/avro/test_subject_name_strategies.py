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

import unittest

from avro.schema import Names, RecordSchema, PrimitiveSchema

import confluent_kafka.avro.serializer.name_strategies as strategies
from confluent_kafka.avro import AvroProducer
from tests.avro.mock_schema_registry_client import MockSchemaRegistryClient


class TestMessageSerializer(unittest.TestCase):
    def test_topic_name(self):
        strategy = strategies.topic_name_strategy
        topic = "some_legal_topic_name"
        expected = topic + "-key"
        actual = strategy(topic, True, None)
        assert expected == actual
        expected = topic + "-value"
        actual = strategy(topic, False, None)
        assert expected == actual

    def test_record_name(self):
        strategy = strategies.record_name_strategy
        schema = RecordSchema("MyRecordType", "my.namespace", fields=[], names=Names())
        expected = "my.namespace.MyRecordType"
        actual = strategy(None, None, schema)
        assert expected == actual

    def test_record_name_primitive_schema(self):
        strategy = strategies.record_name_strategy
        schema = PrimitiveSchema(type='string')
        expected = 'string'
        actual = strategy(None, None, schema)
        assert expected == actual

    def test_record_name_none_schema(self):
        strategy = strategies.record_name_strategy
        schema = None
        expected = 'null'
        actual = strategy(None, None, schema)
        assert expected == actual

    def test_topic_record_name(self):
        strategy = strategies.topic_record_name_strategy
        topic = "some_legal_topic_name"
        schema = RecordSchema("MyRecordType", "my.namespace", fields=[], names=Names())
        expected = "some_legal_topic_name-my.namespace.MyRecordType"
        actual = strategy(topic, None, schema)
        assert expected == actual

    def test_topic_record_name_primitive_schema(self):
        strategy = strategies.topic_record_name_strategy
        topic = "some_legal_topic_name"
        schema = PrimitiveSchema(type='string')
        expected = 'some_legal_topic_name-string'
        actual = strategy(topic, None, schema)
        assert expected == actual

    def test_topic_record_name_none_schema(self):
        strategy = strategies.topic_record_name_strategy
        topic = "some_legal_topic_name"
        schema = None
        expected = 'some_legal_topic_name-null'
        actual = strategy(topic, None, schema)
        assert expected == actual

    def test_default_subject_name_strategy(self):
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer(config={}, schema_registry=schema_registry)
        serializer = producer._serializer
        assert serializer.key_subject_name_strategy is strategies.topic_name_strategy
        assert serializer.value_subject_name_strategy is strategies.topic_name_strategy

    def test_explicit_topic_subject_name_strategy(self):
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer(config={}, schema_registry=schema_registry,
                                key_subject_name_strategy=strategies.topic_name_strategy,
                                value_subject_name_strategy=strategies.topic_name_strategy)
        serializer = producer._serializer
        assert serializer.key_subject_name_strategy is strategies.topic_name_strategy
        assert serializer.value_subject_name_strategy is strategies.topic_name_strategy

    def test_explicit_record_subject_name_strategy(self):
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer(config={}, schema_registry=schema_registry,
                                key_subject_name_strategy=strategies.record_name_strategy,
                                value_subject_name_strategy=strategies.record_name_strategy)
        serializer = producer._serializer
        assert serializer.key_subject_name_strategy is strategies.record_name_strategy
        assert serializer.value_subject_name_strategy is strategies.record_name_strategy

    def test_explicit_topic_record_subject_name_strategy(self):
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer(config={}, schema_registry=schema_registry,
                                key_subject_name_strategy=strategies.topic_record_name_strategy,
                                value_subject_name_strategy=strategies.topic_record_name_strategy)
        serializer = producer._serializer
        assert serializer.key_subject_name_strategy is strategies.topic_record_name_strategy
        assert serializer.value_subject_name_strategy is strategies.topic_record_name_strategy

    def test_differing_key_and_value_subject_name_strategies(self):
        schema_registry = MockSchemaRegistryClient()
        producer = AvroProducer(config={}, schema_registry=schema_registry,
                                key_subject_name_strategy=strategies.record_name_strategy,
                                value_subject_name_strategy=strategies.topic_record_name_strategy)
        serializer = producer._serializer
        assert serializer.key_subject_name_strategy is strategies.record_name_strategy
        assert serializer.value_subject_name_strategy is strategies.topic_record_name_strategy
