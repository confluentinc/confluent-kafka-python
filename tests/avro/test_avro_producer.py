#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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

from requests.exceptions import ConnectionError
import pytest

from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import (KeySerializerError,
                                             ValueSerializerError)


avsc_dir = os.path.dirname(os.path.realpath(__file__))


def test_instantiation():
    obj = AvroProducer({'schema.registry.url': 'http://127.0.0.1:0'})
    assert isinstance(obj, AvroProducer)
    assert obj is not None


def test_produce_no_key(schema_fixture):
    value_schema = schema_fixture("basic_schema")
    producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, default_value_schema=value_schema)
    with pytest.raises(ConnectionError):
        producer.produce(topic='test', value={"name": 'abc"'})


def test_produce_no_value(schema_fixture):
    key_schema = schema_fixture("basic_schema")
    producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, default_key_schema=key_schema)
    with pytest.raises(ConnectionError):
        producer.produce(topic='test', key={"name": 'abc"'})


def test_produce_no_value_schema():
    producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
    with pytest.raises(ValueSerializerError):
        # Producer should not accept a value with no schema
        producer.produce(topic='test', value={"name": 'abc"'})


def test_produce_no_key_schema():
    producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
    with pytest.raises(KeySerializerError):
        # If the key is provided as a dict an avro schema must also be provided
        producer.produce(topic='test', key={"name": 'abc"'})


def test_produce_value_and_key_schemas(schema_fixture):
    value_schema = schema_fixture("basic_schema")
    producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, default_value_schema=value_schema,
                            default_key_schema=value_schema)
    with pytest.raises(ConnectionError):
        producer.produce(topic='test', value={"name": 'abc"'}, key={"name": 'abc"'})


def test_produce_primitive_string_key(schema_fixture):
    value_schema = schema_fixture("basic_schema")
    key_schema = schema_fixture("primitive_string")
    producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
    with pytest.raises(ConnectionError):
        producer.produce(topic='test', value={"name": 'abc"'}, value_schema=value_schema, key='mykey',
                         key_schema=key_schema)


def test_produce_primitive_key_and_value(schema_fixture):
    value_schema = schema_fixture("primitive_float")
    key_schema = schema_fixture("primitive_string")
    producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'})
    with pytest.raises(ConnectionError):
        producer.produce(topic='test', value=32., value_schema=value_schema, key='mykey', key_schema=key_schema)


def test_produce_with_custom_registry(mock_schema_registry_client_fixture, schema_fixture):
    schema_registry = mock_schema_registry_client_fixture
    value_schema = schema_fixture("basic_schema")
    key_schema = schema_fixture("primitive_string")
    producer = AvroProducer({}, schema_registry=schema_registry)
    producer.produce(topic='test', value={"name": 'abc"'}, value_schema=value_schema, key='mykey',
                     key_schema=key_schema)


def test_produce_with_custom_registry_and_registry_url(mock_schema_registry_client_fixture):
    schema_registry = mock_schema_registry_client_fixture
    with pytest.raises(ValueError):
        AvroProducer({'schema.registry.url': 'http://127.0.0.1:9001'}, schema_registry=schema_registry)


def test_produce_with_empty_value_no_schema(mock_schema_registry_client_fixture):
    schema_registry = mock_schema_registry_client_fixture
    producer = AvroProducer({}, schema_registry=schema_registry)
    with pytest.raises(ValueSerializerError):
        producer.produce(topic='test', value='', key='not empty')


def test_produce_with_empty_key_no_schema(mock_schema_registry_client_fixture, schema_fixture):
    value_schema = schema_fixture("primitive_float")
    schema_registry = mock_schema_registry_client_fixture
    producer = AvroProducer({}, schema_registry=schema_registry,
                            default_value_schema=value_schema)
    with pytest.raises(KeySerializerError):
        producer.produce(topic='test', value=0.0, key='')


def test_produce_with_empty_key_value_with_schema(mock_schema_registry_client_fixture, schema_fixture):
    key_schema = schema_fixture("primitive_string")
    value_schema = schema_fixture("primitive_float")
    schema_registry = mock_schema_registry_client_fixture
    producer = AvroProducer({}, schema_registry=schema_registry,
                            default_key_schema=key_schema,
                            default_value_schema=value_schema)
    producer.produce(topic='test', value=0.0, key='')
