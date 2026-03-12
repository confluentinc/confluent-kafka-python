#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2023 Confluent Inc.
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

import json
from unittest.mock import Mock

import orjson
import pytest

from confluent_kafka.schema_registry import (
    RegisteredSchema,
    Schema,
    SchemaReference,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.serialization import SerializationContext


def test_json_deserializer_referenced_schema_no_schema_registry_client(load_avsc):
    """
    Ensures that the deserializer raises a ValueError if a referenced schema is provided but no schema registry
    client is provided.
    """
    schema = Schema(
        load_avsc("order_details.json"),
        'JSON',
        [SchemaReference("http://example.com/customer.schema.json", "customer", 1)],
    )
    with pytest.raises(
        ValueError,
        match="""schema_registry_client must be provided if "schema_str" is a Schema instance with references""",
    ):
        JSONDeserializer(schema, schema_registry_client=None)


def test_json_deserializer_invalid_schema_type():
    """
    Ensures that the deserializer raises a ValueError if an invalid schema type is provided.
    """
    with pytest.raises(TypeError, match="You must pass either str or Schema"):
        JSONDeserializer(1)


def test_custom_json_encoder():
    """Test custom JSON encoder using orjson for better performance"""
    schema_str = """
    {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        }
    }"""

    test_data = {"name": "John", "age": 30}
    ctx = SerializationContext("topic-name", "value")

    # Create mock SchemaRegistryClient
    mock_schema_registry_client = Mock(spec=SchemaRegistryClient)
    mock_schema_registry_client.register_schema_full_response.return_value = RegisteredSchema(
        schema_id=1, guid=None, schema=Schema(schema_str), subject="topic-name-value", version=1
    )
    mock_schema_registry_client.get_associations_by_resource_name.return_value = []

    # Use orjson.dumps as the custom encoder
    serializer = JSONSerializer(
        schema_str, mock_schema_registry_client, json_encode=orjson.dumps, rule_registry=RuleRegistry()
    )

    result = serializer(test_data, ctx)

    # Since result includes schema registry framing (5 bytes prefix),
    # we need to decode from bytes starting after the prefix
    decoded = orjson.loads(result[5:])
    assert decoded["name"] == "John"
    assert decoded["age"] == 30


def test_custom_json_decoder():
    """Test custom JSON decoder using orjson for better performance"""
    schema_str = """
    {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        }
    }"""

    test_data = b'\x00\x00\x00\x00\x01{"name": "John", "age": 30}'

    # Use orjson for decoding with custom transformation
    def custom_decoder(data):
        decoded = orjson.loads(data)
        return {k.upper(): v for k, v in decoded.items()}

    deserializer = JSONDeserializer(schema_str, json_decode=custom_decoder, rule_registry=RuleRegistry())
    ctx = SerializationContext("topic-name", "value")
    result = deserializer(test_data, ctx)

    # Verify custom decoder transformed keys to uppercase
    assert result["NAME"] == "John"
    assert result["AGE"] == 30


def test_custom_encoder_decoder_chain():
    """Test serialization/deserialization chain with custom encoding"""
    schema_str = """
    {
        "type": "object",
        "properties": {
            "data": {"type": "string"}
        }
    }"""

    test_data = {"data": "test value"}
    ctx = SerializationContext("topic-name", "value")

    mock_schema_registry_client = Mock(spec=SchemaRegistryClient)
    mock_schema_registry_client.register_schema_full_response.return_value = RegisteredSchema(
        schema_id=1, guid=None, schema=Schema(schema_str), subject="topic-name-value", version=1
    )
    mock_schema_registry_client.get_associations_by_resource_name.return_value = []

    def custom_encoder(obj):
        return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)

    def custom_decoder(data):
        return orjson.loads(data)

    serializer = JSONSerializer(
        schema_str, mock_schema_registry_client, json_encode=custom_encoder, rule_registry=RuleRegistry()
    )
    deserializer = JSONDeserializer(schema_str, json_decode=custom_decoder, rule_registry=RuleRegistry())

    # Serialize then deserialize
    encoded = serializer(test_data, ctx)
    decoded = deserializer(encoded, ctx)

    assert decoded == test_data


def test_custom_encoding_with_complex_data():
    """Test custom encoding with nested structures"""
    schema_str = """
    {
        "type": "object",
        "properties": {
            "nested": {
                "type": "object",
                "properties": {
                    "array": {"type": "array", "items": {"type": "integer"}},
                    "string": {"type": "string"}
                }
            }
        }
    }"""

    test_data = {"nested": {"array": [1, 2, 3], "string": "test"}}
    mock_schema_registry_client = Mock(spec=SchemaRegistryClient)
    mock_schema_registry_client.register_schema_full_response.return_value = RegisteredSchema(
        schema_id=1, guid=None, schema=Schema(schema_str), subject="topic-name-value", version=1
    )
    mock_schema_registry_client.get_associations_by_resource_name.return_value = []

    def custom_encoder(obj):
        return json.dumps(obj, indent=2)

    def custom_decoder(data):
        return json.loads(data)

    serializer = JSONSerializer(
        schema_str, mock_schema_registry_client, json_encode=custom_encoder, rule_registry=RuleRegistry()
    )
    deserializer = JSONDeserializer(schema_str, json_decode=custom_decoder, rule_registry=RuleRegistry())
    ctx = SerializationContext("topic-name", "value")
    encoded = serializer(test_data, ctx)
    decoded = deserializer(encoded, ctx)

    assert decoded == test_data
