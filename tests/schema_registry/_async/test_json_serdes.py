#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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
import base64
import json

import pytest

from confluent_kafka.schema_registry import (
    AsyncSchemaRegistryClient,
    Schema,
    header_schema_id_serializer,
)
from confluent_kafka.schema_registry._async.serde import (
    FALLBACK_TYPE,
    KAFKA_CLUSTER_ID,
)
from confluent_kafka.schema_registry.common.schema_registry_client import (
    AssociationCreateOrUpdateInfo,
    AssociationCreateOrUpdateRequest,
)
from confluent_kafka.schema_registry.common.serde import SubjectNameStrategyType
from confluent_kafka.schema_registry.json_schema import AsyncJSONDeserializer, AsyncJSONSerializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaReference
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

_BASE_URL = "mock://"
# _BASE_URL = "http://localhost:8081"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


@pytest.fixture(autouse=True)
async def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    yield  # this is where the testing happens

    # Teardown : fill with any logic you want
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    try:
        await client.delete_subject(_SUBJECT, True)
    except Exception:
        pass
    subjects = await client.get_subjects()
    for subject in subjects:
        try:
            await client.delete_subject(subject, True)
        except Exception:
            pass


async def test_json_basic_serialization():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"type": "string", "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_basic_failing_validation():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate': True}
    obj = {
        'intField': '123',
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"type": "string", "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError):
        await ser(obj, ser_ctx)


async def test_json_guid_in_header():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'schema.id.serializer': header_schema_id_serializer}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"type": "string", "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE, {})
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_basic_deserialization_no_client():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"type": "string", "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(json.dumps(schema))
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_serialize_nested():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {'nested': nested}
    schema = {
        "type": "object",
        "properties": {
            "otherField": {
                "type": "object",
                "properties": {
                    "intField": {"type": "integer"},
                    "doubleField": {"type": "number"},
                    "stringField": {"type": "string"},
                    "booleanField": {"type": "boolean"},
                    "bytesField": {"type": "string"},
                },
            }
        },
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_serialize_references():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    referenced = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {'otherField': referenced}
    ref_schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"type": "string", "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }
    await client.register_schema('ref', Schema(json.dumps(ref_schema), "JSON"))
    schema = {"type": "object", "properties": {"otherField": {"$ref": "ref"}}}
    refs = [SchemaReference('ref', 'ref', 1)]
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'JSON', refs))

    ser = await AsyncJSONSerializer(None, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


_JSON_SCHEMA = json.dumps(
    {
        "type": "object",
        "title": "MyRecord",
        "properties": {
            "name": {"type": "string"},
            "id": {"type": "integer"},
        },
    }
)
_JSON_OBJ = {"name": "Kafka", "id": 123}


async def test_json_associated_name_strategy_with_association():
    """Test that AssociatedNameStrategy returns subject from association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="json-resource-id-1",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="my-custom-subject-value",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_JSON_SCHEMA,
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(_JSON_OBJ, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert _JSON_OBJ == obj2

    registered_schema = await client.get_latest_version("my-custom-subject-value")
    assert registered_schema is not None

    await client.delete_associations(resource_id="json-resource-id-1", cascade_lifecycle=True)


async def test_json_associated_name_strategy_with_key_association():
    """Test that AssociatedNameStrategy returns subject for key"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="json-resource-id-2",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="my-key-subject",
                association_type="key",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_JSON_SCHEMA,
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.KEY)
    obj_bytes = await ser(_JSON_OBJ, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert _JSON_OBJ == obj2

    registered_schema = await client.get_latest_version("my-key-subject")
    assert registered_schema is not None

    await client.delete_associations(resource_id="json-resource-id-2", cascade_lifecycle=True)


async def test_json_associated_name_strategy_fallback_to_topic():
    """Test fallback to topic_subject_name_strategy when no association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(_JSON_OBJ, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert _JSON_OBJ == obj2

    registered_schema = await client.get_latest_version(_TOPIC + "-value")
    assert registered_schema is not None


async def test_json_associated_name_strategy_fallback_to_record():
    """Test fallback to record_subject_name_strategy when configured"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: SubjectNameStrategyType.RECORD},
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(_JSON_OBJ, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert _JSON_OBJ == obj2

    # JSON record name comes from schema "title"
    registered_schema = await client.get_latest_version("MyRecord")
    assert registered_schema is not None


async def test_json_associated_name_strategy_fallback_to_topic_record():
    """Test fallback to topic_record_subject_name_strategy when configured"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: SubjectNameStrategyType.TOPIC_RECORD},
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(_JSON_OBJ, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert _JSON_OBJ == obj2

    # JSON topic-record subject: "topic1-MyRecord"
    registered_schema = await client.get_latest_version(_TOPIC + "-MyRecord")
    assert registered_schema is not None


async def test_json_associated_name_strategy_fallback_none_raises():
    """Test that NONE fallback raises an error when no association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: "NONE"},
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    with pytest.raises(SerializationError) as exc_info:
        await ser(_JSON_OBJ, ser_ctx)

    assert "No associated subject found" in str(exc_info.value)


async def test_json_associated_name_strategy_with_kafka_cluster_id():
    """Test that subject.name.strategy.kafka.cluster.id config is used as resource namespace"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="my-cluster-id",
        resource_id="json-resource-id-4",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="cluster-specific-json-subject",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_JSON_SCHEMA,
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {KAFKA_CLUSTER_ID: "my-cluster-id"},
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(_JSON_OBJ, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert _JSON_OBJ == obj2

    registered_schema = await client.get_latest_version("cluster-specific-json-subject")
    assert registered_schema is not None

    await client.delete_associations(resource_id="json-resource-id-4", cascade_lifecycle=True)


async def test_json_associated_name_strategy_caching():
    """Test that results are cached within a strategy instance and serializer works with caching"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="json-resource-id-5",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="json-cached-subject",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_JSON_SCHEMA,
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    obj1 = {"name": "Kafka", "id": 1}
    obj_bytes1 = await ser(obj1, ser_ctx)

    registered_schema = await client.get_latest_version("json-cached-subject")
    assert registered_schema is not None

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    result1 = await deser(obj_bytes1, ser_ctx)
    assert obj1 == result1

    # Delete associations (but serializer should still work due to caching)
    await client.delete_associations(resource_id="json-resource-id-5", cascade_lifecycle=True)

    obj2 = {"name": "Kafka", "id": 2}
    obj_bytes2 = await ser(obj2, ser_ctx)

    result2 = await deser(obj_bytes2, ser_ctx)
    assert obj2 == result2
