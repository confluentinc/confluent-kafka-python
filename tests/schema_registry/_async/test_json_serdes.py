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
    Metadata,
    MetadataProperties,
    Schema,
    header_schema_id_serializer,
)
from confluent_kafka.schema_registry.json_schema import AsyncJSONDeserializer, AsyncJSONSerializer
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import CelFieldExecutor
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import (
    EncryptionExecutor,
    FieldEncryptionExecutor,
)
from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import GcpKmsDriver
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import HcVaultKmsDriver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import LocalKmsDriver
from confluent_kafka.schema_registry.rules.jsonata.jsonata_executor import JsonataExecutor
from confluent_kafka.schema_registry.schema_registry_client import (
    Rule,
    RuleKind,
    RuleMode,
    RuleParams,
    RuleSet,
    SchemaReference,
    ServerConfig,
)
from confluent_kafka.schema_registry._async.serde import (
    FALLBACK_SUBJECT_NAME_STRATEGY_TYPE,
    KAFKA_CLUSTER_ID,
)
from confluent_kafka.schema_registry.common.schema_registry_client import (
    AssociationCreateOrUpdateInfo,
    AssociationCreateOrUpdateRequest,
)
from confluent_kafka.schema_registry.common.serde import SubjectNameStrategyType
from confluent_kafka.schema_registry.serde import RuleConditionError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from tests.schema_registry._async.test_avro_serdes import FakeClock

_BASE_URL = "mock://"
# _BASE_URL = "http://localhost:8081"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


@pytest.fixture(autouse=True)
async def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Setup: fill with any logic you want
    CelExecutor.register()
    CelFieldExecutor.register()
    AwsKmsDriver.register()
    AzureKmsDriver.register()
    GcpKmsDriver.register()
    HcVaultKmsDriver.register()
    JsonataExecutor.register()
    LocalKmsDriver.register()

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


async def test_json_cel_condition():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
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

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField == 'hi'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_cel_condition_fail():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
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

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField != 'hi'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError) as e:
        await ser(obj, ser_ctx)

    assert isinstance(e.value.__cause__, RuleConditionError)


async def test_json_cel_condition_ignore_fail():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
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

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField != 'hi'",
        None,
        "NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_cel_field_transform():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
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

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value + '-suffix'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    obj2 = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi-suffix',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


async def test_json_cel_field_transform_with_nullable():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"type": ["string", "null"], "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value + '-suffix'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    obj2 = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi-suffix',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


async def test_json_cel_field_transform_with_def():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "additionalProperties": False,
        "definitions": {
            "Address": {
                "additionalProperties": False,
                "properties": {
                    "doornumber": {"type": "integer"},
                    "doorpin": {"confluent:tags": ["PII"], "type": "string"},
                },
                "type": "object",
            }
        },
        "properties": {
            "address": {"$ref": "#/definitions/Address"},
            "name": {"confluent:tags": ["PII"], "type": "string"},
        },
        "title": "Sample Event",
        "type": "object",
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        ["PII"],
        None,
        "value + '-suffix'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {'name': 'bob', 'address': {'doornumber': 123, 'doorpin': '1234'}}
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    obj2 = {'name': 'bob-suffix', 'address': {'doornumber': 123, 'doorpin': '1234-suffix'}}
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


async def test_json_cel_field_transform_complex():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "arrayField": {"type": "array", "items": {"type": "string"}},
            "objectField": {"type": "object", "properties": {"stringField": {"type": "string"}}},
            "unionField": {"oneOf": [{"type": "null"}, {"type": "string"}], "confluent:tags": ["PII"]},
        },
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "typeName == 'STRING' ; value + '-suffix'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {'arrayField': ['a', 'b', 'c'], 'objectField': {'stringField': 'hello'}, 'unionField': 'world'}
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    obj2 = {
        'arrayField': ['a-suffix', 'b-suffix', 'c-suffix'],
        'objectField': {'stringField': 'hello-suffix'},
        'unionField': 'world-suffix',
    }
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


async def test_json_cel_field_transform_complex_with_none():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "arrayField": {"type": "array", "items": {"type": "string"}},
            "objectField": {"type": "object", "properties": {"stringField": {"type": "string"}}},
            "unionField": {"oneOf": [{"type": "null"}, {"type": "string"}], "confluent:tags": ["PII"]},
        },
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "typeName == 'STRING' ; value + '-suffix'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {'arrayField': ['a', 'b', 'c'], 'objectField': {'stringField': 'hello'}, 'unionField': None}
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    obj2 = {
        'arrayField': ['a-suffix', 'b-suffix', 'c-suffix'],
        'objectField': {'stringField': 'hello-suffix'},
        'unionField': None,
    }
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


async def test_json_cel_field_condition():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
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

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value == 'hi'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_cel_field_condition_fail():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
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

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value != 'hi'",
        None,
        None,
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError) as e:
        await ser(obj, ser_ctx)
    assert isinstance(e.value.__cause__, RuleConditionError)


async def test_json_encryption():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
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

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'
    obj['bytesField'] = base64.b64encode(b'foobar').decode('utf-8')

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_payloadencryption():
    executor = EncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
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

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT_PAYLOAD",
        None,
        RuleParams({"encrypt.kek.name": "kek1", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_encryption_with_union():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"oneOf": [{"type": "null"}, {"type": "string"}], "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1-union", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'
    obj['bytesField'] = base64.b64encode(b'foobar').decode('utf-8')

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_encryption_with_union_of_refs():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "type": "object",
        "properties": {
            "messageType": {"type": "string"},
            "version": {"type": "string"},
            "payload": {
                "type": "object",
                "oneOf": [{"$ref": "#/$defs/authentication_request"}, {"$ref": "#/$defs/authentication_status"}],
            },
        },
        "required": ["payload", "messageType", "version"],
        "$defs": {
            "authentication_request": {
                "properties": {
                    "messageId": {"type": "string", "confluent:tags": ["PII"]},
                    "timestamp": {"type": "integer", "minimum": 0},
                    "requestId": {"type": "string"},
                },
                "required": ["messageId", "timestamp"],
            },
            "authentication_status": {
                "properties": {
                    "messageId": {"type": "string", "confluent:tags": ["PII"]},
                    "authType": {"type": ["string", "null"]},
                },
                "required": ["messageId", "authType"],
            },
        },
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        "messageType": "authentication_request",
        "version": "1.0",
        "payload": {"messageId": "12345", "timestamp": 1757410647},
    }

    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['payload']['messageId'] != '12345'
    obj['payload']['messageId'] = '12345'

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_encryption_with_references():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"oneOf": [{"type": "null"}, {"type": "string"}], "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }
    await client.register_schema('ref', Schema(json.dumps(schema), "JSON"))

    schema = {"type": "object", "properties": {"otherField": {"$ref": "ref"}}}
    refs = [SchemaReference('ref', 'ref', 1)]
    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1-ref", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", refs, None, RuleSet(None, [rule])))

    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {'otherField': nested}
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['otherField']['stringField'] != 'hi'
    obj['otherField']['stringField'] = 'hi'
    obj['otherField']['bytesField'] = base64.b64encode(b'foobar').decode('utf-8')

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_jsonata_fully_compatible():
    rule1_to_2 = "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"
    rule2_to_1 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])"
    rule2_to_3 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])"
    rule3_to_2 = "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])"

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    await client.set_config(_SUBJECT, ServerConfig(compatibility_group='application.version'))

    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "confluent:tags": ["PII"]},
            "size": {"type": "number"},
            "version": {"type": "integer"},
        },
    }
    await client.register_schema(
        _SUBJECT,
        Schema(
            json.dumps(schema),
            "JSON",
            [],
            Metadata(None, MetadataProperties({"application.version": "v1"}), None),
            None,
        ),
    )

    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "confluent:tags": ["PII"]},
            "height": {"type": "number"},
            "version": {"type": "integer"},
        },
    }

    rule1 = Rule(
        "rule1", "", RuleKind.TRANSFORM, RuleMode.UPGRADE, "JSONATA", None, None, rule1_to_2, None, None, False
    )
    rule2 = Rule(
        "rule2", "", RuleKind.TRANSFORM, RuleMode.DOWNGRADE, "JSONATA", None, None, rule2_to_1, None, None, False
    )
    await client.register_schema(
        _SUBJECT,
        Schema(
            json.dumps(schema),
            "JSON",
            [],
            Metadata(None, MetadataProperties({"application.version": "v2"}), None),
            RuleSet([rule1, rule2], None),
        ),
    )

    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "confluent:tags": ["PII"]},
            "length": {"type": "number"},
            "version": {"type": "integer"},
        },
    }

    rule3 = Rule(
        "rule3", "", RuleKind.TRANSFORM, RuleMode.UPGRADE, "JSONATA", None, None, rule2_to_3, None, None, False
    )
    rule4 = Rule(
        "rule4", "", RuleKind.TRANSFORM, RuleMode.DOWNGRADE, "JSONATA", None, None, rule3_to_2, None, None, False
    )
    await client.register_schema(
        _SUBJECT,
        Schema(
            json.dumps(schema),
            "JSON",
            [],
            Metadata(None, MetadataProperties({"application.version": "v3"}), None),
            RuleSet([rule3, rule4], None),
        ),
    )

    obj = {
        'name': 'alice',
        'size': 123,
        'version': 1,
    }
    obj2 = {
        'name': 'alice',
        'height': 123,
        'version': 1,
    }
    obj3 = {
        'name': 'alice',
        'length': 123,
        'version': 1,
    }

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {'application.version': 'v1'},
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    await deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {'application.version': 'v2'},
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj2, ser_ctx)

    await deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {'application.version': 'v3'},
    }
    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj3, ser_ctx)

    await deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)


async def deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3):
    deser_conf = {'use.latest.with.metadata': {'application.version': 'v1'}}
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, conf=deser_conf)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj == newobj

    deser_conf = {'use.latest.with.metadata': {'application.version': 'v2'}}
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, conf=deser_conf)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj2 == newobj

    deser_conf = {'use.latest.with.metadata': {'application.version': 'v3'}}
    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, conf=deser_conf)
    newobj = await deser(obj_bytes, ser_ctx)
    assert obj3 == newobj


async def test_json_oneof_with_refs_nested_refs():
    """
    Test oneOf with $ref that contains nested $refs.

    This test validates the fix for the bug where oneOf subschemas containing
    $refs that themselves contain nested $refs would fail with:
    PointerToNowhere: '/definitions/...' does not exist

    The fix ensures the resolver context is maintained when validating
    resolved subschemas.
    """
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}

    # Schema with oneOf containing $ref, where the resolved schema has nested $refs
    schema = {
        "type": "object",
        "properties": {
            "transactionType": {"type": "string"},
            "data": {"oneOf": [{"$ref": "#/$defs/Account"}, {"$ref": "#/$defs/Payment"}]},
        },
        "required": ["transactionType", "data"],
        "$defs": {
            "Account": {
                "type": "object",
                "properties": {
                    "accountId": {"type": "string", "confluent:tags": ["PII"]},
                    "partyType": {"type": "string"},
                    "paymentMethod": {"$ref": "#/$defs/PaymentMethod"},  # Nested $ref!
                    "party": {"$ref": "#/$defs/PartyInfo"},  # Another nested $ref!
                },
                "required": ["accountId"],
            },
            "Payment": {
                "type": "object",
                "properties": {
                    "paymentId": {"type": "string", "confluent:tags": ["PII"]},
                    "amount": {"type": "number"},
                },
                "required": ["paymentId", "amount"],
            },
            "PaymentMethod": {
                "type": "object",
                "properties": {"type": {"type": "string"}, "details": {"type": "string", "confluent:tags": ["PII"]}},
            },
            "PartyInfo": {"type": "object", "properties": {"name": {"type": "string"}, "address": {"type": "string"}}},
        },
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    # Test with Account type (has nested $refs)
    obj = {
        "transactionType": "account_update",
        "data": {
            "accountId": "ACC123",
            "partyType": "Payer",
            "paymentMethod": {"type": "card", "details": "1234-5678-9012-3456"},
            "party": {"name": "John Doe", "address": "123 Main St"},
        },
    }

    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # Verify encryption happened
    assert obj['data']['accountId'] != 'ACC123'
    assert obj['data']['paymentMethod']['details'] != '1234-5678-9012-3456'

    # Reset for comparison
    obj['data']['accountId'] = 'ACC123'
    obj['data']['paymentMethod']['details'] = '1234-5678-9012-3456'

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_anyof_with_refs():
    """
    Test anyOf with $refs to ensure all union types are handled correctly.
    """
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}

    schema = {
        "type": "object",
        "properties": {"value": {"anyOf": [{"$ref": "#/$defs/StringValue"}, {"$ref": "#/$defs/NumberValue"}]}},
        "$defs": {
            "StringValue": {
                "type": "object",
                "properties": {"strValue": {"type": "string", "confluent:tags": ["PII"]}},
            },
            "NumberValue": {"type": "object", "properties": {"numValue": {"type": "number"}}},
        },
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {"value": {"strValue": "sensitive-data"}}

    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    assert obj['value']['strValue'] != 'sensitive-data'
    obj['value']['strValue'] = 'sensitive-data'

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_allof_with_refs():
    """
    Test allOf with $refs to ensure composition works correctly.
    """
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}

    schema = {
        "type": "object",
        "properties": {"entity": {"allOf": [{"$ref": "#/$defs/BaseEntity"}, {"$ref": "#/$defs/Timestamped"}]}},
        "$defs": {
            "BaseEntity": {"type": "object", "properties": {"id": {"type": "string", "confluent:tags": ["PII"]}}},
            "Timestamped": {"type": "object", "properties": {"createdAt": {"type": "integer"}}},
        },
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {"entity": {"id": "entity-123", "createdAt": 1234567890}}

    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    assert obj['entity']['id'] != 'entity-123'
    obj['entity']['id'] = 'entity-123'

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_json_deeply_nested_refs():
    """
    Test deeply nested $refs (3 levels) with oneOf to ensure resolver context
    is maintained through multiple levels of resolution.
    """
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}

    # 3-level nesting: oneOf -> Level1 -> Level2 -> Level3
    schema = {
        "type": "object",
        "properties": {"data": {"oneOf": [{"$ref": "#/$defs/Level1"}]}},
        "$defs": {
            "Level1": {"type": "object", "properties": {"level2": {"$ref": "#/$defs/Level2"}}},
            "Level2": {"type": "object", "properties": {"level3": {"$ref": "#/$defs/Level3"}}},
            "Level3": {"type": "object", "properties": {"secretData": {"type": "string", "confluent:tags": ["PII"]}}},
        },
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({"encrypt.kek.name": "kek1", "encrypt.kms.type": "local-kms", "encrypt.kms.key.id": "mykey"}),
        None,
        None,
        "ERROR,NONE",
        False,
    )
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {"data": {"level2": {"level3": {"secretData": "deep-secret"}}}}

    ser = await AsyncJSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    assert obj['data']['level2']['level3']['secretData'] != 'deep-secret'
    obj['data']['level2']['level3']['secretData'] = 'deep-secret'

    deser = await AsyncJSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


_JSON_SCHEMA = json.dumps({
    "type": "object",
    "title": "MyRecord",
    "properties": {
        "name": {"type": "string"},
        "id": {"type": "integer"},
    },
})
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
        'subject.name.strategy.conf': {FALLBACK_SUBJECT_NAME_STRATEGY_TYPE: SubjectNameStrategyType.RECORD},
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
        'subject.name.strategy.conf': {FALLBACK_SUBJECT_NAME_STRATEGY_TYPE: SubjectNameStrategyType.TOPIC_RECORD},
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
        'subject.name.strategy.conf': {FALLBACK_SUBJECT_NAME_STRATEGY_TYPE: "NONE"},
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    with pytest.raises(SerializationError) as exc_info:
        await ser(_JSON_OBJ, ser_ctx)

    assert "No associated subject found" in str(exc_info.value)


async def test_json_associated_name_strategy_multiple_associations_raises():
    """Test that multiple associations raise an error"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="json-resource-id-3",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="json-subject-1",
                association_type="value",
            ),
            AssociationCreateOrUpdateInfo(
                subject="json-subject-2",
                association_type="value",
            ),
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncJSONSerializer(_JSON_SCHEMA, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    with pytest.raises(SerializationError) as exc_info:
        await ser(_JSON_OBJ, ser_ctx)

    assert "Multiple associated subjects found" in str(exc_info.value)


async def test_json_associated_name_strategy_with_kafka_cluster_id():
    """Test that kafka.cluster.id config is used as resource namespace"""
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
    await client.delete_associations("json-resource-id-5")

    obj2 = {"name": "Kafka", "id": 2}
    obj_bytes2 = await ser(obj2, ser_ctx)

    result2 = await deser(obj_bytes2, ser_ctx)
    assert obj2 == result2
