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
import json
import time
from datetime import datetime, timedelta

import pytest
from confluent_kafka.schema_registry.rule_registry import RuleRegistry, \
    RuleOverride
from fastavro._logical_readers import UUID

from confluent_kafka.schema_registry import SchemaRegistryClient, \
    Schema, Metadata, MetadataProperties, header_schema_id_serializer
from confluent_kafka.schema_registry.avro import AvroSerializer, \
    AvroDeserializer
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import \
    CelFieldExecutor
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import \
    AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import \
    AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.dek_registry.dek_registry_client import \
    DekRegistryClient, DekAlgorithm
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor, Clock, EncryptionExecutor
from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import \
    GcpKmsDriver
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import \
    HcVaultKmsDriver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import \
    LocalKmsDriver
from confluent_kafka.schema_registry.rules.jsonata.jsonata_executor import \
    JsonataExecutor
from confluent_kafka.schema_registry.schema_registry_client import RuleSet, \
    Rule, RuleKind, RuleMode, SchemaReference, RuleParams, ServerConfig
from confluent_kafka.schema_registry.serde import RuleConditionError
from confluent_kafka.serialization import SerializationContext, MessageField, SerializationError


class FakeClock(Clock):

    def __init__(self):
        self.fixed_now = int(round(time.time() * 1000))

    def now(self) -> int:
        return self.fixed_now


_BASE_URL = "mock://"
# _BASE_URL = "http://localhost:8081"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
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
    client = SchemaRegistryClient.new_client(conf)
    try:
        client.delete_subject(_SUBJECT, True)
    except Exception:
        pass
    subjects = client.get_subjects()
    for subject in subjects:
        try:
            client.delete_subject(subject, True)
        except Exception:
            pass


def test_avro_basic_serialization():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    ser = AvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_guid_in_header():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'schema.id.serializer': header_schema_id_serializer
    }
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    ser = AvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE, {})
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_use_schema_id():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.schema.id': 1}

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    schema = {
        'type': 'record',
        'name': 'ref',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO'))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_bytes():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    obj = b'\x02\x03\x04'
    schema = 'bytes'
    ser = AvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)
    assert b'\x00\x00\x00\x00\x01\x02\x03\x04' == obj_bytes

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_nested():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    obj = {
        'nested': nested
    }
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'nested', 'type': {
                'type': 'record',
                'name': 'nested',
                'fields': [
                    {'name': 'intField', 'type': 'int'},
                    {'name': 'doubleField', 'type': 'double'},
                    {'name': 'stringField', 'type': 'string'},
                    {'name': 'booleanField', 'type': 'boolean'},
                    {'name': 'bytesField', 'type': 'bytes'},
                ]
            }},
        ]
    }
    ser = AvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_references():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    referenced = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    obj = {
        'refField': referenced
    }
    ref_schema = {
        'type': 'record',
        'name': 'ref',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    client.register_schema('ref', Schema(json.dumps(ref_schema)))
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'refField', 'type': 'ref'},
        ]
    }
    refs = [SchemaReference('ref', 'ref', 1)]
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_union():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    obj = {
        'First': {'stringField': 'hi'},
        'Second': {'stringField': 'hi'},
    }
    schema = ['null', {
        'type': 'record',
        'name': 'A',
        'namespace': 'test',
        'fields': [
            {'name': 'First', 'type': {'type': 'record', 'name': 'B', 'fields': [
                {'name': 'stringField', 'type': 'string'},
            ]}},
            {'name': 'Second', 'type': 'B'}
        ]
    }]
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO'))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_union_with_record_references():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    obj = {
        'First': {'stringField': 'hi'},
        'Second': {'stringField': 'hi'},
    }
    ref_schema = {
        'type': 'record',
        'namespace': 'test',
        'name': 'B',
        'fields': [
            {'name': 'stringField', 'type': 'string'},
        ]
    }
    client.register_schema('ref', Schema(json.dumps(ref_schema)))
    schema = ['null', {
        'type': 'record',
        'name': 'A',
        'namespace': 'test',
        'fields': [
            {'name': 'First', 'type': 'B'},
            {'name': 'Second', 'type': 'B'}
        ]
    }]
    refs = [SchemaReference('test.B', 'ref', 1)]
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_union_with_references():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ref_schema = {
        'type': 'record',
        'name': 'ref',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    client.register_schema('ref', Schema(json.dumps(ref_schema)))
    ref2_schema = {
        'type': 'record',
        'name': 'ref2',
        'fields': [
            {'name': 'otherField', 'type': 'string'}
        ]
    }
    client.register_schema('ref2', Schema(json.dumps(ref2_schema)))
    schema = ['ref', 'ref2']
    refs = [SchemaReference('ref', 'ref', 1), SchemaReference('ref2', 'ref2', 1)]
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_schema_evolution():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    evolution1 = {
        "name": "SchemaEvolution",
        "type": "record",
        "fields": [
            {
                "name": "fieldToDelete",
                "type": "string"
            }
        ]
    }
    evolution2 = {
        "name": "SchemaEvolution",
        "type": "record",
        "fields": [
            {
                "name": "newOptionalField",
                "type": ["string", "null"],
                "default": "optional"
            }
        ]
    }
    obj = {
        'fieldToDelete': 'bye',
    }

    client.register_schema(_SUBJECT, Schema(json.dumps(evolution1)))

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    obj_bytes = ser(obj, ser_ctx)

    client.register_schema(_SUBJECT, Schema(json.dumps(evolution2)))

    client.clear_latest_caches()
    deser = AvroDeserializer(client, conf={'use.latest.version': True})
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj2.get('fieldToDelete') is None
    assert obj2.get('newOptionalField') == 'optional'


def test_avro_cel_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_cel_condition_logical_type():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField',
             'type': {
                 'type': 'string',
                 'logicalType': 'uuid'
              }
             },
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }

    uuid = "550e8400-e29b-41d4-a716-446655440000"

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField == '" + uuid + "'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': UUID(uuid),
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_cel_condition_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError) as e:
        ser(obj, ser_ctx)
    assert isinstance(e.value.__cause__, RuleConditionError)


def test_avro_cel_condition_ignore_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_cel_field_transform():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi-suffix',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    deser = AvroDeserializer(client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_avro_cel_field_transform_missing_prop():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
            {'name': 'missing', 'type': ['null', 'string'], 'default': None},
        ]
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi-suffix-suffix',
        'booleanField': True,
        'bytesField': b'foobar',
        'missing': None,
    }
    deser = AvroDeserializer(client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_avro_cel_field_transform_disable():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }

    registry = RuleRegistry()
    registry.register_rule_executor(CelFieldExecutor())
    registry.register_override(RuleOverride("CEL_FIELD", None, None, True))
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_registry=registry)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    newobj = deser(obj_bytes, ser_ctx)
    assert "hi" == newobj['stringField']


def test_avro_cel_field_transform_complex():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'arrayField', 'type':
                {'type': 'array', 'items': 'string'}
             },
            {'name': 'mapField', 'type':
                {'type': 'map', 'values': 'string'}
             },
            {'name': 'unionField', 'type': ['null', 'string'], 'confluent:tags': ['PII']}
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'arrayField': ['hello'],
        'mapField': {'key': 'world'},
        'unionField': 'bye',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'arrayField': ['hello-suffix'],
        'mapField': {'key': 'world-suffix'},
        'unionField': 'bye-suffix',
    }
    deser = AvroDeserializer(client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_avro_cel_field_transform_complex_with_none():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'arrayField', 'type':
                {'type': 'array', 'items': 'string'}
             },
            {'name': 'mapField', 'type':
                {'type': 'map', 'values': 'string'}
             },
            {'name': 'unionField', 'type': ['null', 'string'], 'confluent:tags': ['PII']}
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'arrayField': ['hello'],
        'mapField': {'key': 'world'},
        'unionField': None,
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'arrayField': ['hello-suffix'],
        'mapField': {'key': 'world-suffix'},
        'unionField': None,
    }
    deser = AvroDeserializer(client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_avro_cel_field_transform_complex_nested():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'UnionTest',
        'namespace': 'test',
        'fields': [
            {
                'name': 'emails',
                'type': [
                    'null',
                    {
                        'type': 'array',
                        'items': {
                            'type': 'record',
                            'name': 'Email',
                            'fields': [
                                {
                                    'name': 'email',
                                    'type': [
                                        'null',
                                        'string'
                                    ],
                                    'doc': 'Email address',
                                    'confluent:tags': [
                                        'PII'
                                    ]
                                }
                            ]
                        }
                    }
                ],
                'doc': 'Communication Email',
            }
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'emails': [{'email': 'john@acme.com'}]
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'emails': [{'email': 'john@acme.com-suffix'}]
    }
    deser = AvroDeserializer(client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_avro_cel_field_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
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
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_cel_field_condition_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value == 'bye'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError) as e:
        ser(obj, ser_ctx)
    assert isinstance(e.value.__cause__, RuleConditionError)


def test_avro_encryption():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes', 'confluent:tags': ['PII']},
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'
    obj['bytesField'] = b'foobar'

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_payload_encryption():
    executor = EncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes', 'confluent:tags': ['PII']},
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT_PAYLOAD",
        None,
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_alternate_keks():
    executor = EncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret', 'encrypt.alternate.kms.key.ids': 'mykey2,mykey3'}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes', 'confluent:tags': ['PII']},
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT_PAYLOAD",
        None,
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_deterministic():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes', 'confluent:tags': ['PII']},
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey",
            "encrypt.dek.algorithm": "AES256_SIV"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'
    obj['bytesField'] = b'foobar'

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_wrapped_union():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "fields": [
            {
                "name": "id",
                "type": "int"
            },
            {
                "name": "result",
                "type": [
                    "null",
                    {
                        "fields": [
                            {
                                "name": "code",
                                "type": "int"
                            },
                            {
                                "confluent:tags": [
                                    "PII"
                                ],
                                "name": "secret",
                                "type": [
                                    "null",
                                    "string"
                                ]
                            }
                        ],
                        "name": "Data",
                        "type": "record"
                    },
                    {
                        "fields": [
                            {
                                "name": "code",
                                "type": "int"
                            },
                            {
                                "name": "reason",
                                "type": [
                                    "null",
                                    "string"
                                ]
                            }
                        ],
                        "name": "Error",
                        "type": "record"
                    }
                ]
            }
        ],
        "name": "Result",
        "namespace": "com.acme",
        "type": "record"
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'id': 123,
        'result': (
            'com.acme.Data', {
                'code': 456,
                'secret': 'mypii'
            }
        )
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['result'][1]['secret'] != 'mypii'
    # remove union wrapper
    obj['result'] = {
        'code': 456,
        'secret': 'mypii'
    }

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_typed_union():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "fields": [
            {
                "name": "id",
                "type": "int"
            },
            {
                "name": "result",
                "type": [
                    "null",
                    {
                        "fields": [
                            {
                                "name": "code",
                                "type": "int"
                            },
                            {
                                "confluent:tags": [
                                    "PII"
                                ],
                                "name": "secret",
                                "type": [
                                    "null",
                                    "string"
                                ]
                            }
                        ],
                        "name": "Data",
                        "type": "record"
                    },
                    {
                        "fields": [
                            {
                                "name": "code",
                                "type": "int"
                            },
                            {
                                "name": "reason",
                                "type": [
                                    "null",
                                    "string"
                                ]
                            }
                        ],
                        "name": "Error",
                        "type": "record"
                    }
                ]
            }
        ],
        "name": "Result",
        "namespace": "com.acme",
        "type": "record"
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'id': 123,
        'result': {
            '-type': 'com.acme.Data',
            'code': 456,
            'secret': 'mypii'
        }
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['result']['secret'] != 'mypii'
    # remove union wrapper
    obj['result'] = {
        'code': 456,
        'secret': 'mypii'
    }

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_cel():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes', 'confluent:tags': ['PII']},
        ]
    }

    rule1 = Rule(
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
        False
    )
    rule2 = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule1, rule2])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi-suffix'
    obj['stringField'] = 'hi-suffix'
    obj['bytesField'] = b'foobar'

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_dek_rotation():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-rot",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey",
            "encrypt.dek.expiry.days": "1"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client: DekRegistryClient = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2

    dek_client = executor.executor.client
    dek = dek_client.get_dek("kek1-rot", _SUBJECT, version=-1)
    assert dek.version == 1

    # advance 2 days
    now = datetime.now() + timedelta(days=2)
    executor.executor.clock.fixed_now = int(round(now.timestamp() * 1000))

    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'

    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2

    dek = dek_client.get_dek("kek1-rot", _SUBJECT, version=-1)
    assert dek.version == 2

    # advance 2 days
    now = datetime.now() + timedelta(days=2)
    executor.executor.clock.fixed_now = int(round(now.timestamp() * 1000))

    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'

    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2

    dek = dek_client.get_dek("kek1-rot", _SUBJECT, version=-1)
    assert dek.version == 3


def test_avro_encryption_f1_preserialized():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'f1Schema',
        'fields': [
            {'name': 'f1', 'type': 'string', 'confluent:tags': ['PII']}
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-f1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,ERROR",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'f1': 'hello world'
    }

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    deser = AvroDeserializer(client, rule_conf=rule_conf)

    dek_client: DekRegistryClient = executor.executor.client
    dek_client.register_kek("kek1-f1", "local-kms", "mykey")

    encrypted_dek = "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4="
    dek_client.register_dek("kek1-f1", _SUBJECT, encrypted_dek)

    obj_bytes = bytes([0, 0, 0, 0, 1, 104, 122, 103, 121, 47, 106, 70, 78, 77,
                       86, 47, 101, 70, 105, 108, 97, 72, 114, 77, 121, 101, 66,
                       103, 100, 97, 86, 122, 114, 82, 48, 117, 100, 71, 101,
                       111, 116, 87, 56, 99, 65, 47, 74, 97, 108, 55, 117, 107,
                       114, 43, 77, 47, 121, 122])

    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_deterministic_f1_preserialized():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'f1Schema',
        'fields': [
            {'name': 'f1', 'type': 'string', 'confluent:tags': ['PII']}
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-det-f1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey",
            "encrypt.dek.algorithm": "AES256_SIV",
        }),
        None,
        None,
        "ERROR,ERROR",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'f1': 'hello world'
    }

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    deser = AvroDeserializer(client, rule_conf=rule_conf)

    dek_client: DekRegistryClient = executor.executor.client
    dek_client.register_kek("kek1-det-f1", "local-kms", "mykey")

    encrypted_dek = ("YSx3DTlAHrmpoDChquJMifmPntBzxgRVdMzgYL82rgWBKn7aUSnG+WIu9oz"
                     "BNS3y2vXd++mBtK07w4/W/G6w0da39X9hfOVZsGnkSvry/QRht84V8yz3dqKxGMOK5A==")
    dek_client.register_dek("kek1-det-f1", _SUBJECT, encrypted_dek, algorithm=DekAlgorithm.AES256_SIV)

    obj_bytes = bytes([0, 0, 0, 0, 1, 72, 68, 54, 89, 116, 120, 114, 108, 66,
                       110, 107, 84, 87, 87, 57, 78, 54, 86, 98, 107, 51, 73,
                       73, 110, 106, 87, 72, 56, 49, 120, 109, 89, 104, 51, 107, 52, 100])

    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_dek_rotation_f1_preserialized():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'f1Schema',
        'fields': [
            {'name': 'f1', 'type': 'string', 'confluent:tags': ['PII']}
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-rot-f1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey",
            "encrypt.dek.expiry.days": "1",
        }),
        None,
        None,
        "ERROR,ERROR",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'f1': 'hello world'
    }

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    deser = AvroDeserializer(client, rule_conf=rule_conf)

    dek_client: DekRegistryClient = executor.executor.client
    dek_client.register_kek("kek1-rot-f1", "local-kms", "mykey")

    encrypted_dek = "W/v6hOQYq1idVAcs1pPWz9UUONMVZW4IrglTnG88TsWjeCjxmtRQ4VaNe/I5dCfm2zyY9Cu0nqdvqImtUk4="
    dek_client.register_dek("kek1-rot-f1", _SUBJECT, encrypted_dek, algorithm=DekAlgorithm.AES256_GCM)

    obj_bytes = bytes([0, 0, 0, 0, 1, 120, 65, 65, 65, 65, 65, 65, 71, 52, 72,
                       73, 54, 98, 49, 110, 88, 80, 88, 113, 76, 121, 71, 56,
                       99, 73, 73, 51, 53, 78, 72, 81, 115, 101, 113, 113, 85,
                       67, 100, 43, 73, 101, 76, 101, 70, 86, 65, 101, 78, 112,
                       83, 83, 51, 102, 120, 80, 110, 74, 51, 50, 65, 61])

    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_references():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}

    referenced = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    obj = {
        'refField': referenced
    }
    ref_schema = {
        'type': 'record',
        'name': 'ref',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    client.register_schema('ref', Schema(json.dumps(ref_schema)))
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'refField', 'type': 'ref'},
        ]
    }
    refs = [SchemaReference('ref', 'ref', 1)]
    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-ref",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        refs,
        None,
        RuleSet(None, [rule])
    ))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['refField']['stringField'] != 'hi'
    obj['refField']['stringField'] = 'hi'
    obj['refField']['bytesField'] = b'foobar'

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_encryption_with_union():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': ['null', 'string'], 'confluent:tags': ['PII']},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': ['null', 'bytes'], 'confluent:tags': ['PII']},
        ]
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-union",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'
    obj['bytesField'] = b'foobar'

    deser = AvroDeserializer(client, rule_conf=rule_conf)
    executor.executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_avro_jsonata_with_cel():
    rule1_to_2 = "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)

    client.set_config(_SUBJECT, ServerConfig(
        compatibility_group='application.version'
    ))

    schema = {
        'type': 'record',
        'name': 'old',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'size', 'type': 'int'},
            {'name': 'version', 'type': 'int'},
        ]
    }
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v1"}),
            None
        ),
        None
    ))

    schema = {
        'type': 'record',
        'name': 'new',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'height', 'type': 'int'},
            {'name': 'version', 'type': 'int'},
        ]
    }

    rule1 = Rule(
        "test-jsonata",
        "",
        RuleKind.TRANSFORM,
        RuleMode.UPGRADE,
        "JSONATA",
        None,
        None,
        rule1_to_2,
        None,
        None,
        False
    )
    rule2 = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.READ,
        "CEL_FIELD",
        None,
        None,
        "name == 'name' ; value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v2"}),
            None
        ),
        RuleSet([rule1], [rule2])
    ))

    obj = {
        'name': 'alice',
        'size': 123,
        'version': 1,
    }
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v1'
        }
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'name': 'alice-suffix',
        'height': 123,
        'version': 1,
    }
    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v2'
        }
    }
    deser = AvroDeserializer(client, conf=deser_conf)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_avro_jsonata_fully_compatible():
    rule1_to_2 = "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"
    rule2_to_1 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])"
    rule2_to_3 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])"
    rule3_to_2 = "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])"

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)

    client.set_config(_SUBJECT, ServerConfig(
        compatibility_group='application.version'
    ))

    schema = {
        'type': 'record',
        'name': 'old',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'size', 'type': 'int'},
            {'name': 'version', 'type': 'int'},
        ]
    }
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v1"}),
            None
        ),
        None
    ))

    schema = {
        'type': 'record',
        'name': 'new',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'height', 'type': 'int'},
            {'name': 'version', 'type': 'int'},
        ]
    }

    rule1 = Rule(
        "rule1",
        "",
        RuleKind.TRANSFORM,
        RuleMode.UPGRADE,
        "JSONATA",
        None,
        None,
        rule1_to_2,
        None,
        None,
        False
    )
    rule2 = Rule(
        "rule2",
        "",
        RuleKind.TRANSFORM,
        RuleMode.DOWNGRADE,
        "JSONATA",
        None,
        None,
        rule2_to_1,
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v2"}),
            None
        ),
        RuleSet([rule1, rule2], None)
    ))

    schema = {
        'type': 'record',
        'name': 'newer',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'length', 'type': 'int'},
            {'name': 'version', 'type': 'int'},
        ]
    }

    rule3 = Rule(
        "rule3",
        "",
        RuleKind.TRANSFORM,
        RuleMode.UPGRADE,
        "JSONATA",
        None,
        None,
        rule2_to_3,
        None,
        None,
        False
    )
    rule4 = Rule(
        "rule4",
        "",
        RuleKind.TRANSFORM,
        RuleMode.DOWNGRADE,
        "JSONATA",
        None,
        None,
        rule3_to_2,
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v3"}),
            None
        ),
        RuleSet([rule3, rule4], None)
    ))

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
        'use.latest.with.metadata': {
            'application.version': 'v1'
        }
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v2'
        }
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj2, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v3'
        }
    }
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj3, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)


def deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3):
    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v1'
        }
    }
    deser = AvroDeserializer(client, conf=deser_conf)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj == newobj

    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v2'
        }
    }
    deser = AvroDeserializer(client, conf=deser_conf)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj

    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v3'
        }
    }
    deser = AvroDeserializer(client, conf=deser_conf)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj3 == newobj


def test_avro_reference():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)

    awarded_user, schema = _register_avro_schemas_and_build_awarded_user_schema(client)

    _references_test_common(client, awarded_user, schema, schema)


def _register_avro_schemas_and_build_awarded_user_schema(client):
    user = User('Bowie', 47, 'purple')
    award_properties = AwardProperties(10, 2023)
    award = Award("Best In Show", award_properties)
    awarded_user = AwardedUser(award, user)

    user_schema_ref = SchemaReference("confluent.io.examples.serialization.avro.User", "user", 1)
    award_properties_schema_ref = SchemaReference("confluent.io.examples.serialization.avro.AwardProperties",
                                                  "award_properties", 1)
    award_schema_ref = SchemaReference("confluent.io.examples.serialization.avro.Award", "award", 1)

    client.register_schema("user", Schema(User.schema_str, 'AVRO'))
    client.register_schema("award_properties", Schema(AwardProperties.schema_str, 'AVRO'))
    client.register_schema("award", Schema(Award.schema_str, 'AVRO', [award_properties_schema_ref]))

    references = [user_schema_ref, award_schema_ref]
    schema = Schema(AwardedUser.schema_str, 'AVRO', references)
    return awarded_user, schema


def _references_test_common(client, awarded_user, serializer_schema, deserializer_schema):
    value_serializer = AvroSerializer(
        client, serializer_schema,
        lambda user, ctx:
        dict(
            award=dict(
                name=user.award.name,
                properties=dict(year=user.award.properties.year,
                                points=user.award.properties.points)),
            user=dict(
                name=user.user.name,
                favorite_number=user.user.favorite_number,
                favorite_color=user.user.favorite_color)))

    value_deserializer = \
        AvroDeserializer(
            client, deserializer_schema,
            lambda user, ctx:
            AwardedUser(
                award=Award(
                    name=user.get('award').get('name'),
                    properties=AwardProperties(
                        year=user.get('award').get('properties').get(
                            'year'),
                        points=user.get('award').get('properties').get(
                            'points'))),
                user=User(
                    name=user.get('user').get('name'),
                    favorite_number=user.get('user').get('favorite_number'),
                    favorite_color=user.get('user').get('favorite_color'))))

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = value_serializer(awarded_user, ser_ctx)

    awarded_user2 = value_deserializer(obj_bytes, ser_ctx)

    assert awarded_user2 == awarded_user


class User(object):
    schema_str = """
        {
            "namespace": "confluent.io.examples.serialization.avro",
            "name": "User",
            "type": "record",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": "int"},
                {"name": "favorite_color", "type": "string"}
            ]
        }
        """

    def __init__(self, name, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color

    def __eq__(self, other):
        return all([
            self.name == other.name,
            self.favorite_number == other.favorite_number,
            self.favorite_color == other.favorite_color])


class AwardProperties(object):
    schema_str = """
        {
            "namespace": "confluent.io.examples.serialization.avro",
            "name": "AwardProperties",
            "type": "record",
            "fields": [
                {"name": "year", "type": "int"},
                {"name": "points", "type": "int"}
            ]
        }
    """

    def __init__(self, points, year):
        self.points = points
        self.year = year

    def __eq__(self, other):
        return all([
            self.points == other.points,
            self.year == other.year
        ])


class Award(object):
    schema_str = """
        {
            "namespace": "confluent.io.examples.serialization.avro",
            "name": "Award",
            "type": "record",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "properties", "type": "AwardProperties"}
            ]
        }
    """

    def __init__(self, name, properties):
        self.name = name
        self.properties = properties

    def __eq__(self, other):
        return all([
            self.name == other.name,
            self.properties == other.properties
        ])


class AwardedUser(object):
    schema_str = """
        {
            "namespace": "confluent.io.examples.serialization.avro",
            "name": "AwardedUser",
            "type": "record",
            "fields": [
                {"name": "award", "type": "Award"},
                {"name": "user", "type": "User"}
            ]
        }
    """

    def __init__(self, award, user):
        self.award = award
        self.user = user

    def __eq__(self, other):
        return all([
            self.award == other.award,
            self.user == other.user
        ])
