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
from confluent_kafka.schema_registry.avro import AsyncAvroDeserializer, AsyncAvroSerializer
from confluent_kafka.schema_registry.common.schema_registry_client import (
    AssociationCreateOrUpdateInfo,
    AssociationCreateOrUpdateRequest,
)
from confluent_kafka.schema_registry.common.serde import SubjectNameStrategyType
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


async def test_avro_basic_serialization():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
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
        ],
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_guid_in_header():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'schema.id.serializer': header_schema_id_serializer}
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
        ],
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE, {})
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_use_schema_id():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
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
        ],
    }
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO'))

    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_bytes():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    obj = b'\x02\x03\x04'
    schema = 'bytes'
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)
    assert b'\x00\x00\x00\x00\x01\x02\x03\x04' == obj_bytes

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_nested():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    obj = {'nested': nested}
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {
                'name': 'nested',
                'type': {
                    'type': 'record',
                    'name': 'nested',
                    'fields': [
                        {'name': 'intField', 'type': 'int'},
                        {'name': 'doubleField', 'type': 'double'},
                        {'name': 'stringField', 'type': 'string'},
                        {'name': 'booleanField', 'type': 'boolean'},
                        {'name': 'bytesField', 'type': 'bytes'},
                    ],
                },
            },
        ],
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_references():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    referenced = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    obj = {'refField': referenced}
    ref_schema = {
        'type': 'record',
        'name': 'ref',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ],
    }
    await client.register_schema('ref', Schema(json.dumps(ref_schema)))
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'refField', 'type': 'ref'},
        ],
    }
    refs = [SchemaReference('ref', 'ref', 1)]
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_references_with_namespace():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    obj = {'payload': {'id': '123'}}
    ref_schema = {
        'type': 'record',
        'name': 'ReferencedRecord',
        'namespace': 'example.references',
        'fields': [
            {'name': 'id', 'type': 'string'},
        ],
    }
    await client.register_schema('test-ReferencedRecord', Schema(json.dumps(ref_schema)))
    schema = {
        'type': 'record',
        'name': 'ReferencingRecord',
        'namespace': 'example.references',
        'fields': [
            {'name': 'payload', 'type': 'ReferencedRecord'},
        ],
    }
    refs = [SchemaReference('ReferencedRecord', 'test-ReferencedRecord', 1)]
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_union():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    obj = {
        'First': {'stringField': 'hi'},
        'Second': {'stringField': 'hi'},
    }
    schema = [
        'null',
        {
            'type': 'record',
            'name': 'A',
            'namespace': 'test',
            'fields': [
                {
                    'name': 'First',
                    'type': {
                        'type': 'record',
                        'name': 'B',
                        'fields': [
                            {'name': 'stringField', 'type': 'string'},
                        ],
                    },
                },
                {'name': 'Second', 'type': 'B'},
            ],
        },
    ]
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO'))

    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_union_with_record_references():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
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
        ],
    }
    await client.register_schema('ref', Schema(json.dumps(ref_schema)))
    schema = [
        'null',
        {
            'type': 'record',
            'name': 'A',
            'namespace': 'test',
            'fields': [{'name': 'First', 'type': 'B'}, {'name': 'Second', 'type': 'B'}],
        },
    ]
    refs = [SchemaReference('test.B', 'ref', 1)]
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_serialize_union_with_references():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
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
        ],
    }
    await client.register_schema('ref', Schema(json.dumps(ref_schema)))
    ref2_schema = {'type': 'record', 'name': 'ref2', 'fields': [{'name': 'otherField', 'type': 'string'}]}
    await client.register_schema('ref2', Schema(json.dumps(ref2_schema)))
    schema = ['ref', 'ref2']
    refs = [SchemaReference('ref', 'ref', 1), SchemaReference('ref2', 'ref2', 1)]
    await client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_diamond_dependency_references():
    # Two sibling references (OrderDetails, InvoiceDetails) both depend on the
    # same named type (Address). Without the fix in _resolve_named_schema, each
    # branch is pre-parsed with Address inlined, and the top-level parse then
    # raises SchemaParseException("redefined named type ...Address").
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    ns = "com.example.diamond"
    address_schema = {
        'type': 'record',
        'name': 'Address',
        'namespace': ns,
        'fields': [{'name': 'street', 'type': 'string'}],
    }
    order_schema = {
        'type': 'record',
        'name': 'OrderDetails',
        'namespace': ns,
        'fields': [{'name': 'shipping_address', 'type': f'{ns}.Address'}],
    }
    invoice_schema = {
        'type': 'record',
        'name': 'InvoiceDetails',
        'namespace': ns,
        'fields': [{'name': 'billing_address', 'type': f'{ns}.Address'}],
    }
    root_schema = {
        'type': 'record',
        'name': 'OrderEvent',
        'namespace': ns,
        'fields': [
            {'name': 'order', 'type': f'{ns}.OrderDetails'},
            {'name': 'invoice', 'type': f'{ns}.InvoiceDetails'},
        ],
    }

    await client.register_schema('diamond-Address', Schema(json.dumps(address_schema)))
    await client.register_schema(
        'diamond-OrderDetails',
        Schema(
            json.dumps(order_schema),
            'AVRO',
            [SchemaReference(f'{ns}.Address', 'diamond-Address', 1)],
        ),
    )
    await client.register_schema(
        'diamond-InvoiceDetails',
        Schema(
            json.dumps(invoice_schema),
            'AVRO',
            [SchemaReference(f'{ns}.Address', 'diamond-Address', 1)],
        ),
    )
    await client.register_schema(
        _SUBJECT,
        Schema(
            json.dumps(root_schema),
            'AVRO',
            [
                SchemaReference(f'{ns}.OrderDetails', 'diamond-OrderDetails', 1),
                SchemaReference(f'{ns}.InvoiceDetails', 'diamond-InvoiceDetails', 1),
            ],
        ),
    )

    obj = {
        'order': {'shipping_address': {'street': '123 Main St'}},
        'invoice': {'billing_address': {'street': '456 Elm St'}},
    }
    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_avro_schema_evolution():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    evolution1 = {"name": "SchemaEvolution", "type": "record", "fields": [{"name": "fieldToDelete", "type": "string"}]}
    evolution2 = {
        "name": "SchemaEvolution",
        "type": "record",
        "fields": [{"name": "newOptionalField", "type": ["string", "null"], "default": "optional"}],
    }
    obj = {
        'fieldToDelete': 'bye',
    }

    await client.register_schema(_SUBJECT, Schema(json.dumps(evolution1)))

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    ser = await AsyncAvroSerializer(client, schema_str=None, conf=ser_conf)
    obj_bytes = await ser(obj, ser_ctx)

    await client.register_schema(_SUBJECT, Schema(json.dumps(evolution2)))

    client.clear_latest_caches()
    deser = await AsyncAvroDeserializer(client, conf={'use.latest.version': True})
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj2.get('fieldToDelete') is None
    assert obj2.get('newOptionalField') == 'optional'


async def test_avro_reference():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    awarded_user, schema = await _register_avro_schemas_and_build_awarded_user_schema(client)

    await _references_test_common(client, awarded_user, schema, schema)


async def test_avro_serialize_strict_extra_fields_rejected():
    """
    Ensures validate.strict=True rejects records with extra fields not in schema
    """
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate.strict': True}

    schema = {
        'type': 'record',
        'name': 'StrictTest',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'age', 'type': 'int'},
        ],
    }

    obj = {
        'name': 'Alice',
        'age': 30,
        'extra_field': 'should_be_rejected',
    }

    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    # Should raise an exception due to extra field with strict=True
    with pytest.raises(ValueError):
        await ser(obj, ser_ctx)


async def test_avro_serialize_strict_missing_field_with_default():
    """
    Ensures validate.strict=True rejects missing field even if it has a default
    """
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate.strict': True}

    schema = {
        'type': 'record',
        'name': 'StrictDefaultTest',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'age', 'type': 'int', 'default': 0},
        ],
    }

    obj = {'name': 'Charlie'}

    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    # Should raise an exception because age is missing with strict=True
    with pytest.raises(ValueError):
        await ser(obj, ser_ctx)


async def test_avro_serialize_strict_allow_default_missing_field_with_default():
    """
    Ensures validate.strict.allow.default=True allows missing field with default
    """
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate.strict.allow.default': True}

    schema = {
        'type': 'record',
        'name': 'StrictAllowDefaultTest',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'age', 'type': 'int', 'default': 0},
            {'name': 'city', 'type': 'string', 'default': 'Unknown'},
        ],
    }

    obj = {'name': 'Diana'}

    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)

    # Should have default values
    assert obj2 == {'name': 'Diana', 'age': 0, 'city': 'Unknown'}


async def test_avro_serialize_strict_allow_default_extra_fields_rejected():
    """
    Ensures validate.strict.allow.default=True still rejects extra fields
    """
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate.strict.allow.default': True}

    schema = {
        'type': 'record',
        'name': 'StrictAllowDefaultExtraTest',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'age', 'type': 'int', 'default': 0},
        ],
    }

    obj = {
        'name': 'Frank',
        'age': 40,
        'extra_field': 'should_be_rejected',
    }

    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    # Should raise an exception due to extra field
    with pytest.raises(ValueError):
        await ser(obj, ser_ctx)


async def test_avro_serialize_strict_allow_default_union_with_default():
    """
    Tests validate.strict.allow.default with union types having defaults
    """
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate.strict.allow.default': True}

    schema = {
        'type': 'record',
        'name': 'UnionDefaultTest',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'email', 'type': ['null', 'string'], 'default': None},
        ],
    }

    obj = {'name': 'Grace'}

    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)

    # Should have None as default value for email
    assert obj2 == {'name': 'Grace', 'email': None}


async def test_avro_serialize_strict_nested_record():
    """
    Tests validate.strict=True with nested records
    """
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate.strict': True}

    schema = {
        'type': 'record',
        'name': 'NestedStrictTest',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {
                'name': 'address',
                'type': {
                    'type': 'record',
                    'name': 'Address',
                    'fields': [
                        {'name': 'street', 'type': 'string'},
                        {'name': 'city', 'type': 'string'},
                    ],
                },
            },
        ],
    }

    obj = {
        'name': 'Henry',
        'address': {
            'street': '123 Main St',
            'city': 'Boston',
            'extra_nested': 'should_be_rejected',
        },
    }

    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    # Should raise an exception due to extra field in nested record
    with pytest.raises(ValueError):
        await ser(obj, ser_ctx)


async def test_avro_serialize_deserialize_strict():
    """
    Ensures validate.strict round trip works correctly with all fields present
    """
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate.strict': True}

    schema = {
        'type': 'record',
        'name': 'Strict',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'age', 'type': 'int'},
            {'name': 'score', 'type': 'double'},
        ],
    }

    obj = {
        'name': 'Jack',
        'age': 28,
        'score': 95.5,
    }

    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)

    assert obj == obj2


async def _register_avro_schemas_and_build_awarded_user_schema(client):
    user = User('Bowie', 47, 'purple')
    award_properties = AwardProperties(10, 2023)
    award = Award("Best In Show", award_properties)
    awarded_user = AwardedUser(award, user)

    user_schema_ref = SchemaReference("confluent.io.examples.serialization.avro.User", "user", 1)
    award_properties_schema_ref = SchemaReference(
        "confluent.io.examples.serialization.avro.AwardProperties", "award_properties", 1
    )
    award_schema_ref = SchemaReference("confluent.io.examples.serialization.avro.Award", "award", 1)

    await client.register_schema("user", Schema(User.schema_str, 'AVRO'))
    await client.register_schema("award_properties", Schema(AwardProperties.schema_str, 'AVRO'))
    await client.register_schema("award", Schema(Award.schema_str, 'AVRO', [award_properties_schema_ref]))

    references = [user_schema_ref, award_schema_ref]
    schema = Schema(AwardedUser.schema_str, 'AVRO', references)
    return awarded_user, schema


async def _references_test_common(client, awarded_user, serializer_schema, deserializer_schema):
    value_serializer = await AsyncAvroSerializer(
        client,
        serializer_schema,
        lambda user, ctx: dict(
            award=dict(
                name=user.award.name,
                properties=dict(year=user.award.properties.year, points=user.award.properties.points),
            ),
            user=dict(
                name=user.user.name, favorite_number=user.user.favorite_number, favorite_color=user.user.favorite_color
            ),
        ),
    )

    value_deserializer = await AsyncAvroDeserializer(
        client,
        deserializer_schema,
        lambda user, ctx: AwardedUser(
            award=Award(
                name=user.get('award').get('name'),
                properties=AwardProperties(
                    year=user.get('award').get('properties').get('year'),
                    points=user.get('award').get('properties').get('points'),
                ),
            ),
            user=User(
                name=user.get('user').get('name'),
                favorite_number=user.get('user').get('favorite_number'),
                favorite_color=user.get('user').get('favorite_color'),
            ),
        ),
    )

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await value_serializer(awarded_user, ser_ctx)

    awarded_user2 = await value_deserializer(obj_bytes, ser_ctx)

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
        return all(
            [
                self.name == other.name,
                self.favorite_number == other.favorite_number,
                self.favorite_color == other.favorite_color,
            ]
        )


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
        return all([self.points == other.points, self.year == other.year])


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
        return all([self.name == other.name, self.properties == other.properties])


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
        return all([self.award == other.award, self.user == other.user])


async def test_associated_name_strategy_with_association():
    """Test that AsyncAssociatedNameStrategy returns subject from association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema and test object
    schema = {
        'type': 'record',
        'name': 'TestRecord',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'stringField', 'type': 'string'},
        ],
    }
    obj = {'intField': 123, 'stringField': 'hello'}

    # Add an association for the custom subject
    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="mock-resource-id-1",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="my-custom-subject-value",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=json.dumps(schema),
                ),
            )
        ],
    )
    await client.create_association(request)

    # Create serializer with associated name strategy
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # Deserialize and verify
    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    # Verify the schema was registered with the custom subject
    registered_schema = await client.get_latest_version("my-custom-subject-value")
    assert registered_schema is not None

    await client.delete_associations(resource_id="mock-resource-id-1", cascade_lifecycle=True)


async def test_associated_name_strategy_with_key_association():
    """Test that AsyncAssociatedNameStrategy returns subject for key"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema and test object
    schema = {
        'type': 'record',
        'name': 'KeyRecord',
        'fields': [
            {'name': 'id', 'type': 'int'},
        ],
    }
    obj = {'id': 42}

    # Add an association for key
    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="mock-resource-id-2",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="my-key-subject",
                association_type="key",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=json.dumps(schema),
                ),
            )
        ],
    )
    await client.create_association(request)

    # Create serializer with associated name strategy for KEY
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.KEY)
    obj_bytes = await ser(obj, ser_ctx)

    # Deserialize and verify
    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    # Verify the schema was registered with the key subject
    registered_schema = await client.get_latest_version("my-key-subject")
    assert registered_schema is not None

    await client.delete_associations(resource_id="mock-resource-id-2", cascade_lifecycle=True)


async def test_associated_name_strategy_fallback_to_topic():
    """Test fallback to topic_subject_name_strategy when no association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema and test object
    schema = {
        'type': 'record',
        'name': 'TestRecord',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'stringField', 'type': 'string'},
        ],
    }
    obj = {'intField': 456, 'stringField': 'world'}

    # No associations added, should fall back to topic strategy
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # Deserialize and verify
    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    # Default fallback is topic_subject_name_strategy which returns topic-value
    registered_schema = await client.get_latest_version(_TOPIC + "-value")
    assert registered_schema is not None


async def test_associated_name_strategy_fallback_to_record():
    """Test fallback to record_subject_name_strategy when configured"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema with a specific record name
    schema = {
        'type': 'record',
        'name': 'MyRecord',
        'fields': [
            {'name': 'value', 'type': 'string'},
        ],
    }
    obj = {'value': 'test'}

    # No associations, configure fallback to RECORD
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: SubjectNameStrategyType.RECORD},
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # Deserialize and verify
    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    # Should have registered under the record name
    registered_schema = await client.get_latest_version("MyRecord")
    assert registered_schema is not None


async def test_associated_name_strategy_fallback_to_topic_record():
    """Test fallback to topic_record_subject_name_strategy when configured"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema with a specific record name
    schema = {
        'type': 'record',
        'name': 'MyRecord',
        'fields': [
            {'name': 'data', 'type': 'int'},
        ],
    }
    obj = {'data': 789}

    # No associations, configure fallback to TOPIC_RECORD
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: SubjectNameStrategyType.TOPIC_RECORD},
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # Deserialize and verify
    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    # Should have registered under topic-record_name
    registered_schema = await client.get_latest_version(_TOPIC + "-MyRecord")
    assert registered_schema is not None


async def test_associated_name_strategy_fallback_none_raises():
    """Test that NONE fallback raises an error when no association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema
    schema = {
        'type': 'record',
        'name': 'MyRecord',
        'fields': [
            {'name': 'value', 'type': 'string'},
        ],
    }
    obj = {'value': 'test'}

    # No associations, configure fallback to NONE
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: "NONE"},
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    with pytest.raises(SerializationError) as exc_info:
        await ser(obj, ser_ctx)

    assert "No associated subject found" in str(exc_info.value)


async def test_associated_name_strategy_with_kafka_cluster_id():
    """Test that subject.name.strategy.kafka.cluster.id config is used as resource namespace"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema
    schema = {
        'type': 'record',
        'name': 'TestRecord',
        'fields': [
            {'name': 'intField', 'type': 'int'},
        ],
    }
    obj = {'intField': 100}

    # Add an association with specific namespace
    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="my-cluster-id",
        resource_id="mock-resource-id-4",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="cluster-specific-subject",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=json.dumps(schema),
                ),
            )
        ],
    )
    await client.create_association(request)

    # Create serializer with matching cluster ID
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {KAFKA_CLUSTER_ID: "my-cluster-id"},
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    # Deserialize and verify
    deser = await AsyncAvroDeserializer(client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    # Verify the schema was registered with the cluster-specific subject
    registered_schema = await client.get_latest_version("cluster-specific-subject")
    assert registered_schema is not None

    await client.delete_associations(resource_id="mock-resource-id-4", cascade_lifecycle=True)


async def test_associated_name_strategy_caching():
    """Test that results are cached within a strategy instance and serializer works with caching"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    # Define schema
    schema = {
        'type': 'record',
        'name': 'CacheTestRecord',
        'fields': [
            {'name': 'count', 'type': 'int'},
        ],
    }

    # Add an association
    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="mock-resource-id-5",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="cached-subject",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=json.dumps(schema),
                ),
            )
        ],
    )
    await client.create_association(request)

    # Create serializer with associated name strategy
    ser_conf = {
        'auto.register.schemas': True,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncAvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    # First serialization
    obj1 = {'count': 1}
    obj_bytes1 = await ser(obj1, ser_ctx)

    # Verify it was registered under cached-subject
    registered_schema = await client.get_latest_version("cached-subject")
    assert registered_schema is not None

    # Deserialize first message
    deser = await AsyncAvroDeserializer(client)
    result1 = await deser(obj_bytes1, ser_ctx)
    assert obj1 == result1

    # Delete associations (but serializer should still work due to caching)
    await client.delete_associations(resource_id="mock-resource-id-5", cascade_lifecycle=True)

    # Second serialization should still work (schema already registered)
    obj2 = {'count': 2}
    obj_bytes2 = await ser(obj2, ser_ctx)

    # Deserialize second message
    result2 = await deser(obj_bytes2, ser_ctx)
    assert obj2 == result2
