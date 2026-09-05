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
import os
import sys

import pytest

from confluent_kafka.schema_registry import Schema, header_schema_id_serializer
from confluent_kafka.schema_registry._async.protobuf import AsyncProtobufDeserializer, AsyncProtobufSerializer
from confluent_kafka.schema_registry._async.schema_registry_client import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry._async.serde import (
    FALLBACK_TYPE,
    KAFKA_CLUSTER_ID,
)
from confluent_kafka.schema_registry.common.protobuf import is_map_field
from confluent_kafka.schema_registry.common.schema_registry_client import (
    AssociationCreateOrUpdateInfo,
    AssociationCreateOrUpdateRequest,
)
from confluent_kafka.schema_registry.common.serde import SubjectNameStrategyType
from confluent_kafka.schema_registry.protobuf import _schema_to_str
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

# Add proto directory to sys.path to resolve protobuf import dependencies
proto_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'proto')
if proto_path not in sys.path:
    sys.path.insert(0, proto_path)

from tests.schema_registry.data.proto import (  # noqa: E402
    cycle_pb2,
    dep_pb2,
    example_pb2,
    map_widget_pb2,
    nested_pb2,
    test_pb2,
)

_BASE_URL = "mock://"
# _BASE_URL = "http://localhost:8081"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


@pytest.fixture(autouse=True)
async def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Setup: fill with any logic you want

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


async def test_proto_basic_serialization():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'use.deprecated.format': False}
    obj = example_pb2.Author(
        name='Kafka', id=123, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser_conf = {'use.deprecated.format': False}
    deser = await AsyncProtobufDeserializer(example_pb2.Author, deser_conf, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_proto_guid_in_header():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'schema.id.serializer': header_schema_id_serializer}
    obj = example_pb2.Author(
        name='Kafka', id=123, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE, {})
    obj_bytes = await ser(obj, ser_ctx)

    deser_conf = {}
    deser = await AsyncProtobufDeserializer(example_pb2.Author, deser_conf, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_proto_use_schema_id_avoids_redundant_lookup_schema():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(
        name='Kafka', id=123, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    # Prime registry/store for this subject and capture its registered schema id.
    primer = await AsyncProtobufSerializer(
        example_pb2.Author, client, conf={'auto.register.schemas': True, 'use.deprecated.format': False}
    )
    await primer(obj, ser_ctx)
    registered = await client.get_latest_version(_SUBJECT, fmt='serialized')

    get_schema_calls = {'count': 0}
    lookup_calls = {'count': 0}
    original_get_schema = client.get_schema
    original_lookup_schema = client.lookup_schema

    async def patched_get_schema(schema_id, subject_name=None, fmt=None, reference_format=None):
        get_schema_calls['count'] += 1
        schema = await original_get_schema(schema_id, subject_name, fmt, reference_format)
        if subject_name is not None and registered.schema_id == schema_id:
            client._cache.set_registered_schema(registered.schema, registered)
        return schema

    async def patched_lookup_schema(subject_name, schema, normalize_schemas=False, fmt=None, deleted=False):
        lookup_calls['count'] += 1
        return await original_lookup_schema(subject_name, schema, normalize_schemas, fmt, deleted)

    client.get_schema = patched_get_schema
    client.lookup_schema = patched_lookup_schema

    serializer = await AsyncProtobufSerializer(
        example_pb2.Author,
        client,
        conf={
            'auto.register.schemas': False,
            'use.schema.id': registered.schema_id,
            'use.deprecated.format': False,
        },
    )
    await serializer(obj, ser_ctx)

    assert get_schema_calls['count'] == 1
    assert lookup_calls['count'] == 0


async def test_proto_basic_deserialization_no_client():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'use.deprecated.format': False}
    obj = example_pb2.Author(
        name='Kafka', id=123, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser_conf = {'use.deprecated.format': False}
    deser = await AsyncProtobufDeserializer(example_pb2.Author, deser_conf)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_proto_second_message():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'use.deprecated.format': False}
    obj = example_pb2.Pizza(
        size="large",
        toppings=["cheese", "pepperoni"],
    )
    ser = await AsyncProtobufSerializer(example_pb2.Pizza, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser_conf = {'use.deprecated.format': False}
    deser = await AsyncProtobufDeserializer(example_pb2.Pizza, deser_conf, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_proto_nested_message():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'use.deprecated.format': False}
    obj = nested_pb2.NestedMessage.InnerMessage(
        id="inner",
    )
    ser = await AsyncProtobufSerializer(nested_pb2.NestedMessage.InnerMessage, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser_conf = {'use.deprecated.format': False}
    deser = await AsyncProtobufDeserializer(nested_pb2.NestedMessage.InnerMessage, deser_conf, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_proto_reference():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'use.deprecated.format': False}
    msg = test_pb2.TestMessage(
        test_string="hi",
        test_bool=True,
        test_bytes=b'foobar',
        test_double=1.23,
        test_float=3.45,
        test_fixed32=67,
        test_fixed64=89,
        test_int32=100,
        test_int64=200,
        test_sfixed32=300,
        test_sfixed64=400,
        test_sint32=500,
        test_sint64=600,
        test_uint32=700,
        test_uint64=800,
    )
    obj = dep_pb2.DependencyMessage(is_active=True, test_message=msg)

    ser = await AsyncProtobufSerializer(dep_pb2.DependencyMessage, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser_conf = {'use.deprecated.format': False}
    deser = await AsyncProtobufDeserializer(dep_pb2.DependencyMessage, deser_conf, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_proto_cycle():
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'use.deprecated.format': False}
    inner = cycle_pb2.LinkedList(value=100)
    obj = cycle_pb2.LinkedList(value=200, next=inner)

    ser = await AsyncProtobufSerializer(cycle_pb2.LinkedList, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser_conf = {'use.deprecated.format': False}
    deser = await AsyncProtobufDeserializer(cycle_pb2.LinkedList, deser_conf, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2


async def test_associated_name_strategy_with_association():
    """Test that AssociatedNameStrategy returns subject from association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(
        name='Kafka', id=123, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="proto-resource-id-1",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="my-custom-subject-value",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_schema_to_str(example_pb2.Author.DESCRIPTOR.file),
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncProtobufDeserializer(example_pb2.Author, {'use.deprecated.format': False}, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    registered_schema = await client.get_latest_version("my-custom-subject-value")
    assert registered_schema is not None

    await client.delete_associations(resource_id="proto-resource-id-1", cascade_lifecycle=True)


async def test_associated_name_strategy_with_key_association():
    """Test that AssociatedNameStrategy returns subject for key"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(name='Kafka', id=42)

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="proto-resource-id-2",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="my-key-subject",
                association_type="key",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_schema_to_str(example_pb2.Author.DESCRIPTOR.file),
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.KEY)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncProtobufDeserializer(example_pb2.Author, {'use.deprecated.format': False}, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    registered_schema = await client.get_latest_version("my-key-subject")
    assert registered_schema is not None

    await client.delete_associations(resource_id="proto-resource-id-2", cascade_lifecycle=True)


async def test_associated_name_strategy_fallback_to_topic():
    """Test fallback to topic_subject_name_strategy when no association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(
        name='Kafka', id=456, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncProtobufDeserializer(example_pb2.Author, {'use.deprecated.format': False}, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    registered_schema = await client.get_latest_version(_TOPIC + "-value")
    assert registered_schema is not None


async def test_associated_name_strategy_fallback_to_record():
    """Test fallback to record_subject_name_strategy when configured"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(
        name='Kafka', id=789, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: SubjectNameStrategyType.RECORD},
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncProtobufDeserializer(example_pb2.Author, {'use.deprecated.format': False}, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    registered_schema = await client.get_latest_version(example_pb2.Author.DESCRIPTOR.full_name)
    assert registered_schema is not None


async def test_associated_name_strategy_fallback_to_topic_record():
    """Test fallback to topic_record_subject_name_strategy when configured"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(
        name='Kafka', id=100, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: SubjectNameStrategyType.TOPIC_RECORD},
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncProtobufDeserializer(example_pb2.Author, {'use.deprecated.format': False}, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    registered_schema = await client.get_latest_version(_TOPIC + "-" + example_pb2.Author.DESCRIPTOR.full_name)
    assert registered_schema is not None


async def test_associated_name_strategy_fallback_none_raises():
    """Test that NONE fallback raises an error when no association"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(name='Kafka', id=1)

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {FALLBACK_TYPE: "NONE"},
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    with pytest.raises(SerializationError) as exc_info:
        await ser(obj, ser_ctx)

    assert "No associated subject found" in str(exc_info.value)


async def test_associated_name_strategy_with_kafka_cluster_id():
    """Test that subject.name.strategy.kafka.cluster.id config is used as resource namespace"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)
    obj = example_pb2.Author(
        name='Kafka', id=100, picture=b'foobar', works=['The Castle', 'TheTrial'], oneof_string='oneof'
    )

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="my-cluster-id",
        resource_id="proto-resource-id-4",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="cluster-specific-proto-subject",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_schema_to_str(example_pb2.Author.DESCRIPTOR.file),
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
        'subject.name.strategy.conf': {KAFKA_CLUSTER_ID: "my-cluster-id"},
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = await ser(obj, ser_ctx)

    deser = await AsyncProtobufDeserializer(example_pb2.Author, {'use.deprecated.format': False}, client)
    obj2 = await deser(obj_bytes, ser_ctx)
    assert obj == obj2

    registered_schema = await client.get_latest_version("cluster-specific-proto-subject")
    assert registered_schema is not None

    await client.delete_associations(resource_id="proto-resource-id-4", cascade_lifecycle=True)


async def test_associated_name_strategy_caching():
    """Test that results are cached within a strategy instance and serializer works with caching"""
    conf = {'url': _BASE_URL}
    client = AsyncSchemaRegistryClient.new_client(conf)

    request = AssociationCreateOrUpdateRequest(
        resource_name=_TOPIC,
        resource_namespace="-",
        resource_id="proto-resource-id-5",
        resource_type="topic",
        associations=[
            AssociationCreateOrUpdateInfo(
                subject="proto-cached-subject",
                association_type="value",
                lifecycle="STRONG",
                schema=Schema(
                    schema_str=_schema_to_str(example_pb2.Author.DESCRIPTOR.file),
                ),
            )
        ],
    )
    await client.create_association(request)

    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False,
        'subject.name.strategy.type': SubjectNameStrategyType.ASSOCIATED,
    }
    ser = await AsyncProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)

    obj1 = example_pb2.Author(name='Kafka', id=1)
    obj_bytes1 = await ser(obj1, ser_ctx)

    registered_schema = await client.get_latest_version("proto-cached-subject")
    assert registered_schema is not None

    deser = await AsyncProtobufDeserializer(example_pb2.Author, {'use.deprecated.format': False}, client)
    result1 = await deser(obj_bytes1, ser_ctx)
    assert obj1 == result1

    # Delete associations (but serializer should still work due to caching)
    await client.delete_associations(resource_id="proto-resource-id-5", cascade_lifecycle=True)

    obj2 = example_pb2.Author(name='Kafka', id=2)
    obj_bytes2 = await ser(obj2, ser_ctx)

    result2 = await deser(obj_bytes2, ser_ctx)
    assert obj2 == result2


def test_is_map_field_identifies_map_fields():
    # Regression: is_map_field previously read the deprecated Descriptor.options
    # attribute, which upb (protobuf >= 7) does not expose, so it reported False for
    # every map field and _transform_field took the repeated branch instead.
    desc = map_widget_pb2.MapWidget.DESCRIPTOR
    assert is_map_field(desc.fields_by_name['labels']) is True
    assert is_map_field(desc.fields_by_name['tags']) is False
    assert is_map_field(desc.fields_by_name['name']) is False
