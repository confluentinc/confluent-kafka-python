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
import time

import pytest

from confluent_kafka.schema_registry import SchemaRegistryClient, \
    Schema, Metadata, MetadataProperties
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, \
    ProtobufDeserializer, _schema_to_str
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import \
    CelFieldExecutor
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import \
    AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import \
    AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor, Clock
from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import \
    GcpKmsDriver
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import \
    HcVaultKmsDriver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import \
    LocalKmsDriver
from confluent_kafka.schema_registry.rules.jsonata.jsonata_executor import \
    JsonataExecutor
from confluent_kafka.schema_registry.schema_registry_client import RuleSet, \
    Rule, RuleKind, RuleMode, RuleParams, ServerConfig
from confluent_kafka.schema_registry.serde import RuleConditionError
from confluent_kafka.serialization import SerializationContext, MessageField
from .data.proto import example_pb2, nested_pb2, test_pb2, dep_pb2, cycle_pb2, \
    widget_pb2, newwidget_pb2, newerwidget_pb2


class FakeClock(Clock):

    def __init__(self):
        self.fixed_now = int(round(time.time() * 1000))

    def now(self) -> int:
        return self.fixed_now


CelExecutor.register()
CelFieldExecutor.register()
AwsKmsDriver.register()
AzureKmsDriver.register()
GcpKmsDriver.register()
HcVaultKmsDriver.register()
JsonataExecutor.register()
LocalKmsDriver.register()

_BASE_URL = "mock://"
# _BASE_URL = "http://localhost:8081"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Setup: fill with any logic you want

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


def test_proto_basic_serialization():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_basic_deserialization_no_client():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_second_message():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    obj = example_pb2.Pizza(
        size="large",
        toppings=["cheese", "pepperoni"],
    )
    ser = ProtobufSerializer(example_pb2.Pizza, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Pizza, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_nested_message():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    obj = nested_pb2.NestedMessage.InnerMessage(
        id="inner",
    )
    ser = ProtobufSerializer(nested_pb2.NestedMessage.InnerMessage, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(nested_pb2.NestedMessage.InnerMessage, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_reference():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
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
    obj = dep_pb2.DependencyMessage(
        is_active=True,
        test_message=msg
    )

    ser = ProtobufSerializer(dep_pb2.DependencyMessage, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(dep_pb2.DependencyMessage, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_cycle():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    inner = cycle_pb2.LinkedList(
        value=100
    )
    obj = cycle_pb2.LinkedList(
        value=200,
        next=inner
    )

    ser = ProtobufSerializer(cycle_pb2.LinkedList, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(cycle_pb2.LinkedList, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_cel_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.name == 'Kafka'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_cel_condition_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.name != 'Kafka'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    try:
        ser(obj, ser_ctx)
    except Exception as e:
        assert isinstance(e.__cause__, RuleConditionError)


def test_proto_cel_field_transform():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'name' ; value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = example_pb2.Author(
        name='Kafka-suffix',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_proto_cel_field_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'name' ; value == 'Kafka'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj == newobj


def test_proto_cel_field_condition_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'name' ; value != 'Kafka'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    try:
        ser(obj, ser_ctx)
    except Exception as e:
        assert isinstance(e.__cause__, RuleConditionError)


def test_proto_encryption():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule_conf = {'secret': 'mysecret'}
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
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client, rule_conf=rule_conf)
    executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_jsonata_fully_compatible():
    rule1_to_2 = "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"
    rule2_to_1 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])"
    rule2_to_3 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])"
    rule3_to_2 = "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])"

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)

    client.set_config(_SUBJECT, ServerConfig(
        compatibility_group='application.version'
    ))

    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(widget_pb2.Widget.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v1"}),
            None
        ),
        None
    ))

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
        _schema_to_str(newwidget_pb2.NewWidget.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v2"}),
            None
        ),
        RuleSet([rule1, rule2], None)
    ))

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
        _schema_to_str(newerwidget_pb2.NewerWidget.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v3"}),
            None
        ),
        RuleSet([rule3, rule4], None)
    ))

    obj = widget_pb2.Widget(
        name='alice',
        size=123,
        version=1
    )
    obj2 = newwidget_pb2.NewWidget(
        name='alice',
        height=123,
        version=1
    )
    obj3 = newerwidget_pb2.NewerWidget(
        name='alice',
        length=123,
        version=1
    )

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v1'
        },
        'use.deprecated.format': False
    }
    ser = ProtobufSerializer(widget_pb2.Widget, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v2'
        },
        'use.deprecated.format': False
    }
    ser = ProtobufSerializer(newwidget_pb2.NewWidget, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj2, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v3'
        },
        'use.deprecated.format': False
    }
    ser = ProtobufSerializer(newerwidget_pb2.NewerWidget, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj3, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)


def deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3):
    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v1'
        },
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(widget_pb2.Widget, deser_conf, client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj.size == newobj.size

    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v2'
        },
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(newwidget_pb2.NewWidget, deser_conf, client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2.height == newobj.height

    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v3'
        },
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(newerwidget_pb2.NewerWidget, deser_conf, client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj3.length == newobj.length
