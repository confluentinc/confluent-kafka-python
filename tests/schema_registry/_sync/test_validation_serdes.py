#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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
"""
Serializer-level tests for inline validation rules — these exercise the
``validation.rules.execution`` wiring. Per-rule CEL semantics are covered in
test_cel_validator.py and walker dispatch in test_validate_message.py.
"""

import json

import pytest

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.common.protobuf import _schema_to_str
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.rules.cel.cel_validator import CelValidator
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from ..data.proto import validation_widget_pb2

_BASE_URL = "mock://"
_TOPIC = "person"
_SUBJECT = _TOPIC + "-value"

# Record-level rule plus two field-level rules, matching the JVM client's test layout.
_AVRO_SCHEMA = {
    "type": "record",
    "name": "Person",
    "namespace": "test",
    "confluent:rules": [{"name": "ageNotInsane", "expr": "this.age <= 150"}],
    "fields": [
        {"name": "age", "type": "int", "confluent:rules": [{"name": "agePositive", "expr": "this >= 0"}]},
        {
            "name": "name",
            "type": "string",
            "confluent:rules": [{"name": "nameNotEmpty", "expr": "size(this) > 0"}],
        },
    ],
}

_JSON_SCHEMA = {
    "type": "object",
    "title": "Person",
    "confluent:rules": [{"name": "ageNotInsane", "expr": "this.age <= 150"}],
    "properties": {
        "age": {"type": "integer", "confluent:rules": [{"name": "agePositive", "expr": "this >= 0"}]},
        "name": {"type": "string", "confluent:rules": [{"name": "nameNotEmpty", "expr": "size(this) > 0"}]},
    },
}


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    yield
    client = SchemaRegistryClient.new_client({'url': _BASE_URL})
    for subject in client.get_subjects():
        try:
            client.delete_subject(subject, True)
        except Exception:
            pass


def ser_ctx():
    return SerializationContext(_TOPIC, MessageField.VALUE)


def client():
    return SchemaRegistryClient.new_client({'url': _BASE_URL})


# --------------------------------------------------------------------------------------
# Avro
# --------------------------------------------------------------------------------------


def avro_serializer(**conf):
    return AvroSerializer(client(), json.dumps(_AVRO_SCHEMA), conf=conf)


def test_avro_serialization_passes_when_all_rules_pass():
    ser = avro_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    assert ser({"age": 30, "name": "Alice"}, ser_ctx()) is not None


def test_avro_serialization_passes_when_validation_disabled():
    # age=-5 would fail agePositive, but validation is disabled by default.
    ser = avro_serializer()
    assert ser({"age": -5, "name": "Alice"}, ser_ctx()) is not None


def test_avro_serialization_fails_when_field_rule_fails():
    ser = avro_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser({"age": -5, "name": "Alice"}, ser_ctx())
    assert "agePositive" in str(exc.value)


def test_avro_serialization_fails_when_record_rule_fails():
    ser = avro_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser({"age": 200, "name": "Alice"}, ser_ctx())
    assert "ageNotInsane" in str(exc.value)


def test_avro_serialization_reports_every_violation():
    ser = avro_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser({"age": -5, "name": ""}, ser_ctx())
    message = str(exc.value)
    assert "2 violations" in message
    assert "agePositive" in message
    assert "nameNotEmpty" in message


def test_avro_fail_fast_reports_a_single_violation():
    ser = avro_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES', 'validation.rules.fail.fast': True})
    with pytest.raises(SerializationError) as exc:
        ser({"age": -5, "name": ""}, ser_ctx())
    message = str(exc.value)
    assert "1 violation" in message
    assert "nameNotEmpty" not in message


@pytest.mark.parametrize("mode", ["BEFORE_DOMAIN_RULES", "AFTER_DOMAIN_RULES"])
def test_avro_both_modes_validate_when_no_domain_rules_exist(mode):
    ser = avro_serializer(**{'validation.rules.execution': mode})
    with pytest.raises(SerializationError, match="agePositive"):
        ser({"age": -5, "name": "Alice"}, ser_ctx())


def test_avro_validation_runs_with_use_latest_version():
    # The registry-resolved schema path, as opposed to the serializer's own schema.
    sr = client()
    sr.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO"))
    ser = AvroSerializer(
        sr,
        json.dumps(_AVRO_SCHEMA),
        conf={
            'auto.register.schemas': False,
            'use.latest.version': True,
            'validation.rules.execution': 'AFTER_DOMAIN_RULES',
        },
    )
    assert ser({"age": 30, "name": "Alice"}, ser_ctx()) is not None
    with pytest.raises(SerializationError, match="agePositive"):
        ser({"age": -5, "name": "Alice"}, ser_ctx())


def test_avro_explicit_executor_is_used():
    ser = avro_serializer(
        **{'validation.rules.execution': 'AFTER_DOMAIN_RULES', 'validation.rules.executor': CelValidator()}
    )
    with pytest.raises(SerializationError, match="agePositive"):
        ser({"age": -5, "name": "Alice"}, ser_ctx())


@pytest.mark.parametrize(
    "conf, match",
    [
        ({'validation.rules.execution': 'NOPE'}, "validation.rules.execution must be one of"),
        ({'validation.rules.fail.fast': 'yes'}, "validation.rules.fail.fast must be a boolean"),
        (
            {'validation.rules.execution': 'AFTER_DOMAIN_RULES', 'validation.rules.executor': object()},
            "validation.rules.executor must be a ValidationRuleExecutor",
        ),
    ],
)
def test_invalid_validation_config_is_rejected(conf, match):
    with pytest.raises(ValueError, match=match):
        avro_serializer(**conf)


# --------------------------------------------------------------------------------------
# JSON Schema
# --------------------------------------------------------------------------------------


def json_serializer(**conf):
    return JSONSerializer(json.dumps(_JSON_SCHEMA), client(), conf=conf)


def test_json_serialization_passes_when_all_rules_pass():
    ser = json_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    assert ser({"age": 30, "name": "Alice"}, ser_ctx()) is not None


def test_json_serialization_passes_when_validation_disabled():
    ser = json_serializer()
    assert ser({"age": -5, "name": "Alice"}, ser_ctx()) is not None


def test_json_serialization_fails_when_field_rule_fails():
    ser = json_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser({"age": -5, "name": "Alice"}, ser_ctx())
    assert "agePositive" in str(exc.value)
    # JSON paths are rooted at $, matching the JVM client.
    assert "$.age" in str(exc.value)


def test_json_serialization_reports_every_violation():
    ser = json_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser({"age": -5, "name": ""}, ser_ctx())
    message = str(exc.value)
    assert "2 violations" in message
    assert "agePositive" in message
    assert "nameNotEmpty" in message


def test_json_validation_runs_with_use_latest_version():
    sr = client()
    sr.register_schema(_SUBJECT, Schema(json.dumps(_JSON_SCHEMA), "JSON"))
    ser = JSONSerializer(
        json.dumps(_JSON_SCHEMA),
        sr,
        conf={
            'auto.register.schemas': False,
            'use.latest.version': True,
            'validation.rules.execution': 'AFTER_DOMAIN_RULES',
        },
    )
    assert ser({"age": 30, "name": "Alice"}, ser_ctx()) is not None
    with pytest.raises(SerializationError, match="agePositive"):
        ser({"age": -5, "name": "Alice"}, ser_ctx())


# --------------------------------------------------------------------------------------
# Protobuf
# --------------------------------------------------------------------------------------


def protobuf_serializer(**conf):
    return ProtobufSerializer(
        validation_widget_pb2.ValidationPerson, client(), conf={'auto.register.schemas': True, **conf}
    )


def person(age, name):
    return validation_widget_pb2.ValidationPerson(age=age, name=name)


def test_protobuf_serialization_passes_when_all_rules_pass():
    ser = protobuf_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    assert ser(person(30, "Alice"), ser_ctx()) is not None


def test_protobuf_serialization_passes_when_validation_disabled():
    ser = protobuf_serializer()
    assert ser(person(-5, "Alice"), ser_ctx()) is not None


def test_protobuf_serialization_fails_when_field_rule_fails():
    ser = protobuf_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser(person(-5, "Alice"), ser_ctx())
    message = str(exc.value)
    assert "agePositive" in message
    # The rule's doc is preferred over its expression in the failure text.
    assert "age must not be negative" in message


def test_protobuf_serialization_fails_when_message_rule_fails():
    ser = protobuf_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser(person(200, "Alice"), ser_ctx())
    assert "ageNotInsane" in str(exc.value)


def test_protobuf_serialization_reports_every_violation():
    ser = protobuf_serializer(**{'validation.rules.execution': 'AFTER_DOMAIN_RULES'})
    with pytest.raises(SerializationError) as exc:
        ser(person(200, ""), ser_ctx())
    message = str(exc.value)
    assert "2 violations" in message
    assert "ageNotInsane" in message
    assert "nameNotEmpty" in message


def test_protobuf_validation_runs_with_use_latest_version():
    sr = client()
    sr.register_schema(
        _SUBJECT,
        Schema(_schema_to_str(validation_widget_pb2.ValidationPerson.DESCRIPTOR.file), "PROTOBUF"),
    )
    ser = ProtobufSerializer(
        validation_widget_pb2.ValidationPerson,
        sr,
        conf={
            'auto.register.schemas': False,
            'use.latest.version': True,
            'validation.rules.execution': 'AFTER_DOMAIN_RULES',
        },
    )
    assert ser(person(30, "Alice"), ser_ctx()) is not None
    with pytest.raises(SerializationError, match="agePositive"):
        ser(person(-5, "Alice"), ser_ctx())


def test_protobuf_dynamic_failure_message_is_reported():
    ser = ProtobufSerializer(
        validation_widget_pb2.ValidationDynamicMessage,
        client(),
        conf={'auto.register.schemas': True, 'validation.rules.execution': 'AFTER_DOMAIN_RULES'},
    )
    with pytest.raises(SerializationError) as exc:
        ser(validation_widget_pb2.ValidationDynamicMessage(age=-5), ser_ctx())
    assert "age must be positive, got -5" in str(exc.value)
