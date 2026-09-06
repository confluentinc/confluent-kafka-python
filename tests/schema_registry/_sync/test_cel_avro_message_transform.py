#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Message-level ``CEL`` transforms over Avro, and specifically that they **replace** rather than
merge: the rule's map is the whole new record, so a field the rule does not name takes the
schema's declared default rather than the value it had on the way in.

This case existed only on the protobuf side, and its absence hid a real defect elsewhere - the
C++ client seeded its result record from the input before applying the map, so it merged. Every
other C6/C7 case names *all* of the record's fields, which makes merge and replace
indistinguishable, the same way a condition fixture cannot tell a passing rule from an unfired
one without a must-fail twin.

Driven end to end through the serializer rather than through the executor alone, which is not
incidental: this client's Avro write-back hands fastavro the rule's result more or less
unchanged, so whether fastavro can fill an omitted field is the whole question. An
executor-level test cannot see it.
"""
import json

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import CelFieldExecutor
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, RuleSet
from confluent_kafka.serialization import MessageField, SerializationContext

CelExecutor.register()
CelFieldExecutor.register()

_TOPIC = "cel-avro-message-transform"

_SCHEMA = {
    "type": "record",
    "name": "Defaults",
    "fields": [
        {"name": "kept", "type": "string"},
        {"name": "withDefault", "type": "string", "default": "fallback"},
        {"name": "nullable", "type": ["null", "string"], "default": None},
    ],
}

_RECORD = {
    "kept": "original-kept",
    "withDefault": "original-withDefault",
    "nullable": "original-nullable",
}


def _round_trip(subject_suffix, expr):
    """Serializes the fixture under one message-level CEL transform and reads it back."""
    topic = _TOPIC + "-" + subject_suffix
    client = SchemaRegistryClient.new_client({"url": "mock://"})
    rule = Rule("r", "", RuleKind.TRANSFORM, RuleMode.WRITE, "CEL", None, None,
                expr, None, None, False)
    client.register_schema(topic + "-value",
                           Schema(json.dumps(_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))
    ser = AvroSerializer(client, schema_str=None,
                         conf={"auto.register.schemas": False, "use.latest.version": True})
    ctx = SerializationContext(topic, MessageField.VALUE)
    return AvroDeserializer(client)(ser(_RECORD, ctx), ctx)


def test_a_field_the_rule_does_not_name_takes_its_declared_default():
    """The case this file exists for. Under merge, `withDefault` would still read
    "original-withDefault"; under replace it takes the schema's declared default.

    `nullable` is the half that used to fail. fastavro fills an omitted field with
    ``datum.get(name, field.get("default"))``, and celpy's MapType.get *raises* KeyError when
    the key is absent and the default is None - so a null default blew up where a non-null one
    worked. The executor now hands fastavro a plain dict.
    """
    out = _round_trip("drop", '{"kept": message.kept}')

    assert out["kept"] == "original-kept"
    assert out["withDefault"] == "fallback"
    assert out["withDefault"] != "original-withDefault", "merged instead of replacing"
    assert out["nullable"] is None


def test_naming_every_field_round_trips():
    """The must-fail twin. Without it, "the other fields took their defaults" is equally
    consistent with the transform having stopped working altogether."""
    out = _round_trip("all", '{"kept": message.kept, "withDefault": message.withDefault, '
                             '"nullable": message.nullable}')

    assert out == _RECORD
