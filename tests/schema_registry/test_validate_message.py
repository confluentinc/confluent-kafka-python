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
Walker-level tests for the per-format ``validate_message`` functions. These use a stub
executor that always fails, so every rule the walker fires becomes a violation; the
assertions are on rule names paired with field paths, which is what verifies the
walker's dispatch (recursion into nested records, array/map iteration, skip-on-null).
"""

from typing import Any, List

import pytest
from fastavro.schema import parse_schema
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from referencing import Registry, Resource

from confluent_kafka.schema_registry import MessageField, SerializationContext
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode

from confluent_kafka.schema_registry.common.avro import validate_message as validate_avro
from confluent_kafka.schema_registry.common.json_schema import DEFAULT_SPEC
from confluent_kafka.schema_registry.common.json_schema import transform as transform_json
from confluent_kafka.schema_registry.common.json_schema import validate_message as validate_json
from confluent_kafka.schema_registry.common.protobuf import validate_message as validate_protobuf
from confluent_kafka.schema_registry.confluent import meta_pb2
from confluent_kafka.schema_registry.rules.cel.cel_validator import CelValidator
from confluent_kafka.schema_registry.serde import (
    RuleContext,
    ValidationRule,
    ValidationRuleError,
    ValidationRuleExecutor,
)

from .data.proto import validation_widget_pb2


class _AlwaysFail(ValidationRuleExecutor):
    """Every rule fires a violation, so we can inspect exactly what the walker did."""

    def execute(self, rule: ValidationRule, schema: Any, message: Any) -> Any:
        return False


ALWAYS_FAIL = _AlwaysFail()

_RULE = [{"name": "r", "expr": "true"}]


def fired(errors: List[ValidationRuleError]) -> List[str]:
    """Names of all rules the walker fired, paired with their field paths."""
    return [f"{e.rule.name}@{e.field_path}" for e in errors]


# --------------------------------------------------------------------------------------
# Avro
# --------------------------------------------------------------------------------------


def _avro(schema: dict, message: Any, fail_fast: bool = False) -> List[str]:
    return fired(validate_avro(ALWAYS_FAIL, parse_schema(schema), message, fail_fast))


_AVRO_ARRAY_OF_RECORDS = {
    "type": "record",
    "name": "Outer",
    "fields": [
        {
            "name": "items",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Item",
                    "fields": [{"name": "x", "type": "int", "confluent:rules": _RULE}],
                },
            },
        }
    ],
}


def test_avro_nested_record_recurses_and_produces_dotted_path():
    schema = {
        "type": "record",
        "name": "Outer",
        "fields": [
            {
                "name": "inner",
                "type": {
                    "type": "record",
                    "name": "Inner",
                    "fields": [{"name": "x", "type": "int", "confluent:rules": _RULE}],
                },
            }
        ],
    }
    assert _avro(schema, {"inner": {"x": 5}}) == ["r@inner.x"]


def test_avro_array_of_records_fires_rule_per_element_with_indexed_path():
    assert _avro(_AVRO_ARRAY_OF_RECORDS, {"items": [{"x": 1}, {"x": 2}]}) == ["r@items[0].x", "r@items[1].x"]


def test_avro_fail_fast_stops_after_first_violation():
    assert _avro(_AVRO_ARRAY_OF_RECORDS, {"items": [{"x": 1}, {"x": 2}]}, fail_fast=True) == ["r@items[0].x"]


def test_avro_map_of_records_fires_rule_per_entry_with_keyed_path():
    schema = {
        "type": "record",
        "name": "Outer",
        "fields": [
            {
                "name": "scores",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "Score",
                        "fields": [{"name": "v", "type": "int", "confluent:rules": _RULE}],
                    },
                },
            }
        ],
    }
    assert _avro(schema, {"scores": {"alice": {"v": 10}, "bob": {"v": 20}}}) == [
        'r@scores["alice"].v',
        'r@scores["bob"].v',
    ]


def test_avro_record_level_rule_fires_at_root():
    schema = {
        "type": "record",
        "name": "Outer",
        "confluent:rules": [{"name": "rr", "expr": "true"}],
        "fields": [{"name": "x", "type": "int"}],
    }
    assert _avro(schema, {"x": 1}) == ["rr@"]


@pytest.mark.parametrize(
    "message, expected",
    [
        ({"maybeName": None}, []),
        ({}, []),
        ({"maybeName": "alice"}, ["r@maybeName"]),
    ],
)
def test_avro_nullable_field_skips_rule_when_value_is_null(message, expected):
    schema = {
        "type": "record",
        "name": "Outer",
        "fields": [{"name": "maybeName", "type": ["null", "string"], "default": None, "confluent:rules": _RULE}],
    }
    assert _avro(schema, message) == expected


def test_avro_multiple_rules_on_same_field_all_fire():
    schema = {
        "type": "record",
        "name": "Outer",
        "fields": [
            {
                "name": "x",
                "type": "int",
                "confluent:rules": [{"name": "r1", "expr": "true"}, {"name": "r2", "expr": "true"}],
            }
        ],
    }
    assert _avro(schema, {"x": 7}) == ["r1@x", "r2@x"]


def test_avro_no_rules_produces_no_violations():
    schema = {"type": "record", "name": "Outer", "fields": [{"name": "x", "type": "int"}]}
    assert _avro(schema, {"x": 1}) == []


def test_avro_malformed_rules_prop_is_ignored():
    schema = {
        "type": "record",
        "name": "Outer",
        "fields": [{"name": "x", "type": "int", "confluent:rules": "not-a-list"}],
    }
    assert _avro(schema, {"x": 1}) == []


# --------------------------------------------------------------------------------------
# JSON Schema
# --------------------------------------------------------------------------------------


def _json(schema: dict, message: Any, fail_fast: bool = False) -> List[str]:
    resource = Resource.from_contents(schema, default_specification=DEFAULT_SPEC)
    registry = Registry().with_resource("", resource)
    return fired(validate_json(ALWAYS_FAIL, schema, registry, registry.resolver(), message, fail_fast))


_JSON_ARRAY_OF_OBJECTS = {
    "type": "object",
    "properties": {
        "items": {
            "type": "array",
            "items": {"type": "object", "properties": {"x": {"type": "integer", "confluent:rules": _RULE}}},
        }
    },
}


def test_json_nested_object_recurses_and_produces_dotted_path():
    schema = {
        "type": "object",
        "properties": {"inner": {"type": "object", "properties": {"x": {"type": "integer", "confluent:rules": _RULE}}}},
    }
    assert _json(schema, {"inner": {"x": 5}}) == ["r@$.inner.x"]


def test_json_array_of_objects_fires_rule_per_element_with_indexed_path():
    assert _json(_JSON_ARRAY_OF_OBJECTS, {"items": [{"x": 1}, {"x": 2}]}) == [
        "r@$.items[0].x",
        "r@$.items[1].x",
    ]


def test_json_fail_fast_stops_after_first_violation():
    assert _json(_JSON_ARRAY_OF_OBJECTS, {"items": [{"x": 1}, {"x": 2}]}, fail_fast=True) == ["r@$.items[0].x"]


def test_json_one_of_fires_rule_only_on_matching_subschema():
    schema = {
        "oneOf": [
            {
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "required": ["a"],
                "confluent:rules": [{"name": "matchA", "expr": "true"}],
            },
            {
                "type": "object",
                "properties": {"b": {"type": "integer"}},
                "required": ["b"],
                "confluent:rules": [{"name": "matchB", "expr": "true"}],
            },
        ]
    }
    assert _json(schema, {"a": "hi"}) == ["matchA@$"]


def test_json_reference_schema_resolves_and_fires_rule():
    schema = {
        "definitions": {
            "Inner": {"type": "object", "properties": {"x": {"type": "integer", "confluent:rules": _RULE}}}
        },
        "type": "object",
        "properties": {"inner": {"$ref": "#/definitions/Inner"}},
    }
    assert _json(schema, {"inner": {"x": 5}}) == ["r@$.inner.x"]


def test_json_multi_type_schema_is_never_mutated():
    # Narrowing a type array to the branch the message satisfies must not touch the
    # schema, which is parsed once and shared across concurrent serializations. The
    # executor observes it mid-walk, when a temporary narrowing would still be in place.
    schema = {
        "type": ["object", "null"],
        "properties": {"x": {"type": "integer", "confluent:rules": _RULE}},
    }
    observed = []

    class _Observer(ValidationRuleExecutor):
        def execute(self, rule: ValidationRule, subschema: Any, message: Any) -> Any:
            observed.append(schema["type"])
            return False

    resource = Resource.from_contents(schema, default_specification=DEFAULT_SPEC)
    registry = Registry().with_resource("", resource)
    violations = validate_json(_Observer(), schema, registry, registry.resolver(), {"x": 5}, False)

    assert fired(violations) == ["r@$.x"]
    assert observed == [["object", "null"]]
    assert schema["type"] == ["object", "null"]


def test_json_object_level_rule_fires_at_root():
    schema = {
        "type": "object",
        "confluent:rules": [{"name": "rr", "expr": "true"}],
        "properties": {"x": {"type": "integer"}},
    }
    assert _json(schema, {"x": 1}) == ["rr@$"]


@pytest.mark.parametrize(
    "message, expected",
    [
        ({}, []),
        ({"maybeName": None}, []),
        ({"maybeName": "alice"}, ["r@$.maybeName"]),
    ],
)
def test_json_nullable_property_skips_rule_when_value_is_null(message, expected):
    schema = {
        "type": "object",
        "properties": {"maybeName": {"type": ["string", "null"], "confluent:rules": _RULE}},
    }
    assert _json(schema, message) == expected


def test_json_multiple_rules_on_same_property_all_fire():
    schema = {
        "type": "object",
        "properties": {
            "x": {
                "type": "integer",
                "confluent:rules": [{"name": "r1", "expr": "true"}, {"name": "r2", "expr": "true"}],
            }
        },
    }
    assert _json(schema, {"x": 7}) == ["r1@$.x", "r2@$.x"]


# --------------------------------------------------------------------------------------
# Protobuf
# --------------------------------------------------------------------------------------


def _proto(message: Any, fail_fast: bool = False) -> List[str]:
    return fired(validate_protobuf(ALWAYS_FAIL, message.DESCRIPTOR, message, fail_fast))


# The rule on the repeated `tags` field has no presence, so it is never skipped and fires
# on every ValidationOuter — matching the JVM client, which binds the empty collection to
# `this`. Fields are walked in field-number order, so `tags` (5) always comes last.
_TAGS = ["tagsNotEmpty@tags"]


def test_protobuf_nested_message_recurses_and_produces_dotted_path():
    message = validation_widget_pb2.ValidationOuter(inner=validation_widget_pb2.ValidationInner(x=5))
    assert _proto(message) == ["r@inner.x"] + _TAGS


def test_protobuf_repeated_message_fires_per_element_message_rule():
    message = validation_widget_pb2.ValidationOuter(
        items=[validation_widget_pb2.ValidationItem(v=1), validation_widget_pb2.ValidationItem(v=2)]
    )
    assert _proto(message) == ["itemRule@items[0]", "itemRule@items[1]"] + _TAGS


def test_protobuf_fail_fast_stops_after_first_violation():
    message = validation_widget_pb2.ValidationOuter(
        items=[validation_widget_pb2.ValidationItem(v=1), validation_widget_pb2.ValidationItem(v=2)]
    )
    assert _proto(message, fail_fast=True) == ["itemRule@items[0]"]


def test_protobuf_optional_field_skips_rule_when_unset():
    # `maybe` unset, and `inner` unset so neither of its rules fire either.
    assert _proto(validation_widget_pb2.ValidationOuter()) == _TAGS
    assert _proto(validation_widget_pb2.ValidationOuter(maybe="hi")) == ["maybeNotEmpty@maybe"] + _TAGS


def test_protobuf_map_field_descends_into_message_values_with_keyed_path():
    message = validation_widget_pb2.ValidationOuter(labels={"a": validation_widget_pb2.ValidationItem(v=1)})
    assert _proto(message) == ['itemRule@labels["a"]'] + _TAGS


def test_protobuf_message_and_field_rules_both_fire():
    message = validation_widget_pb2.ValidationPerson(age=30, name="Alice")
    assert _proto(message) == ["ageNotInsane@", "agePositive@age", "nameNotEmpty@name"]


def _evolved_person_descriptor():
    """
    The ValidationPerson descriptor with an extra rule-carrying field the generated
    class does not have, standing in for a registered schema that has evolved past
    the message class in use (the use.latest.version case).
    """
    fdp = descriptor_pb2.FileDescriptorProto()
    validation_widget_pb2.DESCRIPTOR.CopyToProto(fdp)
    person = next(m for m in fdp.message_type if m.name == "ValidationPerson")
    added = person.field.add()
    added.name = "nickname"
    added.number = 99
    added.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
    added.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    rule = added.options.Extensions[meta_pb2.field_meta].rules.add()
    rule.name = "nicknameNotEmpty"
    rule.expr = "size(this) > 0"

    pool = DescriptorPool()
    added_files = set()

    def add_deps(file_descriptor):
        for dep in file_descriptor.dependencies:
            add_deps(dep)
            if dep.name in added_files:
                continue
            added_files.add(dep.name)
            dep_proto = descriptor_pb2.FileDescriptorProto()
            dep.CopyToProto(dep_proto)
            pool.Add(dep_proto)

    add_deps(validation_widget_pb2.DESCRIPTOR)
    fdp.name = "evolved_validation_widget.proto"
    return pool.Add(fdp).message_types_by_name["ValidationPerson"]


def test_protobuf_schema_field_missing_from_message_class_is_skipped():
    # The walk is driven by the message's own fields: the schema's extra field has no
    # counterpart to read, and validating must not fail because of it.
    message = validation_widget_pb2.ValidationPerson(age=30, name="Alice")
    violations = validate_protobuf(ALWAYS_FAIL, _evolved_person_descriptor(), message, False)
    assert fired(violations) == ["ageNotInsane@", "agePositive@age", "nameNotEmpty@name"]


def test_json_transform_visits_every_scalar_type():
    # The transform walk - unlike the validation walk, which dispatches structurally -
    # dispatches on the FieldType a schema type name maps to, so a mapping that names a type
    # JSON Schema does not have would silently skip those fields: no transform, no error.
    # Go had exactly that bug with "int" instead of "integer".
    schema = {
        "type": "object",
        "properties": {
            "s": {"type": "string"},
            "i": {"type": "integer"},
            "n": {"type": "number"},
            "b": {"type": "boolean"},
        },
    }
    visited = []

    def field_transform(ctx, field_ctx, value):
        visited.append(field_ctx.name)
        return value

    resource = Resource.from_contents(schema, default_specification=DEFAULT_SPEC)
    registry = Registry().with_resource("", resource)
    rule = Rule("t", None, RuleKind.TRANSFORM, RuleMode.WRITE, "TEST", None, None, None, None, None, False)
    ctx = RuleContext(
        None,
        SerializationContext("topic", MessageField.VALUE),
        None,
        None,
        "topic-value",
        RuleMode.WRITE,
        rule,
        0,
        [rule],
        {},
        None,
    )
    message = {"s": "x", "i": 1, "n": 1.5, "b": True}

    transform_json(ctx, schema, registry, registry.resolver(), "$", message, field_transform)

    assert sorted(visited) == ["b", "i", "n", "s"], f'a scalar type was skipped: {visited}'


def _renamed_person_descriptor_with_message_rule():
    """
    ValidationPerson with field 2 renamed and a message-level rule that refers to the new
    name, standing in for a registered schema whose field names have moved on from the
    generated class.
    """
    fdp = descriptor_pb2.FileDescriptorProto()
    validation_widget_pb2.DESCRIPTOR.CopyToProto(fdp)
    person = next(m for m in fdp.message_type if m.name == "ValidationPerson")
    renamed = next(f for f in person.field if f.number == 2)
    renamed.name = "renamed"
    renamed.json_name = "renamed"
    del person.options.Extensions[meta_pb2.message_meta].rules[:]
    rule = person.options.Extensions[meta_pb2.message_meta].rules.add()
    rule.name = "nameIsAlice"
    rule.expr = "this.renamed == 'Alice'"

    pool = DescriptorPool()
    added_files = set()

    def add_deps(file_descriptor):
        for dep in file_descriptor.dependencies:
            add_deps(dep)
            if dep.name in added_files:
                continue
            added_files.add(dep.name)
            dep_proto = descriptor_pb2.FileDescriptorProto()
            dep.CopyToProto(dep_proto)
            pool.Add(dep_proto)

    add_deps(validation_widget_pb2.DESCRIPTOR)
    fdp.name = "renamed_message_rule_widget.proto"
    return pool.Add(fdp).message_types_by_name["ValidationPerson"]


def test_protobuf_message_level_rule_sees_schema_names():
    # A message-level rule binds `this` to the message and its CEL environment is built from
    # the registered schema, so the message it evaluates has to be in the schema's terms too.
    # Otherwise a rule written against a renamed field reads a missing field and rejects a
    # valid message.
    message = validation_widget_pb2.ValidationPerson(age=30, name="Alice")
    violations = validate_protobuf(CelValidator(), _renamed_person_descriptor_with_message_rule(), message, False)
    assert fired(violations) == [], f'the rule was evaluated against the wrong names: {violations}'
