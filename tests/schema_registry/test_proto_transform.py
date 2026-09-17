#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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
Tests for the protobuf field transform walk, which is what drives field-level rules
such as CSFLE encryption. The walk is driven by the message's own fields, matched by
name to the schema-side descriptor that carries the inline tags, and it descends into
nested, repeated and map-valued messages.
"""

from typing import Any

from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool

from confluent_kafka.schema_registry import MessageField, SerializationContext
from confluent_kafka.schema_registry.common.protobuf import transform
from confluent_kafka.schema_registry.confluent import meta_pb2
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import CelFieldExecutor
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, Schema
from confluent_kafka.schema_registry.serde import RuleContext

from .data.proto import validation_widget_pb2
from .test_validate_message import _evolved_person_descriptor


def _rule_context(tags: Any = None) -> RuleContext:
    rule = Rule("t", None, RuleKind.TRANSFORM, RuleMode.WRITE, "TEST", tags, None, None, None, None, False)
    return RuleContext(
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


def _bump(ctx: RuleContext, field_ctx: Any, value: Any) -> Any:
    """Uppercase strings and increment ints, so every visited leaf is observable."""
    if isinstance(value, str):
        return value.upper()
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value + 1
    return value


def test_transform_descends_into_nested_repeated_and_map_messages():
    message = validation_widget_pb2.ValidationOuter(
        inner=validation_widget_pb2.ValidationInner(x=5),
        items=[validation_widget_pb2.ValidationItem(v=1)],
        labels={"a": validation_widget_pb2.ValidationItem(v=2)},
        tags=["t"],
        maybe="m",
    )
    transform(_rule_context(), message.DESCRIPTOR, message, _bump)

    assert message.inner.x == 6
    assert [item.v for item in message.items] == [2]
    assert message.labels["a"].v == 3
    assert list(message.tags) == ["T"]
    assert message.maybe == "M"


def test_transform_ignores_schema_fields_absent_from_the_message_class():
    # The registered schema has a field the generated class does not (use.latest.version),
    # which must not stop the fields the message does have from being transformed.
    message = validation_widget_pb2.ValidationPerson(age=30, name="Alice")
    transform(_rule_context(), _evolved_person_descriptor(), message, _bump)

    assert message.age == 31
    assert message.name == "ALICE"


def test_transform_leaves_absent_fields_absent():
    # A field with explicit presence that is unset has nothing to transform, and writing a
    # value back would materialize it: an absent message or unset optional scalar would
    # become present, carrying a transformed default.
    message = validation_widget_pb2.ValidationOuter(tags=["t"])
    transform(_rule_context(), message.DESCRIPTOR, message, _bump)

    assert not message.HasField('inner'), 'the absent message was materialized'
    assert not message.HasField('maybe'), 'the unset optional scalar was materialized'
    # The fields that are present are still transformed.
    assert list(message.tags) == ["T"]


def _renamed_person_descriptor():
    """
    ValidationPerson with field 2 renamed and tagged, standing in for a registered schema
    whose field names have moved on from the generated class (a compatible change, since
    protobuf identifies a field by its number).
    """
    fdp = descriptor_pb2.FileDescriptorProto()
    validation_widget_pb2.DESCRIPTOR.CopyToProto(fdp)
    person = next(m for m in fdp.message_type if m.name == "ValidationPerson")
    renamed = next(f for f in person.field if f.number == 2)
    renamed.name = "renamed"
    renamed.json_name = "renamed"
    renamed.options.Extensions[meta_pb2.field_meta].tags.append("PII")

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
    fdp.name = "renamed_validation_widget.proto"
    return pool.Add(fdp).message_types_by_name["ValidationPerson"]


def test_transform_resolves_renamed_fields_by_number():
    # Renaming a field at the same number is a compatible change, so resolving the schema
    # field by name would find nothing and silently skip it - leaving a tagged field
    # untransformed. The name reported to the rule is the registered schema's.
    visited = []

    def field_transform(ctx, field_ctx, value):
        visited.append(field_ctx.name)
        return value.upper() if isinstance(value, str) else value

    message = validation_widget_pb2.ValidationPerson(age=30, name="Alice")
    transform(_rule_context(['PII']), _renamed_person_descriptor(), message, field_transform)

    assert message.name == "ALICE", 'the tagged field was not transformed'
    assert visited == ['renamed'], f'expected the registered name, got {visited}'


def _cel_rule_context(expr: str, tags: Any) -> RuleContext:
    rule = Rule("t", None, RuleKind.TRANSFORM, RuleMode.WRITE, "CEL_FIELD", tags, None, expr, None, None, False)
    # The CEL executor reads the target schema for its type and cache key only, so the text
    # does not matter here.
    target = Schema("", "PROTOBUF")
    return RuleContext(
        None,
        SerializationContext("topic", MessageField.VALUE),
        None,
        target,
        "topic-value",
        RuleMode.WRITE,
        rule,
        0,
        [rule],
        {},
        None,
    )


def test_cel_field_rule_runs_on_a_renamed_field():
    # The field context reports the registered schema's name for the field, since that is what
    # a rule refers to - but the producer's message knows the field under its own name. An
    # executor that resolved the reported name against the message would raise before the rule
    # ran, so the walk carries the producer's own field descriptor for the value conversion.
    message = validation_widget_pb2.ValidationPerson(age=30, name="Alice")
    ctx = _cel_rule_context("name == 'renamed' ; value + '-x'", ['PII'])
    field_transform = CelFieldExecutor().new_transform(ctx)

    transform(ctx, _renamed_person_descriptor(), message, field_transform)

    assert message.name == "Alice-x"
