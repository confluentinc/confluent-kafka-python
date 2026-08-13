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

from confluent_kafka.schema_registry import MessageField, SerializationContext
from confluent_kafka.schema_registry.common.protobuf import transform
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode
from confluent_kafka.schema_registry.serde import RuleContext

from .data.proto import validation_widget_pb2
from .test_validate_message import _evolved_person_descriptor


def _rule_context() -> RuleContext:
    rule = Rule("t", None, RuleKind.TRANSFORM, RuleMode.WRITE, "TEST", None, None, None, None, None, False)
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
