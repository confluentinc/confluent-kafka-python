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
Tests for CelValidator — the per-rule CEL semantics, independent of any walker.
"""

import datetime

import pytest
from celpy import celtypes
from google.protobuf.timestamp_pb2 import Timestamp

from confluent_kafka.schema_registry.common.protobuf import validate_message as validate_protobuf
from confluent_kafka.schema_registry.rules.cel.cel_executor import _value_to_cel
from confluent_kafka.schema_registry.rules.cel.cel_validator import CelValidator
from confluent_kafka.schema_registry.serde import RuleError, ValidationRule

from .data.proto import validation_widget_pb2


@pytest.fixture
def validator():
    return CelValidator()


def rule(expr, name="r", doc=None, sql=None):
    return ValidationRule(name, doc, expr, sql)


# --------------------------------------------------------------------------------------
# Result handling
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "expr, value, expected",
    [
        ("this >= 0", 30, True),
        ("this >= 0", -5, False),
        ("size(this) > 0", "alice", True),
        ("size(this) > 0", "", False),
        ("this.age <= 150", {"age": 30}, True),
        ("this.age <= 150", {"age": 200}, False),
        ("this.startsWith('a')", "alice", True),
        ("this in ['a', 'b']", "a", True),
    ],
)
def test_boolean_rules(validator, expr, value, expected):
    assert validator.execute(rule(expr), None, value) is expected


def test_string_result_is_the_failure_message(validator):
    expr = "this >= 0 ? '' : 'age must be positive, got ' + string(this)"
    # An empty string means the rule passed.
    assert validator.execute(rule(expr), None, 5) == ""
    assert validator.execute(rule(expr), None, -5) == "age must be positive, got -5"


def test_now_is_bound_for_every_evaluation(validator):
    assert validator.execute(rule("now > timestamp('2000-01-01T00:00:00Z')"), None, 1) is True


# --------------------------------------------------------------------------------------
# Error surfaces — every one of these becomes a collected violation, not a crash
# --------------------------------------------------------------------------------------


def test_null_value_is_a_contract_violation(validator):
    with pytest.raises(RuleError, match="received a null value"):
        validator.execute(rule("this > 0"), None, None)


def test_missing_expression(validator):
    with pytest.raises(RuleError, match="has no expression"):
        validator.execute(ValidationRule(name="r"), None, 1)


def test_uncompilable_expression(validator):
    with pytest.raises(RuleError, match="Could not compile validation rule 'r'"):
        validator.execute(rule("this >= "), None, 1)


def test_unevaluatable_expression(validator):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule("this.nope > 0"), None, {"a": 1})


def test_evaluation_error_includes_doc_when_present(validator):
    with pytest.raises(RuleError, match=r"Could not execute validation rule 'r' \(some doc\)"):
        validator.execute(rule("this.nope > 0", doc="some doc"), None, {"a": 1})


def test_non_boolean_non_string_result_is_rejected(validator):
    with pytest.raises(RuleError, match="must return bool or string; got IntType"):
        validator.execute(rule("1 + 1"), None, 1)


def test_unnamed_rule_is_reported_as_unnamed(validator):
    with pytest.raises(RuleError, match="Validation rule 'unnamed' has no expression"):
        validator.execute(ValidationRule(), None, 1)


# --------------------------------------------------------------------------------------
# Protobuf value conversion
# --------------------------------------------------------------------------------------


def test_protobuf_message_binds_fields(validator):
    person = validation_widget_pb2.ValidationPerson(age=30, name="Alice")
    assert validator.execute(rule("this.age <= 150"), person.DESCRIPTOR, person) is True
    assert validator.execute(rule("this.name == 'Alice'"), person.DESCRIPTOR, person) is True


def test_protobuf_scalar_field_uses_the_field_descriptor(validator):
    desc = validation_widget_pb2.ValidationPerson.DESCRIPTOR
    assert validator.execute(rule("this >= 0"), desc.fields_by_name["age"], -3) is False
    assert validator.execute(rule("size(this) > 0"), desc.fields_by_name["name"], "Alice") is True


def test_protobuf_repeated_field_binds_the_whole_list(validator):
    message = validation_widget_pb2.ValidationOuter(tags=["a", "b"])
    fd = message.DESCRIPTOR.fields_by_name["tags"]
    assert validator.execute(rule("size(this) == 2"), fd, message.tags) is True


def test_protobuf_map_field_binds_a_map(validator):
    message = validation_widget_pb2.ValidationOuter(labels={"a": validation_widget_pb2.ValidationItem(v=1)})
    fd = message.DESCRIPTOR.fields_by_name["labels"]
    assert validator.execute(rule("'a' in this"), fd, message.labels) is True


# --------------------------------------------------------------------------------------
# `now` end to end through the protobuf walker, mirroring the JVM client's test
# --------------------------------------------------------------------------------------


def _event(when: datetime.datetime):
    ts = Timestamp()
    ts.FromDatetime(when)
    return validation_widget_pb2.ValidationEvent(created_at=ts)


def test_past_timestamp_satisfies_now():
    past = _event(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60))
    assert validate_protobuf(CelValidator(), past.DESCRIPTOR, past) == []


def test_future_timestamp_violates_now():
    future = _event(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3600))
    errors = validate_protobuf(CelValidator(), future.DESCRIPTOR, future)
    assert len(errors) == 1
    assert errors[0].rule.name == "notFuture"
    assert errors[0].field_path == "created_at"


# --------------------------------------------------------------------------------------
# Program caching
# --------------------------------------------------------------------------------------


def test_programs_are_cached_per_expression(validator):
    for value in range(5):
        validator.execute(rule("this >= 0"), None, value)
    validator.execute(rule("this <= 100"), None, 1)
    assert len(validator._cache.programs) == 2


# bool is a subclass of int in Python, so a dispatch that tests int first binds every
# boolean as a CEL int. Nothing about a bool then works: `this` returns an int the walker
# rejects, and every boolean operator fails to find an overload.
@pytest.mark.parametrize(
    "expr,value,expected",
    [
        ("this", True, True),
        ("this", False, False),
        ("!this", False, True),
        ("!this", True, False),
        ("this == true", True, True),
        ("this != false", True, True),
        ("this ? 'y' : 'n'", False, "n"),
    ],
)
def test_bool_values_bind_as_cel_bools(validator, expr, value, expected):
    assert validator.execute(rule(expr), None, value) == expected


# The int branch has to keep working: it is the one bool was being captured by.
@pytest.mark.parametrize(
    "expr,value,expected",
    [
        ("this > 0", 1, True),
        ("this == 1", 1, True),
        ("this % 2 == 1", 1, True),
        ("this < 0", -1, True),
    ],
)
def test_int_values_still_bind_as_cel_ints(validator, expr, value, expected):
    assert validator.execute(rule(expr), None, value) == expected


@pytest.mark.parametrize(
    "value,cel_type",
    [
        (True, celtypes.BoolType),
        (False, celtypes.BoolType),
        (1, celtypes.IntType),
        (0, celtypes.IntType),
        (1.5, celtypes.DoubleType),
        ("s", celtypes.StringType),
        (b"s", celtypes.BytesType),
    ],
)
def test_values_bind_to_their_own_cel_type(value, cel_type):
    # Pins the dispatch order directly: bool must not be captured by the int branch.
    assert type(_value_to_cel(value)) is cel_type


# CEL's `has()` reports protobuf presence, which protobuf tracks three ways: explicit for
# `optional`, oneof members and messages; non-empty for repeated and map fields; and
# difference-from-default for a proto3 scalar with implicit presence. The last of these used
# to report set unconditionally, because the key was always in the bound map - so `has()` was
# true even for a field the producer never wrote. Go, Java, JS and .NET all report false, as
# does every protovalidate implementation.
@pytest.mark.parametrize(
    "expr",
    [
        "has(this.age)",  # implicit-presence scalar
        "has(this.name)",  # implicit-presence string
    ],
)
def test_has_is_false_for_unset_implicit_presence_fields(validator, expr):
    msg = validation_widget_pb2.ValidationPerson(age=0, name="")
    assert validator.execute(rule(expr), msg.DESCRIPTOR, msg) is False


@pytest.mark.parametrize("expr", ["has(this.age)", "has(this.name)"])
def test_has_is_true_once_written(validator, expr):
    msg = validation_widget_pb2.ValidationPerson(age=5, name="x")
    assert validator.execute(rule(expr), msg.DESCRIPTOR, msg) is True


# A field still reads as its default outside has(), which is why the key cannot simply be
# omitted from the bound map.
@pytest.mark.parametrize("expr", ["this.age == 0", "this.name == ''"])
def test_unset_fields_still_read_as_their_default(validator, expr):
    msg = validation_widget_pb2.ValidationPerson(age=0, name="")
    assert validator.execute(rule(expr), msg.DESCRIPTOR, msg) is True


@pytest.mark.parametrize(
    "expr",
    [
        "has(this.inner)",  # message: explicit presence
        "has(this.items)",  # repeated: non-empty
        "has(this.labels)",  # map: non-empty
    ],
)
def test_has_covers_every_presence_shape(validator, expr):
    empty = validation_widget_pb2.ValidationOuter()
    assert validator.execute(rule(expr), empty.DESCRIPTOR, empty) is False

    populated = validation_widget_pb2.ValidationOuter()
    populated.inner.x = 1
    populated.items.add(v=1)
    populated.labels["a"].v = 1
    assert validator.execute(rule(expr), populated.DESCRIPTOR, populated) is True
