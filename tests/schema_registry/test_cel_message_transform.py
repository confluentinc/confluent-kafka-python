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
Message-level ``CEL`` transforms over protobuf: the rule returns a map and the message is
rebuilt from it.

Before this the executor returned the raw celpy map, which the protobuf serializer cannot
write - a decimal and a variant are messages in protobuf and celpy has no rendering for
either - so every message-level transform failed. The equivalent Avro path already worked
because an Avro record is a dict in this client and a celpy map is a dict subclass.

The transform has **replace** semantics: the map is the new message, so an unnamed field is
dropped and a ``null`` clears its field. Those are covered here too, because they are the
part a rule author is most likely to be surprised by.
"""
import datetime
from decimal import Decimal

from confluent_kafka.schema_registry.confluent.types.variant_utils import Variant, parse_json
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, Schema
from confluent_kafka.schema_registry.serde import RuleContext

from .data.proto import value_types_pb2

_SCHEMA = """syntax = "proto3";
package tests;
import "confluent/types/decimal.proto";
import "confluent/types/variant.proto";
import "google/protobuf/timestamp.proto";
message ValueTypes {
  .confluent.type.Decimal amount = 1;
  google.protobuf.Timestamp ts = 2;
  .confluent.type.Variant data = 3;
  string label = 4;
  int32 count = 5;
}
"""

# 0x04D2 = 1234 unscaled, i.e. 12.34 at scale 2.
_UNSCALED = bytes([0x04, 0xD2])


def _message():
    msg = value_types_pb2.ValueTypes()
    msg.amount.value = _UNSCALED
    msg.amount.precision = 8
    msg.amount.scale = 2
    msg.ts.seconds = 1700000000
    msg.ts.nanos = 123000000
    variant = parse_json('{"name":"alice"}')
    msg.data.metadata = variant.metadata
    msg.data.value = variant.value
    msg.label = "hi"
    msg.count = 7
    return msg


def _transform(expr, msg=None):
    rule = Rule("r", None, RuleKind.TRANSFORM, RuleMode.WRITE, "CEL",
                None, None, expr, None, None, False)
    ctx = RuleContext(None, None, None, Schema(_SCHEMA, "PROTOBUF"), "t-value",
                      RuleMode.WRITE, rule, 0, [rule], None, None)
    return CelExecutor().transform(ctx, _message() if msg is None else msg)


def _decimal_of(msg):
    unscaled = int.from_bytes(msg.amount.value, "big", signed=True)
    return Decimal(unscaled).scaleb(-msg.amount.scale)


_ALL = ('"amount": message.amount, "ts": message.ts, '
        '"data": message.data, "label": message.label, "count": message.count')


def test_pass_through_returns_a_message_unchanged():
    """C6. An identity transform is the cheapest regression test for a write-back path:
    it fails for any breakage in the plumbing, without depending on the computation."""
    result = _transform("{" + _ALL + "}")

    assert isinstance(result, value_types_pb2.ValueTypes)
    assert _decimal_of(result) == Decimal("12.34")
    assert result.ts.seconds == 1700000000
    assert result.ts.nanos == 123000000
    assert Variant(result.data.value, result.data.metadata).to_json() == '{"name":"alice"}'
    assert result.label == "hi"
    assert result.count == 7


def test_computed_decimal_is_written_back():
    """C7, decimal. The rule returns a Python Decimal, which has to become a
    confluent.type.Decimal message - unscaled bytes plus the scale."""
    result = _transform(
        '{"amount": decimals.add(decimal(message.amount), decimal("1.00")), '
        '"ts": message.ts, "data": message.data, "label": message.label}')

    assert _decimal_of(result) == Decimal("13.34")
    assert result.amount.scale == 2
    # Not merely the original echoed back.
    assert result.amount.value != _UNSCALED


def test_computed_timestamp_is_written_back():
    """C7, timestamp. The rule returns a datetime, which has to become a
    google.protobuf.Timestamp - and keep its sub-second part."""
    result = _transform(
        '{"amount": message.amount, "ts": message.ts + duration("60s"), '
        '"data": message.data, "label": message.label}')

    assert result.ts.seconds == 1700000060
    assert result.ts.nanos == 123000000


def test_computed_variant_is_written_back():
    """C7, variant. The rule returns a Variant, which has to become a
    confluent.type.Variant message. Asserted through the decoded JSON rather than the
    metadata bytes: metadata holds the field *names*, so {"name":"alice"} and
    {"name":"bob"} share it and comparing metadata would prove nothing."""
    result = _transform(
        '{"amount": message.amount, "ts": message.ts, '
        '"data": variants.parseJson("{\\"name\\":\\"bob\\"}"), "label": message.label}')

    assert Variant(result.data.value, result.data.metadata).to_json() == '{"name":"bob"}'


def test_scalar_can_be_replaced():
    result = _transform('{"label": "changed", "count": 9}')

    assert result.label == "changed"
    assert result.count == 9


def test_a_field_the_rule_does_not_name_is_dropped():
    """Replace semantics, and the consequence most likely to surprise: a rule naming only
    the field it changes discards everything else. Intended, but silent on protobuf -
    proto3 has no required fields, so nothing catches it."""
    result = _transform('{"label": "changed"}')

    assert result.label == "changed"
    assert not result.HasField("amount")
    assert not result.HasField("ts")
    assert not result.HasField("data")
    assert result.count == 0


def test_null_clears_a_field():
    """The idiom for preserving absence across a transform that echoes a field:
    `has(x) ? x : null`. Without a null arm there would be no way to express it."""
    result = _transform(
        '{"amount": null, "ts": message.ts, "data": message.data, "label": message.label}')

    assert not result.HasField("amount")
    assert result.HasField("ts")
    assert result.label == "hi"


def test_echoing_an_absent_field_materialises_it():
    """The other face of replace: reading an absent field produces its default, so echoing
    it writes that default back and `has()` flips from False to True. This documents the
    behaviour rather than endorsing it - `has(x) ? x : null` is the way to avoid it."""
    absent = value_types_pb2.ValueTypes()
    absent.label = "hi"
    assert not absent.HasField("amount")

    echoed = _transform('{"amount": message.amount, "label": message.label}', absent)
    assert echoed.HasField("amount")

    guarded = _transform(
        '{"amount": has(message.amount) ? message.amount : null, "label": message.label}',
        absent)
    assert not guarded.HasField("amount")


def test_condition_rules_are_unaffected():
    """A CONDITION returns a bool, which must not be run through the message rebuild."""
    rule = Rule("r", None, RuleKind.CONDITION, RuleMode.WRITE, "CEL", None, None,
                'decimals.gt(message.amount, decimal("10.00"))', None, None, False)
    ctx = RuleContext(None, None, None, Schema(_SCHEMA, "PROTOBUF"), "t-value",
                      RuleMode.WRITE, rule, 0, [rule], None, None)

    assert CelExecutor().transform(ctx, _message()) is True
