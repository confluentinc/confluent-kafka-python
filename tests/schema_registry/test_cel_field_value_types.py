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
``CEL_FIELD`` rules over protobuf decimal and timestamp fields.

Avro carries these two as logical types on a primitive, so the field is a leaf and a field rule
reaches it. Protobuf carries them as messages, so the walk used to descend *past* the field and
transform ``value``/``scale`` or ``seconds``/``nanos`` one at a time - meaning a rule tagged for
the field never fired at all, and the record came back unchanged with no error. A silent no-op
is the worst of the three possible outcomes: the rule author gets no signal.

This is the port of the JVM client's #4538 (``isCelLeafMessage``). Variant is deliberately not a
leaf - it is a record in Avro too, so skipping it is the behaviour that matches, and a variant
is reached with a message-level ``CEL`` rule instead.
"""
import datetime
from decimal import Decimal

import pytest

from confluent_kafka.schema_registry.common.protobuf import get_type, transform
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import CelFieldExecutor
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, Schema
from confluent_kafka.schema_registry.serde import FieldType, RuleContext, RuleError

from .data.proto import value_type_rules_pb2, value_types_pb2

_SCHEMA = """syntax = "proto3";
package tests;
message ValueTypes {}
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
    msg.label = "hi"
    return msg


def _run(expr, kind, tag, msg=None):
    msg = msg if msg is not None else _message()
    rule = Rule("r", None, kind, RuleMode.WRITE, "CEL_FIELD",
                [tag] if tag else None, None, expr, None, None, False)
    ctx = RuleContext(None, None, None, Schema(_SCHEMA, "PROTOBUF"), "t-value",
                      RuleMode.WRITE, rule, 0, [rule],
                      {"tests.ValueTypes.amount": {"AMOUNT"},
                       "tests.ValueTypes.ts": {"TS"},
                       "tests.ValueTypes.data": {"DATA"}}, None)
    ft = CelFieldExecutor().new_transform(ctx)
    return transform(ctx, msg.DESCRIPTOR, msg, ft)


def _decimal_of(msg):
    unscaled = int.from_bytes(msg.amount.value, "big", signed=True)
    return Decimal(unscaled).scaleb(-msg.amount.scale)


def test_field_types_match_the_avro_counterpart():
    """The declared type is what makes CEL_FIELD apply at all: a RECORD is skipped outright."""
    fields = value_types_pb2.ValueTypes.DESCRIPTOR.fields_by_name

    assert get_type(fields["amount"]) == FieldType.BYTES
    assert get_type(fields["ts"]) == FieldType.LONG
    # Variant stays a record, as in Avro - not a leaf.
    assert get_type(fields["data"]) == FieldType.RECORD


def test_decimal_condition_fires():
    """C4. Before the port this raised nothing because the rule never ran."""
    _run('decimals.gt(decimal(value), decimal("10.00"))', RuleKind.CONDITION, "AMOUNT")


def test_decimal_condition_fails_when_it_should():
    """The must-fail twin. Without it the test above would also pass if no rule ran at all -
    which is exactly how the defect hid."""
    with pytest.raises(Exception):
        _run('decimals.gt(decimal(value), decimal("1000.00"))', RuleKind.CONDITION, "AMOUNT")


def test_timestamp_condition_fires():
    _run('value > timestamp("2000-01-01T00:00:00Z")', RuleKind.CONDITION, "TS")


def test_timestamp_condition_fails_when_it_should():
    with pytest.raises(Exception):
        _run('value > timestamp("2050-01-01T00:00:00Z")', RuleKind.CONDITION, "TS")


def test_decimal_transform_is_written_back():
    """C5. The rule returns a Python Decimal; it has to be encoded back into the message."""
    out = _run('decimals.add(decimal(value), decimal("1.00"))', RuleKind.TRANSFORM, "AMOUNT")

    assert _decimal_of(out) == Decimal("13.34")
    assert out.amount.scale == 2
    # Not merely the original left alone, which is what the defect looked like.
    assert out.amount.value != _UNSCALED


def test_timestamp_transform_is_written_back():
    out = _run('value + duration("60s")', RuleKind.TRANSFORM, "TS")

    assert out.ts.seconds == 1700000060
    assert out.ts.nanos == 123000000


def test_identity_transform_round_trips():
    """The pass-through: the cheapest check that the encode inverts the decode exactly."""
    out = _run("value", RuleKind.TRANSFORM, "AMOUNT")

    assert _decimal_of(out) == Decimal("12.34")
    assert out.amount.scale == 2


def test_variant_is_still_skipped():
    """Variant is a record in both formats, so a field rule must not reach it. The rule below
    would raise if it ran, so passing means it was skipped."""
    msg = _message()
    msg.data.metadata = b"\x01\x01\x00\x04name"
    msg.data.value = b"\x02\x01\x00\x00\x06\x15alice"

    out = _run('variants.type(value) == "not-a-type"', RuleKind.CONDITION, "DATA", msg)

    assert out.data.metadata == b"\x01\x01\x00\x04name"


def test_a_wrong_result_type_is_reported():
    """A rule returning something that is neither a decimal nor the message is a rule-authoring
    mistake; it must be named rather than written back as a default."""
    with pytest.raises(RuleError, match="expected a decimal"):
        _run('"not a decimal"', RuleKind.TRANSFORM, "AMOUNT")


# A repeated value-type field needs its rule's result rebuilt *per element*. The walk applies the
# rule to each element, so what comes back is a list of Decimals; only the singular case was
# rebuilt, and writing the raw list failed with "Expected a message object, but got Decimal(...)".
# So a field rule over a repeated decimal could not be written back at all (the reference answers `[2.11, 3.22]`).
_CONTAINER_SCHEMA = """syntax = "proto3";
package tests;
message ValueTypeContainers {}
"""


def _container_message():
    msg = value_type_rules_pb2.ValueTypeContainers()
    for unscaled in (111, 222):
        d = msg.amounts.add()
        d.value = unscaled.to_bytes(2, "big")
        d.precision = 8
        d.scale = 2
    msg.amount_map["a"].value = (333).to_bytes(2, "big")
    msg.amount_map["a"].precision = 8
    msg.amount_map["a"].scale = 2
    msg.label = "hi"
    return msg


def _run_container(expr, tag):
    msg = _container_message()
    rule = Rule("r", None, RuleKind.TRANSFORM, RuleMode.WRITE, "CEL_FIELD",
                [tag], None, expr, None, None, False)
    ctx = RuleContext(None, None, None, Schema(_CONTAINER_SCHEMA, "PROTOBUF"), "t-value",
                      RuleMode.WRITE, rule, 0, [rule], None, None)
    ft = CelFieldExecutor().new_transform(ctx)
    return transform(ctx, msg.DESCRIPTOR, msg, ft)


def _amounts(msg):
    return [Decimal(int.from_bytes(d.value, "big", signed=True)).scaleb(-d.scale)
            for d in msg.amounts]


def test_repeated_decimal_transform_is_written_back_per_element():
    out = _run_container('decimals.add(decimal(value), decimal("1.00"))', "AMOUNTS")

    assert _amounts(out) == [Decimal("2.11"), Decimal("3.22")]


def test_repeated_decimal_identity_transform_round_trips():
    """The must-pass twin: an identity rule hands back the message it was given, and the
    per-element rebuild has to accept that as readily as a computed decimal."""
    out = _run_container("value", "AMOUNTS")

    assert _amounts(out) == [Decimal("1.11"), Decimal("2.22")]


def test_a_tagged_map_field_is_left_alone():
    """The reference does *not* transform a map value through a tag on the map field - the tag
    does not reach the entry's value leaf - so matching it means leaving the map unchanged."""
    out = _run_container('decimals.add(decimal(value), decimal("1.00"))', "AMOUNTMAP")

    unscaled = int.from_bytes(out.amount_map["a"].value, "big", signed=True)
    assert Decimal(unscaled).scaleb(-out.amount_map["a"].scale) == Decimal("3.33")
