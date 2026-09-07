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
A ``CEL_FIELD`` rule over the *null* branch of an Avro ``["null", T]`` union must be evaluated,
not skipped.

Avro's null is a first-class value, and the reference binds it as CEL null so a rule can guard
with ``value == null``. Skipping the field instead removes that capability and is *silent*: a
rule that never ran and a rule that ran and passed produce the same result, so nothing in a
positive-only test can tell them apart.

Two guards used to prevent it: one in ``CelFieldExecutor`` (``if field_value is None``), and the
blanket ``if message is None`` at the top of the Avro walk. The reference has neither - it guards
a null only in the *record* case, where there are no fields to walk - and leaves the decision to
each format's walk. The protobuf walk still skips an unset field, which is correct there: a field
with presence that is unset has no value, and writing one back would materialise it.
"""
import json
from decimal import Decimal

import pytest

from confluent_kafka.schema_registry.common.avro import transform
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import CelFieldExecutor
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, Schema
from confluent_kafka.schema_registry.serde import RuleConditionError, RuleContext, RuleError

_SCHEMA = {
    "type": "record",
    "name": "Nullable",
    "fields": [
        {
            "name": "amount",
            "type": ["null", {"type": "bytes", "logicalType": "decimal",
                              "precision": 8, "scale": 2}],
            "confluent:tags": ["AMOUNT"],
        },
        {"name": "plain", "type": "string"},
    ],
}


# The tag map the walk would otherwise read off the parsed schema. Without it the rule matches
# no field and the walk returns the record untouched - which looks exactly like a pass.
_INLINE_TAGS = {"Nullable.amount": {"AMOUNT"}}


def _run(expr, amount):
    rule = Rule("r", None, RuleKind.CONDITION, RuleMode.WRITE, "CEL_FIELD",
                ["AMOUNT"], None, expr, None, None, False)
    ctx = RuleContext(None, None, None, Schema(json.dumps(_SCHEMA), "AVRO"), "t-value",
                      RuleMode.WRITE, rule, 0, [rule], _INLINE_TAGS, None)
    ft = CelFieldExecutor().new_transform(ctx)
    return transform(ctx, _SCHEMA, {"amount": amount, "plain": "hi"}, ft)


def test_null_field_reaches_the_rule():
    """`value == null` can only be true if the null was bound and the rule ran."""
    assert _run("value == null", None) is not None


def test_null_field_was_not_merely_skipped():
    """The discriminator: `value != null` is false on a null, so it must FAIL.

    Without it, the test above is satisfied by a rule that never ran - a skipped field
    reports no violation either.
    """
    with pytest.raises((RuleConditionError, RuleError)):
        _run("value != null", None)


def test_unguarded_rule_on_a_null_raises():
    """An expression that cannot handle a null fails loudly, and says why.

    At this level the evaluator's error propagates as-is; the serializer wraps it in a
    RuleError naming the rule. Either way it is loud, which is the point - the reference
    raises here rather than passing silently.
    """
    with pytest.raises(Exception, match="cannot convert null"):
        _run('decimals.gt(decimal(value), decimal("10.00"))', None)


def test_a_present_value_still_evaluates_normally():
    """The must-pass twin: removing the skip must not break the ordinary case."""
    assert _run('decimals.gt(decimal(value), decimal("10.00"))',
                Decimal("12.34")) is not None
    with pytest.raises((RuleConditionError, RuleError)):
        _run('decimals.gt(decimal(value), decimal("100.00"))', Decimal("12.34"))
