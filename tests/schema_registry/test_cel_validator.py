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

import celpy
import pytest
from celpy import celtypes
from google.protobuf import descriptor_pb2, message_factory, wrappers_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.timestamp_pb2 import Timestamp

from confluent_kafka.schema_registry.common.protobuf import validate_message as validate_protobuf
from confluent_kafka.schema_registry.confluent.types import decimal_pb2, variant_pb2
from confluent_kafka.schema_registry.confluent.types import variant_utils as vu
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


# A ``confluent.type.Decimal`` proto field is bound into CEL as a celpy MessageType wrapper
# (the same shape produced whether the Decimal is the whole message or a nested field), so
# ``decimal(...)`` must unwrap it and dispatch the ``decimals.*`` operators against it. This
# mirrors the JVM client's CelValidatorDecimalTest, which reads a ``confluent.type.Decimal``
# field via ``decimal(this)``.
def test_decimal_unwraps_a_confluent_type_decimal_message(validator):
    # 12.34 = unscaled 1234 (0x04D2) at scale 2.
    d = decimal_pb2.Decimal(value=(1234).to_bytes(2, "big"), scale=2)
    assert validator.execute(rule("decimals.gt(decimal(this), decimal('10.00'))"), d.DESCRIPTOR, d) is True
    assert validator.execute(rule("decimals.lt(decimal(this), decimal('10.00'))"), d.DESCRIPTOR, d) is False


# The Python decimal layer must match java.math.BigDecimal's EXACT/unbounded semantics
# for add/sub/mul/mod, setScale/quantize (round/trunc/floor/ceil), and scaleb — rather
# than the thread-local default context (prec=28) which silently rounds or hard-errors on
# values with >28 significant digits. Only div/sqrt cap at 38 digits. These are the
# Java-reference regression cases (#30 exact arithmetic, #31 negative-scale round/trunc,
# #32 no-cap floor/ceil, #33 exact mod, #34 exact decimal-from-bytes).
@pytest.mark.parametrize(
    "expr, expected",
    [
        # #30 exact add/mul — no silent rounding of the >28-digit result.
        ('string(decimals.add(decimal("1E38"), decimal("1")))',
         "100000000000000000000000000000000000001"),
        ('string(decimals.mul(decimal("12345678901234567890"), '
         'decimal("98765432109876543210")))',
         "1219326311370217952237463801111263526900"),
        # Scale preservation still holds for ordinary-magnitude operands.
        ('string(decimals.mul(decimal("2.0"), decimal("3.0")))', "6.00"),
        ('string(decimals.add(decimal("1.5"), decimal("1.25")))', "2.75"),
        # #31 negative-scale round/trunc — quantize target Decimal(1).scaleb(-scale),
        # so scale=-2 rounds/truncates to the hundreds place (not to an integer).
        ('string(decimals.round(decimal("1234.5"), -2))', "1200"),
        ('string(decimals.trunc(decimal("1234"), -2))', "1200"),
        # #32 no 28-digit cap on floor (30-digit value passes through, no error).
        ('string(decimals.floor(decimal("123456789012345678901234567890")))',
         "123456789012345678901234567890"),
        # #33 exact mod — quotient exceeds 38 digits, but remainder is exact.
        ('string(decimals.mod(decimal("1E40"), decimal("3")))', "1"),
        # #34 decimal(dyn) from a >28-digit string round-trips exactly.
        ('string(decimal("12345678901234567890123456789012345"))',
         "12345678901234567890123456789012345"),
    ],
)
def test_decimal_ops_match_java_bigdecimal_exact_semantics(validator, expr, expected):
    assert validator.execute(rule(expr), None, 1) == expected


# #34 decimal(bytes, scale): a 38-digit unscaled value at scale 5 must round-trip
# exactly through _from_bytes_scale (no rounding to the 28-digit default context).
def test_decimal_from_bytes_scale_is_exact(validator):
    unscaled = 12345678901234567890123456789012345678  # 38 digits
    raw = unscaled.to_bytes(16, "big", signed=True)
    result = validator.execute(rule("string(decimal(this, 5))"), None, raw)
    assert result == "123456789012345678901234567890123.45678"


# ``decimal(<string>)`` / ``decimal(<dyn>)`` must match java.math.BigDecimal's
# ``new BigDecimal(String)`` / ``BigDecimal.valueOf(double)``, which throw
# NumberFormatException on non-finite values, underscore digit-grouping, and
# surrounding whitespace. Python's ``Decimal(str)`` silently accepts all of
# these — building a poisoned NaN/Infinity Decimal or a wrongly-parsed 1000 —
# so the constructor must reject them (surfaced as a RuleError). The
# ``decimal(bytes, scale)`` path parses no string and is unaffected.
@pytest.mark.parametrize(
    "expr",
    [
        'decimal("NaN") > decimal("0")',
        'decimal("Infinity") > decimal("0")',
        'decimal("-Infinity") > decimal("0")',
        'decimal("-inf") > decimal("0")',
        'decimal("sNaN") > decimal("0")',
        'decimal("1_000") > decimal("0")',
        # Surrounding whitespace: Java rejects; Python's Decimal strips it.
        "decimal('  5  ') > decimal('0')",
    ],
)
def test_decimal_rejects_inputs_java_bigdecimal_rejects(validator, expr):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule(expr), None, 1)


# A NaN/Infinity double routed through ``decimal(<double>)`` must also be
# rejected — Java's ``BigDecimal.valueOf(double)`` throws on non-finite doubles.
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_decimal_rejects_non_finite_double(validator, value):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule("decimal(this) > decimal('0')"), None, value)


# Legitimate finite decimals must still parse: ordinary decimals, scientific
# notation, negatives, negative zero, a leading '+', and a finite double.
@pytest.mark.parametrize(
    "expr, expected",
    [
        ('string(decimal("123.45"))', "123.45"),
        ('string(decimal("1e40"))', "10000000000000000000000000000000000000000"),
        ('string(decimal("-0.5"))', "-0.5"),
        ('string(decimal("-0"))', "-0"),
        ('string(decimal("+5"))', "5"),
    ],
)
def test_decimal_accepts_legitimate_finite_values(validator, expr, expected):
    assert validator.execute(rule(expr), None, 1) == expected


def test_decimal_accepts_finite_double(validator):
    assert validator.execute(rule("string(decimal(this))"), None, 1.5) == "1.5"


# CEL ``==``/``!=`` on two Decimal values must be NUMERIC (scale-insensitive), matching
# ``decimals.eq`` (java.math.BigDecimal.compareTo) rather than an equals() that also
# compares scale. Python's ``decimal.Decimal.__eq__`` is already numeric
# (``Decimal("2.0") == Decimal("2.00")`` is True), and celpy dispatches ``==`` to it,
# so this is a regression guard — no code change is required.
@pytest.mark.parametrize(
    "expr, expected",
    [
        ('decimal("2.0") == decimal("2.00")', True),
        ('decimal("2.0") == decimal("2.0")', True),
        ('decimal("2.0") == decimal("2.1")', False),
        ('decimal("2.0") != decimal("2.00")', False),
        ('decimal("2.0") != decimal("2.1")', True),
    ],
)
def test_decimal_equality_is_numeric_scale_insensitive(validator, expr, expected):
    assert validator.execute(rule(expr), None, 1) is expected


# --------------------------------------------------------------------------------------
# Variant CEL functions
# --------------------------------------------------------------------------------------

_VARIANT_JSON = (
    '{"name":"alice","age":30,"scores":[10,20,30],"nested":{"x":1},"explicit":null}')


# `this` is bound to a JSON string; variants.parseJson(this) turns it into a Variant, then
# the variants.* accessors navigate and extract. Covers the null model (absent vs
# variant-null), path/field/index navigation, typed extraction, and toJson.
@pytest.mark.parametrize(
    "expr",
    [
        "variants.type(variants.parseJson(this)) == 'object'",
        "variants.as(variants.field(variants.parseJson(this), 'name'), 'string') == 'alice'",
        "variants.as(variants.field(variants.parseJson(this), 'age'), 'int') == 30",
        # Absent (missing field) vs present-but-variant-null (explicit JSON null).
        "variants.field(variants.parseJson(this), 'missing') == null",
        "variants.isNull(variants.field(variants.parseJson(this), 'explicit'))",
        "!variants.isNull(variants.field(variants.parseJson(this), 'missing'))",
        "variants.as(variants.path(variants.parseJson(this), '$.nested.x'), 'int') == 1",
        "variants.as(variants.index("
        "variants.field(variants.parseJson(this), 'scores'), 2), 'int') == 30",
        # tryAs returns CEL null on a type mismatch (age is not a string).
        "variants.tryAs(variants.field(variants.parseJson(this), 'age'), 'string') == null",
        "variants.toJson(variants.field(variants.parseJson(this), 'nested')) == '{\"x\":1}'",
    ],
)
def test_variant_functions_over_parsed_json(validator, expr):
    assert validator.execute(rule(expr), None, _VARIANT_JSON) is True


# An Avro `variant` logical-type field decodes to a Variant (via the logical type registered
# in common/avro.py), which then flows into CEL through variant(this).
def test_avro_variant_field_into_cel(validator):
    import io

    import fastavro

    import confluent_kafka.schema_registry.common.avro  # noqa: F401  (registers the logical type)

    schema = fastavro.parse_schema({
        "type": "record", "name": "confluent.type.Variant", "logicalType": "variant",
        "fields": [{"name": "metadata", "type": "bytes"}, {"name": "value", "type": "bytes"}],
    })
    built = vu.parse_json('{"name":"alice","age":30}')
    value, metadata = built.value, built.metadata
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, vu.Variant(value, metadata))
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)
    assert isinstance(decoded, vu.Variant)
    assert validator.execute(
        rule("variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'"),
        None, decoded) is True


# A confluent.type.Variant proto field is bound into CEL as a celpy MessageType wrapper;
# variant(...) must unwrap it, mirroring the decimal test above and the JVM client.
def test_proto_variant_field_into_cel(validator):
    built = vu.parse_json('{"name":"alice","age":30}')
    value, metadata = built.value, built.metadata
    v = variant_pb2.Variant(value=value, metadata=metadata)
    expr = "variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'"
    assert validator.execute(rule(expr), v.DESCRIPTOR, v) is True


# A string is rejected by variant(...) with a redirect to parseJson.
def test_variant_rejects_string_input(validator):
    with pytest.raises(RuleError, match="Could not execute"):
        validator.execute(rule("variants.type(variant(this)) == 'object'"), None, "not-a-variant")


# variant(null) yields CEL null instead of erroring (matching the Java reference), and it
# composes: a null flows through the accessors as absent.
@pytest.mark.parametrize(
    "expr",
    [
        "variant(null) == null",
        "variants.field(variant(null), 'k') == null",
        # An absent field is null, and variant(null) of it is still null.
        "variant(variants.field(variants.parseJson(this), 'missing')) == null",
    ],
)
def test_variant_of_null_is_cel_null(validator, expr):
    assert validator.execute(rule(expr), None, _VARIANT_JSON) is True


# Non-finite doubles round-trip through CEL as bareword NaN/Infinity/-Infinity (Confluent
# Java contract). Bareword literals parse (Python json.loads accepts them by default).
@pytest.mark.parametrize("tok", ["NaN", "Infinity", "-Infinity"])
def test_variant_non_finite_bareword_roundtrip_through_cel(validator, tok):
    expr = "variants.toJson(variants.parseJson(this)) == '%s'" % tok
    assert validator.execute(rule(expr), None, tok) is True


# variants.tryParseJson of empty/whitespace-only input is a soft failure -> CEL null,
# while the strict variants.parseJson raises (surfaced as a RuleError).
@pytest.mark.parametrize("src", ["", "   ", "\t\n"])
def test_variant_try_parse_json_empty_is_cel_null(validator, src):
    assert validator.execute(rule("variants.tryParseJson(this) == null"), None, src) is True


@pytest.mark.parametrize("src", ["", "   "])
def test_variant_parse_json_empty_raises(validator, src):
    with pytest.raises(RuleError, match="Could not execute"):
        validator.execute(rule("variants.type(variants.parseJson(this)) == 'object'"), None, src)


# --------------------------------------------------------------------------------------
# timestamp(value, precision)
# --------------------------------------------------------------------------------------


# ``timestamp(value, precision)`` must split the epoch value into whole microseconds with
# exact integer FLOOR division (mirroring Java TimestampUtils' Math.floorDiv/floorMod),
# not float division that rounds half-to-even and drops precision. datetime resolution is
# one microsecond, so sub-microsecond nanos are floored away (an inherent, Java-matching
# limit), but the microsecond itself must never round up, and negative epochs must floor
# toward negative infinity.
@pytest.mark.parametrize(
    "expr",
    [
        # nanos floor to the microsecond (1500 ns -> 1 us, not rounded up to 2).
        'timestamp(1500, 9) == timestamp("1970-01-01T00:00:00.000001Z")',
        # 999999500 ns floors to .999999, not rounded up to the next whole second.
        'timestamp(999999500, 9) == timestamp("1970-01-01T00:00:00.999999Z")',
        # Negative epoch floors toward -inf: -500 ns -> the microsecond before the epoch.
        'timestamp(-500, 9) == timestamp("1969-12-31T23:59:59.999999Z")',
        # A large micros value keeps its microsecond (float division would have lost it).
        'timestamp(253402300799000001, 6) == '
        'timestamp("9999-12-31T23:59:59.000001Z")',
        # millis/micros/seconds precisions are exact.
        'timestamp(1500, 3) == timestamp("1970-01-01T00:00:01.500000Z")',
        'timestamp(1, 6) == timestamp("1970-01-01T00:00:00.000001Z")',
        'timestamp(1, 0) == timestamp("1970-01-01T00:00:01Z")',
    ],
)
def test_timestamp_precision_floors_with_exact_integer_arithmetic(validator, expr):
    assert validator.execute(rule(expr), None, 1) is True


def test_timestamp_bool_reports_bool_not_int(validator):
    # celtypes.BoolType subclasses int (MRO: BoolType -> int -> object) and *not*
    # bool, so a plain ``isinstance(v, bool)`` guard never fires for a CEL bool and
    # the value used to be misreported as a unitless raw int.
    with pytest.raises(RuleError) as excinfo:
        validator.execute(rule("timestamp(true) == timestamp(1)"), None, 1)
    assert "cannot convert bool" in str(excinfo.value.__cause__)


@pytest.mark.parametrize("precision", [1, 2, 4, 5, 7, 8, 10, -3])
def test_timestamp_rejects_precision_outside_the_set(validator, precision):
    # With the unit a number rather than a name, rejecting anything outside
    # {0, 3, 6, 9} is the only thing between a typo and a silently wrong instant.
    with pytest.raises(RuleError) as excinfo:
        validator.execute(
            rule(f"timestamp(1700000000, {precision}) == timestamp(0)"), None, 1)
    assert "unknown precision" in str(excinfo.value.__cause__)


def test_timestamp_datetime_components_form_still_works(validator):
    # celpy's components form takes three or more args, so it never collides with
    # the two-arg precision form.
    assert validator.execute(
        rule('timestamp(2009, 2, 13) == timestamp("2009-02-13T00:00:00Z")'), None, 1) is True


# --------------------------------------------------------------------------------------
# stdlib timestamp(...) — the single-int epoch-seconds overload every other client has
# --------------------------------------------------------------------------------------


# celpy binds ``timestamp`` straight to celtypes.TimestampType, which accepts a
# datetime, a string, or an int followed by *at least two more* args (datetime
# components) — but rejects a lone int. cel-java (int64_to_timestamp), Go, C++ and C#
# all read a single int as epoch SECONDS, so the client registers its own "timestamp"
# that adds that overload and delegates every other form to the base implementation.
@pytest.mark.parametrize(
    "expr",
    [
        # The regression: a bare int is epoch seconds.
        'timestamp(1700000000) == timestamp("2023-11-14T22:13:20Z")',
        'timestamp(0) == timestamp("1970-01-01T00:00:00Z")',
        # Negative / pre-epoch ints.
        'timestamp(-1) == timestamp("1969-12-31T23:59:59Z")',
        'timestamp(-2208988800) == timestamp("1900-01-01T00:00:00Z")',
        # Matches timestamp(value, 0) exactly.
        'timestamp(1700000000) == timestamp(1700000000, 0)',
        # The result is a real UTC-aware timestamp, usable with the timestamp methods.
        "timestamp(1700000000).getFullYear() == 2023",
        # Forwarded to the base implementation: the datetime-components form needs
        # arity >= 3 to reach TimestampType, so the override must not swallow it.
        'timestamp(2009, 2, 13) == timestamp("2009-02-13T00:00:00Z")',
        'timestamp(2009, 2, 13, 23, 31, 30) == timestamp("2009-02-13T23:31:30Z")',
        # Forwarded: RFC 3339 strings, including the lenient form celpy accepts.
        'timestamp("2023-11-14T22:13:20Z") == timestamp(1700000000)',
        'timestamp("2020-01-01 00:00:00") == timestamp("2020-01-01T00:00:00Z")',
        # Forwarded: a timestamp is passed through unchanged.
        'timestamp(timestamp("2023-11-14T22:13:20Z")) == timestamp(1700000000)',
    ],
)
def test_timestamp_int_is_epoch_seconds_and_other_forms_still_work(validator, expr):
    assert validator.execute(rule(expr), None, 1) is True


def test_timestamp_bool_raises_rather_than_meaning_epoch_second_one(validator):
    # BoolType subclasses int, so an unguarded int check would read true as 1.
    with pytest.raises(RuleError) as excinfo:
        validator.execute(rule('timestamp(true) == timestamp("1970-01-01T00:00:01Z")'), None, 1)
    assert "cannot convert bool" in str(excinfo.value.__cause__)


def test_timestamp_out_of_range_int_is_a_cel_error(validator):
    with pytest.raises(RuleError) as excinfo:
        validator.execute(rule("timestamp(9223372036854775807) == timestamp(0)"), None, 1)
    cause = excinfo.value.__cause__
    assert isinstance(cause, celpy.CELEvalError)
    assert "out of range" in str(cause)


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


def _wrapped_descriptor():
    """
    A message with one field of each wrapper type, built here rather than added to the
    checked-in widget schema: the generated module predates the current protoc, so
    regenerating it to add fields would rewrite the whole file.
    """
    pool = DescriptorPool()
    for dep in (wrappers_pb2.DESCRIPTOR,):
        dep_proto = descriptor_pb2.FileDescriptorProto()
        dep.CopyToProto(dep_proto)
        pool.Add(dep_proto)

    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = "wrapped.proto"
    fdp.package = "test"
    fdp.syntax = "proto3"
    fdp.dependency.append(wrappers_pb2.DESCRIPTOR.name)
    msg = fdp.message_type.add()
    msg.name = "Wrapped"
    for number, (name, type_name) in enumerate(
        [
            ("name", ".google.protobuf.StringValue"),
            ("count", ".google.protobuf.Int64Value"),
            ("active", ".google.protobuf.BoolValue"),
        ],
        start=1,
    ):
        field = msg.field.add()
        field.name = name
        field.number = number
        field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type_name = type_name
    return pool.Add(fdp).message_types_by_name["Wrapped"]


# A wrapper carries null-or-value rather than the zero value an ordinary message carries -
# which is the whole reason to declare a field as one - so an unset wrapper reads as null.
# Unwrapping its default instead gave "" or 0, erasing the distinction the field exists for.
# cel-go, cel-java and cel-cpp all answer null here, and so now do the Rust and Python
# clients, the two that build the message as a map rather than handing it to the engine.
@pytest.mark.parametrize(
    "expr",
    ["this.name == null", "this.count == null", "this.active == null"],
)
def test_unset_wrapper_fields_read_as_null(validator, expr):
    descriptor = _wrapped_descriptor()
    empty = message_factory.GetMessageClass(descriptor)()
    assert validator.execute(rule(expr), descriptor, empty) is True


def test_unset_wrapper_is_not_its_unwrapped_default(validator):
    # The string case shows the point directly: an unset StringValue is no longer the empty
    # string its default unwrapped to. The numeric wrappers are pinned by the `== null` test
    # above instead, because celpy raises rather than answering false for `null == <int>` -
    # its own equality, and true of the bare expression `null == 0` as much as of a field.
    descriptor = _wrapped_descriptor()
    empty = message_factory.GetMessageClass(descriptor)()
    assert validator.execute(rule("this.name == ''"), descriptor, empty) is False
    assert validator.execute(rule("has(this.count)"), descriptor, empty) is False


def test_written_wrapper_is_the_value_it_holds(validator):
    descriptor = _wrapped_descriptor()
    written = message_factory.GetMessageClass(descriptor)()
    written.name.value = "a"
    written.count.value = 7
    written.active.value = True
    for expr in ("this.name == 'a'", "this.count == 7", "this.active"):
        assert validator.execute(rule(expr), descriptor, written) is True
    # and presence is unchanged either way
    assert validator.execute(rule("has(this.name)"), descriptor, written) is True
    empty = message_factory.GetMessageClass(descriptor)()
    assert validator.execute(rule("has(this.name)"), descriptor, empty) is False
