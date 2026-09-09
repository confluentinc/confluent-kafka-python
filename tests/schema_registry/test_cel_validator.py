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


# A decimal reached by *selection* rather than bound directly must compare numerically too.
# The boundary conversion only sees what is bound, so ``this.a`` stays a confluent.type.Decimal
# message; comparing those structurally - field by field over unscaled bytes and scale - calls
# 1.50 and 1.5 unequal. Containers and ``in`` follow the same rule, or they contradict ``==``.
@pytest.mark.parametrize(
    "expr,expected",
    [
        ("this.a == this.b", True),
        ("this.a != this.b", False),
        ("[this.a] == [this.b]", True),
        ("{'k': this.a} == {'k': this.b}", True),
        ("this.a in [this.b]", True),
        ("decimals.eq(this.a, this.b)", True),
        # Negative controls.
        ("this.a == decimal('9')", False),
        ("[this.a] == [decimal('9')]", False),
        ("this.a in [decimal('9')]", False),
    ],
)
def test_nested_proto_decimal_equality(validator, expr, expected):
    def dec(unscaled, scale):
        return decimal_pb2.Decimal(value=unscaled.to_bytes(2, "big"), scale=scale)

    # 1.50 (unscaled 150, scale 2) and 1.5 (unscaled 15, scale 1) - one number, two encodings.
    holder = {"a": dec(150, 2), "b": dec(15, 1)}
    assert validator.execute(rule(expr), None, holder) is expected


# Overriding the equality operators must not disturb anything that has no decimal in it.
@pytest.mark.parametrize(
    "expr,expected",
    [
        ("1 == 1", True),
        ("1 == 2", False),
        ("1 != 2", True),
        ("'a' == 'a'", True),
        ("[1, 2] == [1, 2]", True),
        ("[1, 2] == [2, 1]", False),
        ("{'a': 1} == {'a': 1}", True),
        ("2 in [1, 2]", True),
        ("3 in [1, 2]", False),
        ("b'x' == b'x'", True),
        ("null == null", True),
    ],
)
def test_equality_unchanged_without_decimals(validator, expr, expected):
    assert validator.execute(rule(expr), None, {"unused": 1}) is expected


# Cross-client parity: a bare ``confluent.type.Decimal`` field is usable with ``decimals.*``,
# ``==``, ``string()`` and ``double()`` with **no ``decimal(...)`` call** on it. The
# discriminating case is the scale-differing equality: a client comparing decimals by their
# protobuf encoding (unscaled bytes plus scale, field by field) answers False for
# ``decimal("12.340")``, because 12.34 and 12.340 are the same number in two encodings.
_BARE_PROTO_DECIMAL_CASES = [
    # Bare: no constructor call on the field.
    ('decimals.eq(this, decimal("12.34"))', True),
    ('decimals.gt(this, decimal("10.00"))', True),
    # The wrapped form must keep working (decimal(...) re-entry).
    ('decimals.eq(decimal(this), decimal("12.34"))', True),
    # `==` is numeric on it: 12.34 equals 12.340 despite the differing scale.
    ('this == decimal("12.340")', True),
    ('this != decimal("12.340")', False),
    ('decimals.lt(this, decimal("100"))', True),
    # Negative control: a false comparison must still be False.
    ('decimals.gt(this, decimal("100"))', False),
    ('string(this) == "12.34"', True),
    ('double(this) == 12.34', True),
]


@pytest.mark.parametrize("expr,expected", _BARE_PROTO_DECIMAL_CASES)
def test_proto_decimal_needs_no_constructor(validator, expr, expected):
    # 12.34 = unscaled 1234 at scale 2.
    d = decimal_pb2.Decimal(value=(1234).to_bytes(2, "big"), scale=2)
    assert validator.execute(rule(expr), d.DESCRIPTOR, d) is expected


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
        ('string(decimals.add(decimal("1E38"), decimal("1")))', "100000000000000000000000000000000000001"),
        (
            'string(decimals.mul(decimal("12345678901234567890"), ' 'decimal("98765432109876543210")))',
            "1219326311370217952237463801111263526900",
        ),
        # Scale preservation still holds for ordinary-magnitude operands.
        ('string(decimals.mul(decimal("2.0"), decimal("3.0")))', "6.00"),
        ('string(decimals.add(decimal("1.5"), decimal("1.25")))', "2.75"),
        # #31 negative-scale round/trunc — quantize target Decimal(1).scaleb(-scale),
        # so scale=-2 rounds/truncates to the hundreds place (not to an integer).
        ('string(decimals.round(decimal("1234.5"), -2))', "1200"),
        ('string(decimals.trunc(decimal("1234"), -2))', "1200"),
        # #32 no 28-digit cap on floor (30-digit value passes through, no error).
        ('string(decimals.floor(decimal("123456789012345678901234567890")))', "123456789012345678901234567890"),
        # #33 exact mod — quotient exceeds 38 digits, but remainder is exact.
        ('string(decimals.mod(decimal("1E40"), decimal("3")))', "1"),
        # #34 decimal(dyn) from a >28-digit string round-trips exactly.
        ('string(decimal("12345678901234567890123456789012345"))', "12345678901234567890123456789012345"),
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
        # An exponent that will not fit BigDecimal's signed-int scale. Measured against the
        # JVM, the accepted band is symmetric -- |exponent| <= INT32_MAX -- with both
        # +/-2147483648 a NumberFormatException there. Python's Decimal accepts them, and
        # rendering one as fixed-point would try to materialise billions of digits.
        'decimal("1e-2147483648") > decimal("0")',
        'decimal("1e2147483648") > decimal("0")',
        'decimal("1E+2147483648") > decimal("0")',
    ],
)
def test_decimal_rejects_inputs_java_bigdecimal_rejects(validator, expr):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule(expr), None, 1)


# The other side of the band: the widest exponents the JVM *accepts* must keep working, so
# the guard above cannot be off by one.
@pytest.mark.parametrize("expr", ['decimal("1e-2147483647")', 'decimal("1e2147483647")'])
def test_decimal_accepts_the_widest_exponents_java_accepts(validator, expr):
    assert validator.execute(rule(f"{expr} != decimal(\"0\")"), None, 1) is True


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
        # BigDecimal has no negative zero: BigDecimal("-0").toPlainString() is "0" (signum 0),
        # and a scale survives the sign being dropped ("-0.00" -> "0.00").
        ('string(decimal("-0"))', "0"),
        ('string(decimal("-0.00"))', "0.00"),
        ('string(decimals.round(decimal("-0.4"), 0))', "0"),
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

_VARIANT_JSON = '{"name":"alice","age":30,"scores":[10,20,30],"nested":{"x":1},"explicit":null}'


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
        "variants.as(variants.index(" "variants.field(variants.parseJson(this), 'scores'), 2), 'int') == 30",
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

    schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "confluent.type.Variant",
            "logicalType": "variant",
            "fields": [{"name": "metadata", "type": "bytes"}, {"name": "value", "type": "bytes"}],
        }
    )
    built = vu.parse_json('{"name":"alice","age":30}')
    value, metadata = built.value, built.metadata
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, vu.Variant(value, metadata))
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)
    assert isinstance(decoded, vu.Variant)
    assert (
        validator.execute(
            rule("variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'"), None, decoded
        )
        is True
    )


# A confluent.type.Variant proto field is bound into CEL as a celpy MessageType wrapper;
# variant(...) must unwrap it, mirroring the decimal test above and the JVM client.
def test_proto_variant_field_into_cel(validator):
    built = vu.parse_json('{"name":"alice","age":30}')
    value, metadata = built.value, built.metadata
    v = variant_pb2.Variant(value=value, metadata=metadata)
    expr = "variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'"
    assert validator.execute(rule(expr), v.DESCRIPTOR, v) is True


# Cross-client parity: a variant value is usable with the variants.* accessors with **no
# variant(...) call**, in both formats, and the wrapped form keeps working alongside it. The
# accessors are plain Python functions that coerce their subject, so they take whatever the
# decoder produced -- a vu.Variant from the Avro logical type, or a proto message.
_BARE_VARIANT_CASES = [
    # Bare: no constructor call.
    ("variants.type(this) == 'object'", True),
    ("variants.as(variants.field(this, 'name'), 'string') == 'alice'", True),
    ("variants.as(variants.path(this, '$.age'), 'int') == 30", True),
    # The wrapped form must keep working (variant(...) re-entry).
    ("variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'", True),
    # A missing key is CEL null, not an error.
    ("variants.field(this, 'nope') == null", True),
    # Negative control.
    ("variants.as(variants.field(this, 'name'), 'string') == 'bob'", False),
]


# ``variants.isNull`` must coerce its receiver like every other accessor. It is declared over
# dyn, so a bare variant field reaches it; a receiver check that only accepts the client's own
# Variant type answers False for the shapes a variant-typed field actually decodes to, reporting
# "not null" for a variant that holds an explicit JSON null. The bare-object cases above cannot
# catch this: isNull on an object is False either way, so only a variant that *is* null
# discriminates.
@pytest.mark.parametrize(
    "expr,expected",
    [
        ("variants.isNull(this)", True),
        # The wrapped form has always worked and must keep working.
        ("variants.isNull(variant(this))", True),
    ],
)
def test_proto_variant_is_null_coerces_bare_receiver(validator, expr, expected):
    built = vu.parse_json("null")
    v = variant_pb2.Variant(value=built.value, metadata=built.metadata)
    assert validator.execute(rule(expr), v.DESCRIPTOR, v) is expected


def test_proto_variant_is_null_false_for_non_null(validator):
    built = vu.parse_json("5")
    v = variant_pb2.Variant(value=built.value, metadata=built.metadata)
    assert validator.execute(rule("variants.isNull(this)"), v.DESCRIPTOR, v) is False


@pytest.mark.parametrize("expr,expected", _BARE_VARIANT_CASES)
def test_avro_variant_needs_no_constructor(validator, expr, expected):
    import io

    import fastavro

    import confluent_kafka.schema_registry.common.avro  # noqa: F401  (registers the logical type)

    schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "confluent.type.Variant",
            "logicalType": "variant",
            "fields": [{"name": "metadata", "type": "bytes"}, {"name": "value", "type": "bytes"}],
        }
    )
    built = vu.parse_json('{"name":"alice","age":30}')
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, vu.Variant(built.value, built.metadata))
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)
    assert validator.execute(rule(expr), None, decoded) is expected


@pytest.mark.parametrize("expr,expected", _BARE_VARIANT_CASES)
def test_proto_variant_needs_no_constructor(validator, expr, expected):
    built = vu.parse_json('{"name":"alice","age":30}')
    v = variant_pb2.Variant(value=built.value, metadata=built.metadata)
    assert validator.execute(rule(expr), v.DESCRIPTOR, v) is expected


# A string is rejected by variant(...) with a redirect to parseJson.
# An *absent* variant -- a protobuf field left unset, or an Avro variant record whose byte
# fields are empty -- carries no metadata, so there is nothing to read. It reads as CEL null and
# every accessor propagates that, rather than the Variant constructor raising on a metadata
# version byte that isn't there.
_ABSENT_VARIANT_CASES = [
    "variants.type(this) == null",
    # isNull is False, not an error: an absent variant is not a JSON null.
    "!variants.isNull(this)",
    "variants.field(this, 'name') == null",
    "variants.path(this, '$.name') == null",
    "variants.toJson(this) == null",
    # The explicit constructor reports it as CEL null too, like variant(null).
    "variant(this) == null",
]


@pytest.mark.parametrize("expr", _ABSENT_VARIANT_CASES)
def test_absent_proto_variant_reads_as_null(validator, expr):
    v = variant_pb2.Variant(value=b"", metadata=b"")
    assert validator.execute(rule(expr), v.DESCRIPTOR, v) is True


@pytest.mark.parametrize("expr", _ABSENT_VARIANT_CASES)
def test_absent_avro_variant_reads_as_null(validator, expr):
    # The mapping an Avro variant record decodes to, with empty byte fields.
    assert validator.execute(rule(expr), None, {"metadata": b"", "value": b""}) is True


def test_explicit_null_variant_is_not_absent(validator):
    # Absent must stay distinguishable from a variant that genuinely holds JSON null: the
    # former is CEL null, the latter a present variant whose type is NULL.
    assert validator.execute(rule("variants.isNull(variants.parseJson('null'))"), None, "null") is True
    assert validator.execute(rule("variants.type(variants.parseJson('null')) != null"), None, "null") is True


def test_variant_from_empty_metadata_bytes_is_rejected(validator):
    # Passing empty metadata explicitly is a rule-authoring mistake rather than an absent
    # field, so it is reported instead of yielding null.
    with pytest.raises(Exception) as exc:
        validator.execute(rule("variants.type(variant(b'', b'')) == 'object'"), None, "x")
    # The validator wraps rule failures, so the explanation is on the cause chain.
    chain = []
    err = exc.value
    while err is not None:
        chain.append(str(err))
        err = err.__cause__
    assert any("metadata is empty" in m for m in chain), chain


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
        'timestamp(253402300799000001, 6) == ' 'timestamp("9999-12-31T23:59:59.000001Z")',
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
        validator.execute(rule(f"timestamp(1700000000, {precision}) == timestamp(0)"), None, 1)
    assert "unknown precision" in str(excinfo.value.__cause__)


def test_timestamp_datetime_components_form_still_works(validator):
    # celpy's components form takes three or more args, so it never collides with
    # the two-arg precision form.
    assert validator.execute(rule('timestamp(2009, 2, 13) == timestamp("2009-02-13T00:00:00Z")'), None, 1) is True


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


# --------------------------------------------------------------------------------------
# string(timestamp) renders the sub-second component
#
# celpy's TimestampType.__str__ formats with strftime("%Y-%m-%dT%H:%M:%S%z") -- no %f -- so it
# dropped the fraction entirely: string(timestamp("...T22:13:20.123Z")) came back as
# "2023-11-14T22:13:20Z". The stored value was always correct (comparisons and getMilliseconds()
# agreed with the other clients), so this was a silent rendering divergence rather than an error.
# decimal_funcs._string now routes a Timestamp through timestamp_funcs.format_timestamp.
#
# Every expectation below is the verbatim output of the Java reference for the same expression.
@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("string(timestamp(1700000000, 0))", "2023-11-14T22:13:20Z"),
        ("string(timestamp(1700000000123, 3))", "2023-11-14T22:13:20.123Z"),
        ("string(timestamp(1700000000123456, 6))", "2023-11-14T22:13:20.123456Z"),
        # A whole millisecond keeps its trailing zeros (3-digit group), not ".1Z".
        ("string(timestamp(1700000000100, 3))", "2023-11-14T22:13:20.100Z"),
        ("string(timestamp('2023-11-14T22:13:20.5Z'))", "2023-11-14T22:13:20.500Z"),
        # A zero fraction emits no decimal point at all.
        ("string(timestamp(1700000000000, 3))", "2023-11-14T22:13:20Z"),
        ("string(timestamp(0))", "1970-01-01T00:00:00Z"),
        # Pre-epoch, where the fraction is a non-negative nano-of-second.
        ("string(timestamp(-1500, 3))", "1969-12-31T23:59:58.500Z"),
        # Rendered in UTC with a Z suffix whatever offset the literal carried.
        ("string(timestamp('2020-01-01T00:00:00+05:00'))", "2019-12-31T19:00:00Z"),
    ],
)
def test_string_timestamp_renders_subsecond(validator, expr, expected):
    assert validator.execute(rule(expr), None, 1) == expected


def test_string_timestamp_nanos_limited_to_microseconds(validator):
    # datetime's resolution is one microsecond, so a nanosecond-precision value renders 6 digits
    # where Java renders 9 (".123456789Z"). That is the same pre-existing limit that floors the
    # value itself in timestamp_funcs._from_epoch, not something the formatting introduces.
    assert (
        validator.execute(rule("string(timestamp(1700000000123456789, 9))"), None, 1) == "2023-11-14T22:13:20.123456Z"
    )


# Rescaling is bounded by this client's own width ceiling (`_SANE_WIDTH`, 10**7 digits), not
# by BigDecimal's. That is a deliberate divergence: BigInteger tops out at Integer.MAX_VALUE
# bits = 646456993 digits, and reproducing that bound across six decimal libraries is neither
# achievable nor the point. What matters is that a wide rescale becomes a *rule error* rather
# than resource exhaustion, which is the one failure a framework cannot attribute after the
# fact. Java is the only client in the family that fails cleanly on width; this stands in.
#
# The bound has to be enforced here rather than delegated, because libmpdec honours any int32
# scale and materialises the whole coefficient: a scale of 2**31-1 costs 918 MB inside
# quantize and 9.2 GB once anything calls as_tuple() on the result.
#
# The quantizer itself is built in _EXACT_CONTEXT, not the ambient one, for an unrelated
# reason kept here because it is the same call: the ambient Emin of -999999 made
# `Decimal(1).scaleb(1000000)` raise decimal.Overflow, so a negative scale past a million was
# refused for values the JVM rounds happily - and Overflow is not an InvalidOperation, so it
# escaped as a raw Python exception rather than a rule error.
#
# So the accepted/rejected split below is *this client's* limit, and the JDK column is
# recorded only where the two now differ.
@pytest.mark.parametrize(
    "expr",
    [
        # Copilot's case, and the one that raised an uncaught decimal.Overflow.
        'string(decimals.round(decimal("1e1000000"), -1000000)) != ""',
        'string(decimals.round(decimal("1.23"), -1000000)) != ""',
        'string(decimals.trunc(decimal("1.23"), -1000000)) != ""',
        # A wide scale in the other direction, at 10**6 - an order under the ceiling.
        'decimals.round(decimal("1e1000000"), 1000000) != decimal("0")',
        # *Coarsening* a scale is free at any distance - the coefficient shrinks rather than
        # grows - so none of these is bounded. Measured, all instant and all one digit wide:
        # 1.23 at scale -1000000 / -100000000 / -2000000000, and 1e-1000000 and 1e-100000000
        # at scale 0. Java agrees: BigDecimal("1.23").setScale(-100000000) is precision 1.
        # An `abs(shift) + digits` formula refused every one of them.
        'decimals.round(decimal("1.23"), -100000000) == decimal("0")',
        'decimals.round(decimal("1.23"), -1000000000) == decimal("0")',
        'decimals.trunc(decimal("1.23"), -1000000000) == decimal("0")',
        'decimals.round(decimal("1e-20000000")) == decimal("0")',
        'decimals.floor(decimal("1e-20000000")) == decimal("0")',
        'decimals.ceil(decimal("1e-20000000")) == decimal("1")',
        'decimals.trunc(decimal("1e-20000000")) == decimal("0")',
        'decimals.round(decimal("1e-100000000")) == decimal("0")',
        # A coarsened result can leave the int32 scale domain - 1.23 at scale -2147483648 is
        # 0E+2147483648 - and that is refused at the wire, not here, the same way mul's result
        # is (see test_cel_message_transform). The operator itself is free.
        'decimals.round(decimal("1.23"), -2147483648) == decimal("0")',
        'decimals.round(decimal("1e1000000"), -2147483648) == decimal("0")',
        # A no-op in Java too, via its `intScale >= v.scale()` early return, so no rescale
        # happens and no bound applies.
        'string(decimals.trunc(decimal("1.23"), 2147483647)) == "1.23"',
        # Zero rescales for free at any scale, so the width formula must exempt it - measured,
        # both directions cost nothing and the result stays compact. BigDecimal agrees:
        # `new BigDecimal(BigInteger.ZERO, 2147483647)` is precision 1. Without the exemption
        # these are false rejections of values the reference handles.
        'decimals.round(decimal(b"", 2147483647), 0) == decimal("0")',
        'decimals.round(decimal("0"), 2147483647) == decimal("0")',
        'decimals.floor(decimal(b"", 2147483647)) == decimal("0")',
        'decimals.ceil(decimal(b"", 2147483647)) == decimal("0")',
    ],
)
def test_round_accepts_the_wide_scales_within_this_clients_ceiling(validator, expr):
    assert validator.execute(rule(expr), None, 1) is True


@pytest.mark.parametrize(
    "expr",
    [
        # Only *expanding* a scale costs anything, and these are just past the 10**7 ceiling.
        # The JDK accepts them - setScale(1e8) on 1.23 is a 100000001-digit BigDecimal, 952 MB
        # measured here - and this client refuses them, by design.
        'decimals.round(decimal("1.23"), 100000000) != decimal("0")',
        'decimals.round(decimal("1.23"), 646456993) != decimal("0")',
        'decimals.round(decimal("1.23"), 1000000000) != decimal("0")',
        'decimals.round(decimal("1.23"), 2147483647) != decimal("0")',
        # trunc is absent on purpose: Java early-returns when `intScale >= v.scale()`, which
        # this client mirrors, so trunc only ever *coarsens* - and coarsening is free. It
        # cannot reach an expanding rescale by any argument, which is why the one-argument
        # forms below list it as unguarded rather than guarded.
    ],
)
def test_round_rejects_the_scales_past_this_clients_ceiling(validator, expr):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule(expr), None, 1)


# The one-argument forms quantize to scale 0 and so are rescales too, but three of the five
# call sites did not go through the guarded helper - `round(x)`, `floor(x)` and `ceil(x)`
# reached `d.quantize(Decimal(1), ...)` directly. Each is a multi-GB allocation reachable from
# a rule that names no scale at all, which is the failure mode of a guard hung off one helper
# rather than off the operation. `trunc(x)` was safe only by accident, via its early return.
@pytest.mark.parametrize(
    "expr",
    [
        # Scale 0 is a coarsening for a fractional value, so the one-argument forms are
        # bounded only when the value's *integer* part is what has to be built: 1e20000000 at
        # scale 0 is a 20000001-digit coefficient. (Which also means these three call sites
        # were unguarded for the wrong reason before - the bug was real, the demonstration
        # of it was not.)
        'decimals.round(decimal("1e20000000")) != decimal("0")',
        'decimals.floor(decimal("1e20000000")) != decimal("0")',
        'decimals.ceil(decimal("1e20000000")) != decimal("0")',
    ],
)
def test_the_one_argument_rounding_family_is_guarded_too(validator, expr):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule(expr), None, 1)


# An order of magnitude under the ceiling, the same expressions answer.
@pytest.mark.parametrize(
    "expr",
    [
        'decimals.round(decimal("1e1000000")) != decimal("0")',
        'decimals.floor(decimal("1e1000000")) != decimal("0")',
        'decimals.ceil(decimal("1e1000000")) != decimal("0")',
        # trunc never rescales here at all - its `scale >= current scale` early return fires
        # for any value with a non-negative exponent - so it is unbounded by construction.
        'decimals.trunc(decimal("1e1000000")) != decimal("0")',
        'decimals.trunc(decimal("1e20000000")) != decimal("0")',
        'decimals.trunc(decimal("1e-2000000000"), -1000000000) == decimal("0")',
        'decimals.round(decimal("2.5")) == decimal("3")',
        'decimals.floor(decimal("-1.5")) == decimal("-2")',
        'decimals.ceil(decimal("1.5")) == decimal("2")',
        'decimals.trunc(decimal("-1.9")) == decimal("-1")',
    ],
)
def test_the_one_argument_rounding_family_still_answers(validator, expr):
    assert validator.execute(rule(expr), None, 1) is True


# Rendering is the third width site, and it does not come from a rescale: `div` holds its
# coefficient to 38 digits while its exponent runs free, so the value below is cheap to
# compute and four billion characters to print. Measured: rendering a 10**8-digit value costs
# 204 MB. No zero shortcut here, unlike the rescale guard - a zero at an extreme scale renders
# as that many zeros.
@pytest.mark.parametrize(
    "expr",
    [
        'string(decimals.div(decimal("1e-2147483647"), decimal("1e2147483647"))) != ""',
        'string(decimal("1e2147483647")) != ""',
        'string(decimal("1e-2147483647")) != ""',
        'string(decimal(b"", 2147483647)) != ""',
    ],
)
def test_rendering_a_wide_plain_form_is_refused(validator, expr):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule(expr), None, 1)


def test_rendering_still_works_below_the_ceiling(validator):
    assert validator.execute(rule('string(decimal("12.34")) == "12.34"'), None, 1) is True
    assert validator.execute(rule('string(decimal("1e1000000")) != ""'), None, 1) is True
    # The value the guard is computed from is the plain form, not the coefficient: this one
    # has a single digit and a million-place exponent.
    assert validator.execute(rule('string(decimal("1e-1000000")) != ""'), None, 1) is True


# variants.index is declared (DYN, INT) and variants.as / variants.tryAs (DYN, STRING), so a
# wrong-typed second argument fails to bind on the JVM whatever the receiver holds. Both
# checks used to come *after* the receiver was inspected, or not at all:
#
#   * variants.index(anObject, 1.5) answered CEL null - the receiver was not an array, so the
#     index's own type was never reached, and the argument error depended on runtime shape;
#   * variants.tryAs(v, 1) stringified the 1 to "1", took the unknown-type branch and
#     returned CEL null. Null is tryAs's answer for a type *mismatch*, so a call that names
#     no type at all was indistinguishable from a variant of the wrong shape.
@pytest.mark.parametrize(
    "expr",
    [
        # A non-array receiver: the index type is still what is wrong with the call.
        "variants.index(variants.parseJson('{\"a\":1}'), 1.5) == null",
        "variants.index(variants.parseJson('{\"a\":1}'), true) == null",
        # And an array receiver, where the check already fired.
        "variants.index(variants.parseJson('[1,2]'), 1.5) == null",
        "variants.index(variants.parseJson('[1,2]'), true) == null",
        # A type name that is not a string.
        "variants.tryAs(variants.parseJson('\"x\"'), 1) == null",
        "variants.tryAs(variants.parseJson('\"x\"'), true) == null",
        "variants.as(variants.parseJson('\"x\"'), 1) == 'x'",
    ],
)
def test_variant_argument_types_are_checked_before_the_receiver(validator, expr):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule(expr), None, 1)


# The must-fail twins: the well-typed calls still work, in both receiver shapes.
@pytest.mark.parametrize(
    "expr",
    [
        "variants.index(variants.parseJson('[10,20,30]'), 2) != null",
        "variants.index(variants.parseJson('[10,20,30]'), 9) == null",
        # A non-array receiver is still CEL null, not an error, once the index type is right.
        "variants.index(variants.parseJson('{\"a\":1}'), 0) == null",
        "variants.tryAs(variants.parseJson('\"x\"'), 'string') == 'x'",
        "variants.tryAs(variants.parseJson('\"x\"'), 'int') == null",
        "variants.as(variants.parseJson('\"x\"'), 'string') == 'x'",
    ],
)
def test_variant_well_typed_arguments_still_work(validator, expr):
    assert validator.execute(rule(expr), None, 1) is True


# Arithmetic is bounded by *width*, and the dividing line is not arithmetic vs. rescale - it
# is whether the operation has to align two exponents. `add` and `sub` do: the narrower
# operand is expanded into the wider one's positional frame before a single digit is computed.
# `remainder` is in the same family but is bounded by its integral quotient, which libmpdec
# short-circuits when the dividend is the smaller operand. `mul` does not align - it adds the
# exponents and multiplies the coefficients. `div` does not - it holds the coefficient to the
# context precision. Comparison does not - libmpdec short-circuits on the adjusted exponent.
#
# Measured, peak RSS, operands 1e2147483647 and 3:
#
#   mul, div, <, ==, compare, min, neg, abs             13 MB
#   add                                               1738 MB
#   sub                                               1738 MB
#   remainder                                         1733 MB
#   add(1e2147483647, 1e-2147483647)                  3125 MB
#   remainder(1e-2147483647, 1e2147483647)              13 MB   <- quotient is 0
#   remainder(1e2147483647, 1e2147483000)               13 MB   <- quotient is 647 digits
#
# So three of six arithmetic operations reach a multi-GB allocation from a single expression
# over two operands each cheap to construct. An earlier design guarded `mul` and `div` on a
# prediction of BigDecimal's own domain errors - the operations that turn out to cost nothing
# - and this is the correction. `mul` is now unguarded; a result whose scale no int32 can
# carry is refused at the wire boundary instead, where it is actually a problem (see
# test_cel_message_transform).
@pytest.mark.parametrize(
    "expr",
    [
        # Alignment: the narrower operand expands into the wider one's frame.
        'decimals.add(decimal("1e2147483647"), decimal("1")) != decimal("0")',
        'decimals.add(decimal("1e-2147483647"), decimal("1")) != decimal("0")',
        'decimals.add(decimal("1e2147483647"), decimal("1e-2147483647")) != decimal("0")',
        'decimals.sub(decimal("1e2147483647"), decimal("1e-2147483647")) != decimal("0")',
        # remainder, via the integral quotient it has to produce.
        'decimals.mod(decimal("1e2147483647"), decimal("3")) != decimal("0")',
        'decimals.mod(decimal("1e2147483647"), decimal("1e-2147483647")) != decimal("0")',
        'decimals.mod(decimal("1.5"), decimal("1e-2147483647")) != decimal("0")',
    ],
)
def test_alignment_width_is_refused(validator, expr):
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule(expr), None, 1)


@pytest.mark.parametrize(
    "expr, expected",
    [
        # Ordinary arithmetic, unchanged.
        ('string(decimals.mul(decimal("1.5"), decimal("2.5")))', "3.75"),
        ('string(decimals.add(decimal("12.34"), decimal("1.5")))', "13.84"),
        ('string(decimals.sub(decimal("12.34"), decimal("1.5")))', "10.84"),
        ('string(decimals.mod(decimal("12.34"), decimal("1.5")))', "0.34"),
        ('string(decimals.mod(decimal("1E40"), decimal("3")))', "1"),
        # mul and div are not guarded at all, at any width - measured, they cost nothing.
        # The first two were refused by the earlier design; both are exact and cheap.
        ('decimals.mul(decimal("1e2147483647"), decimal("1e2147483647")) != decimal("0")', True),
        ('decimals.mul(decimal("1e-2147483647"), decimal("1e-2147483647")) != decimal("0")', True),
        ('decimals.mul(decimal("1e2147483647"), decimal("10")) != decimal("0")', True),
        ('decimals.mul(decimal("1e2147483647"), decimal("1e-2147483647")) == decimal("1")', True),
        ('decimals.div(decimal("1e-2147483647"), decimal("1e2147483647")) != decimal("0")', True),
        # Comparison of two extreme operands, which short-circuits on the adjusted exponent.
        ('decimals.lt(decimal("1e-2147483647"), decimal("1e2147483647"))', True),
        ('decimal("1e2147483647") != decimal("1e-2147483647")', True),
        # Alignment that stays narrow because the exponents are close, however extreme they
        # both are. A guard on the operands' magnitudes rather than their difference would
        # falsely refuse all of these.
        ('decimals.add(decimal("1e2147483647"), decimal("1e2147483647")) != decimal("0")', True),
        ('decimals.sub(decimal("1e2147483647"), decimal("1e2147483646")) != decimal("0")', True),
        ('decimals.add(decimal("1e1000"), decimal("1e-1000")) != decimal("0")', True),
        # remainder whose integral quotient is small, however far apart the operands are.
        ('decimals.mod(decimal("1e-2147483647"), decimal("1e2147483647")) != decimal("0")', True),
        ('decimals.mod(decimal("1e2147483647"), decimal("1e2147483000")) == decimal("0")', True),
    ],
)
def test_the_cheap_operations_stay_unguarded(validator, expr, expected):
    result = validator.execute(rule(expr), None, 1)
    assert result is expected if expected is True else result == expected


# The coefficient arriving from the wire is the width risk on the (bytes, scale) constructor;
# the scale is not, because `scaleb` only sets the exponent. Checked from the byte count
# before `int.from_bytes` builds the integer - one byte carries about 2.41 decimal digits -
# and against the *encodable* ceiling rather than the computation one, since a coefficient
# this client cannot write back is not worth reading in.
def test_a_wide_coefficient_from_bytes_is_refused(validator):
    # 4300 digits is about 1785 bytes, so this is comfortably past it without needing a
    # multi-megabyte literal.
    with pytest.raises(RuleError, match="Could not execute validation rule 'r'"):
        validator.execute(rule('decimal(b"' + "\\x01" * 4000 + '", 0) != decimal("0")'), None, 1)


def test_an_ordinary_coefficient_from_bytes_still_works(validator):
    assert validator.execute(rule('decimal(b"\\x04\\xd2", 2) == decimal("12.34")'), None, 1) is True
    # An extreme scale on a small coefficient is fine: it only sets the exponent.
    assert validator.execute(rule('decimal(b"\\x01", 2147483647) != decimal("0")'), None, 1) is True
