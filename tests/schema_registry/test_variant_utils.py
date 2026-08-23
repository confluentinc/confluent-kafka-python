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
"""Tests for the Variant binary codec (reader, builder, and JSON conversion)."""

import base64
import decimal
import struct
import uuid as uuid_mod

import pytest

from confluent_kafka.schema_registry.confluent.types import variant_utils as vu
from confluent_kafka.schema_registry.confluent.types.variant_utils import (
    Variant,
    VariantError,
    VariantType,
)

# Minimal empty metadata: version 1, offset_size 1, dictionary_size 0. Enough for any
# scalar/array value that references no object keys.
EMPTY_META = b"\x01\x00\x00"


def prim(code, payload=b""):
    """A bare primitive Variant with the given type code and payload."""
    return Variant(bytes([code << 2]) + payload, EMPTY_META)


def decimal_value(code, scale, unscaled, width):
    return prim(code, bytes([scale]) + unscaled.to_bytes(width, "little", signed=True))


# --------------------------------------------------------------------------------------
# parse_json + navigation
# --------------------------------------------------------------------------------------


def test_parse_json_navigation_and_scalars():
    v = vu.parse_json(
        '{"name":"alice","age":30,"scores":[10,20,30],"nested":{"x":1},"explicit":null}')
    assert v.get_type() == VariantType.OBJECT
    assert v.num_object_fields() == 5
    assert v.get_field_by_key("name").get_string() == "alice"
    assert v.get_field_by_key("age").get_long() == 30
    assert v.get_field_by_key("scores").get_element_at_index(2).get_long() == 30
    assert v.get_field_by_key("scores").num_array_elements() == 3
    # A missing field is absent (None); an explicit JSON null is a present NULL-typed variant.
    assert v.get_field_by_key("missing") is None
    assert v.get_field_by_key("explicit").get_type() == VariantType.NULL


def test_field_by_key_binary_search_path():
    # More than the linear/binary-search threshold (32) fields exercises binary search.
    obj = {("k%02d" % i): i for i in range(40)}
    import json
    v = vu.parse_json(json.dumps(obj))
    assert v.get_field_by_key("k39").get_long() == 39
    assert v.get_field_by_key("k00").get_long() == 0
    assert v.get_field_by_key("k40") is None


def test_out_of_bounds_index_is_none():
    v = vu.parse_json("[1, 2, 3]")
    assert v.get_element_at_index(0).get_long() == 1
    assert v.get_element_at_index(3) is None
    assert v.get_element_at_index(-1) is None


# --------------------------------------------------------------------------------------
# get_type for every primitive
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "variant, expected",
    [
        (prim(vu.NULL), VariantType.NULL),
        (prim(vu.TRUE), VariantType.BOOLEAN),
        (prim(vu.FALSE), VariantType.BOOLEAN),
        (prim(vu.INT1, struct.pack("<b", 1)), VariantType.BYTE),
        (prim(vu.INT2, struct.pack("<h", 1)), VariantType.SHORT),
        (prim(vu.INT4, struct.pack("<i", 1)), VariantType.INT),
        (prim(vu.INT8, struct.pack("<q", 1)), VariantType.LONG),
        (prim(vu.DOUBLE, struct.pack("<d", 1.0)), VariantType.DOUBLE),
        (prim(vu.FLOAT, struct.pack("<f", 1.0)), VariantType.FLOAT),
        (decimal_value(vu.DECIMAL4, 2, 1234, 4), VariantType.DECIMAL4),
        (decimal_value(vu.DECIMAL8, 2, 1234, 8), VariantType.DECIMAL8),
        (decimal_value(vu.DECIMAL16, 2, 1234, 16), VariantType.DECIMAL16),
        (prim(vu.DATE, struct.pack("<i", 18262)), VariantType.DATE),
        (prim(vu.TIMESTAMP, struct.pack("<q", 0)), VariantType.TIMESTAMP_TZ),
        (prim(vu.TIMESTAMP_NTZ, struct.pack("<q", 0)), VariantType.TIMESTAMP_NTZ),
        (prim(vu.TIME, struct.pack("<q", 0)), VariantType.TIME),
        (prim(vu.TIMESTAMP_NANOS, struct.pack("<q", 0)), VariantType.TIMESTAMP_NANOS_TZ),
        (prim(vu.TIMESTAMP_NANOS_NTZ, struct.pack("<q", 0)), VariantType.TIMESTAMP_NANOS_NTZ),
        (prim(vu.BINARY, struct.pack("<I", 0)), VariantType.BINARY),
        (prim(vu.UUID, b"\x00" * 16), VariantType.UUID),
    ],
)
def test_get_type(variant, expected):
    assert variant.get_type() == expected


def test_short_and_long_string_type():
    assert vu.parse_json('"hi"').get_type() == VariantType.STRING
    assert vu.parse_json('"%s"' % ("x" * 100)).get_type() == VariantType.STRING


# --------------------------------------------------------------------------------------
# scalar getters
# --------------------------------------------------------------------------------------


def test_integer_widths_read_as_long():
    assert prim(vu.INT1, struct.pack("<b", -5)).get_long() == -5
    assert prim(vu.INT2, struct.pack("<h", -300)).get_long() == -300
    assert prim(vu.INT4, struct.pack("<i", 100000)).get_long() == 100000
    assert prim(vu.INT8, struct.pack("<q", 9876543210)).get_long() == 9876543210


def test_float_and_double():
    assert prim(vu.DOUBLE, struct.pack("<d", 2.5)).get_double() == 2.5
    assert prim(vu.FLOAT, struct.pack("<f", 1.5)).get_float() == 1.5


def test_narrowed_integer_getters():
    # get_byte accepts only INT1.
    assert prim(vu.INT1, struct.pack("<b", -5)).get_byte() == -5
    # get_short widens INT1 -> INT2.
    assert prim(vu.INT1, struct.pack("<b", -5)).get_short() == -5
    assert prim(vu.INT2, struct.pack("<h", -300)).get_short() == -300
    # get_int widens INT1/INT2 -> INT4.
    assert prim(vu.INT1, struct.pack("<b", -5)).get_int() == -5
    assert prim(vu.INT2, struct.pack("<h", -300)).get_int() == -300
    assert prim(vu.INT4, struct.pack("<i", 100000)).get_int() == 100000


def test_narrowed_integer_getters_reject_wider():
    # get_byte rejects anything wider than INT1.
    with pytest.raises(VariantError):
        prim(vu.INT2, struct.pack("<h", 1)).get_byte()
    # get_short rejects INT4.
    with pytest.raises(VariantError):
        prim(vu.INT4, struct.pack("<i", 1)).get_short()
    # get_int rejects INT8.
    with pytest.raises(VariantError):
        prim(vu.INT8, struct.pack("<q", 1)).get_int()


def test_get_double_rejects_float():
    # get_double is now exact DOUBLE-only and must not widen a FLOAT.
    with pytest.raises(VariantError):
        prim(vu.FLOAT, struct.pack("<f", 1.5)).get_double()


def test_get_float_rejects_double():
    with pytest.raises(VariantError):
        prim(vu.DOUBLE, struct.pack("<d", 2.5)).get_float()


def test_boolean_and_binary_and_uuid():
    assert prim(vu.TRUE).get_boolean() is True
    assert prim(vu.FALSE).get_boolean() is False
    payload = struct.pack("<I", 4) + bytes([1, 2, 3, 4])
    assert prim(vu.BINARY, payload).get_binary() == bytes([1, 2, 3, 4])
    u = uuid_mod.UUID("00112233-4455-6677-8899-aabbccddeeff")
    assert prim(vu.UUID, u.bytes).get_uuid() == u


@pytest.mark.parametrize(
    "code, scale, unscaled, width, expected",
    [
        (vu.DECIMAL4, 2, 1234, 4, "12.34"),
        (vu.DECIMAL8, 2, 1234, 8, "12.34"),
        (vu.DECIMAL16, 2, 1234, 16, "12.34"),
        (vu.DECIMAL4, 2, 150, 4, "1.50"),  # scale/trailing zero preserved
    ],
)
def test_decimal_scale_preserved(code, scale, unscaled, width, expected):
    assert decimal_value(code, scale, unscaled, width).get_decimal() == decimal.Decimal(expected)


# #34: get_decimal must scale the unscaled value EXACTLY (java.math.BigDecimal semantics),
# not silently round unscaled values with >28 significant digits under the thread-local
# default context (prec=28). A 35-digit DECIMAL16 unscaled value at scale 5 round-trips.
def test_decimal_large_unscaled_is_not_rounded():
    unscaled = 12345678901234567890123456789012345  # 35 digits, > default prec 28
    result = decimal_value(vu.DECIMAL16, 5, unscaled, 16).get_decimal()
    assert format(result, "f") == "123456789012345678901234567890.12345"
    assert result == decimal.Decimal("123456789012345678901234567890.12345")


# --------------------------------------------------------------------------------------
# to_json — the cross-language contract (matches the Java reference)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "variant, expected",
    [
        # Instant (TZ): seconds always present, 'Z', 0/3/6/9 fractional grouping.
        (prim(vu.TIMESTAMP, struct.pack("<q", 1577836800000000)), '"2020-01-01T00:00:00Z"'),
        (prim(vu.TIMESTAMP, struct.pack("<q", 1577836800123000)), '"2020-01-01T00:00:00.123Z"'),
        (prim(vu.TIMESTAMP, struct.pack("<q", 1577836800123456)), '"2020-01-01T00:00:00.123456Z"'),
        # NTZ: seconds always present (never omitted), no zone.
        (prim(vu.TIMESTAMP_NTZ, struct.pack("<q", 1577836800000000)), '"2020-01-01T00:00:00"'),
        (prim(vu.TIMESTAMP_NTZ, struct.pack("<q", 1577836830000000)), '"2020-01-01T00:00:30"'),
        # Nanos.
        (prim(vu.TIMESTAMP_NANOS, struct.pack("<q", 1577836800123456789)),
         '"2020-01-01T00:00:00.123456789Z"'),
        # Time: seconds always present.
        (prim(vu.TIME, struct.pack("<q", 45296123456)), '"12:34:56.123456"'),
        (prim(vu.TIME, struct.pack("<q", 45240000000)), '"12:34:00"'),
        # Date.
        (prim(vu.DATE, struct.pack("<i", 18262)), '"2020-01-01"'),
        # UUID + binary.
        (prim(vu.UUID, uuid_mod.UUID("00112233-4455-6677-8899-aabbccddeeff").bytes),
         '"00112233-4455-6677-8899-aabbccddeeff"'),
        (prim(vu.BINARY, struct.pack("<I", 4) + bytes([0, 1, 2, 3])),
         '"' + base64.b64encode(bytes([0, 1, 2, 3])).decode() + '"'),
    ],
)
def test_to_json_scalar_contract(variant, expected):
    assert variant.to_json() == expected


def test_to_json_decimal_is_plain_not_scientific():
    # 1E-7 must render fixed-point, matching the Java toPlainString contract.
    assert decimal_value(vu.DECIMAL4, 7, 1, 4).to_json() == "0.0000001"
    assert decimal_value(vu.DECIMAL4, 2, 150, 4).to_json() == "1.50"


def test_to_json_roundtrip_structure():
    src = '{"a":1,"b":[true,null,"x"],"c":{"d":2}}'
    # Object keys are emitted in sorted order.
    assert vu.parse_json(src).to_json() == '{"a":1,"b":[true,null,"x"],"c":{"d":2}}'


def test_to_json_non_ascii_string_is_raw_utf8():
    # Cross-language contract: non-ASCII must pass through raw (no \\uXXXX escapes),
    # matching Java/Rust/JS/C++. Control chars and quotes are still escaped.
    for text, expected in [
        ("café", '"café"'),          # café -> raw, not "café"
        ("日本語", '"日本語"'),  # 日本語 -> raw
        ('a"b', '"a\\"b"'),                     # quote still escaped
        ("a\tb\nc", '"a\\tb\\nc"'),             # control chars still escaped
    ]:
        b = vu.VariantBuilder()
        b.append_string(text)
        rendered = b.build().to_json()
        assert rendered == expected
        assert "\\u" not in rendered.replace("\\\\u", "")

    # Non-ASCII object keys must also pass through raw.
    b = vu.VariantBuilder()
    b.start_object()
    b.append_key("café")
    b.append_string("résumé")
    b.end_object()
    assert b.build().to_json() == '{"café":"résumé"}'


# --------------------------------------------------------------------------------------
# non-finite doubles/floats (Confluent Java contract: bareword NaN/Infinity/-Infinity,
# diverging from Spark which quotes them)
# --------------------------------------------------------------------------------------


def test_to_json_non_finite_double_is_bareword():
    # The builder must accept and store non-finite doubles, and to_json must emit the
    # capitalized bareword tokens (no quotes, not lowercase nan/inf from str()).
    for value, expected in [
        (float("nan"), "NaN"),
        (float("inf"), "Infinity"),
        (float("-inf"), "-Infinity"),
    ]:
        b = vu.VariantBuilder()
        b.append_double(value)
        assert b.build().to_json() == expected


def test_to_json_non_finite_float_is_bareword():
    for value, expected in [
        (float("nan"), "NaN"),
        (float("inf"), "Infinity"),
        (float("-inf"), "-Infinity"),
    ]:
        b = vu.VariantBuilder()
        b.append_float(value)
        assert b.build().to_json() == expected


def test_parse_json_non_finite_barewords_roundtrip():
    # Bareword non-finite literals parse (Python json.loads accepts them by default) and
    # round-trip back to the same bareword tokens.
    for tok in ("NaN", "Infinity", "-Infinity"):
        assert vu.parse_json(tok).to_json() == tok


def test_parse_json_overflow_magnitude_becomes_infinity():
    # An out-of-range magnitude parses to a stored infinity and renders as the bareword.
    assert vu.parse_json("1e400").to_json() == "Infinity"


# --------------------------------------------------------------------------------------
# malformed input
# --------------------------------------------------------------------------------------


def test_unsupported_metadata_version_raises():
    with pytest.raises(VariantError, match="version"):
        Variant(b"\x00", b"\x02\x00\x00")  # version 2 in metadata header


def test_parse_json_malformed_raises():
    with pytest.raises(ValueError):
        vu.parse_json("{not json")


def test_parse_json_empty_or_whitespace_raises_value_error():
    # Empty/whitespace-only input must be a normal typed ValueError (json.JSONDecodeError
    # is a ValueError subclass) so variants.tryParseJson catches it -> CEL null, rather
    # than an unexpected crash.
    for src in ("", "   ", "\t\n"):
        with pytest.raises(ValueError):
            vu.parse_json(src)


def test_wrong_getter_raises():
    with pytest.raises(VariantError):
        prim(vu.TRUE).get_string()
    with pytest.raises(VariantError):
        prim(vu.NULL).get_long()


# --------------------------------------------------------------------------------------
# VariantBuilder (flat streaming writer)
# --------------------------------------------------------------------------------------


def test_builder_matches_parse_json_byte_for_byte():
    # A big integer wider than 64 bits parses as a scale-0 DECIMAL16 - the one decimal
    # form parse_json emits - so the programmatic decimal append can match it exactly.
    big = 10 ** 20
    src = (
        '{"id":42,"name":"hello","active":true,"score":3.5,'
        '"amount":%d,"missing":null,"nums":[1,2,3],"nested":{"a":1}}' % big
    )

    b = vu.VariantBuilder()
    b.start_object()
    b.append_key("id")
    b.append_byte(42)                      # parse_json encodes 42 as INT1
    b.append_key("name")
    b.append_string("hello")
    b.append_key("active")
    b.append_boolean(True)
    b.append_key("score")
    b.append_double(3.5)
    b.append_key("amount")
    b.append_decimal((big).to_bytes(9, byteorder="big", signed=True), 0)
    b.append_key("missing")
    b.append_null()
    b.append_key("nums")
    b.start_array()
    b.append_byte(1)
    b.append_byte(2)
    b.append_byte(3)
    b.end_array()
    b.append_key("nested")
    b.start_object()
    b.append_key("a")
    b.append_byte(1)
    b.end_object()
    b.end_object()
    built = b.build()

    parsed = vu.parse_json(src)

    # Canonical-equivalence via JSON.
    assert built.to_json() == parsed.to_json()
    # Byte-identical value + metadata.
    assert built.value == parsed.value
    assert built.metadata == parsed.metadata


def test_builder_native_decimal_overload_matches_bytes_overload():
    b1 = vu.VariantBuilder()
    b1.append_decimal(decimal.Decimal("1.50"))
    b2 = vu.VariantBuilder()
    b2.append_decimal((150).to_bytes(2, byteorder="big", signed=True), 2)
    assert b1.build().value == b2.build().value
    assert b1.build().to_json() == "1.50"


def test_builder_root_scalar():
    b = vu.VariantBuilder()
    b.append_long(1234567890123)
    v = b.build()
    assert v.get_type() == VariantType.LONG
    assert v.get_long() == 1234567890123
    assert v.to_json() == "1234567890123"


def test_float_renders_float32_shortest():
    # Bug #7: the FLOAT case widened the f64 through the double formatter, emitting the
    # f64-shortest string (e.g. "0.10000000149011612") instead of the float32-shortest
    # string ("0.1") that Java Float.toString / Apache Arrow produce.
    def render(f):
        b = vu.VariantBuilder()
        b.append_float(f)
        return b.build().to_json()

    assert render(0.1) == "0.1"
    assert render(0.3) == "0.3"
    assert render(2.0) == "2.0"  # integer ".0" preserved


def test_builder_append_key_outside_object_raises():
    b = vu.VariantBuilder()
    with pytest.raises(VariantError):
        b.append_key("x")


def test_builder_value_without_key_in_object_raises():
    b = vu.VariantBuilder()
    b.start_object()
    with pytest.raises(VariantError):
        b.append_long(1)


def test_builder_build_with_open_container_raises():
    b = vu.VariantBuilder()
    b.start_array()
    b.append_long(1)
    with pytest.raises(VariantError):
        b.build()


def test_builder_unbalanced_end_raises():
    b = vu.VariantBuilder()
    b.start_object()
    with pytest.raises(VariantError):
        b.end_array()


# --------------------------------------------------------------------------------------
# variants.as('timestamp') extraction (bug #27): NANOS-precision variants must floor
# to microseconds using floor division (matching Java Math.floorDiv/floorMod), so that
# pre-epoch (negative) values round toward negative infinity rather than toward zero.
# celpy's TimestampType is datetime-backed (microsecond resolution), so the residual
# sub-microsecond nanoseconds Java keeps in its protobuf Timestamp cannot be represented.
# --------------------------------------------------------------------------------------

import datetime as _dt  # noqa: E402

from confluent_kafka.schema_registry.rules.cel.variant_funcs import (  # noqa: E402
    _variant_as,
    _variant_get_timestamp,
)

_EPOCH_UTC = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


def _java_nanos_to_micros(ns):
    """Java TimestampUtils.fromEpochNanos split, floored to the microsecond that a
    datetime can hold: sec = floorDiv(ns, 1e9), nanos = floorMod(ns, 1e9), then the
    nanos field floored to micros. Equals floor(ns / 1000)."""
    sec = ns // 1_000_000_000          # Math.floorDiv
    nanos = ns - sec * 1_000_000_000   # Math.floorMod, 0 <= nanos < 1e9
    return sec * 1_000_000 + nanos // 1000


def _nanos_variant(ns, ntz=False):
    code = vu.TIMESTAMP_NANOS_NTZ if ntz else vu.TIMESTAMP_NANOS
    return prim(code, struct.pack("<q", ns))


@pytest.mark.parametrize(
    "ns",
    [
        0,
        1,                              # 1 ns after epoch -> floors to epoch
        999,                            # sub-micro positive -> floors to 0 us
        1000,
        1577836800123456789,            # 2020-01-01T00:00:00.123456789Z
        -1,                             # 1 ns before epoch: floor -> -1 us (NOT 0)
        -999,                           # sub-micro pre-epoch -> -1 us (NOT 0)
        -1000,
        -1500,                          # -1.5 us -> floor -> -2 us (NOT -1)
        -1577836800123456789,           # deep pre-1970 nanos timestamp
    ],
)
def test_variant_as_timestamp_nanos_floors_to_micros_like_java(ns):
    expected_micros = _java_nanos_to_micros(ns)
    expected = _EPOCH_UTC + _dt.timedelta(microseconds=expected_micros)
    # Both the TZ and NTZ nanos types extract identically (celpy carries no zone flag).
    for ntz in (False, True):
        result = _variant_get_timestamp(_nanos_variant(ns, ntz=ntz))
        assert result == expected
        # And the full variants.as(...) dispatch path agrees.
        assert _variant_as(_nanos_variant(ns, ntz=ntz), "timestamp", False) == expected


def test_variant_as_timestamp_nanos_uses_floor_not_truncation_for_negatives():
    # The whole point of bug #27: negative epoch nanos must floor, not truncate toward 0.
    # -1 ns: floor gives -1 us; truncation toward zero would (wrongly) give 0 us.
    trunc_wrong = _EPOCH_UTC + _dt.timedelta(microseconds=int(-1 / 1000))  # == epoch (0 us)
    floored = _variant_get_timestamp(_nanos_variant(-1))
    assert floored == _EPOCH_UTC - _dt.timedelta(microseconds=1)
    assert floored != trunc_wrong


def test_variant_as_timestamp_nanos_residual_precision_is_micros_only():
    # Documented type limit: datetime cannot hold sub-microsecond nanoseconds, so the
    # trailing 789 ns of a NANOS value are dropped (Java keeps them in its Timestamp).
    result = _variant_get_timestamp(_nanos_variant(1577836800123456789))
    assert result.microsecond == 123456
    assert result == _dt.datetime(2020, 1, 1, 0, 0, 0, 123456, tzinfo=_dt.timezone.utc)


def test_variant_as_timestamp_micros_types_are_used_as_is():
    # MICROS-precision variants store microseconds directly (no nanos division).
    micros = 1577836800123456
    expected = _EPOCH_UTC + _dt.timedelta(microseconds=micros)
    assert _variant_get_timestamp(prim(vu.TIMESTAMP, struct.pack("<q", micros))) == expected
    assert _variant_get_timestamp(prim(vu.TIMESTAMP_NTZ, struct.pack("<q", micros))) == expected
    # Negative micros are used verbatim (already the finest datetime resolution).
    assert (_variant_get_timestamp(prim(vu.TIMESTAMP, struct.pack("<q", -1)))
            == _EPOCH_UTC - _dt.timedelta(microseconds=1))
