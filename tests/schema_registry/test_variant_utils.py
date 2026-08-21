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
    assert v.num_object_elements() == 5
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
    assert prim(vu.FLOAT, struct.pack("<f", 1.5)).get_double() == 1.5


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


# --------------------------------------------------------------------------------------
# malformed input
# --------------------------------------------------------------------------------------


def test_unsupported_metadata_version_raises():
    with pytest.raises(VariantError, match="version"):
        Variant(b"\x00", b"\x02\x00\x00")  # version 2 in metadata header


def test_parse_json_malformed_raises():
    with pytest.raises(ValueError):
        vu.parse_json("{not json")


def test_wrong_getter_raises():
    with pytest.raises(VariantError):
        prim(vu.TRUE).get_string()
    with pytest.raises(VariantError):
        prim(vu.NULL).get_long()
