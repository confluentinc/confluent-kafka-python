#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
import binascii
from decimal import Decimal
from io import BytesIO

import pytest
from google.protobuf import descriptor_pb2

from confluent_kafka.schema_registry.confluent.types import decimal_pb2
from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer,
    ProtobufSerializer,
    _create_index_array,
    decimal_to_protobuf,
    protobuf_to_decimal,
)
from confluent_kafka.schema_registry.serde import SchemaId
from confluent_kafka.serialization import SerializationError
from tests.integration.schema_registry.data.proto import DependencyTestProto_pb2, metadata_proto_pb2


@pytest.mark.parametrize(
    "pb2, coordinates",
    [
        (DependencyTestProto_pb2.DependencyMessage, [0]),
        (metadata_proto_pb2.ControlMessage.Watermark, [15, 1]),  # [ControlMessage, Watermark]
        (
            metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig,
            [4, 0, 1, 2],
        ),  # [HdfsOptions, ImportOptions, Generator, KacohaConfig ]
    ],
)
def test_create_index(pb2, coordinates):
    msg_idx = _create_index_array(pb2.DESCRIPTOR)

    assert msg_idx == coordinates


def _two_message_file_proto():
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = "test.proto"
    fdp.package = "pkg"
    first = fdp.message_type.add()
    first.name = "First"
    nested = first.nested_type.add()
    nested.name = "Inner"
    second = fdp.message_type.add()
    second.name = "Second"
    return fdp


def test_message_index_in_range():
    deserializer = object.__new__(ProtobufDeserializer)
    fdp = _two_message_file_proto()

    assert deserializer._get_message_desc_proto("", fdp, [0])[0] == "First"
    assert deserializer._get_message_desc_proto("", fdp, [1])[0] == "Second"
    assert deserializer._get_message_desc_proto("", fdp, [0, 0])[0] == "First.Inner"


@pytest.mark.parametrize("msg_index", [[-1], [2], [0, -1], [0, 5]])
def test_message_index_out_of_range(msg_index):
    # The message index array is attacker-controlled wire framing; a zigzag
    # varint can decode to a negative or out-of-range value. A negative index
    # would otherwise wrap around and resolve to a different message type.
    deserializer = object.__new__(ProtobufDeserializer)
    fdp = _two_message_file_proto()

    with pytest.raises(SerializationError, match="out of range"):
        deserializer._get_message_desc_proto("", fdp, msg_index)


@pytest.mark.parametrize(
    "pb2",
    [
        DependencyTestProto_pb2.DependencyMessage,
        metadata_proto_pb2.ControlMessage.Watermark,
        metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig,
    ],
)
@pytest.mark.parametrize("zigzag", [True, False])
def test_index_serialization(pb2, zigzag):
    msg_idx = _create_index_array(pb2.DESCRIPTOR)
    buf = BytesIO()
    ProtobufSerializer._encode_varints(buf, msg_idx, zigzag=zigzag)
    buf.flush()

    # reset buffer cursor
    buf.seek(0)
    decoded_msg_idx = SchemaId._read_index_array(buf, zigzag=zigzag)
    buf.close()

    assert decoded_msg_idx == msg_idx


@pytest.mark.parametrize(
    "msg_idx, zigzag, expected_hex",
    [
        # b2a_hex returns hex pairs
        ([0], True, b'00'),  # special case [0]
        ([0], False, b'00'),  # special case [0]
        ([1], True, b'0202'),
        ([1], False, b'0101'),
        ([127, 8, 9], True, b'06fe011012'),
        ([127, 8, 9], False, b'037f0809'),
        ([128], True, b'028002'),
        ([128], False, b'018001'),
        ([9223372036854775807], True, b'02feffffffffffffffff01'),
        ([9223372036854775807], False, b'01ffffffffffffffff7f'),
    ],
)
def test_index_encoder(msg_idx, zigzag, expected_hex):
    buf = BytesIO()
    ProtobufSerializer._encode_varints(buf, msg_idx, zigzag=zigzag)
    buf.flush()
    buf.seek(0)
    assert binascii.b2a_hex(buf.read()) == expected_hex

    # reset reader and test decoder
    buf.seek(0)
    decoded_msg_idx = SchemaId._read_index_array(buf, zigzag=zigzag)
    assert decoded_msg_idx == msg_idx


@pytest.mark.parametrize(
    "decimal, scale",
    [
        ("0", 0),
        ("1.01", 2),
        ("123456789123456789.56", 2),
        ("1234", 0),
        ("1234.5", 1),
        ("-0", 0),
        ("-1.01", 2),
        ("-123456789123456789.56", 2),
        ("-1234", 0),
        ("-1234.5", 1),
        ("-1234.56", 2),
    ],
)
def test_proto_decimal(decimal, scale):
    input = Decimal(decimal)
    converted = decimal_to_protobuf(input, scale)
    result = protobuf_to_decimal(converted)
    assert result == input


# BigDecimal.setScale(scale) narrows a scale whenever no rounding is needed -- only the digits
# being dropped must be zeros. decimal_to_protobuf used to refuse every reduction (`delta < 0`),
# which rejected exact conversions: Decimal("1.50") at scale 1, and the negative scale that
# protobuf_to_decimal itself produces for a value like 1E+3. Values requiring real rounding are
# still refused, as setScale does without a rounding mode.
@pytest.mark.parametrize(
    "decimal, scale, unscaled, out_scale",
    [
        ("12.3400", 2, 1234, 2),  # trailing zeros dropped, exact
        ("1.50", 1, 15, 1),
        ("-1.50", 1, -15, 1),
        ("1000", -3, 1, -3),  # negative scale, exact
        ("-1000", -3, -1, -3),
        ("0.00", 0, 0, 0),
        ("12.34", 4, 123400, 4),  # widening still works
        ("12.34", 2, 1234, 2),  # exact match still works
    ],
)
def test_proto_decimal_narrows_scale_losslessly(decimal, scale, unscaled, out_scale):
    msg = decimal_to_protobuf(Decimal(decimal), scale)
    assert int.from_bytes(msg.value, byteorder="big", signed=True) == unscaled
    assert msg.scale == out_scale


@pytest.mark.parametrize("decimal, scale", [("12.345", 2), ("1.01", 1), ("999", -1)])
def test_proto_decimal_rejects_lossy_scale(decimal, scale):
    with pytest.raises(ValueError, match="Scale provided does not match the decimal"):
        decimal_to_protobuf(Decimal(decimal), scale)


# Both rescaling directions used to build a power of ten before deciding anything, which the
# requested scale sizes: `10**-delta` for the exactness check when narrowing, `10**delta` for
# the coefficient when widening. Measured against this function before the guards:
#
#   scale -1e7  ->  5.2s to reach a ValueError
#   scale -1e8  ->  178s to reach the same ValueError
#   scale  1e7  ->  5.3s and a 4.1 MB field written
#   scale  1e8  ->  did not finish inside 240s
#
# BigDecimal.setScale(scale) is the reference for the whole function, and it answers these
# without the arithmetic. Measured against the JDK:
#
#   setScale(1, -1e7)             THROW ArithmeticException: Rounding necessary
#   setScale(0, -1e9)             OK, scale=-1000000000, instant - a zero has no digits to lose
#   setScale(0.00, -1e9)          OK, same
#   setScale(1, 1e7)              OK, precision 10000001 (1.4s - the JVM pays here too)
#   setScale(1, 1e9)              THROW ArithmeticException: BigInteger would overflow ...
#   setScale(1E+1000000000, 0)    THROW, same
@pytest.mark.parametrize(
    "decimal, scale",
    [
        # Narrowing a non-zero value past its trailing zeros: the JVM's "Rounding necessary".
        ("1", -10000000),
        ("1", -1000000000),
        ("1.23", -1000000000),
        # Widening past what a BigInteger coefficient can hold.
        ("1", 1000000000),
        ("1E+1000000000", 0),
        ("12.34", 2000000000),
    ],
)
def test_proto_decimal_rejects_the_scales_java_rejects(decimal, scale):
    with pytest.raises(ValueError):
        decimal_to_protobuf(Decimal(decimal), scale)


@pytest.mark.parametrize(
    "decimal, scale, unscaled",
    [
        # A zero narrows to any scale, which is what setScale does with a zero coefficient.
        ("0", -1000000000, 0),
        ("0.00", -1000000000, 0),
        ("0E+10", -1000000000, 0),
        # And the ordinary cases keep working.
        ("1000", -3, 1),
        ("1.50", 1, 15),
    ],
)
def test_proto_decimal_accepts_the_scales_java_accepts(decimal, scale, unscaled):
    msg = decimal_to_protobuf(Decimal(decimal), scale)
    assert int.from_bytes(msg.value, byteorder="big", signed=True) == unscaled
    assert msg.scale == scale


# A timing bound, because the cost *is* the defect: the answer was already right, it just took
# 5.2s to give at this scale and 178s one power of ten further out. The fixed path measures
# 0.000s, so a one-second budget separates them by three orders of magnitude and cannot flake.
def test_proto_decimal_rejects_a_wide_scale_without_the_arithmetic():
    import time

    start = time.monotonic()
    with pytest.raises(ValueError):
        decimal_to_protobuf(Decimal("1"), -10000000)
    with pytest.raises(ValueError):
        decimal_to_protobuf(Decimal("1"), 10000000000)
    assert time.monotonic() - start < 1.0


# `protobuf_to_decimal` deliberately does **not** apply the message's precision, and these are
# the cases that tell the two policies apart.
#
# Java reads it as `new BigDecimal(unscaled, scale, new MathContext(precision))`, so a precision
# narrower than the coefficient's digit count *rounds the value on read*. Every client - this one
# included - writes precision as the unscaled value's own digit count, which makes that
# MathContext a guaranteed no-op, so it only ever has an effect on a message from a foreign
# producer carrying a declared/column precision. There its effect is to silently round data the
# producer sent exactly, which is why six of the seven clients ignore it and this path now does
# too. See ~/Documents/decimals.md section 2.
#
# The values below are what the reference would have returned for the same inputs, kept in the
# comments so the divergence is legible: unscaled 125 at precision 2 is 1.3E+2 on the JVM.
@pytest.mark.parametrize(
    "unscaled, scale, precision, expected",
    [
        ("12325", 0, 4, "12325"),  # JVM, rounding to 4 digits: 1.233E+4
        ("125", 0, 2, "125"),  # JVM: 1.3E+2
        ("-125", 0, 2, "-125"),  # JVM: -1.3E+2
        ("12315", 0, 4, "12315"),  # JVM: 1.232E+4
        ("135", 0, 2, "135"),  # JVM: 1.4E+2
        ("12345", 2, 3, "123.45"),  # JVM: 123
        # Where precision is at least the digit count - i.e. everything this client family
        # writes - the two policies agree, and always did.
        ("1234", 2, 4, "12.34"),
        ("0", 2, 1, "0.00"),
        ("1", -3, 1, "1E+3"),  # negative scale survives
        # And precision 0, which the reference cannot produce but three of our write paths did
        # until now: MathContext(0) is UNLIMITED, so this agreed too.
        ("12345", 2, 0, "123.45"),
    ],
)
def test_protobuf_to_decimal_ignores_precision(unscaled, scale, precision, expected):
    msg = decimal_pb2.Decimal(
        value=int(unscaled).to_bytes(16, byteorder="big", signed=True),
        scale=scale,
        precision=precision,
    )
    assert str(protobuf_to_decimal(msg)) == expected


# The other half of section 2: the same message read through the CEL binding and through the
# serde must give the same value. It did not - this path applied precision and that one never
# has - so unscaled 125 at precision 2 was 1.3E+2 here and 125 there.
def test_both_read_paths_agree_on_precision():
    from confluent_kafka.schema_registry.confluent.types.decimal_utils import from_proto_decimal

    msg = decimal_pb2.Decimal(value=(125).to_bytes(2, byteorder="big", signed=True), scale=0, precision=2)
    assert protobuf_to_decimal(msg) == from_proto_decimal(msg)


# decimal_to_protobuf left `precision` at 0, which the reference cannot produce -
# `BigDecimal.precision()` is never less than 1, zero's precision being 1 - so a JVM consumer
# rewrites such a message on its next touch. It now carries the unscaled value's digit count,
# derived from the integer actually written so a rescale cannot leave it stale.
@pytest.mark.parametrize(
    "decimal, scale, precision",
    [
        ("12.34", 2, 4),
        ("1000", -3, 1),  # unscaled 1
        ("0", 0, 1),  # BigDecimal.ZERO.precision() == 1
        ("0.00", 2, 1),
        ("-1.50", 1, 2),  # the sign is not a digit
        ("12.3400", 2, 4),  # after the exact narrowing, not before
    ],
)
def test_decimal_to_protobuf_writes_the_digit_count(decimal, scale, precision):
    msg = decimal_to_protobuf(Decimal(decimal), scale)
    assert msg.precision == precision
    assert msg.scale == scale


# The digit count is counted arithmetically, not through `len(str(abs(unscaled)))`. CPython caps
# str <-> int conversion at 4300 digits, so the string form raised `ValueError: Exceeds the limit
# (4300 digits) for integer string conversion` for a coefficient this function otherwise accepts
# (`_MAX_COEFFICIENT_DIGITS` is 646456993 here) - and raised it *after* `result.value` had been
# assigned. Every expected value below is measured on the JDK, which is what the count mirrors:
#   BigDecimal("1").setScale(4300)    -> precision 4301
#   BigDecimal("1").setScale(5000)    -> precision 5001
#   BigDecimal("1").setScale(1000000) -> precision 1000001
#   BigDecimal("0").setScale(5000)    -> precision 1
@pytest.mark.parametrize(
    "decimal, scale, precision",
    [
        ("1", 4299, 4300),  # the widest the string form managed
        ("1", 4300, 4301),  # the first it refused
        ("1", 5000, 5001),
        ("1", 100000, 100001),
        ("12.34", 5000, 5002),  # digits and delta both contribute
        # Zero has the digit tuple (0,) at every scale, so it needs the special case; the
        # reference agrees that it is 1 and not 1 + delta.
        ("0", 5000, 1),
        ("0", 100000, 1),
    ],
)
def test_decimal_to_protobuf_counts_wide_precision_without_str(decimal, scale, precision):
    msg = decimal_to_protobuf(Decimal(decimal), scale)
    assert msg.precision == precision
    assert msg.scale == scale
    # And the value itself still round-trips, so the count describes what was written.
    assert protobuf_to_decimal(msg) == Decimal(decimal)


# The arithmetic count has to agree with the string form everywhere the string form is legal,
# which is what makes it a refactor rather than a second implementation. It is exact in both
# rescale directions only because this function refuses an inexact narrowing: widening appends
# `delta` zeros with no carry, and narrowing only drops digits already proven to be zeros. A
# rounding rescale could carry - 9.9 to scale 0 is 10, one digit becoming two - and would need
# the count taken after the fact.
def test_precision_count_matches_the_string_form():
    checked = 0
    for text in [
        "0",
        "0.00",
        "-0.00",
        "1",
        "12.34",
        "1.50",
        "1000",
        "0.001",
        "-999.5",
        "9.9",
        "99.99",
        "1E+3",
        "1E-3",
        "100",
        "123456789012345678901234567890",
        "0.0000000001",
        "-1.50",
        "1.000000",
        "9" * 100,
        "1" + "0" * 200,
    ]:
        for scale in range(-8, 12):
            try:
                msg = decimal_to_protobuf(Decimal(text), scale)
            except ValueError:
                continue  # an inexact narrowing, which this function refuses
            unscaled = int.from_bytes(msg.value, byteorder="big", signed=True)
            assert msg.precision == len(str(abs(unscaled))), (text, scale)
            checked += 1
    assert checked > 200
