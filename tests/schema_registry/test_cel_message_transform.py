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

import math
from decimal import Decimal

import pytest

from confluent_kafka.schema_registry.confluent.type.variant_utils import Variant, parse_json
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, Schema
from confluent_kafka.schema_registry.serde import RuleContext

from .data.proto import value_types_pb2

_SCHEMA = """syntax = "proto3";
package tests;
import "confluent/type/decimal.proto";
import "confluent/type/variant.proto";
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
    rule = Rule("r", None, RuleKind.TRANSFORM, RuleMode.WRITE, "CEL", None, None, expr, None, None, False)
    ctx = RuleContext(
        None, None, None, Schema(_SCHEMA, "PROTOBUF"), "t-value", RuleMode.WRITE, rule, 0, [rule], None, None
    )
    return CelExecutor().transform(ctx, _message() if msg is None else msg)


def _decimal_of(msg):
    unscaled = int.from_bytes(msg.amount.value, "big", signed=True)
    return Decimal(unscaled).scaleb(-msg.amount.scale)


_ALL = (
    '"amount": message.amount, "ts": message.ts, '
    '"data": message.data, "label": message.label, "count": message.count'
)


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
        '"ts": message.ts, "data": message.data, "label": message.label}'
    )

    assert _decimal_of(result) == Decimal("13.34")
    assert result.amount.scale == 2
    # Not merely the original echoed back.
    assert result.amount.value != _UNSCALED


def test_computed_timestamp_is_written_back():
    """C7, timestamp. The rule returns a datetime, which has to become a
    google.protobuf.Timestamp - and keep its sub-second part."""
    result = _transform(
        '{"amount": message.amount, "ts": message.ts + duration("60s"), '
        '"data": message.data, "label": message.label}'
    )

    assert result.ts.seconds == 1700000060
    assert result.ts.nanos == 123000000


def test_computed_variant_is_written_back():
    """C7, variant. The rule returns a Variant, which has to become a
    confluent.type.Variant message. Asserted through the decoded JSON rather than the
    metadata bytes: metadata holds the field *names*, so {"name":"alice"} and
    {"name":"bob"} share it and comparing metadata would prove nothing."""
    result = _transform(
        '{"amount": message.amount, "ts": message.ts, '
        '"data": variants.parseJson("{\\"name\\":\\"bob\\"}"), "label": message.label}'
    )

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
    result = _transform('{"amount": null, "ts": message.ts, "data": message.data, "label": message.label}')

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

    guarded = _transform('{"amount": has(message.amount) ? message.amount : null, "label": message.label}', absent)
    assert not guarded.HasField("amount")


def test_condition_rules_are_unaffected():
    """A CONDITION returns a bool, which must not be run through the message rebuild."""
    rule = Rule(
        "r",
        None,
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        'decimals.gt(message.amount, decimal("10.00"))',
        None,
        None,
        False,
    )
    ctx = RuleContext(
        None, None, None, Schema(_SCHEMA, "PROTOBUF"), "t-value", RuleMode.WRITE, rule, 0, [rule], None, None
    )

    assert CelExecutor().transform(ctx, _message()) is True


def _wrapper_descriptor():
    """A message with one field per protobuf wrapper type, plus a Duration.

    Built at runtime rather than added to value_types.proto so this needs no regenerated
    ``_pb2`` fixture (the checked-in ones are deliberately free of protoc's runtime-version
    gate, which a regeneration would reintroduce).
    """
    from google.protobuf import descriptor_pb2, descriptor_pool, duration_pb2, message_factory, wrappers_pb2

    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name, fdp.package, fdp.syntax = "wrappers_probe.proto", "tests.wrap", "proto3"
    fdp.dependency.extend(["google/protobuf/wrappers.proto", "google/protobuf/duration.proto"])
    msg = fdp.message_type.add()
    msg.name = "Wrapped"
    types = [
        "StringValue",
        "BytesValue",
        "Int32Value",
        "Int64Value",
        "UInt32Value",
        "UInt64Value",
        "FloatValue",
        "DoubleValue",
        "BoolValue",
        "Duration",
    ]
    for number, type_name in enumerate(types, start=1):
        field = msg.field.add()
        field.name = type_name.lower()
        field.number = number
        field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type_name = ".google.protobuf." + type_name
        field.json_name = type_name.lower()

    pool = descriptor_pool.DescriptorPool()
    for dep in (wrappers_pb2.DESCRIPTOR, duration_pb2.DESCRIPTOR):
        proto = descriptor_pb2.FileDescriptorProto()
        dep.CopyToProto(proto)
        pool.Add(proto)
    pool.Add(fdp)
    desc = pool.FindMessageTypeByName("tests.wrap.Wrapped")
    return desc, message_factory.GetMessageClass(desc)


def _wrapped_message():
    _, cls = _wrapper_descriptor()
    msg = cls()
    msg.stringvalue.value = "hello"
    msg.bytesvalue.value = b"\x01\x02"
    msg.int32value.value = 7
    msg.int64value.value = 2**40
    msg.uint32value.value = 9
    msg.uint64value.value = 2**40 + 1
    msg.floatvalue.value = 1.5
    msg.doublevalue.value = 2.25
    msg.boolvalue.value = True
    msg.duration.seconds, msg.duration.nanos = 3, 500000000
    return msg


def test_wrappers_and_duration_survive_an_identity_transform():
    """The CEL binding unwraps a wrapper to the scalar it holds and a Duration to a CEL
    duration, so the write-back has to put them back. It only handled Decimal/Timestamp/
    Variant/mapping, so every one of these fields came back **empty** - silent data loss on
    an identity transform. The JVM gets this right for free: its message-level write-back
    goes through protobuf JSON, whose parser reads "hello" into a StringValue and "3s" into a
    Duration.
    """
    from confluent_kafka.schema_registry.rules.cel.constraints import _msg_to_cel
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    original = _wrapped_message()
    assert convert(_msg_to_cel(original), original) == original


# A CEL timestamp is a datetime and a CEL duration a timedelta, both microsecond-resolution, so
# a nanos field cannot survive the conversion. It does not have to: an echoed value is copied
# from the message it was read from. Without that, a rule rewriting some *other* field turned
# nanos 1 into 0 - while the decimal in the same message came back byte-identical, because its
# binding already kept the source. These values are chosen to discriminate: the tests above use
# 123000000 and 500000000, whole microsecond counts that cannot detect the truncation.
@pytest.mark.parametrize("nanos", [123456789, 1, 999999999])
def test_an_echoed_timestamp_keeps_its_nanos(nanos):
    msg = _message()
    msg.ts.nanos = nanos

    # Identity, and a rule that rewrites a sibling field and merely passes ts along.
    identity = _transform("{" + _ALL + "}", msg)
    sibling = _transform("{" + _ALL.replace('"label": message.label', '"label": message.label + "!"') + "}", msg)

    assert identity.ts.nanos == nanos
    assert sibling.ts.nanos == nanos
    assert sibling.label == "hi!"


@pytest.mark.parametrize("nanos", [123456789, 1, 999999999])
def test_an_echoed_duration_keeps_its_nanos(nanos):
    from confluent_kafka.schema_registry.rules.cel.constraints import _msg_to_cel
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    original = _wrapped_message()
    original.duration.seconds, original.duration.nanos = 3, nanos

    assert convert(_msg_to_cel(original), original).duration.nanos == nanos


# ...and a *computed* timestamp still lands on the microsecond ceiling, which is inherent to
# datetime and already documented for `timestamp(x, 9)` and `string(ts)`. Pinned so the fix
# above is not mistaken for nanosecond arithmetic.
def test_a_computed_timestamp_keeps_the_microsecond_ceiling():
    msg = _message()
    msg.ts.nanos = 123456789

    result = _transform("{" + _ALL.replace('"ts": message.ts', '"ts": message.ts + duration("0s")') + "}", msg)

    assert result.ts.nanos == 123456000


def test_negative_duration_keeps_matching_signs():
    """A Duration's seconds and nanos must share a sign; timedelta normalises microseconds to
    be non-negative (-3.5s is days=-1, seconds=86396, microseconds=500000), so splitting it
    by floor division produced seconds=-3 with nanos=+499000000."""
    from confluent_kafka.schema_registry.rules.cel.constraints import _msg_to_cel
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _wrapper_descriptor()
    original = cls()
    original.duration.seconds, original.duration.nanos = -3, -500000000
    result = convert(_msg_to_cel(original), original)
    assert (result.duration.seconds, result.duration.nanos) == (-3, -500000000)


# A wrapper's `value` field had its own copy of the scalar conversions, and the copy was the
# unguarded one: an Int32Value took 1.9 as 1 and a BoolValue took the string "false" as *true*,
# while the identical plain fields refused both. The JVM draws no such distinction -
# JsonFormat's parseWrapperFieldValue hands the value to the same parseFieldValue a plain field
# goes through, so the accept/reject sets are identical. Measured against protobuf-java 4.35.1
# with each wrapper as a nested field:
#   Int32Value  <- 2 / 2.0 -> 2;  <- 1.9, 2147483648, true -> REJECT
#   BoolValue   <- true -> true;  <- 0 -> REJECT "Invalid bool value: 0"
#   BytesValue  <- 5 -> REJECT;   FloatValue <- 1.0e40 -> REJECT "Out of range float value"
#   DoubleValue <- 3 -> 3.0;      <- true -> REJECT "Not a double value: true"
def test_a_wrapper_field_is_narrowed_like_a_plain_scalar():
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _wrapper_descriptor()
    original = cls()

    # Exact conversions still reach the wrapper.
    assert convert({"int32value": celtypes.DoubleType(2.0)}, original).int32value.value == 2
    assert convert({"doublevalue": celtypes.IntType(3)}, original).doublevalue.value == 3.0
    assert convert({"boolvalue": celtypes.BoolType(True)}, original).boolvalue.value is True
    assert convert({"stringvalue": celtypes.StringType("ok")}, original).stringvalue.value == "ok"
    assert convert({"bytesvalue": celtypes.BytesType(b"ab")}, original).bytesvalue.value == b"ab"

    # The same rejections a plain field of that type makes.
    for field, value in [
        ("int32value", celtypes.DoubleType(1.9)),
        ("int32value", celtypes.IntType(2**31)),
        ("int32value", celtypes.BoolType(True)),
        ("boolvalue", celtypes.IntType(0)),
        ("boolvalue", celtypes.StringType("false")),
        ("stringvalue", celtypes.IntType(1)),
        ("bytesvalue", celtypes.IntType(5)),
        ("doublevalue", celtypes.BoolType(True)),
        ("floatvalue", celtypes.DoubleType(1e40)),
        ("uint32value", celtypes.IntType(-1)),
    ]:
        with pytest.raises(ValueError):
            convert({field: value}, original)


def test_message_level_decimal_sets_precision_like_the_field_level_writer():
    """Java's ProtobufResultWriter sets precision from the value (``dec.precision()``); this
    writer left it at zero, so the same computed decimal produced different
    confluent.type.Decimal bytes depending on the rule's scope. Safe to set here because this
    writer never rescales, so len(digits) is the digit count of the unscaled value written.
    """
    from confluent_kafka.schema_registry.common.protobuf import set_decimal_message
    from confluent_kafka.schema_registry.confluent.type import decimal_pb2
    from confluent_kafka.schema_registry.confluent.type.decimal_utils import to_proto_decimal
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import _set_decimal

    # (value, java BigDecimal.precision(), java scale())
    for text, precision, scale in [
        ("12.34", 4, 2),
        ("12.3400", 6, 4),
        ("1E+3", 1, -3),
        ("0.00", 1, 2),
        ("100", 3, 0),
    ]:
        message_level = decimal_pb2.Decimal()
        _set_decimal(message_level, Decimal(text))
        field_level = decimal_pb2.Decimal()
        set_decimal_message(field_level, Decimal(text))
        # to_proto_decimal is the third writer; all three must agree byte for byte, so a
        # consumer that honours precision cannot see one value two ways.
        standalone = to_proto_decimal(Decimal(text))

        assert (message_level.precision, message_level.scale) == (precision, scale), text
        assert message_level.SerializeToString() == field_level.SerializeToString(), text
        assert standalone.SerializeToString() == field_level.SerializeToString(), text


# `mul` is no longer guarded on its result's shape - each library's own exponent range is
# delegated and documented, and multiplication is measurably cheap at any width. So a value
# whose scale no int32 can carry now reaches the writer instead of being refused by the
# operator: two 1e2147483647 operands multiply exactly, and need a scale of -4294967294.
# Assigning that raises a bare `ValueError: Value out of range` from the protobuf runtime;
# the writer names the value and the field instead.
def test_a_scale_that_does_not_fit_int32_is_refused_at_the_wire():
    from confluent_kafka.schema_registry.confluent.type import decimal_pb2
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import _set_decimal

    for text in ["1e4294967294", "1e-4294967294"]:
        with pytest.raises(ValueError, match="does not fit the int32 scale field"):
            _set_decimal(decimal_pb2.Decimal(), Decimal(text))

    # The boundary itself still writes, from both sides.
    for text, scale in [("1e-2147483647", 2147483647), ("1e2147483647", -2147483647)]:
        target = decimal_pb2.Decimal()
        _set_decimal(target, Decimal(text))
        assert target.scale == scale, text


# The coefficient, not the scale, is what actually bounds what this client can write. The wire
# form is the unscaled integer in base 256, and decimal <-> binary radix conversion is
# quadratic: CPython caps str <-> int at 4300 digits for exactly that reason (measured in the
# C++ sibling, whose own codec takes 0.04 s at 10**4 digits, 4.2 s at 10**5 and ~420 s at
# 10**6, with mpdecimal's mpd_qexport_u32 only about 10x better and the same quadratic shape).
#
# So the cap is pre-existing - `int("9" * 5000)` has always raised - and reached callers as
# CPython's "Exceeds the limit (4300 digits) for integer string conversion", naming neither
# the decimal nor the field. It now names both.
def test_a_coefficient_past_what_can_be_encoded_is_refused():
    from confluent_kafka.schema_registry.confluent.type import decimal_pb2
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import _set_decimal

    for digits in [4301, 5000, 100000]:
        with pytest.raises(ValueError, match="past the 4300 this client can encode"):
            _set_decimal(decimal_pb2.Decimal(), Decimal("9" * digits))

    # The boundary itself still writes, and so does anything narrower.
    for digits in [1, 38, 4300]:
        target = decimal_pb2.Decimal()
        _set_decimal(target, Decimal("9" * digits))
        assert target.precision == digits


# The field-level twin of the above. `set_decimal_message` is the write-back for a
# confluent.type.Decimal *field*, and it had the unhelpful version of the same failure: the
# `int(...)` inside it is a str -> int conversion, so CPython raised "Exceeds the limit (4300
# digits) for integer string conversion" naming neither the decimal nor the field. Both paths
# now report it the same way, from one shared constant - there were two constants called
# `_MAX_COEFFICIENT_DIGITS` in this client with different values.
def test_the_field_level_writer_reports_a_wide_coefficient_too():
    from confluent_kafka.schema_registry.common.protobuf import set_decimal_message
    from confluent_kafka.schema_registry.confluent.type import decimal_pb2

    for digits in [4301, 5000, 100000]:
        with pytest.raises(ValueError, match="past the 4300 this client can encode"):
            set_decimal_message(decimal_pb2.Decimal(), Decimal("9" * digits))

    # And the two writers agree exactly, boundary included, so which path produced a decimal
    # cannot change whether it is accepted.
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import _set_decimal

    for digits in [1, 38, 4300]:
        field_level = decimal_pb2.Decimal()
        set_decimal_message(field_level, Decimal("9" * digits))
        message_level = decimal_pb2.Decimal()
        _set_decimal(message_level, Decimal("9" * digits))
        assert field_level.SerializeToString() == message_level.SerializeToString()


def _scalar_descriptor():
    """A message with one field of each scalar kind, built at runtime."""
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name, fdp.package, fdp.syntax = "int_coercion.proto", "tests.ic", "proto3"
    choices = fdp.enum_type.add()
    choices.name = "Choice"
    choices.value.add(name="ZERO", number=0)
    choices.value.add(name="ONE", number=1)
    msg = fdp.message_type.add()
    msg.name = "Ints"
    spec = [
        ("i32", 1, descriptor_pb2.FieldDescriptorProto.TYPE_INT32, 1),
        ("u32", 2, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32, 1),
        ("dbl", 3, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, 1),
        ("codes", 4, descriptor_pb2.FieldDescriptorProto.TYPE_STRING, 3),
        ("text", 5, descriptor_pb2.FieldDescriptorProto.TYPE_STRING, 1),
        ("flag", 6, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL, 1),
        ("blob", 7, descriptor_pb2.FieldDescriptorProto.TYPE_BYTES, 1),
        ("choice", 8, descriptor_pb2.FieldDescriptorProto.TYPE_ENUM, 1),
        ("flt", 9, descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT, 1),
        ("u64", 10, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64, 1),
    ]
    for name, number, ftype, label in spec:
        field = msg.field.add()
        field.name, field.number, field.type, field.label = name, number, ftype, label
        field.json_name = name
        if ftype == descriptor_pb2.FieldDescriptorProto.TYPE_ENUM:
            field.type_name = ".tests.ic.Choice"

    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    desc = pool.FindMessageTypeByName("tests.ic.Ints")
    return desc, message_factory.GetMessageClass(desc)


# int() silently truncated, so a CEL double of 1.9 landed in an int32 field as 1 and a
# fractional Decimal lost its fraction. The JVM's message-level write-back goes through a
# protobuf JSON parse, which refuses a non-integral value and range-checks the result.
# Measured against protobuf-java 4.35.1:
#   Int32Value <- 1.9        -> REJECT "Not an int32 value: 1.9"
#   Int32Value <- 2.0        -> 2
#   Int32Value <- 2147483648 -> REJECT "Not an int32 value"
def test_integer_fields_reject_non_integral_and_out_of_range():
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()

    def write(field, value):
        return convert({field: value}, original)

    # Integral values are accepted, whatever their Python type.
    assert write("i32", 2.0).i32 == 2
    assert write("i32", 2).i32 == 2
    assert write("i32", Decimal("3")).i32 == 3
    # A double field still takes a fractional value.
    assert write("dbl", 1.9).dbl == 1.9

    for value in (1.9, Decimal("1.5")):
        with pytest.raises(ValueError, match="non-integral"):
            write("i32", value)
    with pytest.raises(ValueError, match="out of range"):
        write("i32", 2**31)
    with pytest.raises(ValueError, match="out of range"):
        write("u32", -1)
    # Both boolean spellings. bool is an int subclass in Python, and celtypes.BoolType
    # subclasses int rather than bool - so guarding only `bool` caught the case CEL never
    # produces while a real CEL `true` was written as 1. protobuf JSON refuses true for an
    # integer field ("Not an int32 value: true").
    from celpy import celtypes

    for value in (True, celtypes.BoolType(True), celtypes.BoolType(False)):
        with pytest.raises(ValueError, match="bool"):
            write("i32", value)


# The other three scalar arms narrowed unconditionally, so a wrong-typed result was accepted
# and silently changed meaning: bytes(5) fabricated five NUL bytes, bool("false") wrote true,
# float(True) wrote 1.0, and str() turned any value at all into a string field's text.
#
# Every rejection below is one protobuf's own JSON parser makes, which is what the JVM's
# write-back parses the result map with. Measured against protobuf-java 4.35.1:
#   bool  <- 0, "TRUE", ""  -> REJECT "Invalid bool value"
#   bytes <- 5, [97, 98]    -> REJECT
#   float <- true           -> REJECT "Not a double value: true"
#   int   <- 1.9, true      -> REJECT "Not an int32 value"
#
# That parser is also lenient the other way - it stringifies a number into a string field,
# reads "true"/"false" as a bool and a numeric string as a number - and these tests pin the
# decision *not* to follow it. Those coercions only exist because its input crossed a JSON
# transport, which this writer does not; each one turns a rule-authoring mistake into data.
def test_string_fields_take_only_a_string():
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    assert convert({"text": celtypes.StringType("ok")}, original).text == "ok"

    # The JVM would stringify each of these (1 -> "1", true -> "true"). Deliberately not.
    for value in (
        celtypes.IntType(1),
        celtypes.DoubleType(1.5),
        celtypes.BoolType(True),
        celtypes.BytesType(b"ab"),
        celtypes.ListType([celtypes.IntType(1)]),
    ):
        with pytest.raises(ValueError, match="to string field 'text'"):
            convert({"text": value}, original)


def test_bool_fields_do_not_use_python_truthiness():
    """The string "false" is the case that matters: truthiness wrote *true* for it."""
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    assert convert({"flag": celtypes.BoolType(True)}, original).flag is True
    assert convert({"flag": celtypes.BoolType(False)}, original).flag is False

    # A number is refused by the JVM too, so bool(0) accepted what it rejects.
    for value in (celtypes.IntType(0), celtypes.IntType(1), celtypes.DoubleType(1.0)):
        with pytest.raises(ValueError, match="to bool field 'flag'"):
            convert({"flag": value}, original)
    # "true"/"false" are the JVM's own lenient spellings, not followed here; the rest it
    # rejects outright.
    for value in (
        celtypes.StringType("true"),
        celtypes.StringType("false"),
        celtypes.StringType("TRUE"),
        celtypes.StringType("yes"),
        celtypes.StringType(""),
    ):
        with pytest.raises(ValueError, match="to bool field 'flag'"):
            convert({"flag": value}, original)


def test_bytes_fields_take_only_a_byte_string():
    """bytes(5) fabricates five NUL bytes out of a number the JVM refuses."""
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    assert convert({"blob": celtypes.BytesType(b"ab")}, original).blob == b"ab"

    with pytest.raises(ValueError, match="to bytes field 'blob'"):
        convert({"blob": celtypes.IntType(5)}, original)
    for value in (celtypes.BoolType(True), celtypes.ListType([celtypes.IntType(97), celtypes.IntType(98)])):
        with pytest.raises(ValueError, match="to bytes field 'blob'"):
            convert({"blob": value}, original)
    # The JVM base64-decodes a string here, because base64 is how bytes cross its JSON
    # transport. This writer builds against the descriptor, so a CEL string is text that was
    # never encoded and is not reinterpreted as bytes.
    with pytest.raises(ValueError, match="to bytes field 'blob'"):
        convert({"blob": celtypes.StringType("YWI=")}, original)


def test_float_fields_take_only_a_number():
    """A bool gets the same guard an integer field gives it, and so does a numeric string."""
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    # Every numeric kind a rule can compute still reaches a float field.
    assert convert({"dbl": celtypes.DoubleType(1.5)}, original).dbl == 1.5
    assert convert({"dbl": celtypes.IntType(3)}, original).dbl == 3.0
    assert convert({"dbl": celtypes.UintType(3)}, original).dbl == 3.0
    assert convert({"dbl": Decimal("1.5")}, original).dbl == 1.5

    for value in (True, celtypes.BoolType(True), celtypes.BoolType(False)):
        with pytest.raises(ValueError, match="bool"):
            convert({"dbl": value}, original)
    # "1.5" and "NaN" are the JVM's lenient numeric strings, not followed here.
    for value in (celtypes.StringType("1.5"), celtypes.StringType("NaN"), celtypes.StringType("abc")):
        with pytest.raises(ValueError, match="to float field 'dbl'"):
            convert({"dbl": value}, original)


def test_a_float_field_range_checks_the_narrowing():
    """CEL has one floating type, so a `float` field is a narrowing that can overflow.
    float(1e40) gave inf; the JVM says "Out of range float value: 1.0e40". The 1e-6 slack and
    the pass-through for NaN/infinity are both JsonFormat.parseFloat's own behaviour."""
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    # approx because the value comes back as its float32 self, not the double that went in.
    assert convert({"flt": celtypes.DoubleType(3.4028235e38)}, original).flt == pytest.approx(3.4028235e38)
    assert convert({"flt": celtypes.DoubleType(1.5)}, original).flt == 1.5
    assert math.isnan(convert({"flt": celtypes.DoubleType(float("nan"))}, original).flt)
    assert math.isinf(convert({"flt": celtypes.DoubleType(float("inf"))}, original).flt)

    for value in (celtypes.DoubleType(1e40), celtypes.DoubleType(-1e40)):
        with pytest.raises(ValueError, match="out of range float value"):
            convert({"flt": value}, original)
    # A double field takes the same value: only the 32-bit narrowing is range-checked.
    assert convert({"dbl": celtypes.DoubleType(1e40)}, original).dbl == 1e40


# Overflow has to be judged on the *source*, not the result. `float()` saturates a finite but
# too-large value to an infinity, and a range check on the result reads that infinity as one the
# rule asked for and lets it through - so Decimal("1e1000") was written as inf, and a double
# field had no range check at all. A wide Python int is the same case reported differently:
# float(10**400) raises OverflowError, which escaped as a raw Python exception rather than a
# rule error. An *explicitly* non-finite value does pass, because protobuf JSON has canonical
# spellings for those. Measured against protobuf-java 4.35.1:
#
#   double <- 1e308                          1.0E308
#   double <- 1e309, 1e1000, -1e1000         REJECT "Out of range double value"
#   double <- "Infinity", "-Infinity", "NaN" accepted as-is
#   float  <- 1e39, 1e1000                   REJECT "Out of range float value"
#   float  <- "Infinity", "NaN"              accepted as-is
def test_a_finite_value_that_overflows_is_refused():
    from decimal import Decimal as D

    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()

    # The double field had no range check at all, so all four of these were written as inf.
    for value in (D("1e1000"), D("-1e1000"), D("1e309"), D("-1e309")):
        with pytest.raises(ValueError, match="out of range value for float field 'dbl'"):
            convert({"dbl": value}, original)
    # The float field's check was evaded by the saturation itself.
    for value in (D("1e1000"), D("1e39")):
        with pytest.raises(ValueError, match="out of range"):
            convert({"flt": value}, original)
    # A Python int wider than a double raised OverflowError, not a rule error.
    with pytest.raises(ValueError, match="out of range value for float field 'dbl'"):
        convert({"dbl": 10**400}, original)

    # The widest value that still fits, so the guard cannot be off by an order of magnitude.
    assert convert({"dbl": D("1e308")}, original).dbl == 1e308
    assert convert({"dbl": celtypes.IntType(3)}, original).dbl == 3.0


def test_an_explicitly_non_finite_value_still_passes():
    """The JVM's parser takes protobuf JSON's canonical "Infinity"/"-Infinity"/"NaN", so a
    rule that computes one deliberately is not an overflow."""
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    assert math.isinf(convert({"dbl": celtypes.DoubleType(float("inf"))}, original).dbl)
    assert math.isinf(convert({"dbl": celtypes.DoubleType(float("-inf"))}, original).dbl)
    assert math.isnan(convert({"dbl": celtypes.DoubleType(float("nan"))}, original).dbl)
    assert math.isinf(convert({"flt": celtypes.DoubleType(float("inf"))}, original).flt)
    assert math.isnan(convert({"flt": celtypes.DoubleType(float("nan"))}, original).flt)
    # And a Decimal cannot be non-finite without saying so either.
    from decimal import Decimal as D

    assert math.isnan(convert({"dbl": D("NaN")}, original).dbl)


# The same class in the integer arm, found while checking the above: `int()` was called before
# the range check, so int(Decimal("1e100000000")) spent minutes building a hundred million
# digits to reach a rejection its magnitude already settled. The JVM reports that off the token
# without building the number. The timing bound is generous: the fixed path is instant.
def test_an_out_of_range_integer_is_refused_without_building_it():
    import time
    from decimal import Decimal as D

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    start = time.monotonic()
    for value in (D("1e100000000"), D("-1e100000000"), D("1e30")):
        with pytest.raises(ValueError, match="out of range"):
            convert({"i32": value}, original)
    assert time.monotonic() - start < 1.0
    # A non-finite Decimal is not an integer either.
    for value in (D("NaN"), D("Infinity")):
        with pytest.raises(ValueError):
            convert({"i32": value}, original)
    # And the values that do fit still convert.
    assert convert({"i32": D("2")}, original).i32 == 2
    assert convert({"u64": D("18446744073709551615")}, original).u64 == 2**64 - 1


def test_an_enum_still_takes_a_symbol_name():
    """Not a coercion: a name is protobuf JSON's canonical enum form and CEL has no enum
    type, so a string is the only way a rule can name a symbol. The JVM accepts it too."""
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    assert convert({"choice": celtypes.StringType("ONE")}, original).choice == 1
    assert convert({"choice": celtypes.IntType(1)}, original).choice == 1
    with pytest.raises(ValueError, match="bool"):
        convert({"choice": celtypes.BoolType(True)}, original)


def _oneof_descriptor():
    """A message with a two-member oneof and a snake_case field, built at runtime."""
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name, fdp.package, fdp.syntax = "oneof_probe.proto", "tests.oo", "proto3"
    msg = fdp.message_type.add()
    msg.name = "Choice"
    msg.oneof_decl.add(name="choice")
    field_type = descriptor_pb2.FieldDescriptorProto
    msg.field.add(
        name="a", number=1, type=field_type.TYPE_INT32, label=field_type.LABEL_OPTIONAL, oneof_index=0, json_name="a"
    )
    msg.field.add(
        name="b", number=2, type=field_type.TYPE_INT32, label=field_type.LABEL_OPTIONAL, oneof_index=0, json_name="b"
    )
    msg.field.add(
        name="total_amount",
        number=3,
        type=field_type.TYPE_INT32,
        label=field_type.LABEL_OPTIONAL,
        json_name="totalAmount",
    )

    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    desc = pool.FindMessageTypeByName("tests.oo.Choice")
    return desc, message_factory.GetMessageClass(desc)


# Two result entries can name the same slot, and applying both left the outcome to the order the
# rule happened to write them in: `{a: 1, b: 2}` kept b and `{b: 2, a: 1}` kept a, both reported
# as a successful transform. JsonFormat refuses both shapes, and the two have *opposite* null
# handling - mergeField's hasField test sits before its null early-return, mergeOneofField's
# after. Measured against protobuf-java 4.35.1:
#
#   {"a":1,"b":2} / {"b":2,"a":1}          REJECT "...belonging to the same oneof has already
#                                                  been set"
#   {"a":1,"b":null} / {"a":null,"b":2}    accept - a null is treated as absent
#   {"total_amount":1,"totalAmount":2}     REJECT "Field p.M.total_amount has already been set."
#   {"total_amount":1,"totalAmount":null}  REJECT - the same, because the value was already set
#   {"total_amount":null,"totalAmount":null}  accept - neither null set anything
@pytest.mark.parametrize(
    "values",
    [
        {"a": 1, "b": 2},
        {"b": 2, "a": 1},
        {"a": 1, "b": 2, "total_amount": 3},
    ],
)
def test_two_members_of_one_oneof_are_rejected(values):
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _oneof_descriptor()
    celified = {k: celtypes.IntType(v) for k, v in values.items()}
    with pytest.raises(ValueError, match="more than one member of oneof"):
        convert(celified, cls())


@pytest.mark.parametrize(
    "values",
    [
        {"total_amount": 1, "totalAmount": 2},
        {"total_amount": 1, "totalAmount": None},
    ],
)
def test_naming_one_field_twice_is_rejected(values):
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _oneof_descriptor()
    celified = {k: (None if v is None else celtypes.IntType(v)) for k, v in values.items()}
    with pytest.raises(ValueError, match="names field 'tests.oo.Choice.total_amount' twice"):
        convert(celified, cls())


# The accepted half, which is where the two rules differ: a null does not count towards a oneof
# collision but does count as having set a field.
def test_a_null_does_not_collide_with_its_oneof_sibling():
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _oneof_descriptor()
    assert convert({"a": celtypes.IntType(1), "b": None}, cls()).a == 1
    assert convert({"a": None, "b": celtypes.IntType(2)}, cls()).b == 2
    assert convert({"a": None, "b": None}, cls()).WhichOneof("choice") is None
    # Two nulls for one field set nothing, so neither is a duplicate.
    assert convert({"total_amount": None, "totalAmount": None}, cls()).total_amount == 0
    # A null first, then a value, is the same: the null set nothing to collide with.
    assert convert({"total_amount": None, "totalAmount": celtypes.IntType(1)}, cls()).total_amount == 1
    # And one member of the oneof plus an unrelated field is fine.
    out = convert({"a": celtypes.IntType(1), "total_amount": celtypes.IntType(3)}, cls())
    assert (out.a, out.total_amount) == (1, 3)


# A protobuf map value cannot be null either, and dropping the entry reported success while
# deleting it. The JVM's write-back parse says "Map value cannot be null." - measured against
# protobuf-java 4.35.1 on {"mp": {"a":1,"b":null}}.
def test_a_null_map_value_is_rejected():
    from celpy import celtypes

    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _map_descriptor()
    original = cls()
    assert dict(convert({"counts": {"a": celtypes.IntType(1)}}, original).counts) == {"a": 1}
    with pytest.raises(ValueError, match="null value to map field 'counts'"):
        convert({"counts": {"a": celtypes.IntType(1), "b": None}}, original)


def _map_descriptor():
    """A message with a map<string, int32> field, built at runtime."""
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name, fdp.package, fdp.syntax = "map_probe.proto", "tests.mp", "proto3"
    msg = fdp.message_type.add()
    msg.name = "Counts"
    entry = msg.nested_type.add()
    entry.name = "CountsEntry"
    entry.options.map_entry = True
    field_type = descriptor_pb2.FieldDescriptorProto
    entry.field.add(name="key", number=1, type=field_type.TYPE_STRING, label=field_type.LABEL_OPTIONAL, json_name="key")
    entry.field.add(
        name="value", number=2, type=field_type.TYPE_INT32, label=field_type.LABEL_OPTIONAL, json_name="value"
    )
    msg.field.add(
        name="counts",
        number=1,
        type=field_type.TYPE_MESSAGE,
        type_name=".tests.mp.Counts.CountsEntry",
        label=field_type.LABEL_REPEATED,
        json_name="counts",
    )

    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    desc = pool.FindMessageTypeByName("tests.mp.Counts")
    return desc, message_factory.GetMessageClass(desc)


# A protobuf repeated field cannot hold null. Dropping the element changed the list's length
# and hid the mistake; the JVM says "Repeated field elements cannot be null in field: ...".
def test_null_element_in_a_repeated_field_is_rejected():
    from confluent_kafka.schema_registry.rules.cel.protobuf_result_writer import convert

    _, cls = _scalar_descriptor()
    original = cls()
    assert list(convert({"codes": ["a", "b"]}, original).codes) == ["a", "b"]
    with pytest.raises(ValueError, match="cannot write null to repeated field 'codes'"):
        convert({"codes": ["a", None, "b"]}, original)
