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

"""Conversions between :class:`decimal.Decimal` and the ``confluent.type.Decimal`` proto
message - the Python counterpart of Java's ``io.confluent.protobuf.type.utils.DecimalUtils``
(BigDecimal) and C#'s ``DecimalExtensions`` (System.Decimal). Independent of CEL: the Protobuf
serde uses these for ``confluent.type.Decimal`` fields, and the CEL layer reuses them.
"""

from decimal import Decimal

from confluent_kafka.schema_registry.confluent.types import decimal_pb2


def from_proto_decimal(msg: decimal_pb2.Decimal) -> Decimal:
    """Convert a ``confluent.type.Decimal`` message to a :class:`decimal.Decimal`.

    ``value`` is the unscaled integer as big-endian two's-complement bytes; ``scale`` is the
    number of fractional digits (the value is ``unscaled * 10**-scale``).
    """
    scale = int(msg.scale)
    if not msg.value:
        return Decimal(0).scaleb(-scale)
    return Decimal(int.from_bytes(msg.value, "big", signed=True)).scaleb(-scale)


def to_proto_decimal(d: Decimal) -> decimal_pb2.Decimal:
    """Convert a :class:`decimal.Decimal` to a ``confluent.type.Decimal`` message.

    Mirrors Java ``BigDecimal.unscaledValue()``/``scale()``: the scale is the number of
    fractional digits (negative for values like ``1E+2``) and the value is the unscaled
    integer as big-endian two's-complement bytes.
    """
    sign, digits, exponent = d.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError(
            f"cannot convert non-finite Decimal '{d}' to confluent.type.Decimal")
    scale = -exponent
    unscaled = int("".join(map(str, digits)) or "0")
    if sign:
        unscaled = -unscaled
    # Minimal big-endian two's-complement encoding, matching BigInteger.toByteArray().
    length = (unscaled.bit_length() + 8) // 8
    value = unscaled.to_bytes(length, "big", signed=True)
    return decimal_pb2.Decimal(value=value, scale=scale)
