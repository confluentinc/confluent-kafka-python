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

from decimal import Context, Decimal, MAX_EMAX, MAX_PREC, MIN_EMIN

from confluent_kafka.schema_registry.confluent.types import decimal_pb2


# Java builds `new BigDecimal(unscaledValue, scale)`, which is exact. `scaleb` otherwise uses
# the ambient thread-local context (28 significant digits by default) and would silently round
# an unscaled value wider than that.
# Emax/Emin are widened too: the default +/-999999 is narrower than the int32 scale the
# message's field permits, and a wide scale raised decimal.Overflow out of the conversion.
_EXACT_CONTEXT = Context(prec=MAX_PREC, Emax=MAX_EMAX, Emin=MIN_EMIN)


def from_proto_decimal(msg: decimal_pb2.Decimal) -> Decimal:
    """Convert a ``confluent.type.Decimal`` message to a :class:`decimal.Decimal`.

    ``value`` is the unscaled integer as big-endian two's-complement bytes; ``scale`` is the
    number of fractional digits (the value is ``unscaled * 10**-scale``).
    """
    scale = int(msg.scale)
    if not msg.value:
        return Decimal(0).scaleb(-scale, context=_EXACT_CONTEXT)
    unscaled = int.from_bytes(msg.value, "big", signed=True)
    return Decimal(unscaled).scaleb(-scale, context=_EXACT_CONTEXT)


def to_proto_decimal(d: Decimal) -> decimal_pb2.Decimal:
    """Convert a :class:`decimal.Decimal` to a ``confluent.type.Decimal`` message.

    Mirrors Java ``BigDecimal.unscaledValue()``/``scale()``: the scale is the number of
    fractional digits (negative for values like ``1E+2``) and the value is the unscaled
    integer as big-endian two's-complement bytes.
    """
    sign, digits, exponent = d.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError(f"cannot convert non-finite Decimal '{d}' to confluent.type.Decimal")
    scale = -exponent
    unscaled = int("".join(map(str, digits)) or "0")
    if sign:
        unscaled = -unscaled
    value = unscaled_to_bytes(unscaled)
    # Precision is the unscaled value's digit count, as Java's DecimalUtils.fromBigDecimal sets
    # it. Safe here because the scale is derived from the value rather than requested, so
    # len(digits) is exactly the digit count of the unscaled value being written - a reader
    # that treats precision as a MathContext cannot then round it or shift its scale.
    return decimal_pb2.Decimal(value=value, precision=len(digits), scale=scale)


def unscaled_to_bytes(unscaled: int) -> bytes:
    """Minimal big-endian two's-complement encoding of an unscaled integer, matching
    ``BigInteger.toByteArray()``. The single implementation for every writer in this client."""
    return unscaled.to_bytes(_twos_complement_length(unscaled), "big", signed=True)


def _twos_complement_length(n: int) -> int:
    """Byte length of the minimal big-endian two's-complement form of ``n``, matching
    ``BigInteger.toByteArray()``.

    ``bit_length()`` ignores the sign, so deriving the length from it alone over-allocates by a
    byte at every negative power of two that is exactly a signed boundary: -128 needs one byte
    (0x80) but reports a bit length of 8. A negative value's magnitude is taken from ``~n``,
    which is one less, and one bit is reserved for the sign in both cases.
    """
    bits = (n.bit_length() if n >= 0 else (~n).bit_length()) + 1
    return max(1, (bits + 7) // 8)
