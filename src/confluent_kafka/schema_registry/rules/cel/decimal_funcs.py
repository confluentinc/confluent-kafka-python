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

"""CEL bindings for the {@code decimal} constructor and {@code decimals.*} operators.

celpy has no overload-set concept — one function per name, internal arity + type
dispatch. The {@code decimal} constructor handles both shapes
({@code decimal(dyn)} and {@code decimal(bytes, scale)}) in a single Python
callable that branches on {@code len(args)}.

Decimal division uses {@code decimal.Context(prec=38, rounding=ROUND_HALF_UP)} —
matches Flink SQL's MC_DIVIDE and the Java reference implementation. Add/sub/mul
use Python's default exact arithmetic (BigDecimal-like).
"""

import decimal
import typing
from decimal import Decimal

import celpy
from celpy import celtypes

try:
    from confluent_kafka.schema_registry.confluent.types import decimal_pb2
    _PROTO_DECIMAL_CLS: typing.Any = decimal_pb2.Decimal
except ImportError:
    _PROTO_DECIMAL_CLS = None


# 38-digit precision with HALF_UP rounding — matches Flink/PostgreSQL NUMERIC
# division.
_DIV_CONTEXT = decimal.Context(prec=38, rounding=decimal.ROUND_HALF_UP)


def _from_proto_decimal(d: typing.Any) -> Decimal:
    """Decode a confluent.type.Decimal proto message into a Decimal."""
    value = d.value
    scale = int(d.scale)
    if not value:
        return Decimal(0).scaleb(-scale)
    return Decimal(int.from_bytes(value, "big", signed=True)).scaleb(-scale)


def _from_bytes_scale(value: typing.Any, scale: typing.Any) -> Decimal:
    """Construct a Decimal from raw two's-complement big-endian bytes + scale."""
    raw = _coerce_bytes(value)
    s = int(scale)
    if len(raw) == 0:
        return Decimal(0).scaleb(-s)
    return Decimal(int.from_bytes(raw, "big", signed=True)).scaleb(-s)


def _coerce_bytes(v: typing.Any) -> bytes:
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, memoryview):
        return v.tobytes()
    if isinstance(v, celtypes.BytesType):
        return bytes(v)
    raise celpy.CELEvalError(
        f"decimal: expected bytes for the (bytes, scale) overload, got "
        f"{type(v).__name__}")


def _decimal(*args: typing.Any) -> Decimal:
    """Runtime dispatch backing the {@code decimal(...)} constructor.

    Two arities:
      * {@code decimal(dyn)} — convert any supported value to Decimal.
      * {@code decimal(bytes, int)} — explicit bytes + scale construction.
    """
    if len(args) == 2:
        return _from_bytes_scale(args[0], args[1])
    if len(args) != 1:
        raise celpy.CELEvalError(f"decimal: expected 1 or 2 args, got {len(args)}")
    v = args[0]
    if v is None:
        raise celpy.CELEvalError("decimal: cannot convert null to Decimal")
    if isinstance(v, Decimal):
        return v
    if _PROTO_DECIMAL_CLS is not None and isinstance(v, _PROTO_DECIMAL_CLS):
        return _from_proto_decimal(v)
    # Generic proto Message duck-typing — accept any message whose descriptor
    # full_name is confluent.type.Decimal (covers DynamicMessage or alternate
    # generated bindings).
    if hasattr(v, "DESCRIPTOR") and getattr(v.DESCRIPTOR, "full_name", "") == \
            "confluent.type.Decimal":
        return _from_proto_decimal(v)
    if isinstance(v, bool):
        # bool is a subclass of int in Python; reject before the int arm.
        raise celpy.CELEvalError("decimal: cannot convert bool to Decimal")
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, float):
        return Decimal(str(v))
    if isinstance(v, (str, celtypes.StringType)):
        try:
            return Decimal(str(v))
        except decimal.InvalidOperation as ex:
            raise celpy.CELEvalError(f"decimal: invalid number '{v}'") from ex
    if isinstance(v, (bytes, bytearray, memoryview, celtypes.BytesType)):
        raise celpy.CELEvalError(
            "decimal: raw bytes need a scale; use decimal(bytes, scale) or set "
            "useLogicalTypeConverters=true on the Avro client so decimal fields "
            "arrive as Decimal")
    raise celpy.CELEvalError(
        f"decimal: cannot convert {type(v).__name__} to Decimal")


# ---- comparison ----

def _decimals_eq(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) == 0)


def _decimals_ne(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) != 0)


def _decimals_lt(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) < 0)


def _decimals_le(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) <= 0)


def _decimals_gt(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) > 0)


def _decimals_ge(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) >= 0)


# ---- arithmetic ----

def _decimals_add(a: typing.Any, b: typing.Any) -> Decimal:
    return _d(a) + _d(b)


def _decimals_sub(a: typing.Any, b: typing.Any) -> Decimal:
    return _d(a) - _d(b)


def _decimals_mul(a: typing.Any, b: typing.Any) -> Decimal:
    return _d(a) * _d(b)


def _decimals_div(a: typing.Any, b: typing.Any) -> Decimal:
    try:
        return _DIV_CONTEXT.divide(_d(a), _d(b))
    except decimal.DivisionByZero as ex:
        raise celpy.CELEvalError("decimals.div: division by zero") from ex
    except decimal.DecimalException as ex:
        raise celpy.CELEvalError(f"decimals.div: {ex}") from ex


# ---- unary ----

def _decimals_neg(a: typing.Any) -> Decimal:
    return -_d(a)


def _decimals_abs(a: typing.Any) -> Decimal:
    return abs(_d(a))


def _decimals_sign(a: typing.Any) -> celtypes.IntType:
    d = _d(a)
    if d == 0:
        return celtypes.IntType(0)
    return celtypes.IntType(1 if d > 0 else -1)


def _decimals_scale(a: typing.Any) -> celtypes.IntType:
    d = _d(a)
    # Mirror Java BigDecimal.scale() — the negative of the exponent.
    return celtypes.IntType(-d.as_tuple().exponent)


def _decimals_prec(a: typing.Any) -> celtypes.IntType:
    return celtypes.IntType(len(_d(a).as_tuple().digits))


# ---- rounding family ----

def _decimals_round(*args: typing.Any) -> Decimal:
    """Round to the given scale (HALF_UP). One-arg form rounds to integer."""
    if len(args) == 1:
        return _d(args[0]).quantize(Decimal(1), rounding=decimal.ROUND_HALF_UP)
    if len(args) == 2:
        scale = int(args[1])
        return _d(args[0]).quantize(
            Decimal(10) ** -scale, rounding=decimal.ROUND_HALF_UP)
    raise celpy.CELEvalError(
        f"decimals.round: expected 1 or 2 args, got {len(args)}")


def _decimals_trunc(*args: typing.Any) -> Decimal:
    """Truncate to the given scale (toward zero). One-arg form truncates to integer."""
    if len(args) == 1:
        return _d(args[0]).quantize(Decimal(1), rounding=decimal.ROUND_DOWN)
    if len(args) == 2:
        scale = int(args[1])
        return _d(args[0]).quantize(
            Decimal(10) ** -scale, rounding=decimal.ROUND_DOWN)
    raise celpy.CELEvalError(
        f"decimals.trunc: expected 1 or 2 args, got {len(args)}")


def _decimals_floor(a: typing.Any) -> Decimal:
    return _d(a).quantize(Decimal(1), rounding=decimal.ROUND_FLOOR)


def _decimals_ceil(a: typing.Any) -> Decimal:
    return _d(a).quantize(Decimal(1), rounding=decimal.ROUND_CEILING)


def _d(v: typing.Any) -> Decimal:
    """Coerce a rule-argument value to Decimal for operator dispatch."""
    if isinstance(v, Decimal):
        return v
    return _decimal(v)


# ---- string(Decimal) — extend celpy stdlib's string(...) ----

# Capture celpy's stdlib string callable at import time so we can delegate to
# it for non-Decimal inputs. celpy uses StringType(value) as the conversion;
# treating it as the underlying coercion gives us the standard semantics for
# int/uint/double/bytes/timestamp/duration/string args.
_STDLIB_STRING = celtypes.StringType


def _string(v: typing.Any) -> celtypes.StringType:
    """Extension of CEL stdlib {@code string(...)} with a Decimal arm.

    Returns ``Decimal.toPlainString()``-equivalent form (Python's
    ``format(d, 'f')``) for Decimal inputs; delegates to celpy's stdlib
    string coercion for everything else.
    """
    if isinstance(v, Decimal):
        return celtypes.StringType(format(v, "f"))
    return _STDLIB_STRING(v)


DECIMAL_FUNCS: typing.Dict[str, celpy.CELFunction] = {
    "decimal": _decimal,
    "decimals.eq": _decimals_eq,
    "decimals.ne": _decimals_ne,
    "decimals.lt": _decimals_lt,
    "decimals.le": _decimals_le,
    "decimals.gt": _decimals_gt,
    "decimals.ge": _decimals_ge,
    "decimals.add": _decimals_add,
    "decimals.sub": _decimals_sub,
    "decimals.mul": _decimals_mul,
    "decimals.div": _decimals_div,
    "decimals.neg": _decimals_neg,
    "decimals.abs": _decimals_abs,
    "decimals.sign": _decimals_sign,
    "decimals.scale": _decimals_scale,
    "decimals.prec": _decimals_prec,
    "decimals.round": _decimals_round,
    "decimals.trunc": _decimals_trunc,
    "decimals.floor": _decimals_floor,
    "decimals.ceil": _decimals_ceil,
    # string(Decimal) overrides celpy stdlib — the wrapper falls through to
    # stdlib for non-Decimal inputs.
    "string": _string,
}
