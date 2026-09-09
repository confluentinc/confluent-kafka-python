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

from confluent_kafka.schema_registry.rules.cel.timestamp_funcs import format_timestamp

try:
    from confluent_kafka.schema_registry.confluent.types import decimal_pb2
    from confluent_kafka.schema_registry.confluent.types.decimal_utils import from_proto_decimal as _from_proto_decimal

    _PROTO_DECIMAL_CLS: typing.Any = decimal_pb2.Decimal
except ImportError:
    _PROTO_DECIMAL_CLS = None
    _from_proto_decimal = None  # type: ignore[assignment]


# 38-digit precision with HALF_UP rounding — matches Flink/PostgreSQL NUMERIC
# division.
# Emax/Emin are widened alongside: the default +/-999999 is far narrower than the exponent
# range the constructor accepts (BigDecimal's signed-int scale), so dividing a legitimately
# constructed value such as decimal("1e1000000") overflowed where Java returns a result.
_DIV_CONTEXT = decimal.Context(
    prec=38,
    rounding=decimal.ROUND_HALF_UP,
    Emax=decimal.MAX_EMAX,
    Emin=decimal.MIN_EMIN,
)

# Exact/unbounded context for operations Java computes exactly (add/sub/mul/mod,
# setScale/quantize, scaleb) — matches java.math.BigDecimal's exact semantics rather
# than the thread-local default context (prec=28). Only div/sqrt cap at 38 (_DIV_CONTEXT).
_EXACT_CONTEXT = decimal.Context(prec=decimal.MAX_PREC, Emax=decimal.MAX_EMAX, Emin=decimal.MIN_EMIN)


_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1


def _require_int_scale(scale: typing.Any, fn: str) -> int:
    """A scale argument as an int, mirroring Java's ``requireIntScale``.

    Java declares every scale parameter as ``Long`` and narrows it with ``Math.toIntExact``,
    so a double or a bool has no matching overload there and an out-of-int-range value is an
    error rather than a wildly wrong Decimal. Go, C#, C++ and Rust carry the same check.
    A *negative* scale is legitimate - ``BigDecimal.setScale(-2)`` rounds to hundreds - so
    only the type and the width are constrained here.
    """
    if isinstance(scale, (bool, celtypes.BoolType)) or not isinstance(scale, (int, celtypes.IntType)):
        raise celpy.CELEvalError(f"{fn}: scale must be int, got {type(scale).__name__}")
    s = int(scale)
    if s < _INT32_MIN or s > _INT32_MAX:
        raise celpy.CELEvalError(f"{fn}: scale out of int range: {s}")
    return s


def _drop_negative_zero(d: Decimal) -> Decimal:
    """A zero without a sign, because BigDecimal has no negative zero.

    ``BigDecimal("-0").toPlainString()`` is ``"0"`` and its signum is 0, while Python's Decimal
    keeps the sign and renders ``"-0"``. ``abs`` preserves the scale, so ``-0.00`` becomes
    ``0.00`` rather than ``0`` - matching ``BigDecimal("-0.00").toPlainString()``.
    """
    return abs(d) if not d and d.is_signed() else d


def _from_bytes_scale(value: typing.Any, scale: typing.Any) -> Decimal:
    """Construct a Decimal from raw two's-complement big-endian bytes + scale.

    The scale itself costs nothing here - ``scaleb`` only sets the exponent, so a coefficient
    at an extreme scale stays compact and the width guard fires later, where the digits are
    actually needed. The *coefficient* is the width risk on this path, and it is checked
    before ``int.from_bytes`` builds it: one byte carries about 2.41 decimal digits.
    """
    raw = _coerce_bytes(value)
    s = _require_int_scale(scale, "decimal(bytes, scale)")
    if len(raw) == 0:
        return Decimal(0).scaleb(-s, context=_EXACT_CONTEXT)
    _require_sane_width(int(len(raw) * 2.408) + 1, "decimal(bytes, scale)", "the coefficient",
                        _SANE_COEFFICIENT)
    return Decimal(int.from_bytes(raw, "big", signed=True)).scaleb(-s, context=_EXACT_CONTEXT)


def _coerce_bytes(v: typing.Any) -> bytes:
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, memoryview):
        return v.tobytes()
    if isinstance(v, celtypes.BytesType):
        return bytes(v)
    raise celpy.CELEvalError(f"decimal: expected bytes for the (bytes, scale) overload, got " f"{type(v).__name__}")


def _decimal_from_string(text: str, original: typing.Any) -> Decimal:
    """Parse a Decimal the way Java's ``new BigDecimal(String)`` does.

    Python's ``Decimal(str)`` is far more permissive than Java's
    ``BigDecimal(String)`` / ``BigDecimal.valueOf(double)``, silently accepting
    inputs Java rejects with ``NumberFormatException``:

      * non-finite values — ``"NaN"``, ``"sNaN"``, ``"Infinity"``, ``"inf"``,
        ``"-inf"`` (and the ``str()`` of a NaN/Inf float);
      * underscore digit-grouping such as ``"1_000"`` (→ 1000);
      * surrounding whitespace (Python strips it; internal whitespace is already
        rejected by ``Decimal``).

    Reject those to match Java, while still accepting every legitimate finite
    decimal (integers, ``"1e40"``, negatives, ``"-0"``, a leading ``+``, ...).
    """
    if "_" in text or text != text.strip():
        raise celpy.CELEvalError(f"decimal: invalid number '{original}'")
    try:
        d = Decimal(text)
    except decimal.InvalidOperation as ex:
        raise celpy.CELEvalError(f"decimal: invalid number '{original}'") from ex
    if not d.is_finite():
        raise celpy.CELEvalError(f"decimal: invalid number '{original}'")
    # BigDecimal holds its scale in a signed int and rejects a literal whose exponent will not
    # fit, so `1e-2147483648` is a NumberFormatException there while Python's Decimal accepts
    # it happily. Measured against the JVM, the accepted band is symmetric: |exponent| <=
    # INT32_MAX, with both +/-2147483648 refused. Left unchecked, rendering such a value as
    # fixed-point would try to materialise billions of digits.
    exponent = d.as_tuple().exponent
    if not isinstance(exponent, int) or exponent < -_INT32_MAX or exponent > _INT32_MAX:
        raise celpy.CELEvalError(f"decimal: invalid number '{original}'")
    return _drop_negative_zero(d)


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
    if hasattr(v, "DESCRIPTOR") and getattr(v.DESCRIPTOR, "full_name", "") == "confluent.type.Decimal":
        return _from_proto_decimal(v)
    # celpy binds a proto-message field into CEL as a MessageType wrapper (a MapType that
    # keeps the underlying message on ``.msg``), so `decimal(message.decField)` for a
    # confluent.type.Decimal field arrives here rather than as a raw message. Unwrap it.
    proto_msg = getattr(v, "msg", None)
    if (
        proto_msg is not None
        and getattr(getattr(proto_msg, "DESCRIPTOR", None), "full_name", "") == "confluent.type.Decimal"
    ):
        return _from_proto_decimal(proto_msg)
    if isinstance(v, (bool, celtypes.BoolType)):
        # bool is a subclass of int in Python, and celtypes.BoolType subclasses int rather
        # than bool, so both have to be named here or a CEL bool becomes Decimal(1). Java has
        # no decimal(bool) overload at all.
        raise celpy.CELEvalError("decimal: cannot convert bool to Decimal")
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, float):
        # Java uses BigDecimal.valueOf(double), which throws on NaN/Infinity.
        # str() of a non-finite float ("nan"/"inf"/"-inf") builds a poisoned
        # Decimal in Python, so validate through the same finite check.
        return _decimal_from_string(str(v), v)
    if isinstance(v, (str, celtypes.StringType)):
        return _decimal_from_string(str(v), v)
    if isinstance(v, (bytes, bytearray, memoryview, celtypes.BytesType)):
        raise celpy.CELEvalError(
            "decimal: raw bytes need a scale; use decimal(bytes, scale) or set "
            "useLogicalTypeConverters=true on the Avro client so decimal fields "
            "arrive as Decimal"
        )
    raise celpy.CELEvalError(f"decimal: cannot convert {type(v).__name__} to Decimal")


# ---- comparison ----


def decimal_boundary_value(v: typing.Any) -> typing.Optional[Decimal]:
    """A ``confluent.type.Decimal`` message as a :class:`~decimal.Decimal`, else ``None``.

    Presents a decimal-shaped bound value as this client's in-CEL decimal. Without it a bare
    decimal field and ``decimal(...)`` are two different things at runtime: the field is a celpy
    ``MessageType`` wrapper, so ``this == decimal("12.34")`` compares a wrapper against a
    ``Decimal`` and answers False, and ``string(this)`` / ``double(this)`` fail outright with
    ``TypeError: float() argument must be a string or a real number, not 'MessageType'``.

    Converting at the boundary also makes ``==`` scale-insensitive for free, since
    ``Decimal.__eq__`` is numeric: 12.34 and 12.340 are the same number in two encodings, and
    comparing the protobuf fields one by one would call them unequal.

    Only reaches a value bound directly. A decimal reached by selection instead
    (``this.amount``) is resolved inside celpy, past any boundary.
    """
    if isinstance(v, Decimal):
        return v
    if _from_proto_decimal is None:
        return None
    if _PROTO_DECIMAL_CLS is not None and isinstance(v, _PROTO_DECIMAL_CLS):
        return _from_proto_decimal(v)
    if getattr(getattr(v, "DESCRIPTOR", None), "full_name", "") == "confluent.type.Decimal":
        return _from_proto_decimal(v)
    # celpy wraps a proto message as a MessageType keeping the message on ``.msg``.
    proto_msg = getattr(v, "msg", None)
    if (
        proto_msg is not None
        and getattr(getattr(proto_msg, "DESCRIPTOR", None), "full_name", "") == "confluent.type.Decimal"
    ):
        return _from_proto_decimal(proto_msg)
    return None


def _decimals_eq(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) == 0)


def _decimals_lt(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) < 0)


def _decimals_le(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) <= 0)


def _decimals_gt(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) > 0)


def _decimals_ge(a: typing.Any, b: typing.Any) -> celtypes.BoolType:
    return celtypes.BoolType(_d(a).compare(_d(b)) >= 0)


# ---- arithmetic ----


# The width ceiling for a computation. Deliberately *not* BigDecimal's - BigInteger tops out
# at Integer.MAX_VALUE bits, which is 646456993 decimal digits, and reproducing that bound is
# neither achievable across six libraries nor the point. This is a round number chosen so no
# single rule evaluation can exhaust memory: 10**7 digits is ~4 MB of libmpdec coefficient
# (packed 19 digits to a 64-bit word) and ~10 MB rendered. Values above it are refused as a
# rule error, which is the one thing a resource exhaustion cannot be turned into after the
# fact. Java is the only client in the family that fails cleanly on width; this stands in for
# that, as a bound rather than as a domain model.
_SANE_WIDTH = 10_000_000

# A far tighter bound on what can be *encoded*, which is a different resource. The wire form
# is the unscaled integer in base 256, and decimal <-> binary radix conversion is quadratic in
# every client: measured in the C++ client, its digit-string codec takes 0.04 s at 10**4
# digits, 4.2 s at 10**5 and ~420 s at 10**6, and mpdecimal's own mpd_qexport_u32 is only
# about 10x better with the same quadratic shape. So a value can be cheap to hold, cheap to
# compute with, and still unserialisable.
#
# 4300 is not arbitrary: it is CPython's own int_max_str_digits, the limit it puts on
# str <-> int conversion for exactly this reason. This client already could not encode a wider
# coefficient - `int("9" * 5000)` raises ValueError "Exceeds the limit (4300 digits) for
# integer string conversion" - so the bound is pre-existing and the only thing added is that
# it now reads as a decimal error instead of a CPython internal one. Held as a literal rather
# than read from `sys` so the accepted set does not shift with a host's own setting.
_SANE_COEFFICIENT = 4300


def _adjusted_of(d: Decimal) -> int:
    """``d.adjusted()``, guarded for a non-finite value the way ``_exponent_of`` is."""
    if not d.is_finite():
        raise celpy.CELEvalError(f"decimal: not a finite number '{d}'")
    return d.adjusted()


def _rescaled_digits(target_scale: int, d: Decimal) -> int:
    """Digits in the coefficient ``d`` would have at ``target_scale``.

    Only *expanding* a scale costs anything - the coefficient grows by the difference.
    Coarsening one is free at any distance, and the earlier ``abs(shift) + digits`` form
    refused it wrongly. Measured, all instant and all one digit wide:

    * ``1.23`` at scale -1000000, -100000000, -2000000000  -> 0E+1000000 ... 0E+2000000000
    * ``1e-1000000`` and ``1e-100000000`` at scale 0        -> 0

    against ``1.23`` at scale 100000000, which is 952 MB and a 100000001-digit coefficient.
    Java agrees on both sides: ``BigDecimal("1.23").setScale(-100000000)`` is precision 1.

    Computed from ``(exponent, adjusted)`` rather than from the value, so the digits the
    caller is about to refuse are never built - ``as_tuple()`` on a wide value is itself the
    allocation being guarded (9.2 GB for a 2**31-digit coefficient).
    """
    exponent = _exponent_of(d)
    digits = _adjusted_of(d) - exponent + 1
    return max(1, digits + target_scale + exponent)


def _plain_form_length(d: Decimal) -> int:
    """Characters in ``d``'s plain (non-scientific) rendering, to within a couple.

    Unlike a rescale, this *does* pay for the exponent in both directions: a positive
    exponent writes that many trailing zeros and a negative one that many leading zeros, so
    ``0E-2147483647`` renders as two billion characters even though its coefficient is one
    digit.
    """
    exponent = _exponent_of(d)
    digits = _adjusted_of(d) - exponent + 1
    return digits + abs(exponent)


def _require_sane_width(needed: int, fn: str, what: str, limit: int = _SANE_WIDTH) -> None:
    """Refuse a positional form too wide to build.

    Three unrelated-looking things reduce to this one quantity, because each has to
    materialise a value in positional form:

    * **aligning two exponents** - ``add`` and ``sub`` expand the narrower operand into the
      wider one's frame before computing a single digit; ``remainder`` is the same family but
      is bounded by its integral quotient instead (see :func:`_decimals_mod`);
    * **rescaling** - ``round``/``trunc``/``floor``/``ceil`` produce a coefficient at the
      target scale;
    * **rendering** - ``string()`` writes every digit out.

    ``mul``, ``div``, comparison, negation and ``abs`` are absent deliberately: ``mul`` adds
    the exponents and multiplies the coefficients, ``div`` holds the coefficient to the
    context precision and lets the exponent absorb the difference, and libmpdec's comparison
    short-circuits on the adjusted exponent. Measured on operands 1e2147483647 and 3, peak
    RSS: ``mul``, ``div``, ``<``, ``==``, ``compare``, ``min``, ``neg``, ``abs`` all 13 MB;
    ``add`` 1738 MB, ``sub`` 1738 MB, ``remainder`` 1733 MB; and
    ``add(1e2147483647, 1e-2147483647)`` 3125 MB. So the guard follows *alignment*, not
    arithmetic - a single expression over two cheaply constructed operands is enough.
    """
    if needed > limit:
        raise celpy.CELEvalError(
            f"{fn}: {what} needs {needed} digits, past this client's {limit}-digit limit")


def _operand_width(target_scale: int, d: Decimal) -> int:
    """Digits ``d`` needs once expanded to ``target_scale``.

    A **zero** contributes one digit whatever the distance, because expanding a zero appends no
    digits - and that is what decides several of these cases, since alignment expands only the
    operand whose scale is coarser. Measured on libmpdec, and the reference agrees on every
    row:

    ==========================  ==========================  ===============================
    expression                  libmpdec                    JDK
    ==========================  ==========================  ===============================
    ``0E+2e9 + 0E-2e9``         free, 1 digit               precision 1
    ``0E+2e9 + 1``              free, 1 digit               precision 1
    ``0E+2e9 mod 1E-2e9``       free, 1 digit               precision 1
    ``0E-2e9 + 1``              **1601 MB**, 2e9+1 digits   ArithmeticException
    ==========================  ==========================  ===============================

    The last row is the one that must still be refused, and the difference is purely which
    operand expands: aligning to scale 0 expands the zero (free), aligning to scale 2e9
    expands the *one* (2e9 digits).
    """
    if not d:
        return 1
    exponent = _exponent_of(d)
    digits = _adjusted_of(d) - exponent + 1
    return digits + target_scale + exponent


def _require_additive_domain(x: Decimal, y: Decimal, fn: str) -> None:
    """Addition and subtraction align both operands on the finer scale, so the frame is the
    widest either of them needs there - computed per operand, because a zero costs nothing to
    expand however far it moves (see :func:`_operand_width`)."""
    target_scale = -min(_exponent_of(x), _exponent_of(y))
    needed = max(_operand_width(target_scale, x), _operand_width(target_scale, y)) + 1
    _require_sane_width(needed, fn, "aligning the operands")


def _decimals_add(a: typing.Any, b: typing.Any) -> Decimal:
    x, y = _d(a), _d(b)
    _require_additive_domain(x, y, "decimals.add")
    return _EXACT_CONTEXT.add(x, y)


def _decimals_sub(a: typing.Any, b: typing.Any) -> Decimal:
    x, y = _d(a), _d(b)
    _require_additive_domain(x, y, "decimals.sub")
    return _EXACT_CONTEXT.subtract(x, y)


def _decimals_mul(a: typing.Any, b: typing.Any) -> Decimal:
    # No width guard: multiplication adds the exponents and multiplies the coefficients, so
    # the result is as compact as its operands. It was guarded here once, on a prediction of
    # BigDecimal's own domain errors; that prediction is what this design stopped doing, and
    # the operations it guarded turned out to be the cheap ones.
    return _EXACT_CONTEXT.multiply(_d(a), _d(b))


def _decimals_div(a: typing.Any, b: typing.Any) -> Decimal:
    try:
        return _DIV_CONTEXT.divide(_d(a), _d(b))
    except decimal.DivisionByZero as ex:
        raise celpy.CELEvalError("decimals.div: division by zero") from ex
    except decimal.DecimalException as ex:
        raise celpy.CELEvalError(f"decimals.div: {ex}") from ex


def _decimals_mod(a: typing.Any, b: typing.Any) -> Decimal:
    """Remainder with the sign of the dividend (truncated division), matching
    Java BigDecimal.remainder and SQL MOD. A zero divisor raises the canonical
    message.
    """
    da, db = _d(a), _d(b)
    if db == 0:
        raise celpy.CELEvalError("decimals.mod: division by zero")
    # The remainder itself is small - its magnitude is bounded by both operands - but the
    # *integral quotient* has to be produced to get there, and that is the width. Not the
    # aligned frame add and sub are guarded on: libmpdec short-circuits when the operands'
    # magnitudes are close or the dividend is the smaller, so the frame over-refuses. Measured
    # (peak RSS), with the aligned frame in the last column for contrast:
    #
    #   1e2147483647 mod 3               2^31 quotient digits   1733 MB    2^31 frame
    #   1.5 mod 1e-2147483647            2^31                   1519 MB    2^31
    #   1e2147483647 mod 1e-2147483647   4.3e9                  2568 MB    4.3e9
    #   1e-2147483647 mod 1e2147483647   0                        13 MB    4.3e9  <- free
    #   1e2147483647 mod 1e2147483000    647                      13 MB    4.3e9  <- free
    #   1e40 mod 3                       40                       13 MB
    #
    # so the last two are what the frame would have cost us, and both are values the JVM
    # accepts: `1e-2147483647 mod 1e2147483647` is the dividend itself at precision 1.
    # A zero dividend has a quotient of zero whatever the scales, and the adjusted exponent
    # says nothing useful about it - a zero keeps whatever scale it was built with, so
    # `0E+2e9 mod 1E-2e9` estimated 4e9 digits for a result that is just zero. Measured free on
    # libmpdec, and the JDK returns 0 at precision 1.
    quotient_digits = (
        1 if not da else max(0, _adjusted_of(da) - _adjusted_of(db)) + 1
    )
    _require_sane_width(quotient_digits, "decimals.mod", "the integral quotient")
    return _EXACT_CONTEXT.remainder(da, db)


# ---- selection ----


def _decimals_greatest(a: typing.Any, b: typing.Any) -> Decimal:
    return max(_d(a), _d(b))


def _decimals_least(a: typing.Any, b: typing.Any) -> Decimal:
    return min(_d(a), _d(b))


# ---- square root ----


def _decimals_sqrt(a: typing.Any) -> Decimal:
    """Square root with 38-digit HALF_UP precision (same context as div).

    A negative input raises the canonical ``decimals.sqrt: square root of
    negative number`` message (no complex result); zero passes through to 0.
    """
    d = _d(a)
    if d < 0:
        raise celpy.CELEvalError("decimals.sqrt: square root of negative number")
    return _DIV_CONTEXT.sqrt(d)


# ---- unary ----


def _decimals_neg(a: typing.Any) -> Decimal:
    return _d(a).copy_negate()


def _decimals_abs(a: typing.Any) -> Decimal:
    return _d(a).copy_abs()


def _decimals_sign(a: typing.Any) -> celtypes.IntType:
    d = _d(a)
    if d == 0:
        return celtypes.IntType(0)
    return celtypes.IntType(1 if d > 0 else -1)


# ---- rounding family ----


def _exponent_of(d: Decimal) -> int:
    """The decimal's exponent, as an int.

    ``Decimal.as_tuple().exponent`` is ``int | Literal['n', 'N', 'F']`` - the strings stand
    for NaN, sNaN and Infinity - so it cannot be compared or negated as it comes. ``_d``
    rejects a non-finite value before this is reached; the check keeps that guarantee local
    rather than assumed.
    """
    exponent = d.as_tuple().exponent
    if not isinstance(exponent, int):
        raise celpy.CELEvalError(f"decimal: not a finite number '{d}'")
    return exponent


def _quantize(d: Decimal, scale: int, rounding: str, fn: str) -> Decimal:
    """``d`` at ``scale``, or a rule error. The single Guard B site.

    Two things have to hold, and they pull in opposite directions.

    The quantizer is built in ``_EXACT_CONTEXT``, not the ambient one. The default context's
    Emin of -999999 made ``Decimal(1).scaleb(1000000)`` raise ``decimal.Overflow``, so a
    negative scale past a million was refused for values the JVM rounds happily:
    ``BigDecimal("1e1000000").setScale(-1000000)`` is a no-op, and ``setScale(-1000000)`` on
    1.23 gives 0E+1000000. ``Overflow`` is not an ``InvalidOperation`` either, so it escaped
    the handler below as a raw Python exception rather than a rule error.

    Building it in ``_EXACT_CONTEXT`` alone goes too far the other way: libmpdec then honours
    any int32 scale, and quantizing to one materialises the whole coefficient. A scale of
    2**31-1 costs 918 MB inside ``quantize`` (2**31 digits, packed 19 to a 64-bit word) and
    9.2 GB the moment anything calls ``as_tuple()`` on the result - which ``_exponent_of``
    and the protobuf writer's ``_set_decimal`` both do. So the shift is bounded, by
    :data:`_SANE_WIDTH`.

    Every rounding call site goes through here, the one-argument forms included. Three of the
    five did not, and each was a multi-GB allocation reachable from a rule with no scale
    argument at all: ``round(x)``, ``floor(x)`` and ``ceil(x)`` on a value with a large
    negative exponent quantize to scale 0. That is the failure mode of a guard hung off one
    helper rather than off the operation.
    """
    # Zero is one digit at any scale. Rescaling it never expands anything - measured, both
    # directions are free, and its result stays compact - and BigDecimal agrees:
    # `new BigDecimal(BigInteger.ZERO, 2147483647)` is precision 1. Without this, the width
    # formula reads the exponent and refuses `round(decimal(b"", 2147483647))`, a false
    # rejection of a value the reference handles.
    if d:
        _require_sane_width(_rescaled_digits(scale, d), fn, f"a scale of {scale}")
    try:
        return d.quantize(
            Decimal(1).scaleb(-scale, context=_EXACT_CONTEXT),
            rounding=rounding,
            context=_EXACT_CONTEXT,
        )
    except (decimal.InvalidOperation, decimal.Overflow, decimal.Underflow) as e:
        raise celpy.CELEvalError(f"{fn}: cannot represent a scale of {scale}") from e


def _decimals_round(*args: typing.Any) -> Decimal:
    """Round to the given scale (HALF_UP). One-arg form rounds to integer."""
    if len(args) == 1:
        return _quantize(_d(args[0]), 0, decimal.ROUND_HALF_UP, "decimals.round")
    if len(args) == 2:
        scale = _require_int_scale(args[1], "decimals.round")
        return _quantize(_d(args[0]), scale, decimal.ROUND_HALF_UP, "decimals.round")
    raise celpy.CELEvalError(f"decimals.round: expected 1 or 2 args, got {len(args)}")


def _decimals_trunc(*args: typing.Any) -> Decimal:
    """Truncate to the given scale (toward zero). One-arg form truncates to integer.

    Matches Flink's TRUNCATE early-return: if the target scale is at-or-finer
    than the input's current scale, return the input unchanged. Without this
    guard, ``quantize`` would zero-pad and the string representation would
    diverge from Flink (numerically identical, but ``string(trunc(d, n>=cur))``
    output would differ).
    """
    if len(args) == 1:
        d = _d(args[0])
        # current scale = -exponent. Early-return if 0 >= current_scale.
        if _exponent_of(d) >= 0:
            return d
        return _quantize(d, 0, decimal.ROUND_DOWN, "decimals.trunc")
    if len(args) == 2:
        d = _d(args[0])
        scale = _require_int_scale(args[1], "decimals.trunc")
        if scale >= -_exponent_of(d):
            return d
        return _quantize(d, scale, decimal.ROUND_DOWN, "decimals.trunc")
    raise celpy.CELEvalError(f"decimals.trunc: expected 1 or 2 args, got {len(args)}")


def _decimals_floor(a: typing.Any) -> Decimal:
    return _quantize(_d(a), 0, decimal.ROUND_FLOOR, "decimals.floor")


def _decimals_ceil(a: typing.Any) -> Decimal:
    return _quantize(_d(a), 0, decimal.ROUND_CEILING, "decimals.ceil")


def _d(v: typing.Any) -> Decimal:
    """Coerce a rule-argument value to Decimal for operator dispatch.

    Non-finite values are rejected here as well as in the string constructor: Java's and
    Rust's decimals cannot represent NaN or an infinity at all, so no operator should see
    one. An already-Decimal argument - from arithmetic, or from a decimal field - is the
    path that skipped the constructor's check.
    """
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise celpy.CELEvalError(f"decimal: not a finite number '{v}'")
        return v
    return _decimal(v)


# ---- string(Decimal) — extend celpy stdlib's string(...) ----

# Capture celpy's stdlib string callable at import time so we can delegate to
# it for non-Decimal inputs. celpy uses StringType(value) as the conversion;
# treating it as the underlying coercion gives us the standard semantics for
# int/uint/double/bytes/timestamp/duration/string args.
_STDLIB_STRING = celtypes.StringType


def _string(v: typing.Any) -> celtypes.StringType:
    """Extension of CEL stdlib {@code string(...)} with Decimal and Timestamp arms.

    Returns ``Decimal.toPlainString()``-equivalent form (Python's
    ``format(d, 'f')``) for Decimal inputs; renders a Timestamp through
    :func:`~confluent_kafka.schema_registry.rules.cel.timestamp_funcs.format_timestamp`,
    because celpy's own ``TimestampType.__str__`` silently drops the
    sub-second component; delegates to celpy's stdlib string coercion for
    everything else.
    """
    if isinstance(v, celtypes.TimestampType):
        return celtypes.StringType(format_timestamp(v))
    # A decimal reached by selection (`this.amount`) is a celpy MessageType wrapper, not a
    # Decimal. Java's string() resolves it through asDecimalOrNull, which accepts the
    # confluent.type.Decimal message form as well as its own decimal.
    d = decimal_boundary_value(v)
    if d is not None:
        # Guard C. `format(d, "f")` writes every digit of the positional form, and that form
        # can be enormous for a value that was cheap to compute: `div` holds its coefficient
        # to 38 digits while its exponent runs free, so
        # `decimals.div(decimal("1e-2147483647"), decimal("1e2147483647"))` costs nothing and
        # renders as four billion characters. Measured: rendering a 10**8-digit value takes
        # 204 MB. No zero shortcut here, unlike the rescale guard - a zero at an extreme
        # scale renders as that many zeros.
        _require_sane_width(_plain_form_length(d), "string", "the plain form")
        return celtypes.StringType(format(_drop_negative_zero(d), "f"))
    return _STDLIB_STRING(v)


# ---- double(Decimal) — extend celpy stdlib's double(...) ----

# Capture celpy's stdlib double callable so we can delegate non-Decimal inputs.
_STDLIB_DOUBLE = celtypes.DoubleType


def _double(v: typing.Any) -> celtypes.DoubleType:
    """Extension of CEL stdlib {@code double(...)} with a Decimal arm.

    Narrowing conversion (``float(Decimal)``) for Decimal inputs — may lose
    precision, and out-of-range magnitudes become ``inf``; delegates to celpy's
    stdlib double coercion for everything else.
    """
    d = decimal_boundary_value(v)
    if d is not None:
        return celtypes.DoubleType(float(d))
    return _STDLIB_DOUBLE(v)


def _as_decimal_or_none(o: typing.Any) -> typing.Optional[Decimal]:
    """``o`` as a Decimal if it *is* one, else ``None``.

    Deliberately narrow — unlike :func:`_d`, it does not coerce ints, floats or strings. This runs
    on every ``==`` in every rule, so turning ``1 == "1"`` into a decimal comparison would be
    wrong, and it must stay cheap for the common case.
    """
    return decimal_boundary_value(o)


def _has_decimal(o: typing.Any) -> bool:
    """Whether ``o`` is a Decimal or holds one at any depth.

    Only consulted once both operands are containers, so it never runs on the scalar path.
    """
    if _as_decimal_or_none(o) is not None:
        return True
    if isinstance(o, (list, tuple)):
        return any(_has_decimal(e) for e in o)
    if isinstance(o, dict):
        return any(_has_decimal(v) for v in o.values())
    return False


def _cel_equals(a: typing.Any, b: typing.Any) -> bool:
    """CEL ``==`` with decimals made numeric, as a plain bool.

    A decimal operand may be a :class:`~decimal.Decimal` or a ``confluent.type.Decimal`` message,
    and comparing the latter structurally - field by field over unscaled bytes and scale - calls
    12.34 and 12.340 unequal even though they are the same number.

    Containers are handled too, but only when a decimal is actually inside one of them: the base
    implementation recurses with its own equality, so a Decimal nested in a list or map was
    compared structurally and ``[a] == [b]`` disagreed with ``a == b`` on the same values. Gating
    on :func:`_has_decimal` leaves every decimal-free comparison on the base path untouched, and
    each element pair recurses back through here so non-decimal elements keep base semantics.
    """
    da = _as_decimal_or_none(a)
    db = _as_decimal_or_none(b)
    if da is not None and db is not None:
        return da == db
    if da is not None or db is not None:
        # A decimal is never equal to a non-decimal.
        return False
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)) and (_has_decimal(a) or _has_decimal(b)):
        return len(a) == len(b) and all(_cel_equals(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict) and (_has_decimal(a) or _has_decimal(b)):
        return len(a) == len(b) and all(k in b and _cel_equals(v, b[k]) for k, v in a.items())
    return bool(celpy.evaluation.bool_eq(a, b))


def _decimal_aware_eq(a: typing.Any, b: typing.Any) -> typing.Any:
    if (
        _as_decimal_or_none(a) is None
        and _as_decimal_or_none(b) is None
        and not _has_decimal(a)
        and not _has_decimal(b)
    ):
        # No decimal anywhere: hand it straight back to the base implementation, errors and all.
        return celpy.evaluation.bool_eq(a, b)
    return celtypes.BoolType(_cel_equals(a, b))


def _decimal_aware_ne(a: typing.Any, b: typing.Any) -> typing.Any:
    if (
        _as_decimal_or_none(a) is None
        and _as_decimal_or_none(b) is None
        and not _has_decimal(a)
        and not _has_decimal(b)
    ):
        return celpy.evaluation.bool_ne(a, b)
    return celtypes.BoolType(not _cel_equals(a, b))


def _decimal_aware_in(item: typing.Any, container: typing.Any) -> typing.Any:
    """``in`` has to follow ``==`` or the two contradict each other."""
    if not _has_decimal(item) and not _has_decimal(container):
        return celpy.evaluation.operator_in(item, container)
    if isinstance(container, dict):
        return celtypes.BoolType(any(_cel_equals(item, k) for k in container))
    if isinstance(container, (list, tuple)):
        return celtypes.BoolType(any(_cel_equals(item, e) for e in container))
    return celpy.evaluation.operator_in(item, container)


# The CEL operators, overridden so a decimal compares numerically. celpy resolves functions
# through a ChainMap that consults these before its own base_functions.
DECIMAL_OPERATOR_FUNCS: typing.Dict[str, typing.Any] = {
    "_==_": _decimal_aware_eq,
    "_!=_": _decimal_aware_ne,
    "_in_": _decimal_aware_in,
}


# Typed as Any rather than celpy.CELFunction: these functions return this client's own
# Decimal and Variant values, which are not in celpy's declared return union - the CEL
# surface is extended with opaque types celpy does not know. celpy dispatches them fine
# at runtime; only its annotation is narrower than what an extension can return.
DECIMAL_FUNCS: typing.Dict[str, typing.Any] = {
    "decimal": _decimal,
    "decimals.eq": _decimals_eq,
    "decimals.lt": _decimals_lt,
    "decimals.le": _decimals_le,
    "decimals.gt": _decimals_gt,
    "decimals.ge": _decimals_ge,
    "decimals.add": _decimals_add,
    "decimals.sub": _decimals_sub,
    "decimals.mul": _decimals_mul,
    "decimals.div": _decimals_div,
    "decimals.mod": _decimals_mod,
    "decimals.greatest": _decimals_greatest,
    "decimals.least": _decimals_least,
    "decimals.sqrt": _decimals_sqrt,
    "decimals.neg": _decimals_neg,
    "decimals.abs": _decimals_abs,
    "decimals.sign": _decimals_sign,
    "decimals.round": _decimals_round,
    "decimals.trunc": _decimals_trunc,
    "decimals.floor": _decimals_floor,
    "decimals.ceil": _decimals_ceil,
    # string(Decimal) overrides celpy stdlib — the wrapper falls through to
    # stdlib for non-Decimal inputs.
    "string": _string,
    # double(Decimal) overrides celpy stdlib — same fall-through pattern.
    "double": _double,
}
