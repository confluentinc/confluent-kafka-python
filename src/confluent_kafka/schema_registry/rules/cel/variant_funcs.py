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

"""CEL bindings for the ``variant(...)`` constructor and the ``variants.*`` accessor
functions - the Python counterpart of Java's ``rules/cel/builtin`` variant glue.

celpy has no overload-set concept (one callable per name, internal arity/type dispatch) and
no opaque type, so a :class:`Variant` flows through CEL as a plain Python object, exactly as
``decimal.Decimal`` does for the decimal functions.

Null model (matching the Java reference / Spark Variant semantics):

* CEL null (Python ``None``) = *absent*: a missing field, an out-of-bounds index, a
  type-mismatched receiver, or a non-Variant input.
* a Variant whose top type is ``NULL`` = *present, but the value is variant-null*.

``variants.field``/``path``/``index`` return CEL null on a miss; ``variants.isNull`` is true
only for a real Variant with top type NULL. Distinguish the two with
``result == null`` (absent) vs ``variants.isNull(result)`` (variant-null).
"""

import typing
from datetime import datetime, timedelta, timezone

import celpy
from celpy import celtypes

from confluent_kafka.schema_registry.confluent.types import variant_utils as vu
from confluent_kafka.schema_registry.confluent.types.variant_utils import Variant, VariantType
from confluent_kafka.schema_registry.rules.cel import variant_path

try:
    from confluent_kafka.schema_registry.confluent.types import variant_pb2
    _PROTO_VARIANT_CLS: typing.Any = variant_pb2.Variant
except ImportError:
    _PROTO_VARIANT_CLS = None

_VARIANT_PROTO_NAME = "confluent.type.Variant"
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
_INT32_MAX = 2 ** 31 - 1

# VariantType -> the coarse label variants.type returns, matching Java variantTypeName:
# integer widths collapse to "int", float/double to "double", decimal widths to "decimal",
# and all four timestamp variants to "timestamp".
_TYPE_LABELS = {
    VariantType.OBJECT: "object",
    VariantType.ARRAY: "array",
    VariantType.NULL: "null",
    VariantType.BOOLEAN: "boolean",
    VariantType.BYTE: "int",
    VariantType.SHORT: "int",
    VariantType.INT: "int",
    VariantType.LONG: "int",
    VariantType.FLOAT: "double",
    VariantType.DOUBLE: "double",
    VariantType.DECIMAL4: "decimal",
    VariantType.DECIMAL8: "decimal",
    VariantType.DECIMAL16: "decimal",
    VariantType.DATE: "date",
    VariantType.TIME: "time",
    VariantType.TIMESTAMP_TZ: "timestamp",
    VariantType.TIMESTAMP_NTZ: "timestamp",
    VariantType.TIMESTAMP_NANOS_TZ: "timestamp",
    VariantType.TIMESTAMP_NANOS_NTZ: "timestamp",
    VariantType.STRING: "string",
    VariantType.BINARY: "bytes",
    VariantType.UUID: "uuid",
}

_INT_TYPES = (VariantType.BYTE, VariantType.SHORT, VariantType.INT, VariantType.LONG)
_DECIMAL_TYPES = (VariantType.DECIMAL4, VariantType.DECIMAL8, VariantType.DECIMAL16)
_TIMESTAMP_TYPES = (
    VariantType.TIMESTAMP_TZ, VariantType.TIMESTAMP_NTZ,
    VariantType.TIMESTAMP_NANOS_TZ, VariantType.TIMESTAMP_NANOS_NTZ,
)
_MICROS_TIMESTAMP_TYPES = (VariantType.TIMESTAMP_TZ, VariantType.TIMESTAMP_NTZ)


def _coerce_bytes(v: typing.Any) -> bytes:
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, memoryview):
        return v.tobytes()
    if isinstance(v, celtypes.BytesType):
        return bytes(v)
    raise celpy.CELEvalError(
        f"variant: expected bytes, got {type(v).__name__}")


def _to_variant(v: typing.Any) -> Variant:
    """Runtime dispatch backing ``variant(dyn)``: accept the shapes proto/Avro decoders
    produce. Rejects strings (use ``variants.parseJson``)."""
    if v is None:
        raise celpy.CELEvalError("variant: cannot convert null to Variant")
    if isinstance(v, Variant):
        return v
    # A confluent.type.Variant proto message: the generated class, or any message whose
    # descriptor full name matches (covers DynamicMessage / alternate bindings).
    if _PROTO_VARIANT_CLS is not None and isinstance(v, _PROTO_VARIANT_CLS):
        return Variant(_coerce_bytes(v.value), _coerce_bytes(v.metadata))
    if getattr(getattr(v, "DESCRIPTOR", None), "full_name", "") == _VARIANT_PROTO_NAME:
        return Variant(_coerce_bytes(v.value), _coerce_bytes(v.metadata))
    # celpy binds a proto-message field as a wrapper that keeps the message on ``.msg``.
    proto_msg = getattr(v, "msg", None)
    if proto_msg is not None and getattr(
            getattr(proto_msg, "DESCRIPTOR", None), "full_name", "") == _VARIANT_PROTO_NAME:
        return Variant(_coerce_bytes(proto_msg.value), _coerce_bytes(proto_msg.metadata))
    # An Avro variant-logical field reaches CEL as a map with {"metadata", "value"} byte
    # entries (celpy MapType is a dict subclass).
    if isinstance(v, dict):
        md = v.get("metadata")
        val = v.get("value")
        if md is not None and val is not None:
            return Variant(_coerce_bytes(val), _coerce_bytes(md))
        if md is not None or val is not None:
            missing = "value" if val is None else "metadata"
            raise celpy.CELEvalError(
                f"variant: cannot convert map to Variant: missing '{missing}' entry")
    if isinstance(v, (str, celtypes.StringType)):
        raise celpy.CELEvalError(
            "variant: cannot convert string to Variant; use variants.parseJson(s) for "
            "strict JSON parsing or variants.tryParseJson(s) for soft mode")
    raise celpy.CELEvalError(f"variant: cannot convert {type(v).__name__} to Variant")


def _variant(*args: typing.Any) -> Variant:
    """The ``variant(...)`` constructor. ``variant(dyn)`` runtime-dispatches;
    ``variant(bytes, bytes)`` builds directly from (value, metadata) bytes."""
    if len(args) == 2:
        return Variant(_coerce_bytes(args[0]), _coerce_bytes(args[1]))
    if len(args) != 1:
        raise celpy.CELEvalError(f"variant: expected 1 or 2 args, got {len(args)}")
    return _to_variant(args[0])


def _parse_json(s: typing.Any) -> Variant:
    """``variants.parseJson(string)`` - strict; raises on malformed JSON."""
    if not isinstance(s, (str, celtypes.StringType)):
        raise celpy.CELEvalError("variants.parseJson: expected a string")
    try:
        return vu.parse_json(str(s))
    except (vu.VariantError, ValueError) as ex:
        raise celpy.CELEvalError(f"variants.parseJson: {ex}") from ex


def _try_parse_json(s: typing.Any) -> typing.Optional[Variant]:
    """``variants.tryParseJson(string)`` - soft; CEL null on any parse failure."""
    try:
        return vu.parse_json(str(s))
    except Exception:  # noqa: BLE001 - soft form: any failure -> CEL null
        return None


def _type(v: typing.Any) -> typing.Any:
    """``variants.type(Variant)`` - the type label as a string; propagates CEL null."""
    if v is None:
        return None
    if not isinstance(v, Variant):
        raise celpy.CELEvalError(
            f"variants.type: expected Variant, got {type(v).__name__}")
    return celtypes.StringType(_TYPE_LABELS[v.get_type()])


def _is_null(o: typing.Any) -> celtypes.BoolType:
    """``variants.isNull(dyn)`` - true iff input is a Variant whose top type is NULL."""
    return celtypes.BoolType(isinstance(o, Variant) and o.get_type() == VariantType.NULL)


def _require_variant_or_null(o: typing.Any, fn: str) -> typing.Optional[Variant]:
    """A ``variants.*`` navigation argument: CEL null passes through as ``None``; a real
    Variant is returned; anything else is a hard error (the dyn signature lets a misused
    non-Variant reach the binding)."""
    if o is None:
        return None
    if not isinstance(o, Variant):
        raise celpy.CELEvalError(f"{fn}: expected Variant, got {type(o).__name__}")
    return o


def _path(o: typing.Any, path: typing.Any) -> typing.Optional[Variant]:
    """``variants.path(dyn, string)`` - navigate a JSONPath subset; CEL null on a miss;
    malformed path raises."""
    v = _require_variant_or_null(o, "variants.path")
    if v is None:
        return None
    try:
        return variant_path.walk(v, str(path))
    except ValueError as ex:
        raise celpy.CELEvalError(f"variants.path: {ex}") from ex


def _field(o: typing.Any, key: typing.Any) -> typing.Optional[Variant]:
    """``variants.field(dyn, string)`` - object field by key; CEL null on a miss or a
    non-object receiver."""
    v = _require_variant_or_null(o, "variants.field")
    if v is None or v.get_type() != VariantType.OBJECT:
        return None
    return v.get_field_by_key(str(key))


def _index(o: typing.Any, idx: typing.Any) -> typing.Optional[Variant]:
    """``variants.index(dyn, int)`` - array element by index; CEL null on out-of-bounds or a
    non-array receiver."""
    v = _require_variant_or_null(o, "variants.index")
    if v is None or v.get_type() != VariantType.ARRAY:
        return None
    i = int(idx)
    if i < 0 or i > _INT32_MAX:
        return None
    return v.get_element_at_index(i)


def _variant_get_timestamp(v: Variant) -> celtypes.TimestampType:
    raw = v.get_long()
    micros = raw if v.get_type() in _MICROS_TIMESTAMP_TYPES else raw // 1000
    return celtypes.TimestampType(_EPOCH_UTC + timedelta(microseconds=micros))


def _variant_as(o: typing.Any, type_str: str, null_on_error: bool) -> typing.Any:
    """Backing for ``variants.as`` (strict) and ``variants.tryAs`` (soft). Extracts a typed
    value; on a type mismatch the strict form raises and the soft form returns CEL null.
    Types with no CEL extraction (object/array/null/date/time/uuid) always raise."""
    fn = "variants.tryAs" if null_on_error else "variants.as"
    v = _require_variant_or_null(o, fn)
    if v is None:
        return None
    t = v.get_type()
    if type_str == "string":
        if t == VariantType.STRING:
            return celtypes.StringType(v.get_string())
    elif type_str == "int":
        if t in _INT_TYPES:
            return celtypes.IntType(v.get_long())
    elif type_str == "double":
        if t == VariantType.FLOAT:
            return celtypes.DoubleType(float(v.get_float()))
        if t == VariantType.DOUBLE:
            return celtypes.DoubleType(v.get_double())
    elif type_str == "boolean":
        if t == VariantType.BOOLEAN:
            return celtypes.BoolType(v.get_boolean())
    elif type_str == "decimal":
        if t in _DECIMAL_TYPES:
            return v.get_decimal()
    elif type_str == "timestamp":
        if t in _TIMESTAMP_TYPES:
            return _variant_get_timestamp(v)
    elif type_str == "bytes":
        if t == VariantType.BINARY:
            return celtypes.BytesType(v.get_binary())
    elif type_str in ("object", "array", "null", "date", "time", "uuid"):
        # Not extractable as a CEL scalar - always an error, even in the soft form.
        raise celpy.CELEvalError(
            f"variants.as: type '{type_str}' is not supported for extraction "
            "(use variants.type/variants.path/variants.field/variants.index instead)")
    else:
        if null_on_error:
            return None
        raise celpy.CELEvalError(
            f"variants.as: unknown type '{type_str}' (expected one of: string, int, "
            "double, boolean, decimal, timestamp, bytes)")
    # Recognized type string, but the variant's actual type does not match.
    if null_on_error:
        return None
    raise celpy.CELEvalError(
        f"variants.as: variant is not {type_str}-typed (type={t.value})")


def _as(o: typing.Any, type_str: typing.Any) -> typing.Any:
    """``variants.as(dyn, string)`` - typed extraction; raises on type mismatch."""
    return _variant_as(o, str(type_str), null_on_error=False)


def _try_as(o: typing.Any, type_str: typing.Any) -> typing.Any:
    """``variants.tryAs(dyn, string)`` - typed extraction; CEL null on type mismatch."""
    return _variant_as(o, str(type_str), null_on_error=True)


def _to_json(v: typing.Any) -> typing.Any:
    """``variants.toJson(Variant)`` - serialize to a JSON string; propagates CEL null."""
    if v is None:
        return None
    if not isinstance(v, Variant):
        raise celpy.CELEvalError(
            f"variants.toJson: expected Variant, got {type(v).__name__}")
    return celtypes.StringType(v.to_json())


VARIANT_FUNCS: typing.Dict[str, celpy.CELFunction] = {
    "variant": _variant,
    "variants.parseJson": _parse_json,
    "variants.tryParseJson": _try_parse_json,
    "variants.type": _type,
    "variants.isNull": _is_null,
    "variants.path": _path,
    "variants.field": _field,
    "variants.index": _index,
    "variants.as": _as,
    "variants.tryAs": _try_as,
    "variants.toJson": _to_json,
}
