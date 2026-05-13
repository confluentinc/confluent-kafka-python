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

"""CEL binding for the {@code timestamp.of} constructor.

celpy already provides a stdlib ``timestamp(string)`` (RFC 3339 parsing) plus
the standard timestamp operators (``<``, ``>``, ``==``, ``-``, ``+ duration``,
``.getDate()`` etc.). The extension we add here is the namespaced
``timestamp.of(...)`` constructor:

  * ``timestamp.of(dyn) -> timestamp`` — runtime-dispatches on the value's
    Python type (``datetime``, ``int`` (with hint about the 2-arg form), proto
    ``Timestamp``, etc.).
  * ``timestamp.of(int, string) -> timestamp`` — epoch numeric + unit string.

Singular namespace (``timestamp.*``) mirrors CEL stdlib's ``optional.of(x)``
pattern. We can't extend stdlib ``timestamp(...)`` with a ``(dyn)`` overload
because ``(dyn)`` and ``(string)`` would overlap per the CEL signature-overlap
rule on conformant impls (cel-java/go/cpp); for cross-client parity Python
also uses the namespaced form even though celpy itself has no overlap check.
"""

import datetime
import typing
from datetime import datetime as Datetime
from datetime import timezone

import celpy
from celpy import celtypes

try:
    from google.protobuf.timestamp_pb2 import Timestamp as _ProtoTimestamp
except ImportError:  # pragma: no cover
    _ProtoTimestamp = None  # type: ignore[assignment]


_UNIT_MILLIS = "millis"
_UNIT_MICROS = "micros"
_UNIT_NANOS = "nanos"
_UNIT_SECONDS = "seconds"


def _from_epoch(value: int, unit: str) -> celtypes.TimestampType:
    """Construct from epoch numeric value plus unit string."""
    if unit == _UNIT_MILLIS:
        return celtypes.TimestampType(
            Datetime.fromtimestamp(value / 1_000, tz=timezone.utc))
    if unit == _UNIT_MICROS:
        return celtypes.TimestampType(
            Datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc))
    if unit == _UNIT_NANOS:
        # Datetime supports microsecond precision; nanos round down.
        return celtypes.TimestampType(
            Datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc))
    if unit == _UNIT_SECONDS:
        return celtypes.TimestampType(
            Datetime.fromtimestamp(value, tz=timezone.utc))
    raise celpy.CELEvalError(
        f"timestamp.of: unknown unit '{unit}'; expected one of "
        "millis, micros, nanos, seconds")


def _from_proto_timestamp(t: typing.Any) -> celtypes.TimestampType:
    """Decode a google.protobuf.Timestamp into a CEL TimestampType."""
    seconds = int(t.seconds)
    nanos = int(t.nanos)
    return celtypes.TimestampType(
        Datetime.fromtimestamp(seconds + nanos / 1_000_000_000, tz=timezone.utc))


def _timestamp_of(*args: typing.Any) -> celtypes.TimestampType:
    """Runtime dispatch backing {@code timestamp.of(...)}.

    Two arities:
      * {@code timestamp.of(dyn)} — accept whatever the format decoder produces.
      * {@code timestamp.of(int, string)} — epoch + unit.
    """
    if len(args) == 2:
        value = args[0]
        unit = args[1]
        if not isinstance(value, (int, celtypes.IntType)):
            raise celpy.CELEvalError(
                f"timestamp.of: epoch value must be int, got {type(value).__name__}")
        if not isinstance(unit, (str, celtypes.StringType)):
            raise celpy.CELEvalError(
                f"timestamp.of: unit must be string, got {type(unit).__name__}")
        return _from_epoch(int(value), str(unit))
    if len(args) != 1:
        raise celpy.CELEvalError(
            f"timestamp.of: expected 1 or 2 args, got {len(args)}")
    v = args[0]
    if v is None:
        raise celpy.CELEvalError("timestamp.of: cannot convert null to Timestamp")
    if isinstance(v, celtypes.TimestampType):
        return v
    if isinstance(v, Datetime):
        if v.tzinfo is None:
            # Avro local-timestamp-* logical types produce naive datetimes that
            # carry no timezone — refuse rather than silently picking UTC.
            raise celpy.CELEvalError(
                "timestamp.of: naive datetime (no timezone) cannot be converted. "
                "Use the regular timestamp-* logical type (UTC by spec), or pass "
                "an offset-adjusted epoch value via timestamp.of(value, unit).")
        return celtypes.TimestampType(v)
    if _ProtoTimestamp is not None and isinstance(v, _ProtoTimestamp):
        return _from_proto_timestamp(v)
    # Generic proto Timestamp duck-typing for DynamicMessage / alternate
    # generated bindings.
    if hasattr(v, "DESCRIPTOR") and getattr(v.DESCRIPTOR, "full_name", "") == \
            "google.protobuf.Timestamp":
        return _from_proto_timestamp(v)
    if isinstance(v, (str, celtypes.StringType)):
        # Delegate to celpy's stdlib timestamp parser for RFC 3339 strings.
        return celtypes.TimestampType(str(v))
    if isinstance(v, bool):
        raise celpy.CELEvalError("timestamp.of: cannot convert bool to Timestamp")
    if isinstance(v, (int, celtypes.IntType)):
        raise celpy.CELEvalError(
            "timestamp.of: raw int has no unit; use timestamp.of(value, "
            "\"millis\"|\"micros\"|\"nanos\"|\"seconds\") or set "
            "useLogicalTypeConverters=true on the Avro client so timestamp "
            "fields arrive as datetime")
    raise celpy.CELEvalError(
        f"timestamp.of: cannot convert {type(v).__name__} to Timestamp")


TIMESTAMP_FUNCS: typing.Dict[str, celpy.CELFunction] = {
    "timestamp.of": _timestamp_of,
}
