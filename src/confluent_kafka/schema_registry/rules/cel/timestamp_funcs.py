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

"""CEL bindings for the {@code timestamp} constructor.

celpy already provides a stdlib ``timestamp(string)`` (RFC 3339 parsing) plus
the standard timestamp operators (``<``, ``>``, ``==``, ``-``, ``+ duration``,
``.getDate()`` etc.). What we add, by overriding the ``timestamp`` name itself:

  * ``timestamp(int) -> timestamp`` — epoch **seconds**, the overload every
    other CEL implementation declares (cel-java's ``int64_to_timestamp``, plus
    Go/C++/C#). celpy's base ``timestamp`` is ``TimestampType`` itself, which
    accepts a ``datetime``, a ``str``, or an int *plus at least two more args*
    (datetime components) but rejects a lone int -- so without this a
    single-int call would error on Python only.
  * ``timestamp(dyn) -> timestamp`` — the shapes a format decoder produces
    that the base implementation doesn't handle: a proto ``Timestamp``, and a
    naive ``datetime`` (Avro ``local-timestamp-*``), which is refused rather
    than silently read at UTC.
  * ``timestamp(int, int) -> timestamp`` — an epoch value at a Flink-style
    decimal precision: 0 seconds, 3 millis, 6 micros, 9 nanos.

Every other form is forwarded to the base implementation verbatim, including
the *datetime components* form (``timestamp(2009, 2, 13)``), which needs three
or more args and so never collides with the two-arg precision form above.

There is no ``timestamp.of`` namespace any more: these are overloads of the standard
constructor in all seven clients.
"""

import datetime
import typing
from datetime import datetime as Datetime
from datetime import timedelta, timezone

import celpy
from celpy import celtypes

try:
    from google.protobuf.timestamp_pb2 import Timestamp as _ProtoTimestamp
except ImportError:  # pragma: no cover
    _ProtoTimestamp = None  # type: ignore[assignment]

# celpy's stdlib ``timestamp`` binding. Registering our own "timestamp" entry
# *replaces* it (``Activation.functions`` is a ChainMap where local functions
# shadow the base ones), so we capture the base callable here and delegate to it
# for every form it already handles.
try:
    from celpy.evaluation import base_functions as _base_functions

    _BASE_TIMESTAMP: typing.Callable[..., celtypes.TimestampType] = _base_functions.get(
        "timestamp", celtypes.TimestampType)
except ImportError:  # pragma: no cover
    _BASE_TIMESTAMP = celtypes.TimestampType


_PRECISION_SECONDS = 0
_PRECISION_MILLIS = 3
_PRECISION_MICROS = 6
_PRECISION_NANOS = 9

_EPOCH_UTC = Datetime(1970, 1, 1, tzinfo=timezone.utc)


def _from_epoch(value: int, precision: int) -> celtypes.TimestampType:
    """Construct from an epoch numeric value at a decimal precision.

    Splits the epoch value into whole microseconds using exact integer floor
    division (Python ``//`` matches Java ``Math.floorDiv``), then builds the
    datetime as ``_EPOCH_UTC + timedelta`` -- so the result floors toward
    negative infinity for both positive and negative epochs, and never loses
    precision to float rounding (the old ``Datetime.fromtimestamp(value / 1e9)``
    rounded half-to-even to the microsecond). ``datetime`` resolution is one
    microsecond, so nanos below that are floored away -- an inherent limit of
    the CEL timestamp type, matching Java, not a rounding discrepancy.

    Precisions outside {0, 3, 6, 9} are rejected rather than generalized to
    "any p means 10^-p": with the unit a number rather than a name, that check
    is the only thing between a typo and a silently wrong instant.
    """
    if precision == _PRECISION_SECONDS:
        micros = value * 1_000_000
    elif precision == _PRECISION_MILLIS:
        micros = value * 1_000
    elif precision == _PRECISION_MICROS:
        micros = value
    elif precision == _PRECISION_NANOS:
        micros = value // 1_000
    else:
        raise celpy.CELEvalError(
            f"timestamp: unknown precision {precision}; expected 0 (seconds), "
            "3 (millis), 6 (micros) or 9 (nanos)")
    return celtypes.TimestampType(_EPOCH_UTC + timedelta(microseconds=micros))


def _from_proto_timestamp(t: typing.Any) -> celtypes.TimestampType:
    """Decode a google.protobuf.Timestamp into a CEL TimestampType.

    Uses exact integer arithmetic: whole seconds plus the nanos field floored
    to microseconds (``nanos // 1000``), mirroring the Java reference rather
    than the float ``seconds + nanos / 1e9`` that lost precision.
    """
    seconds = int(t.seconds)
    nanos = int(t.nanos)
    return celtypes.TimestampType(
        _EPOCH_UTC + timedelta(seconds=seconds, microseconds=nanos // 1_000))


def _timestamp_one(v: typing.Any) -> celtypes.TimestampType:
    """The one-argument ``timestamp(dyn)`` dispatch."""
    if v is None:
        raise celpy.CELEvalError("timestamp: cannot convert null to Timestamp")
    # ``celtypes.BoolType`` subclasses ``int``, *not* ``bool`` (its MRO is
    # BoolType -> int -> object), so it has to be named explicitly here or a CEL
    # bool falls through to the epoch-seconds branch below and means epoch 1.
    if isinstance(v, (bool, celtypes.BoolType)):
        raise celpy.CELEvalError("timestamp: cannot convert bool to Timestamp")
    if isinstance(v, celtypes.TimestampType):
        return v
    if isinstance(v, Datetime):
        if v.tzinfo is None:
            # Avro local-timestamp-* logical types produce naive datetimes that
            # carry no timezone — refuse rather than silently picking UTC.
            raise celpy.CELEvalError(
                "timestamp: naive datetime (no timezone) cannot be converted. "
                "Use the regular timestamp-* logical type (UTC by spec), or pass "
                "an offset-adjusted epoch value via timestamp(value, precision).")
        return celtypes.TimestampType(v)
    if _ProtoTimestamp is not None and isinstance(v, _ProtoTimestamp):
        return _from_proto_timestamp(v)
    # Generic proto Timestamp duck-typing for DynamicMessage / alternate
    # generated bindings.
    if hasattr(v, "DESCRIPTOR") and getattr(v.DESCRIPTOR, "full_name", "") == \
            "google.protobuf.Timestamp":
        return _from_proto_timestamp(v)
    if isinstance(v, (int, celtypes.IntType)):
        # A bare int is epoch seconds, matching cel-java's int64_to_timestamp
        # and Go/C++/C#. Any other unit needs the two-arg precision form.
        try:
            return _from_epoch(int(v), _PRECISION_SECONDS)
        except (OverflowError, ValueError, OSError) as e:
            raise celpy.CELEvalError(
                f"timestamp: epoch seconds value out of range: {int(v)}") from e
    # str (lenient RFC 3339) and anything else the base implementation handles.
    return _BASE_TIMESTAMP(v)


def _timestamp(*args: typing.Any) -> celtypes.TimestampType:
    """CEL stdlib ``timestamp(...)`` plus the epoch-seconds, dyn and precision overloads.

    Three or more args is celpy's *datetime components* form
    (``timestamp(2009, 2, 13)``) and is forwarded to the base implementation
    verbatim; two args is the epoch + precision form; one arg dispatches on the
    value's Python type.
    """
    if len(args) == 2:
        value, precision = args
        # Bools before ints: BoolType subclasses int (see _timestamp_one).
        if isinstance(value, (bool, celtypes.BoolType)) \
                or not isinstance(value, (int, celtypes.IntType)):
            raise celpy.CELEvalError(
                f"timestamp: epoch value must be int, got {type(value).__name__}")
        if isinstance(precision, (bool, celtypes.BoolType)) \
                or not isinstance(precision, (int, celtypes.IntType)):
            raise celpy.CELEvalError(
                f"timestamp: precision must be int, got {type(precision).__name__}")
        return _from_epoch(int(value), int(precision))
    if len(args) == 1:
        return _timestamp_one(args[0])
    return _BASE_TIMESTAMP(*args)


TIMESTAMP_FUNCS: typing.Dict[str, celpy.CELFunction] = {
    "timestamp": _timestamp,
}
