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
"""Writes the result of a message-level ``CEL`` transform back into a protobuf message.

A ``CEL`` rule that returns a map is returning **the whole new message**: the transform has
replace semantics, not merge. The result map is therefore rebuilt into a fresh message, which
gives three behaviours that a rule author needs to know about and that every client has to
match:

* a field the rule does not name is **dropped** - a rule naming only the field it changes
  discards the rest;
* a ``null`` in the map **clears** its field;
* echoing a field that was absent **materialises** it, because reading it produced a value.
  Preserve absence with ``has(x) ? x : null``.

Without this the executor handed back a raw ``celpy`` map, which the protobuf serializer
cannot write - a decimal, a timestamp and a variant are messages in protobuf and celpy has no
rendering for any of them.

**Mechanism note.** The JVM client rebuilds by rendering the result to JSON and parsing it
back (``ProtobufResultWriter`` plus ``ProtobufSchema.fromJson``). This builds the message
directly against the descriptor instead: Python has no ``fromJson`` on the protobuf schema,
and going through JSON would mean base64-encoding every bytes field and formatting every
timestamp as RFC 3339 only for ``ParseDict`` to parse them straight back. Direct construction
is fewer conversions and fewer places to lose fidelity. The behaviours the JVM client gets
for free from the JSON mapping - null clearing a field, and a key matching either the
declared name or the JSON name - are reproduced explicitly below.
"""

import datetime
import decimal
import math
from typing import Any, Mapping, Optional

import celpy.celtypes as celtypes
from google.protobuf import descriptor, message

from confluent_kafka.schema_registry.common.protobuf import _is_repeated
from confluent_kafka.schema_registry.rules.cel.constraints import _WRAPPER_TYPES
from confluent_kafka.schema_registry.confluent.types.variant_utils import Variant
from confluent_kafka.schema_registry.confluent.types.decimal_utils import (
    unscaled_to_bytes,
)

__all__ = ["convert"]

_DECIMAL_TYPE_NAME = "confluent.type.Decimal"
_VARIANT_TYPE_NAME = "confluent.type.Variant"
_DURATION_TYPE_NAME = "google.protobuf.Duration"
_TIMESTAMP_TYPE_NAME = "google.protobuf.Timestamp"

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def convert(result: Any, msg: Any) -> Any:
    """Rebuild ``msg``'s type from a CEL result map, or return ``result`` unchanged.

    ``msg`` is the message the rule ran against; its concrete class is reused so the caller
    gets back the type it passed in rather than a dynamic message.
    """
    if not isinstance(result, Mapping) or not isinstance(msg, message.Message):
        return result
    out = type(msg)()
    _fill(out, result)
    return out


def _fill(out: message.Message, values: Mapping) -> None:
    desc = out.DESCRIPTOR
    for key, value in values.items():
        fd = _find_field(desc, str(key))
        if fd is None:
            # A key the schema does not declare has nowhere to go. Dropping it matches the
            # JVM client, whose JSON parse ignores unknown fields.
            continue
        if _is_null(value):
            # An explicit null clears the field, which is how a rule preserves an absent
            # value across a transform that echoes it.
            out.ClearField(fd.name)
            continue
        _set_field(out, fd, value)


def _find_field(desc: descriptor.Descriptor, name: str) -> Optional[descriptor.FieldDescriptor]:
    """Resolves a result key to a field by declared name, then by JSON name.

    A rule may legitimately return either, so matching only the declared name would silently
    skip a field like ``total_amount``.
    """
    fd = desc.fields_by_name.get(name)
    if fd is not None:
        return fd
    return desc.fields_by_camelcase_name.get(name)


def _is_null(value: Any) -> bool:
    return value is None or isinstance(value, celtypes.NullType)


def _set_field(out: message.Message, fd: descriptor.FieldDescriptor, value: Any) -> None:
    # protobuf >=7 dropped the instance .label attribute; the client's own helper covers
    # both runtimes.
    if _is_repeated(fd):
        if fd.message_type is not None and fd.message_type.GetOptions().map_entry:
            _set_map(out, fd, value)
            return
        _set_repeated(out, fd, value)
        return
    if fd.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
        _set_message(getattr(out, fd.name), fd, value)
        return
    setattr(out, fd.name, _scalar(fd, value))


def _set_map(out: message.Message, fd: descriptor.FieldDescriptor, value: Any) -> None:
    if not isinstance(value, Mapping):
        # Silence here dropped the field from the rebuilt message, turning a rule-authoring
        # type error into lost data. The JVM's message-level path rejects the same mismatch,
        # because it writes through a protobuf JSON parse.
        raise ValueError(
            f"cannot write {type(value).__name__} to map field '{fd.name}'")
    target = getattr(out, fd.name)
    value_fd = fd.message_type.fields_by_name["value"]
    for k, v in value.items():
        if _is_null(v):
            continue
        if value_fd.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
            _set_message(target[k], value_fd, v)
        else:
            target[k] = _scalar(value_fd, v)


def _set_repeated(out: message.Message, fd: descriptor.FieldDescriptor, value: Any) -> None:
    # A Mapping is iterable, so without naming it here a map result silently wrote the map's
    # *keys* as the list - corruption rather than loss. A scalar or a string wrote an empty
    # list. Both are rule-authoring type errors that the JVM's protobuf JSON parse rejects.
    if isinstance(value, (str, bytes, Mapping)) or not hasattr(value, "__iter__"):
        raise ValueError(
            f"cannot write {type(value).__name__} to repeated field '{fd.name}'")
    target = getattr(out, fd.name)
    del target[:]
    for item in value:
        if _is_null(item):
            # Dropping it changed the list's length and hid the mistake. protobuf JSON says
            # "Repeated field elements cannot be null in field: ..." and refuses the document.
            raise ValueError(f"cannot write null to repeated field '{fd.name}'")
        if fd.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
            _set_message(target.add(), fd, item)
        else:
            target.append(_scalar(fd, item))


def _set_message(target: message.Message, fd: descriptor.FieldDescriptor, value: Any) -> None:
    """Writes one message-valued field, inverting how the CEL binding read it.

    The three value types do not arrive as maps of their own fields once a rule has computed
    one: a decimal comes back as a Python ``Decimal``, a timestamp as a ``datetime`` and a
    variant as a ``Variant``. Echoed unchanged they arrive as the binding's own wrapper, which
    still holds the original message and can be copied outright.
    """
    full_name = fd.message_type.full_name

    # Echoed unchanged: the binding wrapper kept the message it read.
    inner = getattr(value, "msg", None)
    if isinstance(inner, message.Message):
        target.CopyFrom(inner)
        return
    if isinstance(value, message.Message):
        target.CopyFrom(value)
        return

    if full_name == _DECIMAL_TYPE_NAME and isinstance(value, decimal.Decimal):
        _set_decimal(target, value)
        return
    if full_name == _TIMESTAMP_TYPE_NAME and isinstance(value, datetime.datetime):
        _set_timestamp(target, value)
        return
    if full_name == _VARIANT_TYPE_NAME and isinstance(value, Variant):
        target.metadata = bytes(value.metadata)
        target.value = bytes(value.value)
        return

    # A wrapper, or a Duration. The CEL binding unwraps these on the way in - a StringValue
    # field is bound as a plain string, a Duration as a CEL duration (see
    # _MSG_TYPE_URL_TO_CTOR in constraints.py) - so the inverse has to put them back. Without
    # it an identity transform over such a field wrote an empty message and the value was
    # silently lost. The JVM gets this for free: its message-level write-back goes through
    # protobuf JSON, whose parser reads "hello" into a StringValue and "3s" into a Duration.
    if full_name in _WRAPPER_TYPES:
        _set_wrapper(target, value)
        return
    if full_name == _DURATION_TYPE_NAME and isinstance(value, datetime.timedelta):
        _set_duration(target, value)
        return

    # A nested message the rule rebuilt field by field.
    if isinstance(value, Mapping):
        _fill(target, value)
        return

    # A message the rule echoed unchanged rather than rebuilding.
    if isinstance(value, message.Message) and value.DESCRIPTOR.full_name == full_name:
        target.CopyFrom(value)
        return

    raise ValueError(
        f"cannot write {type(value).__name__} to {full_name} (field '{fd.name}')")


def _set_duration(target: message.Message, value: datetime.timedelta) -> None:
    """Sets a ``google.protobuf.Duration`` from a timedelta.

    Split by truncation toward zero, not by floor division: a Duration's ``seconds`` and
    ``nanos`` must carry the same sign, whereas timedelta normalises to a non-negative
    microseconds component (-3.5s is stored as days=-1, seconds=86396, microseconds=500000).
    """
    total_us = (value.days * 86400 + value.seconds) * 1_000_000 + value.microseconds
    sign = -1 if total_us < 0 else 1
    magnitude = abs(total_us)
    target.seconds = sign * (magnitude // 1_000_000)
    target.nanos = sign * (magnitude % 1_000_000) * 1000


def _set_wrapper(target: message.Message, value: Any) -> None:
    """Sets a wrapper's single ``value`` field from the scalar the rule returned.

    Narrowed by ``_scalar``, the same as a plain scalar field of that type. This arm had its
    own copy of the conversions, and the copy was the unguarded one: an Int32Value took 1.9 as
    1 and a BoolValue took the string "false" as *true*, while the identical plain fields
    refused both. The JVM makes no such distinction - JsonFormat's parseWrapperFieldValue
    hands the value to the same parseFieldValue a plain field goes through, so the wrapper's
    accept/reject set is identical. Measured against protobuf-java 4.35.1:

    * Int32Value  <- 1.9, 2147483648, true -> refused; 2.0 -> 2
    * BoolValue   <- 0                     -> "Invalid bool value: 0"
    * FloatValue  <- 1.0e40                -> "Out of range float value"
    * DoubleValue <- true                  -> "Not a double value: true"
    """
    target.value = _scalar(target.DESCRIPTOR.fields_by_name["value"], value)


def _set_decimal(target: message.Message, value: decimal.Decimal) -> None:
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("cannot write a non-finite decimal to " + _DECIMAL_TYPE_NAME)
    unscaled = int("".join(str(d) for d in digits) or "0")
    if sign:
        unscaled = -unscaled
    # Negated exponent, negative included - see set_decimal_message in common/protobuf.py.
    # Precision is the unscaled value's digit count, as Java's ProtobufResultWriter sets it
    # (`m.put("precision", dec.precision())`). Safe to set here because this writer never
    # rescales: len(digits) is exactly the digit count of the unscaled value being written,
    # so the MathContext the reader builds from it cannot round the value or shift its scale.
    target.value = unscaled_to_bytes(unscaled)
    target.precision = len(digits)
    target.scale = -exponent


def _set_timestamp(target: message.Message, value: datetime.datetime) -> None:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    delta = value - _EPOCH
    target.seconds = delta.days * 86400 + delta.seconds
    target.nanos = delta.microseconds * 1000


def _text(fd: descriptor.FieldDescriptor, value: Any) -> str:
    """``value`` as a string field's value, or a rule error."""
    if not isinstance(value, str):
        raise ValueError(
            f"cannot write {type(value).__name__} to string field '{fd.name}'")
    return str(value)


def _boolean(fd: descriptor.FieldDescriptor, value: Any) -> bool:
    """``value`` as a bool field's value, or a rule error.

    Truthiness is not the rule: it read the string "false" as true, and accepted the
    numbers protobuf JSON refuses.
    """
    if not isinstance(value, (bool, celtypes.BoolType)):
        raise ValueError(
            f"cannot write {type(value).__name__} to bool field '{fd.name}'")
    return bool(value)


# The widest float32 magnitude, with the 1e-6 slack JsonFormat.parseFloat allows. CEL has one
# floating type, so writing to a `float` field is a narrowing that can overflow; float(1e40)
# gave inf, where the JVM says "Out of range float value: 1.0e40".
_FLOAT32_LIMIT = 3.4028234663852886e38 * (1 + 1e-6)


def _is_finite(value: Any) -> bool:
    """Whether the value itself is finite. Only a float or a Decimal can be otherwise; a
    Python int always is, and ``math.isfinite`` would raise OverflowError on a wide one."""
    if isinstance(value, decimal.Decimal):
        return value.is_finite()
    if isinstance(value, float):
        return math.isfinite(value)
    return True


def _floating(fd: descriptor.FieldDescriptor, value: Any) -> float:
    """``value`` as a float field's value, or a rule error.

    A bool is an int subclass in Python and ``celtypes.BoolType`` subclasses int, so both
    spellings have to be named before the numeric check - the same reason ``_integral``
    names them.

    Overflow is judged on the *source*, not the result. ``float()`` saturates a finite but
    too-large value to an infinity, and the range check below reads that infinity as one the
    rule asked for and lets it through - so ``Decimal("1e1000")`` was silently written as inf
    to a double field, and a double field had no range check at all. A wide Python int is the
    same case reported differently: ``float(10**400)`` raises OverflowError, which escaped as
    a raw Python exception rather than a rule error.

    An *explicitly* non-finite value does pass, because protobuf JSON has canonical spellings
    for those and the JVM's parser takes them. Measured against protobuf-java 4.35.1:

    * double <- 1e308        -> 1.0E308
    * double <- 1e309, 1e1000, -1e1000  -> "Out of range double value"
    * double <- "Infinity", "-Infinity", "NaN"  -> accepted as-is
    * float  <- 1e39, 1e1000 -> "Out of range float value"
    * float  <- "Infinity", "NaN"  -> accepted as-is
    """
    if isinstance(value, (bool, celtypes.BoolType)):
        raise ValueError(f"cannot write bool to float field '{fd.name}'")
    if not isinstance(value, (int, float, decimal.Decimal)):
        raise ValueError(
            f"cannot write {type(value).__name__} to float field '{fd.name}'")
    source_is_finite = _is_finite(value)
    try:
        as_float = float(value)
    except OverflowError as e:
        raise ValueError(
            f"out of range value for float field '{fd.name}': {value}") from e
    if not source_is_finite:
        return as_float
    if not math.isfinite(as_float):
        raise ValueError(
            f"out of range value for float field '{fd.name}': {value}")
    if fd.type == descriptor.FieldDescriptor.TYPE_FLOAT and abs(as_float) > _FLOAT32_LIMIT:
        raise ValueError(
            f"out of range float value for field '{fd.name}': {as_float}")
    return as_float


def _scalar(fd: descriptor.FieldDescriptor, value: Any) -> Any:
    """Narrows a celpy value to what protobuf's setter accepts.

    A field takes a value of its own kind, and nothing else. Narrowing unconditionally
    accepted wrong-typed results and silently changed their meaning: ``bytes(5)``
    fabricated five NUL bytes out of a number, ``bool("false")`` wrote **true**, and
    ``float(True)`` wrote 1.0. A number and a bool are both writable to a *string* field
    with `str`, which is worse still - no error, and a rule-authoring mistake becomes data.

    The JVM's write-back renders the result map to JSON and parses it with protobuf's own
    JSON parser, so that parser's rejections are the contract, and this matches all of them
    (measured against protobuf-java 4.35.1):

    * bool  <- 0, "TRUE", ""    -> "Invalid bool value"
    * bytes <- 5, [97, 98]      -> refused
    * float <- true             -> "Not a double value: true"
    * int   <- 1.9, true        -> "Not an int32 value"

    That parser is also *lenient* in one direction, which this deliberately does not follow:
    it stringifies a number or a bool into a string field (1 -> "1"), reads the exact
    strings "true"/"false" as a bool, and reads a numeric string as a number. Those are
    artifacts of crossing a JSON transport, which this writer does not do - it builds
    against the descriptor (see the module note) - and every coercion of that kind turns a
    rule-authoring mistake into silently wrong data instead of an error. Refusing them is
    the cross-client contract; the JVM accepting a base64 *string* for a bytes field is the
    same artifact, so a CEL string is never reinterpreted as bytes either.

    An **enum** is the one exception, and not a coercion: a symbol name is protobuf JSON's
    canonical form for an enum and CEL has no enum type, so a string is the only way a rule
    can name a symbol. ``_integral`` passes it through for protobuf's own setter to resolve.
    """
    if fd.type == descriptor.FieldDescriptor.TYPE_BYTES:
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise ValueError(
                f"cannot write {type(value).__name__} to bytes field '{fd.name}'")
        return bytes(value)
    if fd.type == descriptor.FieldDescriptor.TYPE_STRING:
        return _text(fd, value)
    if fd.type == descriptor.FieldDescriptor.TYPE_BOOL:
        return _boolean(fd, value)
    if fd.type in (
        descriptor.FieldDescriptor.TYPE_FLOAT,
        descriptor.FieldDescriptor.TYPE_DOUBLE,
    ):
        return _floating(fd, value)
    # Integer-valued fields (and enums, which take an int). int() silently truncated, so a CEL
    # double of 1.9 landed as 1 and a fractional Decimal lost its fraction. The JVM's
    # message-level write-back goes through a protobuf JSON parse, which refuses a non-integral
    # value ("Not an int32 value: 1.9") while accepting an integral one (2.0 -> 2), and
    # range-checks the result. Measured against protobuf-java 4.35.1.
    return _integral(fd, value)


# Inclusive value ranges for protobuf's integer scalar types, which its JSON parser enforces.
_INT_RANGES = {
    descriptor.FieldDescriptor.TYPE_INT32: (-(2**31), 2**31 - 1),
    descriptor.FieldDescriptor.TYPE_SINT32: (-(2**31), 2**31 - 1),
    descriptor.FieldDescriptor.TYPE_SFIXED32: (-(2**31), 2**31 - 1),
    descriptor.FieldDescriptor.TYPE_UINT32: (0, 2**32 - 1),
    descriptor.FieldDescriptor.TYPE_FIXED32: (0, 2**32 - 1),
    descriptor.FieldDescriptor.TYPE_INT64: (-(2**63), 2**63 - 1),
    descriptor.FieldDescriptor.TYPE_SINT64: (-(2**63), 2**63 - 1),
    descriptor.FieldDescriptor.TYPE_SFIXED64: (-(2**63), 2**63 - 1),
    descriptor.FieldDescriptor.TYPE_UINT64: (0, 2**64 - 1),
    descriptor.FieldDescriptor.TYPE_FIXED64: (0, 2**64 - 1),
    descriptor.FieldDescriptor.TYPE_ENUM: (-(2**31), 2**31 - 1),
}


# Digits in the widest protobuf integer, 2**64-1. A value whose leading digit sits past this
# cannot fit any of the ranges below, so its magnitude settles the question before the digits
# are ever built.
_MAX_INT_DIGITS = 20


def _integral(fd: descriptor.FieldDescriptor, value: Any) -> Any:
    """``value`` as an int for an integer-valued field, or a rule error.

    A fractional value is a rule-authoring mistake rather than something to round: the JVM
    rejects it, and truncating would write a different number than the rule computed. An
    integral float or Decimal is accepted, as protobuf JSON accepts ``2.0`` for an int32.
    """
    if isinstance(value, (bool, celtypes.BoolType)):
        # Both spellings: bool is an int subclass in Python, and celtypes.BoolType subclasses
        # int rather than bool - so naming only `bool` caught the case CEL never produces and
        # let a real CEL `true` through as 1. protobuf JSON refuses true for an integer field.
        raise ValueError(f"cannot write bool to integer field '{fd.name}'")
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            raise ValueError(
                f"cannot write non-finite {value} to integer field '{fd.name}'")
        # Magnitude first, before int() materialises the digits. The widest protobuf integer
        # is 2**64-1, twenty digits, so anything with a larger adjusted exponent is out of
        # range for every one of them - and int(Decimal("1e100000000")) would spend minutes
        # building a hundred million digits just to reach that same rejection, which the JVM
        # reports off the token without building the number.
        if value.adjusted() > _MAX_INT_DIGITS - 1:
            raise ValueError(
                f"value {value} is out of range for field '{fd.name}'")
        if value != value.to_integral_value():
            raise ValueError(
                f"cannot write non-integral {value} to integer field '{fd.name}'")
        as_int = int(value)
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(
                f"cannot write non-integral {value!r} to integer field '{fd.name}'")
        as_int = int(value)
    elif isinstance(value, int):
        as_int = int(value)
    else:
        # Not a number at all - left for protobuf's own setter to reject, which names the
        # field and the offending type.
        return value

    bounds = _INT_RANGES.get(fd.type)
    if bounds is not None and not (bounds[0] <= as_int <= bounds[1]):
        raise ValueError(
            f"value {as_int} is out of range for field '{fd.name}'")
    return as_int
