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
from typing import Any, Mapping, Optional

import celpy.celtypes as celtypes
from google.protobuf import descriptor, message

from confluent_kafka.schema_registry.common.protobuf import _is_repeated
from confluent_kafka.schema_registry.confluent.types.variant_utils import Variant
from confluent_kafka.schema_registry.confluent.types.decimal_utils import (
    unscaled_to_bytes,
)

__all__ = ["convert"]

_DECIMAL_TYPE_NAME = "confluent.type.Decimal"
_VARIANT_TYPE_NAME = "confluent.type.Variant"
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
        return
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
    if isinstance(value, (str, bytes)) or not hasattr(value, "__iter__"):
        return
    target = getattr(out, fd.name)
    del target[:]
    for item in value:
        if _is_null(item):
            continue
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

    # A nested message the rule rebuilt field by field.
    if isinstance(value, Mapping):
        _fill(target, value)


def _set_decimal(target: message.Message, value: decimal.Decimal) -> None:
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("cannot write a non-finite decimal to " + _DECIMAL_TYPE_NAME)
    unscaled = int("".join(str(d) for d in digits) or "0")
    if sign:
        unscaled = -unscaled
    # Negated exponent, negative included - see set_decimal_message in common/protobuf.py.
    target.value = unscaled_to_bytes(unscaled)
    target.scale = -exponent


def _set_timestamp(target: message.Message, value: datetime.datetime) -> None:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    delta = value - _EPOCH
    target.seconds = delta.days * 86400 + delta.seconds
    target.nanos = delta.microseconds * 1000


def _scalar(fd: descriptor.FieldDescriptor, value: Any) -> Any:
    """Narrows a celpy value to what protobuf's setter accepts."""
    if fd.type == descriptor.FieldDescriptor.TYPE_BYTES:
        return bytes(value)
    if fd.type == descriptor.FieldDescriptor.TYPE_STRING:
        return str(value)
    if fd.type == descriptor.FieldDescriptor.TYPE_BOOL:
        return bool(value)
    if fd.type in (
        descriptor.FieldDescriptor.TYPE_FLOAT,
        descriptor.FieldDescriptor.TYPE_DOUBLE,
    ):
        return float(value)
    if fd.type == descriptor.FieldDescriptor.TYPE_ENUM:
        return int(value)
    if isinstance(value, decimal.Decimal):
        return int(value)
    return int(value) if isinstance(value, (int, float)) else value
