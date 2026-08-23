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

"""Codec for the Spark/Parquet Variant binary type (a metadata key-dictionary plus a
self-describing value stream) - the Python counterpart of Java's
``io.confluent.kafka.schemaregistry.type`` ``Variant`` / ``VariantFormat`` / ``VariantUtils``.

The binary decode/encode is ported from Apache Spark's ``pyspark.sql.variant_utils`` (which
itself derives from ``org.apache.spark.types.variant.VariantUtil``) and extended with the
Parquet Variant additions Spark lacks: ``TIME`` (17), ``TIMESTAMP_NANOS`` tz/ntz (18/19), and
``UUID`` (20).

Two behaviors deliberately match the Confluent Java reference rather than Spark:

* ``to_json`` renders temporal types as ISO-8601 with ``T``/``Z`` and the seconds field
  always present (the cross-language contract), not Python ``str()``.
* ``parse_json`` follows Java ``VariantUtils.fromJsonNode`` number handling - a JSON
  fractional number becomes a ``DOUBLE`` (never a decimal), matching a default Jackson
  ``ObjectMapper``; only integers wider than 64 bits fall back to a scale-0 decimal.
"""

import base64
import datetime
import decimal
import json
import struct
import uuid as uuid_mod
from enum import Enum
from typing import Any, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Format constants (see VariantFormat.java).
# ---------------------------------------------------------------------------

BASIC_TYPE_BITS = 2
BASIC_TYPE_MASK = 0x3
TYPE_INFO_MASK = 0x3F
MAX_SHORT_STR_SIZE = 0x3F

# Exact/unbounded context so scaling an unscaled value with >28 significant digits
# is not silently rounded by the thread-local default context (prec=28) — matches
# java.math.BigDecimal's exact scaleb/setScale semantics.
_EXACT_CONTEXT = decimal.Context(
    prec=decimal.MAX_PREC, Emax=decimal.MAX_EMAX, Emin=decimal.MIN_EMIN)

# Basic types (low 2 bits of the header byte).
PRIMITIVE = 0
SHORT_STR = 1
OBJECT = 2
ARRAY = 3

# Primitive type codes (upper 6 bits of the header byte when basic type == PRIMITIVE).
NULL = 0
TRUE = 1
FALSE = 2
INT1 = 3
INT2 = 4
INT4 = 5
INT8 = 6
DOUBLE = 7
DECIMAL4 = 8
DECIMAL8 = 9
DECIMAL16 = 10
DATE = 11
TIMESTAMP = 12
TIMESTAMP_NTZ = 13
FLOAT = 14
BINARY = 15
LONG_STR = 16
TIME = 17
TIMESTAMP_NANOS = 18
TIMESTAMP_NANOS_NTZ = 19
UUID = 20

VERSION = 1
VERSION_MASK = 0x0F

U8_MAX = 0xFF
U16_MAX = 0xFFFF
U24_MAX = 0xFFFFFF
U24_SIZE = 3
U32_SIZE = 4

I8_MAX = 0x7F
I8_MIN = -0x80
I16_MAX = 0x7FFF
I16_MIN = -0x8000
I32_MAX = 0x7FFFFFFF
I32_MIN = -0x80000000
I64_MAX = 0x7FFFFFFFFFFFFFFF
I64_MIN = -0x8000000000000000

UUID_SIZE = 16

MAX_DECIMAL4_PRECISION = 9
MAX_DECIMAL4_VALUE = 10 ** MAX_DECIMAL4_PRECISION
MAX_DECIMAL8_PRECISION = 18
MAX_DECIMAL8_VALUE = 10 ** MAX_DECIMAL8_PRECISION
MAX_DECIMAL16_PRECISION = 38
MAX_DECIMAL16_VALUE = 10 ** MAX_DECIMAL16_PRECISION

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_NAIVE = datetime.datetime(1970, 1, 1)


class VariantError(ValueError):
    """Raised for a malformed or unsupported Variant binary value."""


class VariantType(Enum):
    """The value type of a Variant, mirroring Java ``Variant.Type``.

    Integer, decimal, and timestamp widths are kept distinct here (as in Java); the CEL
    layer collapses them into coarse labels (int/decimal/timestamp) where appropriate.
    """

    OBJECT = "OBJECT"
    ARRAY = "ARRAY"
    NULL = "NULL"
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    STRING = "STRING"
    DOUBLE = "DOUBLE"
    DECIMAL4 = "DECIMAL4"
    DECIMAL8 = "DECIMAL8"
    DECIMAL16 = "DECIMAL16"
    DATE = "DATE"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    FLOAT = "FLOAT"
    BINARY = "BINARY"
    TIME = "TIME"
    TIMESTAMP_NANOS_TZ = "TIMESTAMP_NANOS_TZ"
    TIMESTAMP_NANOS_NTZ = "TIMESTAMP_NANOS_NTZ"
    UUID = "UUID"


# ---------------------------------------------------------------------------
# Low-level byte helpers.
# ---------------------------------------------------------------------------

def _check_index(pos: int, length: int) -> None:
    if pos < 0 or pos >= length:
        raise VariantError("malformed variant: index out of bounds")


def _read_long(data: bytes, pos: int, num_bytes: int, signed: bool) -> int:
    _check_index(pos, len(data))
    _check_index(pos + num_bytes - 1, len(data))
    return int.from_bytes(data[pos:pos + num_bytes], byteorder="little", signed=signed)


def _get_type_info(value: bytes, pos: int) -> Tuple[int, int]:
    basic_type = value[pos] & BASIC_TYPE_MASK
    type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
    return basic_type, type_info


def _get_metadata_key(metadata: bytes, key_id: int) -> str:
    _check_index(0, len(metadata))
    offset_size = ((metadata[0] >> 6) & 0x3) + 1
    dict_size = _read_long(metadata, 1, offset_size, signed=False)
    if key_id >= dict_size:
        raise VariantError("malformed variant: field id out of range")
    string_start = 1 + (dict_size + 2) * offset_size
    offset = _read_long(metadata, 1 + (key_id + 1) * offset_size, offset_size, signed=False)
    next_offset = _read_long(metadata, 1 + (key_id + 2) * offset_size, offset_size, signed=False)
    if offset > next_offset:
        raise VariantError("malformed variant: non-monotonic metadata offsets")
    _check_index(string_start + next_offset - 1, len(metadata))
    return metadata[string_start + offset:string_start + next_offset].decode("utf-8")


# ---------------------------------------------------------------------------
# Cross-language JSON temporal contract.
#
# ISO-8601 with a 0/3/6/9-digit fractional-second grouping (as in Java Instant.toString())
# and the seconds field ALWAYS present. UTC instants append 'Z'; NTZ/time forms omit the
# zone. This intentionally deviates from Java LocalDateTime/LocalTime.toString() (which omit
# the seconds field when both seconds and fraction are zero); the Java reference is aligned
# to always emit seconds so NTZ stays consistent with the TZ form.
# ---------------------------------------------------------------------------

def _frac_nanos(nanos: int) -> str:
    """Fractional-second suffix using Java's 0/3/6/9-digit grouping (empty if zero)."""
    if nanos == 0:
        return ""
    if nanos % 1_000_000 == 0:
        return ".%03d" % (nanos // 1_000_000)
    if nanos % 1_000 == 0:
        return ".%06d" % (nanos // 1_000)
    return ".%09d" % nanos


def _ymd_hms(total_nanos: int, tz: Optional[datetime.timezone]) -> Tuple[datetime.datetime, int]:
    """Split epoch-nanos into a (whole-second datetime, nano-of-second). Uses floor
    semantics so negative instants match Java's Math.floorDiv/floorMod."""
    epoch_sec, nano = divmod(total_nanos, 1_000_000_000)  # Python divmod floors, like Java
    base = _EPOCH_UTC if tz is not None else _EPOCH_NAIVE
    return base + datetime.timedelta(seconds=epoch_sec), nano


def _format_instant(total_nanos: int) -> str:
    """ISO-8601 instant with 'Z', seconds always present - matches Instant.toString()."""
    dt, nano = _ymd_hms(total_nanos, datetime.timezone.utc)
    return "%04d-%02d-%02dT%02d:%02d:%02d%sZ" % (
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, _frac_nanos(nano))


def _format_local_datetime(total_nanos: int) -> str:
    """ISO local date-time, seconds always present. This is the cross-language contract: it
    deviates from Java LocalDateTime.toString() (which omits the seconds field when both
    seconds and fraction are zero) - the Java reference is aligned to always emit seconds,
    keeping NTZ consistent with the TZ (Instant) form."""
    dt, nano = _ymd_hms(total_nanos, None)
    return "%04d-%02d-%02dT%02d:%02d:%02d%s" % (
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, _frac_nanos(nano))


def _format_local_time(micros: int) -> str:
    """ISO local time, seconds always present (see :func:`_format_local_datetime`)."""
    nano_of_day = micros * 1000
    seconds, nano = divmod(nano_of_day, 1_000_000_000)
    hour, rem = divmod(seconds, 3600)
    minute, second = divmod(rem, 60)
    return "%02d:%02d:%02d%s" % (hour, minute, second, _frac_nanos(nano))


def _format_double(d: float) -> str:
    """A JSON number rendering of a double. Non-finite values render as the BAREWORD tokens
    ``NaN``/``Infinity``/``-Infinity`` (the Confluent Java contract, diverging from Spark
    which quotes them). Integral values render as ``N.0``; other values use Python's shortest
    round-tripping ``repr``. (Java Double.toString scientific-notation edge cases for very
    large/small magnitudes are a known minor divergence.)"""
    if d != d:
        return "NaN"
    if d == float("inf"):
        return "Infinity"
    if d == float("-inf"):
        return "-Infinity"
    if d.is_integer() and abs(d) < 1e16:
        return "%d.0" % int(d)
    return repr(d)


def _format_float(f: float) -> str:
    """A JSON number rendering of a 32-bit float. Emits the shortest decimal that round-trips
    to the SAME float32 (matching Java ``Float.toString`` / Apache Arrow) rather than the
    f64-shortest string produced by widening then formatting as a double. Integral values
    render as ``N.0``; mirrors :func:`_format_double`'s non-finite handling (bareword
    ``NaN``/``Infinity``/``-Infinity``)."""
    if f != f:
        return "NaN"
    if f == float("inf"):
        return "Infinity"
    if f == float("-inf"):
        return "-Infinity"
    if f == int(f) and abs(f) < 1e16:
        return "%d.0" % int(f)
    for p in range(1, 10):
        s = "%.*g" % (p, f)
        if struct.unpack("<f", struct.pack("<f", float(s)))[0] == f:
            return repr(float(s))
    return repr(f)


# ---------------------------------------------------------------------------
# Variant reader.
# ---------------------------------------------------------------------------

class Variant:
    """A read-only view over a Variant (value + metadata) at a byte position. Navigation
    (``get_field_by_key`` / ``get_element_at_index``) returns a sub-``Variant`` sharing the
    same buffers, so nothing is copied. Mirrors Java ``io.confluent...type.Variant``.
    """

    _BINARY_SEARCH_THRESHOLD = 32

    def __init__(self, value: bytes, metadata: bytes, pos: int = 0):
        self.value = bytes(value)
        self.metadata = bytes(metadata)
        self.pos = pos
        _check_index(0, len(self.metadata))
        if (self.metadata[0] & VERSION_MASK) != VERSION:
            raise VariantError(
                "unsupported variant metadata version: %d" % (self.metadata[0] & VERSION_MASK))

    # -- type ---------------------------------------------------------------

    def get_type(self) -> VariantType:
        _check_index(self.pos, len(self.value))
        basic_type, type_info = _get_type_info(self.value, self.pos)
        if basic_type == SHORT_STR:
            return VariantType.STRING
        if basic_type == OBJECT:
            return VariantType.OBJECT
        if basic_type == ARRAY:
            return VariantType.ARRAY
        mapping = {
            NULL: VariantType.NULL,
            TRUE: VariantType.BOOLEAN,
            FALSE: VariantType.BOOLEAN,
            INT1: VariantType.BYTE,
            INT2: VariantType.SHORT,
            INT4: VariantType.INT,
            INT8: VariantType.LONG,
            DOUBLE: VariantType.DOUBLE,
            DECIMAL4: VariantType.DECIMAL4,
            DECIMAL8: VariantType.DECIMAL8,
            DECIMAL16: VariantType.DECIMAL16,
            DATE: VariantType.DATE,
            TIMESTAMP: VariantType.TIMESTAMP_TZ,
            TIMESTAMP_NTZ: VariantType.TIMESTAMP_NTZ,
            FLOAT: VariantType.FLOAT,
            BINARY: VariantType.BINARY,
            LONG_STR: VariantType.STRING,
            TIME: VariantType.TIME,
            TIMESTAMP_NANOS: VariantType.TIMESTAMP_NANOS_TZ,
            TIMESTAMP_NANOS_NTZ: VariantType.TIMESTAMP_NANOS_NTZ,
            UUID: VariantType.UUID,
        }
        result = mapping.get(type_info)
        if result is None:
            raise VariantError("unknown variant primitive type: %d" % type_info)
        return result

    # -- scalar getters -----------------------------------------------------

    def _primitive_info(self) -> Tuple[int, int]:
        _check_index(self.pos, len(self.value))
        basic_type, type_info = _get_type_info(self.value, self.pos)
        if basic_type != PRIMITIVE:
            raise VariantError("expected a primitive variant value")
        return basic_type, type_info

    def get_boolean(self) -> bool:
        _, type_info = self._primitive_info()
        if type_info not in (TRUE, FALSE):
            raise VariantError("variant is not a boolean")
        return type_info == TRUE

    def get_byte(self) -> int:
        """8-bit integer (``INT1`` only) - mirrors Java ``getByte``. Wider integer widths
        raise; use :meth:`get_short`/:meth:`get_int`/:meth:`get_long` for those."""
        _, type_info = self._primitive_info()
        if type_info == INT1:
            return _read_long(self.value, self.pos + 1, 1, signed=True)
        raise VariantError("variant is not a byte-width integer")

    def get_short(self) -> int:
        """16-bit integer, widening from ``INT1`` (byte) - mirrors Java ``getShort``."""
        _, type_info = self._primitive_info()
        if type_info == INT1:
            return _read_long(self.value, self.pos + 1, 1, signed=True)
        if type_info == INT2:
            return _read_long(self.value, self.pos + 1, 2, signed=True)
        raise VariantError("variant is not a short-width integer")

    def get_int(self) -> int:
        """32-bit integer, widening from ``INT1``/``INT2`` - mirrors Java ``getInt``."""
        _, type_info = self._primitive_info()
        if type_info == INT1:
            return _read_long(self.value, self.pos + 1, 1, signed=True)
        if type_info == INT2:
            return _read_long(self.value, self.pos + 1, 2, signed=True)
        if type_info == INT4:
            return _read_long(self.value, self.pos + 1, 4, signed=True)
        raise VariantError("variant is not an int-width integer")

    def get_long(self) -> int:
        """Raw integer for any integer-backed type (byte/short/int/long, date days,
        timestamp micros, time micros, timestamp-nanos) - mirrors Java ``getLong``."""
        _, type_info = self._primitive_info()
        if type_info == INT1:
            return _read_long(self.value, self.pos + 1, 1, signed=True)
        if type_info == INT2:
            return _read_long(self.value, self.pos + 1, 2, signed=True)
        if type_info in (INT4, DATE):
            return _read_long(self.value, self.pos + 1, 4, signed=True)
        if type_info in (INT8, TIMESTAMP, TIMESTAMP_NTZ, TIME,
                         TIMESTAMP_NANOS, TIMESTAMP_NANOS_NTZ):
            return _read_long(self.value, self.pos + 1, 8, signed=True)
        raise VariantError("variant is not an integer-backed type")

    def get_float(self) -> float:
        """32-bit float (``FLOAT`` only, exact) - mirrors Java ``getFloat``. Note the
        returned Python ``float`` is 64-bit, but the value is decoded from 4 bytes."""
        _, type_info = self._primitive_info()
        if type_info == FLOAT:
            _check_index(self.pos + 4, len(self.value))
            return struct.unpack("<f", self.value[self.pos + 1:self.pos + 5])[0]
        raise VariantError("variant is not a float")

    def get_double(self) -> float:
        """64-bit double (``DOUBLE`` only, exact) - mirrors Java ``getDouble``. Does not
        widen a ``FLOAT``; use :meth:`get_float` for that."""
        _, type_info = self._primitive_info()
        if type_info == DOUBLE:
            _check_index(self.pos + 8, len(self.value))
            return struct.unpack("<d", self.value[self.pos + 1:self.pos + 9])[0]
        raise VariantError("variant is not a double")

    def get_decimal(self) -> decimal.Decimal:
        _, type_info = self._primitive_info()
        scale = self.value[self.pos + 1]
        if type_info == DECIMAL4:
            unscaled = _read_long(self.value, self.pos + 2, 4, signed=True)
            _check_decimal(unscaled, scale, MAX_DECIMAL4_VALUE, MAX_DECIMAL4_PRECISION)
        elif type_info == DECIMAL8:
            unscaled = _read_long(self.value, self.pos + 2, 8, signed=True)
            _check_decimal(unscaled, scale, MAX_DECIMAL8_VALUE, MAX_DECIMAL8_PRECISION)
        elif type_info == DECIMAL16:
            _check_index(self.pos + 17, len(self.value))
            unscaled = int.from_bytes(
                self.value[self.pos + 2:self.pos + 18], byteorder="little", signed=True)
            _check_decimal(unscaled, scale, MAX_DECIMAL16_VALUE, MAX_DECIMAL16_PRECISION)
        else:
            raise VariantError("variant is not a decimal")
        return decimal.Decimal(unscaled).scaleb(-scale, context=_EXACT_CONTEXT)

    def get_binary(self) -> bytes:
        _, type_info = self._primitive_info()
        if type_info != BINARY:
            raise VariantError("variant is not binary")
        length = _read_long(self.value, self.pos + 1, U32_SIZE, signed=False)
        start = self.pos + 1 + U32_SIZE
        _check_index(start + length - 1, len(self.value))
        return bytes(self.value[start:start + length])

    def get_uuid(self) -> uuid_mod.UUID:
        _, type_info = self._primitive_info()
        if type_info != UUID:
            raise VariantError("variant is not a uuid")
        start = self.pos + 1
        _check_index(start + UUID_SIZE - 1, len(self.value))
        return uuid_mod.UUID(bytes=bytes(self.value[start:start + UUID_SIZE]))  # big-endian

    def get_string(self) -> str:
        _check_index(self.pos, len(self.value))
        basic_type, type_info = _get_type_info(self.value, self.pos)
        if basic_type == SHORT_STR:
            start = self.pos + 1
            length = type_info
        elif basic_type == PRIMITIVE and type_info == LONG_STR:
            length = _read_long(self.value, self.pos + 1, U32_SIZE, signed=False)
            start = self.pos + 1 + U32_SIZE
        else:
            raise VariantError("variant is not a string")
        _check_index(start + length - 1, len(self.value))
        return self.value[start:start + length].decode("utf-8")

    # -- object / array navigation -----------------------------------------

    def _object_info(self) -> Tuple[int, int, int, int, int, int]:
        _check_index(self.pos, len(self.value))
        basic_type, type_info = _get_type_info(self.value, self.pos)
        if basic_type != OBJECT:
            raise VariantError("variant is not an object")
        large_size = ((type_info >> 4) & 0x1) != 0
        size_bytes = U32_SIZE if large_size else 1
        num_fields = _read_long(self.value, self.pos + 1, size_bytes, signed=False)
        id_size = ((type_info >> 2) & 0x3) + 1
        offset_size = (type_info & 0x3) + 1
        id_start = self.pos + 1 + size_bytes
        offset_start = id_start + num_fields * id_size
        data_start = offset_start + (num_fields + 1) * offset_size
        return num_fields, id_size, offset_size, id_start, offset_start, data_start

    def _array_info(self) -> Tuple[int, int, int, int]:
        _check_index(self.pos, len(self.value))
        basic_type, type_info = _get_type_info(self.value, self.pos)
        if basic_type != ARRAY:
            raise VariantError("variant is not an array")
        large_size = ((type_info >> 2) & 0x1) != 0
        size_bytes = U32_SIZE if large_size else 1
        num_fields = _read_long(self.value, self.pos + 1, size_bytes, signed=False)
        offset_size = (type_info & 0x3) + 1
        offset_start = self.pos + 1 + size_bytes
        data_start = offset_start + (num_fields + 1) * offset_size
        return num_fields, offset_size, offset_start, data_start

    def num_object_fields(self) -> int:
        return self._object_info()[0]

    def num_array_elements(self) -> int:
        return self._array_info()[0]

    def _field_id_and_offset(self, idx: int) -> Tuple[int, int]:
        num_fields, id_size, offset_size, id_start, offset_start, data_start = self._object_info()
        key_id = _read_long(self.value, id_start + id_size * idx, id_size, signed=False)
        offset = _read_long(self.value, offset_start + offset_size * idx, offset_size, signed=False)
        return key_id, data_start + offset

    def get_field_by_key(self, key: str) -> Optional["Variant"]:
        """Returns the object field with the given key, or ``None`` if absent. Linear scan
        for small objects, binary search past the threshold (fields are key-sorted)."""
        num_fields, id_size, offset_size, id_start, offset_start, data_start = self._object_info()
        if num_fields < self._BINARY_SEARCH_THRESHOLD:
            for i in range(num_fields):
                key_id = _read_long(self.value, id_start + id_size * i, id_size, signed=False)
                if _get_metadata_key(self.metadata, key_id) == key:
                    offset = _read_long(
                        self.value, offset_start + offset_size * i, offset_size, signed=False)
                    return Variant(self.value, self.metadata, data_start + offset)
            return None
        low, high = 0, num_fields - 1
        while low <= high:
            mid = (low + high) >> 1
            mid_id = _read_long(self.value, id_start + id_size * mid, id_size, signed=False)
            mid_key = _get_metadata_key(self.metadata, mid_id)
            if mid_key < key:
                low = mid + 1
            elif mid_key > key:
                high = mid - 1
            else:
                offset = _read_long(
                    self.value, offset_start + offset_size * mid, offset_size, signed=False)
                return Variant(self.value, self.metadata, data_start + offset)
        return None

    def get_field_at_index(self, idx: int) -> Tuple[str, "Variant"]:
        """Returns the (key, value) of the field at ``idx`` (fields are key-sorted)."""
        key_id, value_pos = self._field_id_and_offset(idx)
        return _get_metadata_key(self.metadata, key_id), Variant(
            self.value, self.metadata, value_pos)

    def get_element_at_index(self, index: int) -> Optional["Variant"]:
        """Returns the array element at ``index``, or ``None`` if out of bounds."""
        num_fields, offset_size, offset_start, data_start = self._array_info()
        if index < 0 or index >= num_fields:
            return None
        offset = _read_long(
            self.value, offset_start + offset_size * index, offset_size, signed=False)
        return Variant(self.value, self.metadata, data_start + offset)

    # -- JSON ---------------------------------------------------------------

    def to_json(self) -> str:
        """Serialize to a JSON string, matching Java ``VariantUtils.toJsonString``."""
        t = self.get_type()
        if t == VariantType.OBJECT:
            parts = []
            for i in range(self.num_object_fields()):
                key, child = self.get_field_at_index(i)
                parts.append(json.dumps(key, ensure_ascii=False) + ":" + child.to_json())
            return "{" + ",".join(parts) + "}"
        if t == VariantType.ARRAY:
            parts = [self.get_element_at_index(i).to_json()
                     for i in range(self.num_array_elements())]
            return "[" + ",".join(parts) + "]"
        if t == VariantType.NULL:
            return "null"
        if t == VariantType.BOOLEAN:
            return "true" if self.get_boolean() else "false"
        if t == VariantType.STRING:
            return json.dumps(self.get_string(), ensure_ascii=False)
        if t in (VariantType.BYTE, VariantType.SHORT, VariantType.INT, VariantType.LONG):
            return str(self.get_long())
        if t == VariantType.FLOAT:
            return _format_float(self.get_float())
        if t == VariantType.DOUBLE:
            return _format_double(self.get_double())
        if t in (VariantType.DECIMAL4, VariantType.DECIMAL8, VariantType.DECIMAL16):
            # Fixed-point (never scientific), matching Java's toPlainString contract.
            return format(self.get_decimal(), "f")
        if t == VariantType.DATE:
            return '"' + (_EPOCH_DATE + datetime.timedelta(days=self.get_long())).isoformat() + '"'
        if t == VariantType.TIMESTAMP_TZ:
            return '"' + _format_instant(self.get_long() * 1000) + '"'
        if t == VariantType.TIMESTAMP_NTZ:
            return '"' + _format_local_datetime(self.get_long() * 1000) + '"'
        if t == VariantType.TIMESTAMP_NANOS_TZ:
            return '"' + _format_instant(self.get_long()) + '"'
        if t == VariantType.TIMESTAMP_NANOS_NTZ:
            return '"' + _format_local_datetime(self.get_long()) + '"'
        if t == VariantType.TIME:
            return '"' + _format_local_time(self.get_long()) + '"'
        if t == VariantType.BINARY:
            return '"' + base64.b64encode(self.get_binary()).decode("ascii") + '"'
        if t == VariantType.UUID:
            return '"' + str(self.get_uuid()) + '"'
        raise VariantError("unsupported variant type for JSON: %s" % t)


def _check_decimal(unscaled: int, scale: int, max_unscaled: int, max_scale: int) -> None:
    if unscaled >= max_unscaled or unscaled <= -max_unscaled or scale > max_scale:
        raise VariantError("malformed variant: decimal out of range")


# ---------------------------------------------------------------------------
# Module-level convenience API.
# ---------------------------------------------------------------------------

def from_bytes(value: bytes, metadata: bytes) -> Variant:
    """Construct a Variant from its raw ``value`` + ``metadata`` byte strings."""
    return Variant(value, metadata)


def to_json_string(variant: Variant) -> str:
    """Serialize a Variant to its JSON string form."""
    return variant.to_json()


def parse_json(json_str: str) -> Variant:
    """Parse a JSON string into a Variant, matching Java ``VariantUtils.fromJsonNode``."""
    builder = VariantBuilder()
    # Default float parsing (no parse_float=Decimal): a JSON fractional number becomes a
    # Python float and is written as a DOUBLE, matching a default Jackson ObjectMapper.
    builder._process_parsed_json(json.loads(json_str))
    value, metadata = builder._finalize()
    return Variant(value, metadata)


# ---------------------------------------------------------------------------
# Variant builder, ported from Spark's VariantBuilder and exposed as a flat
# streaming writer (arrow-dotnet ``VariantValueWriter`` shape): a single object
# with an internal nesting stack. Each scalar/container append fills the "current
# slot" - the root, the next array element, or the current object field's value
# (after :meth:`append_key`). Object fields are sorted by key on :meth:`end_object`
# (canonical form); the metadata dictionary accumulates every key seen.
#
# The same internal machinery drives :func:`parse_json`: :meth:`_process_parsed_json`
# walks a parsed JSON tree using the same low-level writers and object/array
# finishers, so a programmatic build is byte-identical to ``parse_json`` of an
# equivalent value.
#
# Number handling in the JSON path follows Java VariantUtils.fromJsonNode (default
# Jackson ObjectMapper): a JSON fractional number becomes a DOUBLE (never a decimal);
# an integer becomes the smallest int1/2/4/8 that fits, or a scale-0 decimal when
# wider than 64 bits.
# ---------------------------------------------------------------------------

class _FieldEntry:
    __slots__ = ("key", "id", "offset")

    def __init__(self, key: str, field_id: int, offset: int):
        self.key = key
        self.id = field_id
        self.offset = offset


class _ObjectContext:
    """Nesting-stack frame for an in-progress object."""
    __slots__ = ("start", "fields", "pending_key", "pending_id", "has_pending_key")

    def __init__(self, start: int):
        self.start = start
        self.fields: List[_FieldEntry] = []
        self.pending_key: Optional[str] = None
        self.pending_id = 0
        self.has_pending_key = False


class _ArrayContext:
    """Nesting-stack frame for an in-progress array."""
    __slots__ = ("start", "offsets")

    def __init__(self, start: int):
        self.start = start
        self.offsets: List[int] = []


class VariantBuilder:
    """A flat streaming writer for Variant values, with an internal nesting stack.

    Scalars (``append_*``) and containers (``start_object``/``start_array``) each fill
    the current slot: the root, the next array element, or the value of the current
    object field (set with :meth:`append_key`). Call :meth:`build` to obtain the
    finished :class:`Variant`.
    """

    DEFAULT_SIZE_LIMIT = 16 * 1024 * 1024

    def __init__(self, size_limit: int = DEFAULT_SIZE_LIMIT):
        self.value = bytearray()
        self.dictionary = {}
        self.dictionary_keys: List[bytes] = []
        self.size_limit = size_limit
        self._stack: List[Any] = []
        self._root_written = False

    # -- public streaming API ----------------------------------------------

    def build(self) -> Variant:
        """Finalize and return the built :class:`Variant`. Raises if a container is
        still open or nothing has been written."""
        if self._stack:
            raise VariantError("cannot build with an open container")
        if not self._root_written:
            raise VariantError("cannot build an empty variant")
        value, metadata = self._finalize()
        return Variant(value, metadata)

    def append_null(self) -> None:
        self._before_append()
        self._append_null()

    def append_boolean(self, b: bool) -> None:
        self._before_append()
        self._append_boolean(bool(b))

    def append_byte(self, value: int) -> None:
        """Append an 8-bit integer (``INT1``)."""
        self._before_append()
        self._write_fixed_int(INT1, value, 1)

    def append_short(self, value: int) -> None:
        """Append a 16-bit integer (``INT2``)."""
        self._before_append()
        self._write_fixed_int(INT2, value, 2)

    def append_int(self, value: int) -> None:
        """Append a 32-bit integer (``INT4``)."""
        self._before_append()
        self._write_fixed_int(INT4, value, 4)

    def append_long(self, value: int) -> None:
        """Append a 64-bit integer (``INT8``)."""
        self._before_append()
        self._write_fixed_int(INT8, value, 8)

    def append_float(self, value: float) -> None:
        """Append a 32-bit float (``FLOAT``)."""
        self._before_append()
        self._check_capacity(1 + 4)
        self.value.append(self._primitive_header(FLOAT))
        self.value.extend(struct.pack("<f", value))

    def append_double(self, value: float) -> None:
        """Append a 64-bit double (``DOUBLE``)."""
        self._before_append()
        self._append_double(value)

    def append_decimal(self, unscaled: Any, scale: Optional[int] = None) -> None:
        """Append a decimal. Either ``append_decimal(unscaled_big_endian_bytes, scale)``
        with a big-endian two's-complement unscaled value, or the native overload
        ``append_decimal(decimal.Decimal)`` (scale taken from the value)."""
        self._before_append()
        if isinstance(unscaled, (bytes, bytearray)):
            if scale is None:
                raise VariantError("scale is required when appending a decimal from bytes")
            unscaled_int = int.from_bytes(bytes(unscaled), byteorder="big", signed=True)
            self._write_decimal(unscaled_int, scale)
        elif isinstance(unscaled, decimal.Decimal):
            if scale is not None:
                raise VariantError("scale must not be given with a Decimal value")
            self._append_decimal(unscaled)
        elif isinstance(unscaled, int):
            if scale is None:
                raise VariantError("scale is required when appending an unscaled integer")
            self._write_decimal(unscaled, scale)
        else:
            raise VariantError("invalid append_decimal arguments")

    def append_string(self, s: str) -> None:
        self._before_append()
        self._append_string(s)

    def append_binary(self, data: bytes) -> None:
        self._before_append()
        data = bytes(data)
        self._check_capacity(1 + U32_SIZE + len(data))
        self.value.append(self._primitive_header(BINARY))
        self.value.extend(len(data).to_bytes(U32_SIZE, byteorder="little"))
        self.value.extend(data)

    def append_uuid(self, value: Any) -> None:
        """Append a UUID. Accepts a :class:`uuid.UUID` or 16 big-endian bytes."""
        self._before_append()
        if isinstance(value, uuid_mod.UUID):
            raw = value.bytes  # big-endian
        else:
            raw = bytes(value)
        if len(raw) != UUID_SIZE:
            raise VariantError("uuid must be 16 bytes")
        self._check_capacity(1 + UUID_SIZE)
        self.value.append(self._primitive_header(UUID))
        self.value.extend(raw)

    def append_date(self, days_since_epoch: int) -> None:
        self._before_append()
        self._write_fixed_int(DATE, days_since_epoch, 4)

    def append_time(self, micros_since_midnight: int) -> None:
        """Append a ``TIME`` (TIME_NTZ) as microseconds since midnight."""
        self._before_append()
        self._write_fixed_int(TIME, micros_since_midnight, 8)

    def append_timestamp_tz(self, micros: int) -> None:
        self._before_append()
        self._write_fixed_int(TIMESTAMP, micros, 8)

    def append_timestamp_ntz(self, micros: int) -> None:
        self._before_append()
        self._write_fixed_int(TIMESTAMP_NTZ, micros, 8)

    def append_timestamp_nanos_tz(self, nanos: int) -> None:
        self._before_append()
        self._write_fixed_int(TIMESTAMP_NANOS, nanos, 8)

    def append_timestamp_nanos_ntz(self, nanos: int) -> None:
        self._before_append()
        self._write_fixed_int(TIMESTAMP_NANOS_NTZ, nanos, 8)

    def start_object(self) -> None:
        self._before_append()
        self._stack.append(_ObjectContext(len(self.value)))

    def append_key(self, key: str) -> None:
        if not self._stack or not isinstance(self._stack[-1], _ObjectContext):
            raise VariantError("append_key called outside of an object")
        ctx = self._stack[-1]
        if ctx.has_pending_key:
            raise VariantError("append_key called twice without an intervening value")
        ctx.pending_key = key
        ctx.pending_id = self._add_key(key)
        ctx.has_pending_key = True

    def end_object(self) -> None:
        if not self._stack or not isinstance(self._stack[-1], _ObjectContext):
            raise VariantError("end_object without a matching start_object")
        ctx = self._stack.pop()
        if ctx.has_pending_key:
            raise VariantError("end_object with a dangling append_key (no value)")
        self._finish_writing_object(ctx.start, ctx.fields)

    def start_array(self) -> None:
        self._before_append()
        self._stack.append(_ArrayContext(len(self.value)))

    def end_array(self) -> None:
        if not self._stack or not isinstance(self._stack[-1], _ArrayContext):
            raise VariantError("end_array without a matching start_array")
        ctx = self._stack.pop()
        self._finish_writing_array(ctx.start, ctx.offsets)

    # -- current-slot bookkeeping ------------------------------------------

    def _before_append(self) -> None:
        """Register the slot that the value about to be written will occupy, recording its
        offset in the enclosing container (or marking the root as written)."""
        if not self._stack:
            if self._root_written:
                raise VariantError("cannot append multiple root values")
            self._root_written = True
            return
        ctx = self._stack[-1]
        if isinstance(ctx, _ObjectContext):
            if not ctx.has_pending_key:
                raise VariantError("a value in an object must follow append_key")
            ctx.fields.append(
                _FieldEntry(ctx.pending_key, ctx.pending_id, len(self.value) - ctx.start))
            ctx.pending_key = None
            ctx.has_pending_key = False
        else:  # _ArrayContext
            ctx.offsets.append(len(self.value) - ctx.start)

    # -- metadata finalization ---------------------------------------------

    def _finalize(self) -> Tuple[bytes, bytes]:
        num_keys = len(self.dictionary_keys)
        dictionary_string_size = sum(len(k) for k in self.dictionary_keys)
        max_size = max(dictionary_string_size, num_keys)
        if max_size > self.size_limit:
            raise VariantError("variant size limit exceeded")
        offset_size = _integer_size(max_size)

        offset_start = 1 + offset_size
        string_start = offset_start + (num_keys + 1) * offset_size
        if string_start + dictionary_string_size > self.size_limit:
            raise VariantError("variant size limit exceeded")

        metadata = bytearray()
        metadata.append(VERSION | ((offset_size - 1) << 6))
        metadata.extend(num_keys.to_bytes(offset_size, byteorder="little"))
        current_offset = 0
        for key in self.dictionary_keys:
            metadata.extend(current_offset.to_bytes(offset_size, byteorder="little"))
            current_offset += len(key)
        metadata.extend(current_offset.to_bytes(offset_size, byteorder="little"))
        for key in self.dictionary_keys:
            metadata.extend(key)
        return bytes(self.value), bytes(metadata)

    # -- internal JSON-tree driver (used by parse_json) --------------------

    def _process_parsed_json(self, parsed: Any) -> None:
        if isinstance(parsed, dict):
            fields = []
            start = len(self.value)
            for key, val in parsed.items():
                field_id = self._add_key(key)
                fields.append(_FieldEntry(key, field_id, len(self.value) - start))
                self._process_parsed_json(val)
            self._finish_writing_object(start, fields)
        elif isinstance(parsed, list):
            offsets = []
            start = len(self.value)
            for elem in parsed:
                offsets.append(len(self.value) - start)
                self._process_parsed_json(elem)
            self._finish_writing_array(start, offsets)
        elif isinstance(parsed, str):
            self._append_string(parsed)
        elif isinstance(parsed, bool):
            # bool must precede int (bool is a subclass of int in Python).
            self._append_boolean(parsed)
        elif isinstance(parsed, int):
            if not self._append_int(parsed):
                # Wider than 64 bits: a scale-0 decimal, matching Java's BigInteger branch.
                self._append_decimal(decimal.Decimal(parsed))
        elif isinstance(parsed, float):
            self._append_double(parsed)
        elif isinstance(parsed, decimal.Decimal):
            self._append_decimal(parsed)
        elif parsed is None:
            self._append_null()
        else:
            raise VariantError("unsupported JSON value: %r" % type(parsed))

    def _check_capacity(self, additional: int) -> None:
        if len(self.value) + additional > self.size_limit:
            raise VariantError("variant size limit exceeded")

    @staticmethod
    def _primitive_header(type_code: int) -> int:
        return (type_code << 2) | PRIMITIVE

    @staticmethod
    def _short_string_header(size: int) -> int:
        return (size << 2) | SHORT_STR

    @staticmethod
    def _array_header(large_size: bool, offset_size: int) -> int:
        return ((int(large_size) << (BASIC_TYPE_BITS + 2))
                | ((offset_size - 1) << BASIC_TYPE_BITS) | ARRAY)

    @staticmethod
    def _object_header(large_size: bool, id_size: int, offset_size: int) -> int:
        return ((int(large_size) << (BASIC_TYPE_BITS + 4))
                | ((id_size - 1) << (BASIC_TYPE_BITS + 2))
                | ((offset_size - 1) << BASIC_TYPE_BITS) | OBJECT)

    def _add_key(self, key: str) -> int:
        if key in self.dictionary:
            return self.dictionary[key]
        field_id = len(self.dictionary_keys)
        self.dictionary[key] = field_id
        self.dictionary_keys.append(key.encode("utf-8"))
        return field_id

    def _append_boolean(self, b: bool) -> None:
        self._check_capacity(1)
        self.value.append(self._primitive_header(TRUE if b else FALSE))

    def _append_null(self) -> None:
        self._check_capacity(1)
        self.value.append(self._primitive_header(NULL))

    def _append_string(self, s: str) -> None:
        text = s.encode("utf-8")
        long_str = len(text) > MAX_SHORT_STR_SIZE
        self._check_capacity((1 + U32_SIZE if long_str else 1) + len(text))
        if long_str:
            self.value.append(self._primitive_header(LONG_STR))
            self.value.extend(len(text).to_bytes(U32_SIZE, byteorder="little"))
        else:
            self.value.append(self._short_string_header(len(text)))
        self.value.extend(text)

    def _append_int(self, i: int) -> bool:
        self._check_capacity(1 + 8)
        if I8_MIN <= i <= I8_MAX:
            self.value.append(self._primitive_header(INT1))
            self.value.extend(i.to_bytes(1, byteorder="little", signed=True))
        elif I16_MIN <= i <= I16_MAX:
            self.value.append(self._primitive_header(INT2))
            self.value.extend(i.to_bytes(2, byteorder="little", signed=True))
        elif I32_MIN <= i <= I32_MAX:
            self.value.append(self._primitive_header(INT4))
            self.value.extend(i.to_bytes(4, byteorder="little", signed=True))
        elif I64_MIN <= i <= I64_MAX:
            self.value.append(self._primitive_header(INT8))
            self.value.extend(i.to_bytes(8, byteorder="little", signed=True))
        else:
            return False
        return True

    def _write_fixed_int(self, type_code: int, value: int, width: int) -> None:
        """Write a fixed-width signed little-endian integer primitive."""
        self._check_capacity(1 + width)
        try:
            payload = int(value).to_bytes(width, byteorder="little", signed=True)
        except OverflowError:
            raise VariantError("integer value out of range for a %d-byte width" % width)
        self.value.append(self._primitive_header(type_code))
        self.value.extend(payload)

    def _write_decimal(self, unscaled: int, scale: int) -> None:
        """Write a decimal primitive from an unscaled integer and scale, choosing the
        smallest of DECIMAL4/8/16 that fits."""
        if scale < 0:
            raise VariantError("cannot encode decimal with negative scale")
        self._check_capacity(2 + 16)
        precision = len(str(abs(unscaled)))
        if scale <= MAX_DECIMAL4_PRECISION and precision <= MAX_DECIMAL4_PRECISION:
            code, width = DECIMAL4, 4
        elif scale <= MAX_DECIMAL8_PRECISION and precision <= MAX_DECIMAL8_PRECISION:
            code, width = DECIMAL8, 8
        elif scale <= MAX_DECIMAL16_PRECISION and precision <= MAX_DECIMAL16_PRECISION:
            code, width = DECIMAL16, 16
        else:
            raise VariantError("decimal exceeds maximum precision (38)")
        self.value.append(self._primitive_header(code))
        self.value.append(scale)
        self.value.extend(unscaled.to_bytes(width, byteorder="little", signed=True))

    def _append_decimal(self, d: decimal.Decimal) -> None:
        sign, digits, exponent = d.as_tuple()
        if not isinstance(exponent, int):
            raise VariantError("cannot encode non-finite decimal")
        unscaled = int("".join(map(str, digits)) or "0")
        if sign:
            unscaled = -unscaled
        self._write_decimal(unscaled, -exponent)

    def _append_double(self, f: float) -> None:
        self._check_capacity(1 + 8)
        self.value.append(self._primitive_header(DOUBLE))
        self.value.extend(struct.pack("<d", f))

    def _finish_writing_array(self, start: int, offsets: List[int]) -> None:
        data_size = len(self.value) - start
        num_offsets = len(offsets)
        large_size = num_offsets > U8_MAX
        size_bytes = U32_SIZE if large_size else 1
        offset_size = _integer_size(data_size)
        header_size = 1 + size_bytes + (num_offsets + 1) * offset_size
        self._check_capacity(header_size)
        self.value.extend(bytearray(header_size))
        self.value[start + header_size:] = bytes(self.value[start:start + data_size])
        offset_start = start + 1 + size_bytes
        self.value[start:start + 1] = bytes([self._array_header(large_size, offset_size)])
        self.value[start + 1:offset_start] = num_offsets.to_bytes(size_bytes, byteorder="little")
        offset_list = bytearray()
        for offset in offsets:
            offset_list.extend(offset.to_bytes(offset_size, byteorder="little"))
        offset_list.extend(data_size.to_bytes(offset_size, byteorder="little"))
        self.value[offset_start:offset_start + len(offset_list)] = offset_list

    def _finish_writing_object(self, start: int, fields: List[_FieldEntry]) -> None:
        num_fields = len(fields)
        fields.sort(key=lambda f: f.key)
        max_id = max((f.id for f in fields), default=0)
        data_size = len(self.value) - start
        large_size = num_fields > U8_MAX
        size_bytes = U32_SIZE if large_size else 1
        id_size = _integer_size(max_id)
        offset_size = _integer_size(data_size)
        header_size = 1 + size_bytes + num_fields * id_size + (num_fields + 1) * offset_size
        self._check_capacity(header_size)
        self.value.extend(bytearray(header_size))
        self.value[start + header_size:] = bytes(self.value[start:start + data_size])
        self.value[start:start + 1] = bytes(
            [self._object_header(large_size, id_size, offset_size)])
        self.value[start + 1:start + 1 + size_bytes] = num_fields.to_bytes(
            size_bytes, byteorder="little")
        id_start = start + 1 + size_bytes
        offset_start = id_start + num_fields * id_size
        id_list = bytearray()
        offset_list = bytearray()
        for field in fields:
            id_list.extend(field.id.to_bytes(id_size, byteorder="little"))
            offset_list.extend(field.offset.to_bytes(offset_size, byteorder="little"))
        offset_list.extend(data_size.to_bytes(offset_size, byteorder="little"))
        self.value[id_start:id_start + len(id_list)] = id_list
        self.value[offset_start:offset_start + len(offset_list)] = offset_list


def _integer_size(value: int) -> int:
    if value <= U8_MAX:
        return 1
    if value <= U16_MAX:
        return 2
    return U24_SIZE
