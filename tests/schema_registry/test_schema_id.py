#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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
import io

import pytest

from confluent_kafka.schema_registry import (
    SerializationError,
    dual_schema_id_deserializer,
    header_schema_id_serializer,
    prefix_schema_id_serializer,
)
from confluent_kafka.schema_registry.serde import SchemaId
from confluent_kafka.serialization import MessageField, SerializationContext


def test_schema_guid():
    schema_id = SchemaId("AVRO")
    input = bytes(
        [0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9B, 0x90, 0xA8, 0x02, 0xE2]
    )
    schema_id.from_bytes(io.BytesIO(input))
    guid_str = str(schema_id.guid)
    assert guid_str == "89791762-2336-4186-9674-299b90a802e2"
    output = schema_id.guid_to_bytes()
    assert output == input


def test_schema_id():
    schema_id = SchemaId("AVRO")
    input = bytes([0x00, 0x00, 0x00, 0x00, 0x01])
    schema_id.from_bytes(io.BytesIO(input))
    id = schema_id.id
    assert id == 1
    output = schema_id.id_to_bytes()
    assert output == input


def test_schema_guid_with_message_indexes():
    schema_id = SchemaId("PROTOBUF")
    input = bytes(
        [
            0x01,
            0x89,
            0x79,
            0x17,
            0x62,
            0x23,
            0x36,
            0x41,
            0x86,
            0x96,
            0x74,
            0x29,
            0x9B,
            0x90,
            0xA8,
            0x02,
            0xE2,
            0x06,
            0x02,
            0x04,
            0x06,
        ]
    )
    schema_id.from_bytes(io.BytesIO(input))
    guid_str = str(schema_id.guid)
    assert guid_str == "89791762-2336-4186-9674-299b90a802e2"
    indexes = schema_id.message_indexes
    assert indexes == [1, 2, 3]
    output = schema_id.guid_to_bytes()
    assert output == input


def test_schema_id_with_message_indexes():
    schema_id = SchemaId("PROTOBUF")
    input = bytes([0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x02, 0x04, 0x06])
    schema_id.from_bytes(io.BytesIO(input))
    id = schema_id.id
    assert id == 1
    indexes = schema_id.message_indexes
    assert indexes == [1, 2, 3]
    output = schema_id.id_to_bytes()
    assert output == input


def test_dual_schema_id_deserializer_handles_none_context():
    """
    Ensures dual_schema_id_deserializer handles None SerializationContext properly.
    """
    schema_id = SchemaId("AVRO")
    test_data = b'\x00\x00\x00\x00\x01'  # Valid schema ID format

    result = dual_schema_id_deserializer(test_data, ctx=None, schema_id=schema_id)

    # Verify it returns BytesIO and parsed the schema ID
    assert isinstance(result, io.BytesIO)
    assert schema_id.id == 1


def test_header_schema_id_serializer_handles_none_context():
    """
    Ensures header_schema_id_serializer handles None SerializationContext properly.
    """
    # schema_id won't be used since function raises error early when ctx=None
    with pytest.raises(SerializationError, match="SerializationContext is required"):
        header_schema_id_serializer(b"test_payload", ctx=None, schema_id=None)


@pytest.mark.parametrize(
    "framing, match",
    [
        (b'', "expected a magic byte but found no data"),
        (b'\x00', "expected 4 bytes after magic byte 0 but found 0"),
        (b'\x00\x00\x00\x01', "expected 4 bytes after magic byte 0 but found 3"),
        (b'\x01', "expected 16 bytes after magic byte 1 but found 0"),
        (b'\x01' + b'\x89' * 15, "expected 16 bytes after magic byte 1 but found 15"),
    ],
)
def test_schema_id_from_bytes_raises_serialization_error_when_framing_is_truncated(framing, match):
    schema_id = SchemaId("AVRO")

    with pytest.raises(SerializationError, match=match):
        schema_id.from_bytes(io.BytesIO(framing))


def test_schema_id_from_bytes_returns_empty_remainder_for_framing_only_prefix():
    schema_id = SchemaId("AVRO")

    remainder = schema_id.from_bytes(io.BytesIO(b'\x00\x00\x00\x00\x01'))

    assert schema_id.id == 1
    assert remainder.read() == b''


def test_dual_schema_id_deserializer_raises_serialization_error_when_header_is_truncated():
    schema_id = SchemaId("AVRO")
    ctx = SerializationContext("topic1", MessageField.VALUE, [("__value_schema_id", b'\x01\x89\x79')])

    with pytest.raises(SerializationError, match="expected 16 bytes after magic byte 1 but found 2"):
        dual_schema_id_deserializer(b'\x02', ctx, schema_id)


def test_dual_schema_id_deserializer_returns_full_payload_when_shorter_than_prefix():
    schema_id = SchemaId("AVRO")
    guid = bytes([0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9B, 0x90, 0xA8, 0x02, 0xE2])
    ctx = SerializationContext("topic1", MessageField.KEY, [("__key_schema_id", guid)])

    payload = dual_schema_id_deserializer(b'\x02', ctx, schema_id)

    assert str(schema_id.guid) == "89791762-2336-4186-9674-299b90a802e2"
    assert payload.read() == b'\x02'


_GUID_1 = "89791762-2336-4186-9674-299b90a802e2"
_GUID_2 = "0f2b3c4d-5e6f-4a1b-8c2d-3e4f5a6b7c8d"


def _guid_header(guid):
    return SchemaId("AVRO", guid=guid).guid_to_bytes()


def test_dual_schema_id_deserializer_uses_last_header_when_duplicated():
    schema_id = SchemaId("AVRO")
    headers = [("__value_schema_id", _guid_header(_GUID_1)), ("__value_schema_id", _guid_header(_GUID_2))]
    ctx = SerializationContext("topic1", MessageField.VALUE, headers)

    dual_schema_id_deserializer(b'\x02', ctx, schema_id)

    assert str(schema_id.guid) == _GUID_2


@pytest.mark.parametrize("headers_type", [dict, list])
def test_header_schema_id_serializer_replaces_existing_schema_id_header(headers_type):
    stale = _guid_header(_GUID_1)
    headers = headers_type([("__value_schema_id", stale), ("__key_schema_id", stale), ("other", b"x")])
    ctx = SerializationContext("topic1", MessageField.VALUE, headers)

    header_schema_id_serializer(b'\x02', ctx, SchemaId("AVRO", guid=_GUID_2))

    items = list(headers.items()) if isinstance(headers, dict) else headers
    assert [v for k, v in items if k == "__value_schema_id"] == [_guid_header(_GUID_2)]
    assert ("__key_schema_id", stale) in items
    assert ("other", b"x") in items


@pytest.mark.parametrize("headers_type", [dict, list])
def test_prefix_schema_id_serializer_removes_stale_schema_id_header(headers_type):
    stale = _guid_header(_GUID_1)
    headers = headers_type([("__value_schema_id", stale), ("__key_schema_id", stale), ("other", b"x")])
    ctx = SerializationContext("topic1", MessageField.VALUE, headers)

    payload = prefix_schema_id_serializer(b'\x02', ctx, SchemaId("AVRO", schema_id=1))

    assert payload == b'\x00\x00\x00\x00\x01\x02'
    items = list(headers.items()) if isinstance(headers, dict) else headers
    assert "__value_schema_id" not in [k for k, _ in items]
    assert ("__key_schema_id", stale) in items
    assert ("other", b"x") in items


@pytest.mark.parametrize("ctx", [None, SerializationContext("topic1", MessageField.VALUE)])
def test_prefix_schema_id_serializer_handles_missing_context_or_headers(ctx):
    payload = prefix_schema_id_serializer(b'\x02', ctx, SchemaId("AVRO", schema_id=1))

    assert payload == b'\x00\x00\x00\x00\x01\x02'


def test_schema_id_from_bytes_raises_serialization_error_when_message_indexes_are_truncated():
    schema_id = SchemaId("PROTOBUF")

    with pytest.raises(SerializationError, match="truncated message indexes"):
        schema_id.from_bytes(io.BytesIO(b'\x00\x00\x00\x00\x01'))
