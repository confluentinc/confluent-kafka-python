#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
import binascii
from decimal import Decimal
from io import BytesIO

import pytest
from google.protobuf import descriptor_pb2

from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer,
    ProtobufSerializer,
    _create_index_array,
    decimal_to_protobuf,
    protobuf_to_decimal,
)
from confluent_kafka.schema_registry.serde import SchemaId
from confluent_kafka.serialization import SerializationError
from tests.integration.schema_registry.data.proto import DependencyTestProto_pb2, metadata_proto_pb2


@pytest.mark.parametrize(
    "pb2, coordinates",
    [
        (DependencyTestProto_pb2.DependencyMessage, [0]),
        (metadata_proto_pb2.ControlMessage.Watermark, [15, 1]),  # [ControlMessage, Watermark]
        (
            metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig,
            [4, 0, 1, 2],
        ),  # [HdfsOptions, ImportOptions, Generator, KacohaConfig ]
    ],
)
def test_create_index(pb2, coordinates):
    msg_idx = _create_index_array(pb2.DESCRIPTOR)

    assert msg_idx == coordinates


def _two_message_file_proto():
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = "test.proto"
    fdp.package = "pkg"
    first = fdp.message_type.add()
    first.name = "First"
    nested = first.nested_type.add()
    nested.name = "Inner"
    second = fdp.message_type.add()
    second.name = "Second"
    return fdp


def test_message_index_in_range():
    deserializer = object.__new__(ProtobufDeserializer)
    fdp = _two_message_file_proto()

    assert deserializer._get_message_desc_proto("", fdp, [0])[0] == "First"
    assert deserializer._get_message_desc_proto("", fdp, [1])[0] == "Second"
    assert deserializer._get_message_desc_proto("", fdp, [0, 0])[0] == "First.Inner"


@pytest.mark.parametrize("msg_index", [[-1], [2], [0, -1], [0, 5]])
def test_message_index_out_of_range(msg_index):
    # The message index array is attacker-controlled wire framing; a zigzag
    # varint can decode to a negative or out-of-range value. A negative index
    # would otherwise wrap around and resolve to a different message type.
    deserializer = object.__new__(ProtobufDeserializer)
    fdp = _two_message_file_proto()

    with pytest.raises(SerializationError, match="out of range"):
        deserializer._get_message_desc_proto("", fdp, msg_index)


@pytest.mark.parametrize(
    "pb2",
    [
        DependencyTestProto_pb2.DependencyMessage,
        metadata_proto_pb2.ControlMessage.Watermark,
        metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig,
    ],
)
@pytest.mark.parametrize("zigzag", [True, False])
def test_index_serialization(pb2, zigzag):
    msg_idx = _create_index_array(pb2.DESCRIPTOR)
    buf = BytesIO()
    ProtobufSerializer._encode_varints(buf, msg_idx, zigzag=zigzag)
    buf.flush()

    # reset buffer cursor
    buf.seek(0)
    decoded_msg_idx = SchemaId._read_index_array(buf, zigzag=zigzag)
    buf.close()

    assert decoded_msg_idx == msg_idx


@pytest.mark.parametrize(
    "msg_idx, zigzag, expected_hex",
    [
        # b2a_hex returns hex pairs
        ([0], True, b'00'),  # special case [0]
        ([0], False, b'00'),  # special case [0]
        ([1], True, b'0202'),
        ([1], False, b'0101'),
        ([127, 8, 9], True, b'06fe011012'),
        ([127, 8, 9], False, b'037f0809'),
        ([128], True, b'028002'),
        ([128], False, b'018001'),
        ([9223372036854775807], True, b'02feffffffffffffffff01'),
        ([9223372036854775807], False, b'01ffffffffffffffff7f'),
    ],
)
def test_index_encoder(msg_idx, zigzag, expected_hex):
    buf = BytesIO()
    ProtobufSerializer._encode_varints(buf, msg_idx, zigzag=zigzag)
    buf.flush()
    buf.seek(0)
    assert binascii.b2a_hex(buf.read()) == expected_hex

    # reset reader and test decoder
    buf.seek(0)
    decoded_msg_idx = SchemaId._read_index_array(buf, zigzag=zigzag)
    assert decoded_msg_idx == msg_idx


@pytest.mark.parametrize(
    "decimal, scale",
    [
        ("0", 0),
        ("1.01", 2),
        ("123456789123456789.56", 2),
        ("1234", 0),
        ("1234.5", 1),
        ("-0", 0),
        ("-1.01", 2),
        ("-123456789123456789.56", 2),
        ("-1234", 0),
        ("-1234.5", 1),
        ("-1234.56", 2),
    ],
)
def test_proto_decimal(decimal, scale):
    input = Decimal(decimal)
    converted = decimal_to_protobuf(input, scale)
    result = protobuf_to_decimal(converted)
    assert result == input
