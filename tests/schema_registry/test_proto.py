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
from io import BytesIO

import pytest

from confluent_kafka.schema_registry.protobuf import (ProtobufSerializer,
                                                      ProtobufDeserializer,
                                                      _create_index_array)
from tests.integration.schema_registry.data.proto import (DependencyTestProto_pb2,
                                                          metadata_proto_pb2)


@pytest.mark.parametrize("pb2, coordinates", [
    (DependencyTestProto_pb2.DependencyMessage, [0]),
    (metadata_proto_pb2.ControlMessage.Watermark, [15, 1]),  # [ControlMessage, Watermark]
    (metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig,
     [4, 0, 1, 2])  # [HdfsOptions, ImportOptions, Generator, KacohaConfig ]
])
def test_create_index(pb2, coordinates):
    msg_idx = _create_index_array(pb2.DESCRIPTOR)

    assert msg_idx == coordinates


@pytest.mark.parametrize("pb2", [
    DependencyTestProto_pb2.DependencyMessage,
    metadata_proto_pb2.ControlMessage.Watermark,
    metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig
])
@pytest.mark.parametrize("zigzag", [True, False])
def test_index_serialization(pb2, zigzag):
    msg_idx = _create_index_array(pb2.DESCRIPTOR)
    buf = BytesIO()
    ProtobufSerializer._encode_varints(buf, msg_idx, zigzag=zigzag)
    buf.flush()

    # reset buffer cursor
    buf.seek(0)
    decoded_msg_idx = ProtobufDeserializer._read_index_array(buf, zigzag=zigzag)
    buf.close()

    assert decoded_msg_idx == msg_idx


@pytest.mark.parametrize("msg_idx, zigzag, expected_hex", [
    # b2a_hex returns hex pairs
    ([0], True, b'00'),   # special case [0]
    ([0], False, b'00'),  # special case [0]
    ([1], True, b'0202'),
    ([1], False, b'0101'),
    ([127, 8, 9], True, b'06fe011012'),
    ([127, 8, 9], False, b'037f0809'),
    ([128], True, b'028002'),
    ([128], False, b'018001'),
    ([9223372036854775807], True, b'02feffffffffffffffff01'),
    ([9223372036854775807], False, b'01ffffffffffffffff7f')
])
def test_index_encoder(msg_idx, zigzag, expected_hex):
    buf = BytesIO()
    ProtobufSerializer._encode_varints(buf, msg_idx, zigzag=zigzag)
    buf.flush()
    buf.seek(0)
    assert binascii.b2a_hex(buf.read()) == expected_hex

    # reset reader and test decoder
    buf.seek(0)
    decoded_msg_idx = ProtobufDeserializer._read_index_array(buf, zigzag=zigzag)
    assert decoded_msg_idx == msg_idx
