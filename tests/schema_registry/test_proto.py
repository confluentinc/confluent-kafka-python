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
                                                      _create_msg_index)
from tests.integration.schema_registry.gen import (DependencyTestProto_pb2,
                                                   metadata_proto_pb2)


@pytest.mark.parametrize("pb2, coordinates", [
    (DependencyTestProto_pb2.DependencyMessage, [0]),
    (metadata_proto_pb2.ControlMessage.Watermark, [15, 1]),  # [ControlMessage, Watermark]
    (metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig,
     [4, 0, 1, 2])  # [HdfsOptions, ImportOptions, Generator, KacohaConfig ]
])
def test_create_index(pb2, coordinates):
    msg_idx = _create_msg_index(pb2.DESCRIPTOR)

    if coordinates == [0]:
        assert msg_idx == coordinates
    else:
        assert msg_idx[0] == len(coordinates)
        assert msg_idx[1:] == coordinates


@pytest.mark.parametrize("pb2", [
    DependencyTestProto_pb2.DependencyMessage,
    metadata_proto_pb2.ControlMessage.Watermark,
    metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig
])
def test_index_serialization(pb2):
    msg_idx = _create_msg_index(pb2.DESCRIPTOR)
    buf = BytesIO()
    ProtobufSerializer._encode_uvarints(buf, msg_idx)
    buf.flush()

    # reset buffer cursor
    buf.seek(0)
    decoded_msg_idx = ProtobufDeserializer._decode_index(buf)
    buf.close()

    assert decoded_msg_idx == msg_idx


@pytest.mark.parametrize("msg_idx, expected_hex", [
    ([1, 0], b'00'),   # b2a_hex always returns hex pairs
    ([1, 1], b'01'),
    ([1, 127], b'7f'),
    ([1, 128], b'8001'),
    ([1, 9223372036854775807], b'ffffffffffffffff7f')
])
def test_index_encoder(msg_idx, expected_hex):
    buf = BytesIO()
    ProtobufSerializer._encode_uvarints(buf, msg_idx)
    buf.flush()
    # ignore array length prefix
    buf.seek(1)
    assert binascii.b2a_hex(buf.read()) == expected_hex

    # reset reader and test decoder
    buf.seek(0)
    assert msg_idx == ProtobufDeserializer._decode_index(buf)
