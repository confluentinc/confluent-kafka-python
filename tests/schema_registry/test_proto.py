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
from google.protobuf.message_factory import MessageFactory

from confluent_kafka.schema_registry.protobuf import (ProtobufSerializer,
                                                      ProtobufDeserializer,
                                                      _create_msg_index)
from confluent_kafka.serialization import SerializationContext, MessageField
from tests.integration.schema_registry.data.proto import (DependencyTestProto_pb2,
                                                          metadata_proto_pb2,
                                                          TestProto_pb2,
                                                          PublicTestProto_pb2,
                                                          NestedTestProto_pb2)
from tests.schema_registry.test_api_client import TEST_URL

TEST_DATA = [
    (TestProto_pb2.TestMessage, {'test_string': "abc",
                                 'test_bool': True,
                                 'test_bytes': b'look at these bytes',
                                 'test_double': 1.0,
                                 'test_float': 12.0}),
    (PublicTestProto_pb2.TestMessage, {'test_string': "abc",
                                       'test_bool': True,
                                       'test_bytes': b'look at these bytes',
                                       'test_double': 1.0,
                                       'test_float': 12.0}),
    (NestedTestProto_pb2.NestedMessage, {'user_id':
     NestedTestProto_pb2.UserId(
            kafka_user_id='oneof_str'),
        'is_active': True,
        'experiments_active': ['x', 'y', '1'],
        'status': NestedTestProto_pb2.INACTIVE,
        'complex_type':
            NestedTestProto_pb2.ComplexType(
                one_id='oneof_str',
                is_active=False)})
]

@pytest.mark.parametrize("pb2, coordinates", [
    (DependencyTestProto_pb2.DependencyMessage, [0]),
    (metadata_proto_pb2.ControlMessage.Watermark, [15, 1]),  # [ControlMessage, Watermark]
    (metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig,
     [4, 0, 1, 2])  # [HdfsOptions, ImportOptions, Generator, KacohaConfig ]
])
def test_create_index(pb2, coordinates):
    msg_idx = _create_msg_index(pb2.DESCRIPTOR)

    assert msg_idx == coordinates


@pytest.mark.parametrize("pb2", [
    DependencyTestProto_pb2.DependencyMessage,
    metadata_proto_pb2.ControlMessage.Watermark,
    metadata_proto_pb2.HDFSOptions.ImportOptions.Generator.KacohaConfig
])
@pytest.mark.parametrize("zigzag", [True, False])
def test_index_serialization(pb2, zigzag):
    msg_idx = _create_msg_index(pb2.DESCRIPTOR)
    buf = BytesIO()
    ProtobufSerializer._encode_varints(buf, msg_idx, zigzag=zigzag)
    buf.flush()

    # reset buffer cursor
    buf.seek(0)
    decoded_msg_idx = ProtobufDeserializer._decode_index(buf, zigzag=zigzag)
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
    decoded_msg_idx = ProtobufDeserializer._decode_index(buf, zigzag=zigzag)
    assert decoded_msg_idx == msg_idx


@pytest.mark.parametrize("pb2, data", TEST_DATA)
def test_round_trip(mock_schema_registry, pb2, data):

    conf = {"use.deprecated.format": False}
    test_client = mock_schema_registry({"url": TEST_URL})
    context = SerializationContext("topic-name", MessageField.VALUE)
    serializer = ProtobufSerializer(pb2, test_client, conf=conf)
    deserializer = ProtobufDeserializer(pb2, conf=conf)

    message_before = pb2(**data)
    bytes_before = serializer(message_before, context)
    message_after = deserializer(bytes_before, context)
    bytes_after = serializer(message_after, context)
    assert isinstance(message_after, pb2)

    assert message_before == message_after
    assert bytes_after == bytes_before


def test_foo_bar():
    message =TestProto_pb2.TestMessage
    assert MessageFactory().GetPrototype(message.DESCRIPTOR) is message