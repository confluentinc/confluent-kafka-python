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
from confluent_kafka.schema_registry.serde import SchemaId


def test_schema_guid():
    schema_id = SchemaId("AVRO")
    input = bytes([
        0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
        0xa8, 0x02, 0xe2
    ])
    schema_id.from_bytes(io.BytesIO(input))
    guid_str = str(schema_id.guid)
    assert guid_str == "89791762-2336-4186-9674-299b90a802e2"
    output = schema_id.guid_to_bytes()
    assert output == input


def test_schema_id():
    schema_id = SchemaId("AVRO")
    input = bytes([
        0x00, 0x00, 0x00, 0x00, 0x01
    ])
    schema_id.from_bytes(io.BytesIO(input))
    id = schema_id.id
    assert id == 1
    output = schema_id.id_to_bytes()
    assert output == input


def test_schema_guid_with_message_indexes():
    schema_id = SchemaId("PROTOBUF")
    input = bytes([
        0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
        0xa8, 0x02, 0xe2, 0x06, 0x02, 0x04, 0x06
    ])
    schema_id.from_bytes(io.BytesIO(input))
    guid_str = str(schema_id.guid)
    assert guid_str == "89791762-2336-4186-9674-299b90a802e2"
    indexes = schema_id.message_indexes
    assert indexes == [1, 2, 3]
    output = schema_id.guid_to_bytes()
    assert output == input


def test_schema_id_with_message_indexes():
    schema_id = SchemaId("PROTOBUF")
    input = bytes([
        0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x02, 0x04, 0x06
    ])
    schema_id.from_bytes(io.BytesIO(input))
    id = schema_id.id
    assert id == 1
    indexes = schema_id.message_indexes
    assert indexes == [1, 2, 3]
    output = schema_id.id_to_bytes()
    assert output == input
