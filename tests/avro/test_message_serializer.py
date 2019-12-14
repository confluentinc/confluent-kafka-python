#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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

#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#

import struct

from tests.avro import data_gen


def assert_message_equals(serializer, message, expected, schema_id):
    assert message
    assert len(message) > 5
    magic, sid = struct.unpack('>bI', message[0:5])
    assert magic == 0
    assert sid == schema_id
    decoded = serializer.decode_message(message)
    assert decoded
    assert decoded == expected


def test_encode_with_schema_id(mock_schema_registry_client_fixture,
                               message_serializer_fixture,
                               schema_fixture):
    client = mock_schema_registry_client_fixture
    serializer = message_serializer_fixture(client)

    adv = schema_fixture("adv_schema")
    basic = schema_fixture("basic_schema")
    subject = 'test'
    schema_id = client.register(subject, basic)

    records = data_gen.BASIC_ITEMS
    for record in records:
        message = serializer.encode_record_with_schema_id(schema_id, record)
        assert_message_equals(serializer, message, record, schema_id)

    subject = 'test_adv'
    adv_schema_id = client.register(subject, adv)
    assert adv_schema_id != schema_id
    records = data_gen.ADVANCED_ITEMS
    for record in records:
        message = serializer.encode_record_with_schema_id(adv_schema_id, record)
        assert_message_equals(serializer, message, record, adv_schema_id)


def test_encode_record_with_schema(mock_schema_registry_client_fixture,
                                   message_serializer_fixture,
                                   schema_fixture):
    client = mock_schema_registry_client_fixture
    serializer = message_serializer_fixture(client)

    topic = 'test'
    basic = schema_fixture("basic_schema")
    subject = 'test-value'
    schema_id = client.register(subject, basic)
    records = data_gen.BASIC_ITEMS
    for record in records:
        message = serializer.encode_record_with_schema(topic, basic, record)
        assert_message_equals(serializer, message, record, schema_id)


def test_decode_none(mock_schema_registry_client_fixture,
                     message_serializer_fixture):
    """"null/None messages should decode to None"""
    client = mock_schema_registry_client_fixture
    serializer = message_serializer_fixture(client)

    assert serializer.decode_message(None) is None
