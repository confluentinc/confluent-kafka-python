#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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
import json

from confluent_kafka.schema_registry import (
    Metadata,
    MetadataProperties,
    Schema,
    SchemaRegistryClient,
)

_SUBJECT = 'mock-versions-value'
_NUM_VERSIONS = 8


def _register_versions(client, metadata_props=None):
    for i in range(_NUM_VERSIONS):
        record = {
            'type': 'record',
            'name': 'test',
            'fields': [{'name': f'f{j}', 'type': 'int', 'default': 0} for j in range(i + 1)],
        }
        metadata = Metadata(None, MetadataProperties(metadata_props), None) if metadata_props else None
        client.register_schema(_SUBJECT, Schema(json.dumps(record), 'AVRO', [], metadata, None))


def test_mock_get_versions_is_ordered():
    client = SchemaRegistryClient.new_client({'url': 'mock://'})
    _register_versions(client)

    expected = list(range(1, _NUM_VERSIONS + 1))
    assert client.get_versions(_SUBJECT) == expected
    assert client.get_versions(_SUBJECT, offset=2, limit=3) == expected[2:5]


def test_mock_get_latest_with_metadata_returns_latest_match():
    client = SchemaRegistryClient.new_client({'url': 'mock://'})
    _register_versions(client, {'app': 'x'})

    latest = client.get_latest_with_metadata(_SUBJECT, {'app': 'x'})
    assert latest.version == _NUM_VERSIONS
