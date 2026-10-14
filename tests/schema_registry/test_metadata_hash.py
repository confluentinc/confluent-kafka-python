#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
"""Regression tests for hashing schemas whose metadata carries tags (#2385)."""

from confluent_kafka.schema_registry.common.schema_registry_client import (
    Metadata,
    MetadataTags,
    Schema,
)


def test_metadata_tags_hash_does_not_raise():
    tags = MetadataTags.from_dict({"**.ssn": ["PII"]})

    hash(tags)


def test_metadata_tags_hash_distinguishes_values():
    same_a = MetadataTags.from_dict({"**.ssn": ["PII"]})
    same_b = MetadataTags.from_dict({"**.ssn": ["PII"]})
    different = MetadataTags.from_dict({"**.ssn": ["PII", "SENSITIVE"]})

    assert hash(same_a) == hash(same_b)
    assert hash(same_a) != hash(different)


def test_schema_with_metadata_tags_is_hashable():
    tags = MetadataTags.from_dict({"**.ssn": ["PII"]})
    metadata = Metadata(tags=tags, properties=None, sensitive=None)
    schema = Schema(
        schema_str='{"type": "string"}',
        schema_type="AVRO",
        metadata=metadata,
    )

    # Mirrors how _SchemaCache.set_schema() uses a Schema as a dict key.
    schema_index = {schema: 1}

    assert schema_index[schema] == 1
