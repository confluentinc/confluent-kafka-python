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

from confluent_kafka.schema_registry.common.schema_registry_client import (
    Metadata,
    MetadataProperties,
    MetadataTags,
    RegisteredSchema,
    Schema,
    _SchemaCache,
)


def test_metadata_tags_hashable():
    tags1 = MetadataTags.from_dict({"**.ssn": ["PII"]})
    tags2 = MetadataTags.from_dict({"**.ssn": ["PII"]})
    tags3 = MetadataTags.from_dict({"**.ssn": ["OTHER"]})
    empty_tags = MetadataTags()

    assert hash(tags1) == hash(tags2)
    assert hash(tags1) != hash(tags3)
    assert isinstance(hash(empty_tags), int)

    # Key insertion order independence
    tags_ab = MetadataTags(tags={"a": ["1", "2"], "b": ["3"]})
    tags_ba = MetadataTags(tags={"b": ["3"], "a": ["1", "2"]})
    assert tags_ab == tags_ba
    assert hash(tags_ab) == hash(tags_ba)


def test_schema_with_metadata_tags_hashable():
    tags1 = MetadataTags.from_dict({"user.email": ["PII", "SENSITIVE"]})
    tags2 = MetadataTags.from_dict({"user.email": ["PII", "SENSITIVE"]})

    metadata1 = Metadata(
        tags=tags1,
        properties=MetadataProperties(properties={"owner": "security"}),
        sensitive=["user.email"],
    )
    metadata2 = Metadata(
        tags=tags2,
        properties=MetadataProperties(properties={"owner": "security"}),
        sensitive=["user.email"],
    )

    schema1 = Schema(
        schema_str='{"type": "string"}',
        schema_type="AVRO",
        metadata=metadata1,
    )
    schema2 = Schema(
        schema_str='{"type": "string"}',
        schema_type="AVRO",
        metadata=metadata2,
    )

    assert schema1 == schema2
    assert hash(schema1) == hash(schema2)


def test_schema_cache_with_metadata_tags():
    tags = MetadataTags.from_dict({"**.ssn": ["PII"]})
    schema = Schema(
        schema_str='{"type": "string"}',
        schema_type="AVRO",
        metadata=Metadata(tags=tags, properties=None, sensitive=None),
    )
    schema_lookup = Schema(
        schema_str='{"type": "string"}',
        schema_type="AVRO",
        metadata=Metadata(tags=MetadataTags.from_dict({"**.ssn": ["PII"]}), properties=None, sensitive=None),
    )

    cache = _SchemaCache()
    cache.set_schema("user-value", 42, "guid-42", schema)

    assert cache.schema_index["user-value"][schema_lookup] == 42

    registered_schema = RegisteredSchema(
        schema_id=42,
        schema=schema,
        subject="user-value",
        version=1,
        guid="guid-42",
    )
    cache.set_registered_schema(schema, registered_schema)
    assert cache.rs_schema_index["user-value"][schema_lookup] == registered_schema
