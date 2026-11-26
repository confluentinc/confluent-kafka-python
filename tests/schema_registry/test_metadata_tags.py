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


def test_schema_cache_format_isolation():
    cache = _SchemaCache()
    raw_schema = Schema('syntax = "proto3";', schema_type="PROTOBUF")
    serialized_schema = Schema('Cg1leGFtcGxlLnByb3Rv', schema_type="PROTOBUF")

    # set_schema format isolation
    cache.set_schema("user-value", 1, "guid-1", raw_schema)
    cache.set_schema("user-value", 1, "guid-1", serialized_schema, fmt="serialized")

    assert cache.get_schema_by_id("user-value", 1)[1] == raw_schema
    assert cache.get_schema_by_id("user-value", 1, fmt="serialized")[1] == serialized_schema
    assert cache.get_schema_by_guid("guid-1") == raw_schema
    assert cache.get_schema_by_guid("guid-1", fmt="serialized") == serialized_schema

    # set_registered_schema format isolation
    rs_raw = RegisteredSchema(schema_id=1, schema=raw_schema, subject="user-value", version=1, guid="guid-1")
    rs_serialized = RegisteredSchema(
        schema_id=1, schema=serialized_schema, subject="user-value", version=1, guid="guid-1"
    )

    cache.set_registered_schema(raw_schema, rs_raw)
    cache.set_registered_schema(serialized_schema, rs_serialized, fmt="serialized")

    assert cache.get_registered_by_subject_version("user-value", 1) == rs_raw
    assert cache.get_registered_by_subject_version("user-value", 1, fmt="serialized") == rs_serialized
    assert cache.get_registered_by_subject_schema("user-value", raw_schema) == rs_raw
    assert cache.get_registered_by_subject_schema("user-value", serialized_schema, fmt="serialized") == rs_serialized
    assert cache.get_registered_by_subject_id("user-value", 1) == rs_raw
    assert cache.get_registered_by_subject_id("user-value", 1, fmt="serialized") == rs_serialized

    # remove_by_subject_version cleans all format variants
    cache.remove_by_subject_version("user-value", 1)
    assert cache.get_registered_by_subject_version("user-value", 1) is None
    assert cache.get_registered_by_subject_version("user-value", 1, fmt="serialized") is None
    assert cache.get_schema_by_id("user-value", 1) is None
    assert cache.get_schema_by_id("user-value", 1, fmt="serialized") is None
    assert cache.get_id_by_schema("user-value", raw_schema) is None
    assert cache.get_id_by_schema("user-value", serialized_schema) is None


def test_schema_cache_lookup_schema_response_indexes():
    """Verify formatted lookup_schema stores response schema in ID and GUID indexes."""
    cache = _SchemaCache()
    raw_schema = Schema('syntax = "proto3";', schema_type="PROTOBUF")
    serialized_schema = Schema('Cg1leGFtcGxlLnByb3Rv', schema_type="PROTOBUF")

    rs = RegisteredSchema(schema_id=42, schema=serialized_schema, subject="test-sub", version=1, guid="guid-42")
    cache.set_registered_schema(raw_schema, rs, fmt="serialized")

    # ID and GUID indexes must return the formatted response schema
    assert cache.get_schema_by_id("test-sub", 42, fmt="serialized")[1] == serialized_schema
    assert cache.get_schema_by_guid("guid-42", fmt="serialized") == serialized_schema

    # Schema-based indexes must key by the input schema
    assert cache.get_id_by_schema("test-sub", raw_schema) == 42
    assert cache.get_registered_by_subject_schema("test-sub", raw_schema, fmt="serialized") == rs


def test_schema_cache_custom_reference_format_exclusion():
    """Verify custom-reference responses are excluded from format-blind indexes."""
    cache = _SchemaCache()
    default_schema = Schema('syntax = "proto3";', schema_type="PROTOBUF")
    custom_ref_schema = Schema('syntax = "proto3"; // custom ref', schema_type="PROTOBUF")

    # set_schema with custom reference_format
    cache.set_schema("user-value", 10, "guid-10", default_schema)
    cache.set_schema("user-value", 10, "guid-10", custom_ref_schema, reference_format="CUSTOM")

    # schema_id_index can resolve custom reference_format
    assert cache.get_schema_by_id("user-value", 10)[1] == default_schema
    assert cache.get_schema_by_id("user-value", 10, reference_format="CUSTOM")[1] == custom_ref_schema

    # Format-blind guid index retains default schema only
    assert cache.get_schema_by_guid("guid-10") == default_schema

    # set_registered_schema with custom reference_format
    rs_default = RegisteredSchema(schema_id=10, schema=default_schema, subject="user-value", version=1, guid="guid-10")
    rs_custom = RegisteredSchema(
        schema_id=10, schema=custom_ref_schema, subject="user-value", version=1, guid="guid-10"
    )

    cache.set_registered_schema(default_schema, rs_default)
    cache.set_registered_schema(custom_ref_schema, rs_custom, reference_format="CUSTOM")

    # rs_id_index can resolve custom reference_format
    assert cache.get_registered_by_subject_id("user-value", 10) == rs_default
    assert cache.get_registered_by_subject_id("user-value", 10, reference_format="CUSTOM") == rs_custom

    # Format-blind indexes retain default schema only
    assert cache.get_registered_by_subject_version("user-value", 1) == rs_default
    assert cache.get_registered_by_subject_schema("user-value", default_schema) == rs_default
    assert cache.get_registered_by_subject_schema("user-value", custom_ref_schema) is None
    assert cache.get_id_by_schema("user-value", default_schema) == 10

    # Version removal cleans both default and custom-reference entries
    cache.remove_by_subject_version("user-value", 1)
    assert cache.get_schema_by_id("user-value", 10) is None
    assert cache.get_schema_by_id("user-value", 10, reference_format="CUSTOM") is None
    assert cache.get_registered_by_subject_id("user-value", 10) is None
    assert cache.get_registered_by_subject_id("user-value", 10, reference_format="CUSTOM") is None
