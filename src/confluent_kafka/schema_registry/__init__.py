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
import io
from typing import Optional

from .schema_registry_client import (
  ConfigCompatibilityLevel,
  Metadata,
  MetadataProperties,
  MetadataTags,
  RegisteredSchema,
  Rule,
  RuleKind,
  RuleMode,
  RuleParams,
  RuleSet,
  Schema,
  SchemaRegistryClient,
  AsyncSchemaRegistryClient,
  SchemaRegistryError,
  SchemaReference,
  ServerConfig
)
from ..serialization import SerializationError, MessageField

_KEY_SCHEMA_ID = "__key_schema_id"
_VALUE_SCHEMA_ID = "__value_schema_id"

_MAGIC_BYTE = 0
_MAGIC_BYTE_V0 = _MAGIC_BYTE
_MAGIC_BYTE_V1 = 1

__all__ = [
  "ConfigCompatibilityLevel",
  "Metadata",
  "MetadataProperties",
  "MetadataTags",
  "RegisteredSchema",
  "Rule",
  "RuleKind",
  "RuleMode",
  "RuleParams",
  "RuleSet",
  "Schema",
  "SchemaRegistryClient",
  "AsyncSchemaRegistryClient",
  "SchemaRegistryError",
  "SchemaReference",
  "ServerConfig",
  "topic_subject_name_strategy",
  "topic_record_subject_name_strategy",
  "record_subject_name_strategy",
  "header_schema_id_serializer",
  "prefix_schema_id_serializer",
  "dual_schema_id_deserializer",
  "prefix_schema_id_deserializer"
]


def topic_subject_name_strategy(ctx, record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-key|value.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (Optional[str]): Record name.

    """
    return ctx.topic + "-" + ctx.field


def topic_record_subject_name_strategy(ctx, record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-{record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (Optional[str]): Record name.

    """
    return ctx.topic + "-" + record_name if record_name is not None else None


def record_subject_name_strategy(ctx, record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (Optional[str]): Record name.

    """
    return record_name if record_name is not None else None


def reference_subject_name_strategy(ctx, schema_ref: SchemaReference) -> Optional[str]:
    """
    Constructs a subject reference name in the form of {reference name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        schema_ref (SchemaReference): SchemaReference instance.

    """
    return schema_ref.name if schema_ref is not None else None


def header_schema_id_serializer(payload: bytes, ctx, schema_id) -> bytes:
    """
    Serializes the schema guid into the header.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    headers = ctx.headers
    if headers is None:
        raise SerializationError("Missing headers")
    header_key = _KEY_SCHEMA_ID if ctx.field == MessageField.KEY else _VALUE_SCHEMA_ID
    header_value = schema_id.guid_to_bytes()
    if isinstance(headers, list):
        headers.append((header_key, header_value))
    elif isinstance(headers, dict):
        headers[header_key] = header_value
    else:
        raise SerializationError("Invalid headers type")
    return payload


def prefix_schema_id_serializer(payload: bytes, ctx, schema_id) -> bytes:
    """
    Serializes the schema id into the payload prefix.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload prefixed with the schema id
    """
    return schema_id.id_to_bytes() + payload


def dual_schema_id_deserializer(payload: bytes, ctx, schema_id) -> io.BytesIO:
    """
    Deserializes the schema id by first checking the header, then the payload prefix.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    headers = ctx.headers
    header_key = _KEY_SCHEMA_ID if ctx.field == MessageField.KEY else _VALUE_SCHEMA_ID
    if headers is not None:
        header_value = None
        if isinstance(headers, list):
            # look for header_key in headers
            for header in headers:
                if header[0] == header_key:
                    header_value = header[1]
                    break
        elif isinstance(headers, dict):
            header_value = headers.get(header_key, None)
        if header_value is not None:
            schema_id.from_bytes(io.BytesIO(header_value))
            return io.BytesIO(payload)
    return schema_id.from_bytes(io.BytesIO(payload))


def prefix_schema_id_deserializer(payload: bytes, ctx, schema_id) -> io.BytesIO:
    """
    Deserializes the schema id from the payload prefix.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    return schema_id.from_bytes(io.BytesIO(payload))
