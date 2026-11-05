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

from ..serialization import MessageField, SerializationContext, SerializationError
from .schema_registry_client import (
    Association,
    AssociationCreateOrUpdateInfo,
    AssociationCreateOrUpdateRequest,
    AssociationInfo,
    AssociationResponse,
    AsyncSchemaRegistryClient,
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
    SchemaReference,
    SchemaRegistryClient,
    SchemaRegistryError,
    ServerConfig,
)

_KEY_SCHEMA_ID = "__key_schema_id"
_VALUE_SCHEMA_ID = "__value_schema_id"

_MAGIC_BYTE = 0
_MAGIC_BYTE_V0 = _MAGIC_BYTE
_MAGIC_BYTE_V1 = 1

__all__ = [
    "Association",
    "AssociationCreateOrUpdateInfo",
    "AssociationCreateOrUpdateRequest",
    "AssociationInfo",
    "AssociationResponse",
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
    "prefix_schema_id_deserializer",
]


def topic_subject_name_strategy(ctx: Optional[SerializationContext], record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-key|value.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Required** - will raise ValueError if None.

        record_name (Optional[str]): Record name.

    Raises:
        ValueError: If ctx is None.

    """
    if ctx is None:
        raise ValueError(
            "SerializationContext is required for topic_subject_name_strategy. "
            "Either provide a SerializationContext or use record_subject_name_strategy."
        )
    return ctx.topic + "-" + ctx.field


def topic_record_subject_name_strategy(
    ctx: Optional[SerializationContext], record_name: Optional[str]
) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-{record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Required** - will raise ValueError if None.

        record_name (Optional[str]): Record name.

    Raises:
        ValueError: If ctx is None.

    """
    if ctx is None:
        raise ValueError(
            "SerializationContext is required for topic_record_subject_name_strategy. "
            "Either provide a SerializationContext or use record_subject_name_strategy."
        )
    return ctx.topic + "-" + record_name if record_name is not None else None


def record_subject_name_strategy(ctx: Optional[SerializationContext], record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Not used** by this strategy.

        record_name (Optional[str]): Record name.

    Note:
        This strategy does not require SerializationContext and can be used
        when ctx is None.

    """
    return record_name if record_name is not None else None


def reference_subject_name_strategy(ctx: Optional[SerializationContext], schema_ref: SchemaReference) -> Optional[str]:
    """
    Constructs a subject reference name in the form of {reference name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Not used** by this strategy.

        schema_ref (SchemaReference): SchemaReference instance.

    Note:
        This strategy does not require SerializationContext and can be used
        when ctx is None.

    """
    return schema_ref.name if schema_ref is not None else None


def _schema_id_header_key(ctx: SerializationContext) -> str:
    return _KEY_SCHEMA_ID if ctx.field == MessageField.KEY else _VALUE_SCHEMA_ID


def _remove_schema_id_header(headers, header_key: str) -> None:
    """
    Removes every header named header_key, in place, so a message never
    carries a stale schema id header (for example one copied from an inbound
    message) alongside the schema id written by the serializer.
    """
    if isinstance(headers, list):
        headers[:] = [header for header in headers if header[0] != header_key]
    elif isinstance(headers, dict):
        headers.pop(header_key, None)


def header_schema_id_serializer(payload: bytes, ctx: Optional[SerializationContext], schema_id) -> bytes:
    """
    Serializes the schema guid into the header.

    Any existing schema id header for the same field is replaced.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    if ctx is None:
        raise SerializationError("SerializationContext is required for header_schema_id_serializer")

    headers = ctx.headers
    if headers is None:
        raise SerializationError("Missing headers")
    if not isinstance(headers, (list, dict)):
        raise SerializationError("Invalid headers type")
    header_key = _schema_id_header_key(ctx)
    header_value = schema_id.guid_to_bytes()
    _remove_schema_id_header(headers, header_key)
    if isinstance(headers, list):
        headers.append((header_key, header_value))
    else:
        headers[header_key] = header_value
    return payload


def prefix_schema_id_serializer(payload: bytes, ctx, schema_id) -> bytes:
    """
    Serializes the schema id into the payload prefix.

    Any existing schema id header for the same field is removed from
    ``ctx.headers``, since a deserializer would otherwise read the schema id
    from that header and decode the prefix as part of the data.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload prefixed with the schema id
    """
    if ctx is not None and ctx.headers is not None:
        _remove_schema_id_header(ctx.headers, _schema_id_header_key(ctx))
    return schema_id.id_to_bytes() + payload


def dual_schema_id_deserializer(payload: bytes, ctx: Optional[SerializationContext], schema_id) -> io.BytesIO:
    """
    Deserializes the schema id by first checking the header, then the payload prefix.

    If the header appears more than once, the last occurrence is used, matching
    the Java client.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    # Look for schema ID in headers
    header_value = None

    if ctx is not None:
        headers = ctx.headers
        if headers is not None:
            header_key = _schema_id_header_key(ctx)
            if isinstance(headers, list):
                # use the last header_key in headers, like Java's Headers.lastHeader()
                for header in reversed(headers):
                    if header[0] == header_key:
                        header_value = header[1]
                        break
            elif isinstance(headers, dict):
                header_value = headers.get(header_key, None)

    # Parse schema ID from determined source and return appropriate payload
    if header_value is not None:
        schema_id.from_bytes(io.BytesIO(header_value))  # type: ignore[arg-type]
        return io.BytesIO(payload)  # Return full payload when schema ID is in header
    else:
        return schema_id.from_bytes(io.BytesIO(payload))  # Parse from payload, return remainder


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
