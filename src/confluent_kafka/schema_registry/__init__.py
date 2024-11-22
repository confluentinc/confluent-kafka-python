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
import struct

from .schema_registry_client import (RegisteredSchema,
                                     Schema,
                                     SchemaRegistryClient,
                                     SchemaRegistryError,
                                     SchemaReference)

_MAGIC_BYTE = 0

__all__ = ["RegisteredSchema",
           "Schema",
           "SchemaRegistryClient",
           "SchemaRegistryError",
           "SchemaReference",
           "topic_subject_name_strategy",
           "topic_record_subject_name_strategy",
           "record_subject_name_strategy",
           "confluent_payload_framing",
           "apicurio_payload_framing",
           ]


def topic_subject_name_strategy(ctx, record_name):
    """
    Constructs a subject name in the form of {topic}-key|value.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (str): Record name.

    """
    return ctx.topic + "-" + ctx.field


def topic_record_subject_name_strategy(ctx, record_name):
    """
    Constructs a subject name in the form of {topic}-{record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (str): Record name.

    """
    return ctx.topic + "-" + record_name


def record_subject_name_strategy(ctx, record_name):
    """
    Constructs a subject name in the form of {record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (str): Record name.

    """
    return record_name


def reference_subject_name_strategy(ctx, schema_ref):
    """
    Constructs a subject reference name in the form of {reference name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        schema_ref (SchemaReference): SchemaReference instance.

    """
    return schema_ref.name

def confluent_payload_framing(ctx):
    def reader(payload):
        if len(payload) <= 5:
            raise SerializationError("Expecting data framing of length 6 bytes or "
                                     "more but total data size is {} bytes. This "
                                     "message was not produced with a Confluent "
                                     "Schema Registry serializer".format(len(data)))
        magic, schema_id = struct.unpack('>bI', payload.read(5))
        if magic != _MAGIC_BYTE:
            raise SerializationError("Unexpected magic byte {}. This message "
                                     "was not produced with a Confluent "
                                     "Schema Registry serializer".format(magic))
        return schema_id

    def writer(fo, schema_id):
        fo.write(struct.pack('>bI', _MAGIC_BYTE, schema_id))

    return reader, writer

def apicurio_payload_framing(ctx):
    def reader(payload):
        if len(payload) <= 9:
            raise SerializationError("Expecting data framing of length 10 bytes or "
                                     "more but total data size is {} bytes. This "
                                     "message was not produced with an Apicurio "
                                     "Schema Registry serializer".format(len(data)))
        magic, schema_id = struct.unpack('>bq', payload.read(9))
        if magic != _MAGIC_BYTE:
            raise SerializationError("Unexpected magic byte {}. This message "
                                     "was not produced with an Apicurio "
                                     "Schema Registry serializer".format(magic))
        return schema_id

    def writer(fo, schema_id):
        fo.write(struct.pack('>bq', _MAGIC_BYTE, schema_id))

    return reader, writer
