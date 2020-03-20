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

# from .schema_registry_client import SchemaRegistryClient, CompatibilityType
# from confluent_kafka.serialization.serdes import RecordNameStrategy, \
#     TopicNameStrategy, TopicRecordNameStrategy
#
import io

_MAGIC_BYTE = 0


class MessageField(object):
    """
    Enum like object for identifying Message fields.

    Attributes:
        NONE (int): None
        KEY (int): Message key
        VALUE (int): Message value

    """
    NONE = 0
    KEY = 1
    VALUE = 2
    _str = ("none", "key", "value")

    @staticmethod
    def __str__(field):
        """
        Returns a string representation for a MessageField value.

        Args:
            field (MessageField): MessageField value

        Returns:
             String value for MessageField

        """
        return MessageField._str[field]


class SerializationContext(object):
    """
    SerializationContext provides additional context to the serializer about
    the data it's serializing.

    Args:
        - topic (str): Topic the serialized data will be sent to
        - field (MessageField): Describes what part of the message is
            being serialized.

    """
    __slots__ = ["topic", "field"]

    def __init__(self, topic, field):
        self.topic = topic
        self.field = field


class SubjectNameStrategy(object):
    """
    Instructs the Serializer on how to construct subject names when registering
    schemas. See the Schema Registry documentation for additional details.

    .. _SchemaRegistry Documentation:
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#group-by-topic-or-other-relationships

    """
    def __call__(self, schema, ctx):
        raise NotImplementedError

    @classmethod
    def __str__(cls, strategy):
        return cls._str[strategy]


class TopicNameStrategy(SubjectNameStrategy):
    """
    Constructs a subject name in the form of {topic}={key|value}.

    Args:
        schema (Schema): Schema associated with a subject
        ctx (SerializationContext): Serialization context.

    """

    def __call__(self, schema, ctx):
        return ctx.topic + "-" + str(ctx.field)


class TopicRecordNameStrategy(SubjectNameStrategy):
    """
    Constructs a subject name in the form of {topic}={record.name}.

    Args:
        schema (Schema): Schema associated with a subject
        ctx (SerializationContext): Serialization context.

    """

    def __call__(self, schema, ctx):
        return ctx.topic + "-" + schema.name


class RecordNameStrategy(SubjectNameStrategy):
    """
    Constructs a subject name in the form of {record.name}.

    Args:
        schema (Schema): Schema associated with a subject
        ctx (SerializationContext): Serialization context.

    """

    def __call__(self, schema, ctx):
        return schema.name


class ReferenceSubjectNameStrategy(SubjectNameStrategy):
    """
    Constructs a subject reference name in the form of {reference.name}.

    Args:
        reference (SchemaReference): SchemaReference associated with some Schema.
        ctx (SerializationContext): Serialization context.
    """
    def __call__(self, reference, ctx):
        return reference.name


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.

    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False
