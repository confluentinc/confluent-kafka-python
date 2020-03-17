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

from confluent_kafka.schema_registry.serdes import \
    AvroSerializer, AvroDeserializer
from .error import KeyDeserializationError, KeySerializationError,\
    SerializationError, ValueDeserializationError, ValueSerializationError
from .kafka_serdes import Deserializer,\
    DoubleSerializer, DoubleDeserializer,\
    IntegerSerializer, IntegerDeserializer,\
    Serializer, StringSerializer, StringDeserializer

__all__ = ['AvroSerializer', 'AvroDeserializer',
           'Deserializer', 'DoubleSerializer', 'DoubleDeserializer',
           'IntegerSerializer', 'IntegerDeserializer',
           'KeyDeserializationError', 'KeySerializationError',
           'MessageField', 'Serializer', 'SerializationContext',
           'SerializationError', 'StringSerializer', 'StringDeserializer',
           'ValueDeserializationError', 'ValueSerializationError']


class MessageField(object):
    """
    Enum like object for identifying Message fields.

    Attributes:
        NONE (int): Unknown
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
        topic (str): Topic the serialized data will be sent to

    Keyword Args:
        field (MessageField, optional): Describes what part of the message is
            being serialized.

    """
    __slots__ = ["topic", "field"]

    def __init__(self, topic, field=MessageField.NONE):
        self.topic = topic
        self.field = field
