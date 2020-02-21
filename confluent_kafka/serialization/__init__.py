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

from .avro import AvroSerializer
from .error import SerializerError, KeySerializerError, ValueSerializerError
from .kafka_builtins import DoubleSerializer, FloatSerializer, \
    LongSerializer, IntegerSerializer, ShortSerializer, StringSerializer

__all__ = ["MessageField", "SerializationContext",
           "SerializerError", 'KeySerializerError', 'ValueSerializerError',
           'AvroSerializer', 'DoubleSerializer', 'FloatSerializer',
           'LongSerializer', 'IntegerSerializer', 'ShortSerializer',
           'StringSerializer']

"""
All serializers must conform to the following interface

class Serializer(object):
    __slots__ = []

    def serialize(self, datum, ctx):
        # encode stuff
        return bytes

    def deserialize(self, data, ctx):
        # decode stuff
        return decoded object
"""


class MessageField(object):
    NONE = 0
    KEY = 1
    VALUE = 2
    _str = ("none", "key", "value")

    @staticmethod
    def __str__(field):
        return MessageField._str[field - 1]


class SerializationContext(object):
    __slots__ = ["topic", "field"]

    def __init__(self, topic, field=0):
        self.topic = topic
        self.field = field
