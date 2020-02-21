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

from .cimpl import Consumer as _cConsumer
from .serialization import SerializerError, ValueSerializerError, KeySerializerError


class Consumer(_cConsumer):
    """
    .. py:function:: Consumer(config)

    Producer factory
    """

    def __new__(cls, *args, **kwargs):
        if 'key_serializer' in kwargs or 'value_serializer' in kwargs:
            return super(Consumer, cls).__new__(SerializingConsumer,
                                                kwargs.get('key_serializer', None),
                                                kwargs.get('value_serializer', None))

        return super(Consumer, cls).__new__(cls, *args, **kwargs)


class SerializingConsumer(Consumer):
    __slots__ = ['_key_serializer', '_value_serializer']

    def __new__(cls, conf, key_serializer=None, value_serializer=None):
        raise TypeError("SerializingConsumer is a non user-instantiable class")

    def __init__(self, conf, key_serializer=None, value_serializer=None):
        """

        :param conf:
        :param key_serializer:
        :param value_serializer:
        """

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

        super(SerializingConsumer, self).__init__(conf)

    def poll(self, timeout=-1):
        """

        :param timeout:
        :return:
        """
        msg = super(SerializingConsumer, self).poll(timeout)

        if msg is None:
            return None

        if not msg.error():
            try:
                if self._value_serializer:
                    msg.set_value(self._value_serializer.deserialize(msg.value(), None))
            except SerializerError as se:
                raise ValueSerializerError(se.message)
            try:
                if self._key_serializer:
                    msg.set_key(self._key_serializer.deserialize(msg.key(), None))
            except SerializerError as se:
                raise KeySerializerError(se.message)
        return msg

    def consume(self, num_messages=1, timeout=-1):
        """

        :param num_messages:
        :param timeout:
        :return:
        """
        msglist = super(SerializingConsumer, self).consume(num_messages, timeout)

        for msg in msglist:
            if not msg.error():
                try:
                    msg.set_value(self._value_serializer.deserialize(msg.value()))
                except SerializerError as se:
                    raise ValueSerializerError(se.message)
                try:
                    msg.set_key(self._key_serializer.deserialize(msg.key()))
                except SerializerError as se:
                    raise KeySerializerError(se.message)
            yield msg
