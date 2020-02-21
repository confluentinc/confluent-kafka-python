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

from .cimpl import Producer as _cProducer
from .serialization import SerializationContext, MessageField


class Producer(_cProducer):
    """
    .. py:function:: Producer(config)

    Producer factory
    """

    def __new__(cls, *args, **kwargs):
        if 'key_serializer' in kwargs or 'value_serializer' in kwargs:
            return super(Producer, cls).__new__(SerializingProducer,
                                                *args, **kwargs)

        return super(Producer, cls).__new__(cls, *args, **kwargs)


class SerializingProducer(Producer):
    __slots__ = ['_key_serializer', '_value_serializer']

    def __new__(cls, conf, key_serializer=None, value_serializer=None):
        raise TypeError("SerializingProducer is a non user-instantiable class")

    def __init__(self, conf, key_serializer=None, value_serializer=None):
        """

        :param conf:
        :param key_serializer:
        :param value_serializer:
        """

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

        super(SerializingProducer, self).__init__(conf)

    def produce(self, topic, value=None, key=None, partition=-1,
                on_delivery=None, timestamp=0, headers=None):

        ctx = SerializationContext(topic, MessageField.KEY)
        if self._key_serializer is not None:
            key = self._key_serializer.serialize(key, ctx)
        if self._value_serializer:
            value = self._value_serializer.serialize(value, ctx)

        super(SerializingProducer, self).produce(topic, value, key,
                                                 headers=headers,
                                                 partition=partition,
                                                 timestamp=timestamp,
                                                 on_delivery=on_delivery)
