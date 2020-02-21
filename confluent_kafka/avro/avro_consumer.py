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

from .cached_schema_registry_client import CachedSchemaRegistryClient
from .serializer import SerializerError
from confluent_kafka import Consumer
from confluent_kafka.serialization import AvroSerializer


class AvroConsumer(Consumer):
    """
    Kafka Consumer client which does avro schema decoding of messages.
    Handles message deserialization.

    Constructor takes below parameters

    :param dict config: Config parameters containing url for schema registry (``schema.registry.url``)
                        and the standard Kafka client configuration (``bootstrap.servers`` et.al)
    :param Schema reader_key_schema: a reader schema for the message key
    :param Schema reader_value_schema: a reader schema for the message value
    :raises ValueError: For invalid configurations
    """
    __slots__ = ['_key_serializer', '_value_serializer']

    def __init__(self, config, schema_registry=None,
                 reader_key_schema=None, reader_value_schema=None):

        sr_conf = {key.replace("schema.registry.", ""): value
                   for key, value in config.items() if key.startswith("schema.registry")}

        if sr_conf.get("basic.auth.credentials.source") == 'SASL_INHERIT':
            sr_conf['sasl.mechanisms'] = config.get('sasl.mechanisms', '')
            sr_conf['sasl.username'] = config.get('sasl.username', '')
            sr_conf['sasl.password'] = config.get('sasl.password', '')

        ap_conf = {key: value
                   for key, value in config.items() if not key.startswith("schema.registry")}

        if schema_registry is None:
            schema_registry = CachedSchemaRegistryClient(sr_conf)
        elif sr_conf.get("url", None) is not None:
            raise ValueError("Cannot pass schema_registry along with schema.registry.url config")

        self._key_serializer = AvroSerializer(schema_registry, reader_schema=reader_key_schema)
        self._value_serializer = AvroSerializer(schema_registry, reader_schema=reader_value_schema)

        super(AvroConsumer, self).__init__(ap_conf)

    def poll(self, timeout=-1.0):
        """
        This is an overridden method from confluent_kafka.Consumer class. This handles message
        deserialization using Avro schema

        :param float timeout: Poll timeout in seconds (default: indefinite)
        :returns: message object with deserialized key and value as dict objects
        :rtype: Message
        """

        message = super(AvroConsumer, self).poll(timeout)
        if message is None:
            return None

        if not message.error():
            try:
                message.set_value(self._value_serializer.deserialize(message.value(), None))
                message.set_key(self._key_serializer.deserialize(message.key(), None))
            except SerializerError as e:
                raise SerializerError("Message deserialization failed for message at {} [{}] "
                                      "offset {}: {}".format(message.topic(),
                                                             message.partition(),
                                                             message.offset(), e))
        return message
