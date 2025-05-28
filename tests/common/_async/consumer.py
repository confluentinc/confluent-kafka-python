#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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

import asyncio

from confluent_kafka.cimpl import Consumer
from confluent_kafka.error import ConsumeError, KeyDeserializationError, ValueDeserializationError
from confluent_kafka.serialization import MessageField, SerializationContext

ASYNC_CONSUMER_POLL_INTERVAL_SECONDS: int = 0.2
ASYNC_CONSUMER_POLL_INFINITE_TIMEOUT_SECONDS: int = -1


class AsyncConsumer(Consumer):
    def __init__(
        self,
        conf: dict,
        loop: asyncio.AbstractEventLoop = None,
        poll_interval_seconds: int = ASYNC_CONSUMER_POLL_INTERVAL_SECONDS
    ):
        super().__init__(conf)

        self._loop = loop or asyncio.get_event_loop()
        self._poll_interval = poll_interval_seconds

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.poll(None)

    async def poll(self, timeout: int = -1):
        timeout = None if timeout == -1 else timeout
        async with asyncio.timeout(timeout):
            while True:
                # Zero timeout here is what makes it non-blocking
                msg = super().poll(0)
                if msg is not None:
                    return msg
                else:
                    await asyncio.sleep(self._poll_interval)


class TestAsyncDeserializingConsumer(AsyncConsumer):
    def __init__(self, conf):
        conf_copy = conf.copy()
        self._key_deserializer = conf_copy.pop('key.deserializer', None)
        self._value_deserializer = conf_copy.pop('value.deserializer', None)
        super().__init__(conf_copy)

    async def poll(self, timeout=-1):
        msg = await super().poll(timeout)

        if msg is None:
            return None

        if msg.error() is not None:
            raise ConsumeError(msg.error(), kafka_message=msg)

        ctx = SerializationContext(msg.topic(), MessageField.VALUE, msg.headers())
        value = msg.value()
        if self._value_deserializer is not None:
            try:
                value = await self._value_deserializer(value, ctx)
            except Exception as se:
                raise ValueDeserializationError(exception=se, kafka_message=msg)

        key = msg.key()
        ctx.field = MessageField.KEY
        if self._key_deserializer is not None:
            try:
                key = await self._key_deserializer(key, ctx)
            except Exception as se:
                raise KeyDeserializationError(exception=se, kafka_message=msg)

        msg.set_key(key)
        msg.set_value(value)
        return msg

    def consume(self, num_messages=1, timeout=-1):
        """
        :py:func:`Consumer.consume` not implemented, use
        :py:func:`DeserializingConsumer.poll` instead
        """

        raise NotImplementedError
