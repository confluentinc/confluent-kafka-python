#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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
"""Unit tests for DeserializingConsumer.

Broker-free: the deserialization logic (``DeserializingConsumer._deserialize``)
is exercised by constructing ``confluent_kafka.cimpl.Message`` objects directly.
The key is deserialized before the value so a key deserializer can stash the
original key for the value deserializer (used by the Schema Registry
dead-letter-queue rule action).
"""

import pytest

from confluent_kafka import DeserializingConsumer
from confluent_kafka.cimpl import Message
from confluent_kafka.error import KeyDeserializationError, ValueDeserializationError
from confluent_kafka.serialization import MessageField, StringDeserializer


def _make_message(value=None, key=None, topic='t', partition=0, offset=0, headers=None):
    # Positional signature matches tests/test_message.py.
    return Message(topic, partition, offset, key, value, headers, None, (0, 0), -1.0, -1)


class _RecordingDeserializer:
    """Deserializer double; records ``(data, ctx.field)`` at call time."""

    def __init__(self, result):
        self._result = result
        self.calls = []

    def __call__(self, data, ctx):
        self.calls.append((data, ctx.field))
        return self._result


def _boom(_data, _ctx):
    raise ValueError('cannot deserialize')


@pytest.fixture
def make_dc():
    """Factory for offline DeserializingConsumers (construction does not connect)."""

    def _make(key_deserializer=None, value_deserializer=None):
        conf = {'group.id': 'test-dc', 'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}
        if key_deserializer is not None:
            conf['key.deserializer'] = key_deserializer
        if value_deserializer is not None:
            conf['value.deserializer'] = value_deserializer
        return DeserializingConsumer(conf)

    return _make


def test_deserialize_both(make_dc):
    dc = make_dc(key_deserializer=StringDeserializer(), value_deserializer=StringDeserializer())
    msg = dc._deserialize(_make_message(value=b'v', key=b'k'))
    assert msg.key() == 'k'
    assert msg.value() == 'v'


def test_deserialize_processes_key_before_value(make_dc):
    # the key must be deserialized before the value so a key deserializer can
    # stash the original key for the value deserializer (e.g. the SR DLQ action).
    order = []

    def kd(_data, _ctx):
        order.append('key')
        return 'K'

    def vd(_data, _ctx):
        order.append('value')
        return 'V'

    dc = make_dc(key_deserializer=kd, value_deserializer=vd)
    dc._deserialize(_make_message(value=b'v', key=b'k'))
    assert order == ['key', 'value']


def test_deserialize_serialization_context_fields(make_dc):
    kd = _RecordingDeserializer('K')
    vd = _RecordingDeserializer('V')
    dc = make_dc(key_deserializer=kd, value_deserializer=vd)
    dc._deserialize(_make_message(value=b'v', key=b'k'))
    assert kd.calls == [(b'k', MessageField.KEY)]
    assert vd.calls == [(b'v', MessageField.VALUE)]


def test_key_failure_short_circuits_value(make_dc):
    # the key is deserialized first, so a key failure bails before the value.
    vd = _RecordingDeserializer('V')
    dc = make_dc(key_deserializer=_boom, value_deserializer=vd)
    with pytest.raises(KeyDeserializationError):
        dc._deserialize(_make_message(value=b'v', key=b'k'))
    assert vd.calls == []


def test_value_failure_raises(make_dc):
    dc = make_dc(key_deserializer=StringDeserializer(), value_deserializer=_boom)
    with pytest.raises(ValueDeserializationError):
        dc._deserialize(_make_message(value=b'v', key=b'k'))


def test_none_topic_raises_type_error(make_dc):
    dc = make_dc(value_deserializer=StringDeserializer())
    with pytest.raises(TypeError, match='Message topic is None'):
        dc._deserialize(_make_message(value=b'v', topic=None))
