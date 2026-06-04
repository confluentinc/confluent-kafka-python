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

"""
Unit tests for DeserializingShareConsumer.

Broker-free: the deserialization logic is exercised by constructing
``confluent_kafka.cimpl.Message`` objects directly and driving
``DeserializingShareConsumer._deserialize``, plus the offline construction
paths shared with ShareConsumer.

A record that fails to deserialize is marked via ``msg.set_error(...)`` and
kept; ``_deserialize`` never raises. share.acknowledgement.mode is handed
straight to librdkafka and does not change this. On failure the message is left
fully raw (value and key are written back only after *both* deserialize), so a
failed record stays acknowledgeable.
"""

import pytest

from confluent_kafka import KafkaError, ShareConsumer
from confluent_kafka.cimpl import Message
from confluent_kafka.serialization import MessageField, StringDeserializer
from tests.common import TestDeserializingShareConsumer, unique_id


def _make_message(value=None, key=None, topic='t', partition=0, offset=0, headers=None, error=None):
    """Build a Message the way the C layer hands it to poll().

    Positional signature (see tests/test_message.py):
    ``Message(topic, partition, offset, key, value, headers, error, timestamp,
    latency, leader_epoch)``.
    """
    return Message(topic, partition, offset, key, value, headers, error, (0, 0), -1.0, -1)


class _RecordingDeserializer:
    """Deserializer double; records ``(data, topic, field, headers)`` at call time.

    The field is captured eagerly because ``_deserialize`` reuses a single
    :class:`SerializationContext` and flips ``.field`` from VALUE to KEY between
    the value and key calls.
    """

    def __init__(self, result):
        self._result = result
        self.calls = []

    def __call__(self, data, ctx):
        self.calls.append((data, ctx.topic, ctx.field, ctx.headers))
        return self._result


def _boom(_data, _ctx):
    raise ValueError('cannot deserialize')


@pytest.fixture
def make_dsc():
    """Factory for offline DeserializingShareConsumers, closed on teardown.

    Construction does not connect (mirrors tests/test_ShareConsumer.py), so these
    are true unit tests. Pass ``explicit=True`` for explicit acknowledgement mode
    and ``key_deserializer`` / ``value_deserializer`` to populate the
    deserializers under test.
    """
    created = []

    def _make(explicit=False, key_deserializer=None, value_deserializer=None, **extra):
        conf = {'group.id': unique_id('test-dsc'), 'socket.timeout.ms': 100}
        if explicit:
            conf['share.acknowledgement.mode'] = 'explicit'
        if key_deserializer is not None:
            conf['key.deserializer'] = key_deserializer
        if value_deserializer is not None:
            conf['value.deserializer'] = value_deserializer
        conf.update(extra)
        dsc = TestDeserializingShareConsumer(conf)
        created.append(dsc)
        return dsc

    yield _make
    for dsc in created:
        dsc.close()


# --------------------------------------------------------------------------- #
# Construction / configuration
# --------------------------------------------------------------------------- #


def test_subclass_overrides_poll():
    assert issubclass(TestDeserializingShareConsumer, ShareConsumer)
    assert TestDeserializingShareConsumer.poll is not ShareConsumer.poll


def test_constructor_pops_deserializers(make_dsc):
    """key/value.deserializer are stored on the instance and removed from the
    conf before it reaches librdkafka (which would reject the unknown keys)."""
    kd = StringDeserializer()
    vd = StringDeserializer()
    dsc = make_dsc(key_deserializer=kd, value_deserializer=vd)
    assert dsc._key_deserializer is kd
    assert dsc._value_deserializer is vd


def test_constructor_without_deserializers(make_dsc):
    dsc = make_dsc()
    assert dsc._key_deserializer is None
    assert dsc._value_deserializer is None


@pytest.mark.parametrize("mode", [None, 'implicit', 'explicit'])
def test_acknowledgement_mode_is_inert(mode):
    """share.acknowledgement.mode is passed through to librdkafka; the class keeps
    no mode flag and reacts to failures the same either way."""
    conf = {'group.id': unique_id('test-dsc'), 'socket.timeout.ms': 100}
    if mode is not None:
        conf['share.acknowledgement.mode'] = mode
    dsc = TestDeserializingShareConsumer(conf)
    try:
        assert not hasattr(dsc, '_explicit')
    finally:
        dsc.close()


def test_constructor_requires_group_id():
    with pytest.raises(ValueError, match='group.id must be set'):
        TestDeserializingShareConsumer({'bootstrap.servers': 'localhost:9092'})


def test_constructor_rejects_share_incompatible_config():
    """on_commit (a share-incompatible key) is rejected through the subclass."""
    with pytest.raises(ValueError, match='on_commit is not supported'):
        TestDeserializingShareConsumer({'group.id': unique_id('test-dsc'), 'on_commit': lambda *a, **k: None})


# --------------------------------------------------------------------------- #
# Deserialization success (mode-independent)
# --------------------------------------------------------------------------- #


def test_deserialize_value_only(make_dsc):
    dsc = make_dsc(value_deserializer=StringDeserializer())
    msg = _make_message(value=b'hello', key=b'rawkey')
    dsc._deserialize(msg)
    assert msg.value() == 'hello'
    assert msg.key() == b'rawkey'  # untouched: no key deserializer
    assert msg.error() is None


def test_deserialize_key_only(make_dsc):
    dsc = make_dsc(key_deserializer=StringDeserializer())
    msg = _make_message(value=b'rawval', key=b'mykey')
    dsc._deserialize(msg)
    assert msg.key() == 'mykey'
    assert msg.value() == b'rawval'
    assert msg.error() is None


@pytest.mark.parametrize("explicit", [False, True])
def test_deserialize_both(make_dsc, explicit):
    dsc = make_dsc(
        explicit=explicit,
        key_deserializer=StringDeserializer(),
        value_deserializer=StringDeserializer(),
    )
    msg = _make_message(value=b'v', key=b'k')
    dsc._deserialize(msg)
    assert msg.key() == 'k'
    assert msg.value() == 'v'
    assert msg.error() is None


def test_deserialize_passthrough_without_deserializers(make_dsc):
    dsc = make_dsc()
    msg = _make_message(value=b'v', key=b'k')
    dsc._deserialize(msg)
    assert msg.value() == b'v'
    assert msg.key() == b'k'
    assert msg.error() is None


def test_deserialize_serialization_context(make_dsc):
    """Deserializers receive the right SerializationContext: same topic and
    headers, field VALUE for the value and KEY for the key."""
    headers = [('h', b'1')]
    kd = _RecordingDeserializer('K')
    vd = _RecordingDeserializer('V')
    dsc = make_dsc(key_deserializer=kd, value_deserializer=vd)
    msg = _make_message(value=b'v', key=b'k', topic='my-topic', headers=headers)

    dsc._deserialize(msg)

    assert vd.calls == [(b'v', 'my-topic', MessageField.VALUE, headers)]
    assert kd.calls == [(b'k', 'my-topic', MessageField.KEY, headers)]


def test_deserialize_none_payload_passed_through(make_dsc):
    """A tombstone (None value/key) is still handed to the deserializer, matching
    DeserializingConsumer; StringDeserializer returns None for None."""
    dsc = make_dsc(key_deserializer=StringDeserializer(), value_deserializer=StringDeserializer())
    msg = _make_message(value=None, key=None)
    dsc._deserialize(msg)
    assert msg.value() is None
    assert msg.key() is None
    assert msg.error() is None


# --------------------------------------------------------------------------- #
# Failure handling -- a bad record is marked and kept, never raised
# --------------------------------------------------------------------------- #
# Parametrized over both modes to pin down that share.acknowledgement.mode has no
# bearing on how _deserialize reacts to a failure.


@pytest.mark.parametrize("explicit", [False, True])
def test_value_failure_marks_error(make_dsc, explicit):
    dsc = make_dsc(explicit=explicit, value_deserializer=_boom)
    msg = _make_message(value=b'raw', key=b'k')
    dsc._deserialize(msg)  # must not raise
    assert msg.error().code() == KafkaError._VALUE_DESERIALIZATION
    assert msg.value() == b'raw'  # raw bytes preserved


@pytest.mark.parametrize("explicit", [False, True])
def test_key_failure_marks_error(make_dsc, explicit):
    dsc = make_dsc(explicit=explicit, key_deserializer=_boom)
    msg = _make_message(value=b'v', key=b'raw')
    dsc._deserialize(msg)
    assert msg.error().code() == KafkaError._KEY_DESERIALIZATION
    assert msg.key() == b'raw'


@pytest.mark.parametrize("explicit", [False, True])
def test_none_topic_marks_error(make_dsc, explicit):
    dsc = make_dsc(explicit=explicit, value_deserializer=StringDeserializer())
    msg = _make_message(value=b'v', topic=None)
    dsc._deserialize(msg)
    assert msg.error().code() == KafkaError._VALUE_DESERIALIZATION


@pytest.mark.parametrize("explicit", [False, True])
def test_failure_preserves_coordinates(make_dsc, explicit):
    # acks are keyed by topic/partition/offset, so a marked record has to stay a
    # valid acknowledge target.
    dsc = make_dsc(explicit=explicit, value_deserializer=_boom)
    msg = _make_message(value=b'raw', topic='t', partition=4, offset=99)
    dsc._deserialize(msg)
    assert (msg.topic(), msg.partition(), msg.offset()) == ('t', 4, 99)


@pytest.mark.parametrize("explicit", [False, True])
def test_key_failure_does_not_write_back_value(make_dsc, explicit):
    # value decodes but key fails; both are written back only after both succeed,
    # so value() must stay raw.
    dsc = make_dsc(explicit=explicit, value_deserializer=StringDeserializer(), key_deserializer=_boom)
    msg = _make_message(value=b'hello', key=b'k')
    dsc._deserialize(msg)
    assert msg.error().code() == KafkaError._KEY_DESERIALIZATION
    assert msg.value() == b'hello'
    assert msg.key() == b'k'


@pytest.mark.parametrize("explicit", [False, True])
def test_value_failure_short_circuits_key(make_dsc, explicit):
    # a value failure should bail before touching the key deserializer.
    kd = _RecordingDeserializer('K')
    dsc = make_dsc(explicit=explicit, value_deserializer=_boom, key_deserializer=kd)
    msg = _make_message(value=b'v', key=b'k')
    dsc._deserialize(msg)
    assert msg.error().code() == KafkaError._VALUE_DESERIALIZATION
    assert kd.calls == []


# --------------------------------------------------------------------------- #
# poll() basics (broker-free)
# --------------------------------------------------------------------------- #


def test_poll_returns_empty_list_on_timeout(make_dsc):
    """With no broker, poll returns an empty list (not None)."""
    dsc = make_dsc(value_deserializer=StringDeserializer())
    dsc.subscribe(['nonexistent-topic'])
    out = dsc.poll(timeout=0.1)
    assert out == []
    assert isinstance(out, list)
