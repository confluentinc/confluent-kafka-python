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

"""
Reference counting of type objects in the C extension.

The C code compared and printed object types through PyObject_Type(), which
returns a new reference, so every such check leaked one reference to the type.
These tests count the references to a type around repeated calls.

TopicPartition and Message are static types. Python 3.12 and later make those
immortal and ignore their reference counts, so the tests for them can only
catch a leak on Python 3.11 and older. Classes defined in Python are heap
types, which are never immortal, so the error-path tests catch a leak on every
version.
"""

import gc
import os
import subprocess
import sys
import textwrap

import pytest

from confluent_kafka import KafkaException, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType
from tests.common import TestConsumer

CALLS = 100


class _Unrelated:
    """An instance of a heap type that no C code accepts."""


def _type_ref_growth(tp, call):
    """Return how many references to tp CALLS calls of call() gained."""
    # Collect garbage first and keep the collector off while counting: when it
    # frees a dead subclass of tp made by another test, tp loses references.
    gc.collect()
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        before = sys.getrefcount(tp)
        for _ in range(CALLS):
            call()
        return sys.getrefcount(tp) - before
    finally:
        if gc_was_enabled:
            gc.enable()


def _ignoring_kafka_errors(call):
    def wrapper():
        try:
            call()
        except KafkaException:
            # There is no broker: only the argument checks matter here.
            pass

    return wrapper


def _raising(exc_type, call):
    def wrapper():
        with pytest.raises(exc_type):
            call()

    return wrapper


@pytest.fixture
def consumer():
    c = TestConsumer(
        {
            'group.id': 'test',
            'enable.auto.offset.store': False,
            'socket.timeout.ms': 50,
            'session.timeout.ms': 100,
        }
    )
    yield c
    c.close()


TOPIC_PARTITION_CALLS = {
    # These go through py_to_c_parts().
    'assign': lambda c, tp: c.assign([tp]),
    'pause': lambda c, tp: c.pause([tp]),
    'resume': lambda c, tp: c.resume([tp]),
    'store_offsets': lambda c, tp: c.store_offsets(offsets=[tp]),
    'commit': lambda c, tp: c.commit(offsets=[tp], asynchronous=True),
    # These check a single TopicPartition themselves.
    'seek': lambda c, tp: c.seek(tp),
    'get_watermark_offsets': lambda c, tp: c.get_watermark_offsets(tp, cached=True),
}


@pytest.mark.parametrize('method', sorted(TOPIC_PARTITION_CALLS))
def test_topic_partition_type_check_does_not_leak(consumer, method):
    tp = TopicPartition('test', 0, 42)
    call = TOPIC_PARTITION_CALLS[method]
    assert _type_ref_growth(TopicPartition, _ignoring_kafka_errors(lambda: call(consumer, tp))) == 0


MESSAGE_CALLS = {
    'store_offsets': lambda c, msg: c.store_offsets(message=msg),
    'commit': lambda c, msg: c.commit(message=msg, asynchronous=True),
}


@pytest.mark.parametrize('method', sorted(MESSAGE_CALLS))
def test_message_type_check_does_not_leak(consumer, method):
    msg = Message('test', 0, 42)
    call = MESSAGE_CALLS[method]
    assert _type_ref_growth(Message, _ignoring_kafka_errors(lambda: call(consumer, msg))) == 0


@pytest.mark.parametrize(
    'make_headers',
    [lambda v: [('k', v)], lambda v: {'k': v}, lambda v: v],
    ids=['list-value', 'dict-value', 'headers'],
)
def test_header_type_error_does_not_leak(make_headers):
    p = Producer({'socket.timeout.ms': 10, 'message.timeout.ms': 10})
    headers = make_headers(_Unrelated())
    call = _raising(TypeError, lambda: p.produce('test', headers=headers))
    assert _type_ref_growth(_Unrelated, call) == 0


def test_attribute_type_error_does_not_leak():
    a = AdminClient({'socket.timeout.ms': 10})
    resource = ConfigResource(ResourceType.TOPIC, 'test')
    resource.restype_int = _Unrelated()
    call = _raising(TypeError, lambda: a.describe_configs([resource]))
    assert _type_ref_growth(_Unrelated, call) == 0


def test_bool_option_type_error_does_not_leak():
    a = AdminClient({'socket.timeout.ms': 10})
    value = _Unrelated()
    call = _raising(TypeError, lambda: a.create_topics([NewTopic('test', 1, 1)], validate_only=value))
    assert _type_ref_growth(_Unrelated, call) == 0


_FRESH_ATTRIBUTE_SCRIPT = textwrap.dedent("""
    from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

    class Fresh:
        pass

    class FreshRestype(ConfigResource):
        # Every read returns a new object that only the reader references.
        restype_int = property(lambda self: Fresh(), lambda self, value: None)

    admin = AdminClient({'socket.timeout.ms': 10})
    try:
        admin.describe_configs([FreshRestype(ResourceType.TOPIC, 'test')])
    except TypeError as e:
        print(e)
    """)


def test_attribute_type_error_names_type_before_release():
    """
    The error for an attribute of the wrong type must read the attribute's
    type before dropping the reference to it: when that is the last reference,
    the attribute is freed. PYTHONMALLOC=debug overwrites freed memory, so
    reading it afterwards crashes the subprocess.
    """
    result = subprocess.run(
        [sys.executable, '-c', _FRESH_ATTRIBUTE_SCRIPT],
        capture_output=True,
        text=True,
        timeout=60,
        env=dict(os.environ, PYTHONMALLOC='debug'),
    )
    assert result.returncode == 0, f'returncode={result.returncode}\nstderr:\n{result.stderr}'
    assert result.stdout.strip() == 'Expected .restype_int to be int type, not Fresh'
