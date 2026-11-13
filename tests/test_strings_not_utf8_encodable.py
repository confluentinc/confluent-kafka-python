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
Strings that cannot be encoded as UTF-8, such as a str with a lone surrogate
(os.fsdecode() of bytes that are not UTF-8 makes them), have no UTF-8 data:
the C code must raise the UnicodeEncodeError instead of passing a NULL pointer
on. Before, most of these calls crashed the process, so they run in a
subprocess.
"""

import sys

import pytest

from confluent_kafka import Consumer, Message, Producer, ShareConsumer, TopicCollection
from confluent_kafka.admin import (
    AdminClient,
    ConfigResource,
    NewTopic,
    ResourceType,
    ScramMechanism,
    UserScramCredentialDeletion,
)
from tests.concurrency._subprocess_isolation import subprocess_isolated

NOT_UTF8 = 'name-\ud800'
_CONF = {'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}


@subprocess_isolated
def test_admin_client_names():
    admin = AdminClient(_CONF)
    with pytest.raises(UnicodeEncodeError):
        admin.delete_topics([NOT_UTF8])
    with pytest.raises(UnicodeEncodeError):
        admin.describe_topics(TopicCollection([NOT_UTF8]))
    with pytest.raises(UnicodeEncodeError):
        admin.create_topics([NewTopic('t', num_partitions=1, replication_factor=1, config={NOT_UTF8: 'x'})])
    with pytest.raises(UnicodeEncodeError):
        admin.alter_configs([ConfigResource(ResourceType.TOPIC, 't', set_config={NOT_UTF8: 'x'})])
    with pytest.raises(UnicodeEncodeError):
        admin.alter_user_scram_credentials([UserScramCredentialDeletion(NOT_UTF8, ScramMechanism.SCRAM_SHA_256)])


@subprocess_isolated
def test_producer_configuration_and_headers():
    with pytest.raises(UnicodeEncodeError):
        Producer({NOT_UTF8: 'x'})
    with pytest.raises(UnicodeEncodeError):
        Producer({'client.id': NOT_UTF8})
    producer = Producer(_CONF)
    with pytest.raises(UnicodeEncodeError):
        producer.produce('t', value=b'v', headers=[(NOT_UTF8, b'v')])
    with pytest.raises(UnicodeEncodeError):
        producer.produce('t', value=b'v', headers={NOT_UTF8: b'v'})


def test_produce_batch_value_and_key():
    """produce_batch() raised the error only after producing the message,
    with an empty value or key."""
    producer = Producer(_CONF)
    for message in ({'value': NOT_UTF8}, {'value': b'v', 'key': NOT_UTF8}):
        with pytest.raises(UnicodeEncodeError):
            producer.produce_batch('t', [message])
        assert len(producer) == 0


@subprocess_isolated
def test_consumer_topics():
    consumer = Consumer(dict(_CONF, **{'group.id': 'g', 'enable.auto.offset.store': False}))
    with pytest.raises(UnicodeEncodeError):
        consumer.subscribe([NOT_UTF8])
    message = Message(topic=NOT_UTF8, partition=0, offset=1)
    with pytest.raises(UnicodeEncodeError):
        consumer.store_offsets(message=message)
    with pytest.raises(UnicodeEncodeError):
        consumer.commit(message=message, asynchronous=False)


@subprocess_isolated
def test_share_consumer_topics():
    share_consumer = ShareConsumer(dict(_CONF, **{'group.id': 'g'}))
    try:
        with pytest.raises(UnicodeEncodeError):
            share_consumer.subscribe([NOT_UTF8])
    finally:
        share_consumer.close()


def test_alter_user_scram_credentials_error_on_later_item_keeps_user_references():
    """The error path released the user name of the previous alteration a
    second time."""
    user = ''.join(['user', '-1'])
    alterations = [UserScramCredentialDeletion(user, ScramMechanism.SCRAM_SHA_256), object()]
    before = sys.getrefcount(user)
    with pytest.raises(TypeError):
        super(AdminClient, AdminClient(_CONF)).alter_user_scram_credentials(alterations, AdminClient._create_future())
    assert sys.getrefcount(user) == before
