#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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

import os
import signal
import time
import uuid

from confluent_kafka import Consumer, ShareConsumer
from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource

_GROUP_PROTOCOL_ENV = 'TEST_CONSUMER_GROUP_PROTOCOL'

DEFAULT_BOOTSTRAP_SERVERS = 'localhost:9092'


class TestUtils:
    @staticmethod
    def send_sigint_after_delay(delay_seconds):
        """Send SIGINT to current process after delay.

        Utility function for testing interruptible poll/flush/consume operations.
        Used to simulate Ctrl+C in automated tests.

        Args:
            delay_seconds: Delay in seconds before sending SIGINT
        """
        time.sleep(delay_seconds)
        os.kill(os.getpid(), signal.SIGINT)

    # TODO KIP-932: broker_version() previously branched on
    # use_group_protocol_consumer() to return '4.0.0' or '3.9.0'. It is now
    # hardcoded to '4.2.0' because share groups require >=4.2.0.
    # Remove this method if not needed in other contexts
    @staticmethod
    def broker_version():
        return '4.2.0'

    @staticmethod
    def broker_conf():
        return [
            'transaction.state.log.replication.factor=1',
            'transaction.state.log.min.isr=1',
            # Single-broker cluster: __consumer_offsets defaults to RF=3.
            # Required for classic and KIP-848 consumer offset commits.
            'offsets.topic.replication.factor=1',
            'offsets.topic.min.isr=1',
            # KIP-932: __share_group_state topic defaults are RF=3 / min.isr=2
            # — must be 1/1 on a single-broker test cluster.
            'share.coordinator.state.topic.replication.factor=1',
            'share.coordinator.state.topic.min.isr=1',
            # KIP-932: shorten lock duration to 1s for fast redelivery tests.
            # Both must be set: actual duration must be >= min (default min=15000).
            'group.share.record.lock.duration.ms=1000',
            'group.share.min.record.lock.duration.ms=1000',
        ]

    # TODO KIP-932: use_kraft() used to honor the TEST_TRIVUP_CLUSTER_TYPE env
    # var (and the now-deleted _trivup_cluster_type_kraft helper) so callers
    # could opt into ZooKeeper. It now hardcodes True because broker 4.2.0 is
    # KRaft-only. Callers that need ZK (e.g. tests/integration/admin/
    # test_user_scram_credentials.py) lose their escape hatch — restore the
    # branching, or drop the ZK code paths from those callers, when the
    # broker-version pinning is revisited.
    @staticmethod
    def use_kraft():
        # broker_version() always returns 4.2.0, which is KRaft-only.
        return True

    @staticmethod
    def use_group_protocol_consumer():
        return _GROUP_PROTOCOL_ENV in os.environ and os.environ[_GROUP_PROTOCOL_ENV] == 'consumer'

    @staticmethod
    def update_conf_group_protocol(conf=None):
        if TestUtils.can_upgrade_group_protocol_to_consumer(conf):
            conf['group.protocol'] = 'consumer'

    @staticmethod
    def can_upgrade_group_protocol_to_consumer(conf):
        return (
            conf is not None
            and 'group.id' in conf
            and 'group.protocol' not in conf
            and TestUtils.use_group_protocol_consumer()
        )

    @staticmethod
    def remove_forbidden_conf_group_protocol_consumer(conf):
        if (
            conf is None
            or not TestUtils.use_group_protocol_consumer()
            or conf.get('group.protocol', 'consumer') != 'consumer'
        ):
            return
        forbidden_conf_properties = [
            "session.timeout.ms",
            "partition.assignment.strategy",
            "heartbeat.interval.ms",
            "group.protocol.type",
        ]
        for prop in forbidden_conf_properties:
            if prop in conf:
                print(f"Skipping setting forbidden configuration {prop} for `CONSUMER` protocol")
                del conf[prop]


class TestConsumer(Consumer):
    def __init__(self, conf=None, **kwargs):
        TestUtils.update_conf_group_protocol(conf)
        TestUtils.remove_forbidden_conf_group_protocol_consumer(conf)
        super(TestConsumer, self).__init__(conf, **kwargs)

    def assign(self, partitions):
        if TestUtils.use_group_protocol_consumer():
            super(TestConsumer, self).incremental_assign(partitions)
        else:
            super(TestConsumer, self).assign(partitions)

    def unassign(self, partitions):
        if TestUtils.use_group_protocol_consumer():
            super(TestConsumer, self).incremental_unassign(partitions)
        else:
            super(TestConsumer, self).unassign()


def unique_id(prefix):
    """Generate a topic/group id unique to this test run.

    Avoids cross-test interference when running against a shared broker.
    """
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def set_group_config(kafka_cluster, group_id, name, value):
    """Set one dynamic group config via incremental_alter_configs."""
    res = ConfigResource(
        ConfigResource.Type.GROUP,
        group_id,
        incremental_configs=[
            ConfigEntry(name, str(value), incremental_operation=AlterConfigOpType.SET),
        ],
    )
    for f in kafka_cluster.admin().incremental_alter_configs([res]).values():
        f.result()


def poll_first_batch(consumer, timeout_s=20.0, poll_timeout_s=0.5):
    """Return the first non-empty batch of non-error messages from a share consumer.

    Polls repeatedly until at least one non-error message arrives, or until
    timeout_s elapses. Returns the contents of a single poll() call — does not
    accumulate across polls and does not acknowledge anything.

    Use this when a test needs to inspect the first records the broker delivers
    *before* any implicit- or explicit-ack happens. Unlike drain_share_consumers,
    which loops poll() calls and therefore implicit-acks each prior batch on
    every iteration, poll_first_batch returns whatever the first non-empty
    poll() returned and stops, leaving acknowledgement to the caller.

    Returns an empty list if timeout_s elapses without any non-error message.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        batch = [msg for msg in consumer.poll(timeout=poll_timeout_s) if msg.error() is None]
        if batch:
            return batch
    return []


def drain_share_consumers(consumers, total_messages, timeout_s=20.0, poll_timeout_s=0.5, *, ack_type=None):
    """Round-robin poll consumers until total_messages non-error messages arrive (across all consumers combined).

    Returns one message list per consumer, in input order. Stops at
    total_messages or timeout_s.

    With N>2 consumers and the suite-wide 1s lock duration, drop poll_timeout_s
    so each round finishes within the lock window — otherwise records leak to
    other consumers and look like duplicate deliveries.

    ack_type=None drains in implicit-ack mode (next poll() auto-acks the prior
    batch; the tail batch is left unacked). Pass an AcknowledgeType to ack each
    message inline — required when share.acknowledgement.mode=explicit.

    TODO KIP-932: once commit_sync() / commit_async() are exposed on the
    ShareConsumer binding, the implicit-ack drain (ack_type=None) should
    still emit a commit on the tail batch so callers don't have to chase
    leaked records. Add a commit step here once those APIs land.
    """
    received = [[] for _ in consumers]
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for sc, bucket in zip(consumers, received):
            for msg in sc.poll(timeout=poll_timeout_s):
                if msg.error() is None:
                    bucket.append(msg)
                    if ack_type is not None:
                        sc.acknowledge(msg, ack_type)
        if sum(len(bucket) for bucket in received) >= total_messages:
            break
    return received


class TestShareConsumer(ShareConsumer):
    """Test wrapper around ShareConsumer."""

    __test__ = False  # not a pytest collection target despite the Test* prefix

    def __init__(self, conf=None, **kwargs):
        effective_conf = {
            'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS,
        }
        if conf:
            effective_conf.update(conf)
        super().__init__(effective_conf, **kwargs)
