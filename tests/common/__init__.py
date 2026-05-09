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

    @staticmethod
    def broker_version():
        return '4.2.0'

    @staticmethod
    def broker_conf():
        return [
            'transaction.state.log.replication.factor=1',
            'transaction.state.log.min.isr=1',
            # KIP-932: enable share groups on this test broker. Production uses
            # the share.version feature flag; this internal config is what the
            # Apache Kafka project's own integration tests use.
            'group.share.enable=true',
            # KIP-932: __share_group_state topic defaults are RF=3 / min.isr=2
            # — must be 1/1 on a single-broker test cluster.
            'share.coordinator.state.topic.replication.factor=1',
            'share.coordinator.state.topic.min.isr=1',
            # KIP-932: shorten lock duration to 1s for fast redelivery tests.
            # Both must be set: actual duration must be >= min.
            'group.share.record.lock.duration.ms=1000',
            'group.share.min.record.lock.duration.ms=1000',
        ]

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


def drain_share_consumers(consumers, n_expected, timeout_s=20.0):
    """Round-robin poll until total non-error messages reach n_expected.

    Returns a list of message lists, one per input consumer, in the same order.
    Stops early once the expected total is reached, or when timeout_s elapses.
    """
    received = [[] for _ in consumers]
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for sc, bucket in zip(consumers, received):
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    bucket.append(m)
        if sum(len(b) for b in received) >= n_expected:
            break
    return received


class TestShareConsumer(ShareConsumer):
    """Test wrapper around ShareConsumer.

    Defaults auto.offset.reset to 'earliest' so tests are not sensitive to
    consumer-group-join timing: records produced before subscribe() are still
    delivered. Tests that need 'latest' semantics must override explicitly.
    """

    __test__ = False  # not a pytest collection target despite the Test* prefix

    def __init__(self, conf=None, **kwargs):
        effective_conf = {
            'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS,
            'auto.offset.reset': 'earliest',
        }
        if conf:
            effective_conf.update(conf)
        super().__init__(effective_conf, **kwargs)
