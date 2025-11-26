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

import time

from tests.common import TestConsumer


def test_poll_message_delivery_with_wakeable_pattern(kafka_cluster):
    """Test that poll() correctly delivers messages when using wakeable pattern.

    This integration test verifies that the wakeable poll pattern doesn't
    interfere with normal message delivery callbacks.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-poll-message-delivery')

    # Track delivery callbacks
    delivery_called = []
    delivery_errors = []

    def delivery_callback(err, msg):
        if err:
            delivery_errors.append(err)
        else:
            delivery_called.append(msg)

    # Create producer with wakeable poll pattern settings
    producer_conf = kafka_cluster.client_conf(
        {
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10000,
        }
    )
    producer = kafka_cluster.cimpl_producer(producer_conf)

    # Produce a test message with delivery callback
    producer.produce(topic, value=b'test-message', on_delivery=delivery_callback)

    # Poll with wakeable pattern - should trigger delivery callback
    start = time.time()
    events_handled = producer.poll(timeout=2.0)
    elapsed = time.time() - start

    # Verify delivery callback was called
    assert len(delivery_called) > 0, "Expected delivery callback to be called"
    assert len(delivery_errors) == 0, f"Unexpected delivery errors: {delivery_errors}"
    assert events_handled >= 0, "poll() should return non-negative int"
    # Allow time for delivery callback, but should complete reasonably quickly
    assert elapsed < 2.5, f"Poll took {elapsed:.2f}s, expected < 2.5s"

    # Flush to ensure message is committed to Kafka
    producer.flush(timeout=1.0)

    # Verify message was actually delivered by consuming it
    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': 'test-poll-verify-delivery',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    # Wait for subscription and message availability
    time.sleep(2.0)

    msg = consumer.poll(timeout=2.0)
    assert msg is not None, "Expected message to be delivered"
    assert not msg.error(), f"Message has error: {msg.error()}"
    assert msg.value() == b'test-message', "Message value mismatch"

    producer.close()
    consumer.close()


def test_flush_message_delivery_with_wakeable_pattern(kafka_cluster):
    """Test that flush() correctly delivers messages when using wakeable pattern.

    This integration test verifies that the wakeable flush pattern doesn't
    interfere with normal message delivery.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-flush-message-delivery')

    # Track delivery callbacks
    delivery_called = []
    delivery_errors = []

    def delivery_callback(err, msg):
        if err:
            delivery_errors.append(err)
        else:
            delivery_called.append(msg)

    # Create producer with wakeable flush pattern settings
    producer_conf = kafka_cluster.client_conf(
        {
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10000,
        }
    )
    producer = kafka_cluster.cimpl_producer(producer_conf)

    # Produce multiple test messages with delivery callbacks
    num_messages = 5
    for i in range(num_messages):
        producer.produce(topic, value=f'test-message-{i}'.encode(), on_delivery=delivery_callback)

    # Flush with wakeable pattern - should trigger all delivery callbacks
    start = time.time()
    remaining = producer.flush(timeout=2.0)
    elapsed = time.time() - start

    # Verify all delivery callbacks were called
    assert (
        len(delivery_called) == num_messages
    ), f"Expected {num_messages} delivery callbacks, got {len(delivery_called)}"
    assert len(delivery_errors) == 0, f"Unexpected delivery errors: {delivery_errors}"
    assert remaining == 0, f"Expected 0 remaining messages after flush, got {remaining}"
    # Allow time for flush, but should complete reasonably quickly
    assert elapsed < 2.5, f"Flush took {elapsed:.2f}s, expected < 2.5s"

    # Verify messages were actually delivered by consuming them
    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': 'test-flush-verify-delivery',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    # Wait for subscription and message availability
    time.sleep(2.0)

    # Consume all messages
    msglist = []
    start = time.time()
    while len(msglist) < num_messages and (time.time() - start) < 5.0:
        msg = consumer.poll(timeout=1.0)
        if msg is not None and not msg.error():
            msglist.append(msg)

    assert len(msglist) == num_messages, f"Expected {num_messages} messages, got {len(msglist)}"

    # Verify message values
    for i, msg in enumerate(msglist):
        expected_value = f'test-message-{i}'.encode()
        assert (
            msg.value() == expected_value
        ), f"Message {i} value mismatch: expected {expected_value}, got {msg.value()}"

    producer.close()
    consumer.close()
