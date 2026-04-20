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

# ============================================================================
# Consumer Wakeability Integration Testing
# ============================================================================
#
# These integration tests verify that the wakeable pattern works correctly
# with actual Kafka clusters and real message delivery scenarios.
#
# How We Test Consumer Wakeability in Integration:
# -----------------------------------------------
# 1. Message Availability Testing:
#    - Produce messages to Kafka topics using a producer
#    - Create consumers with wakeable pattern settings (timeouts >= 200ms)
#    - Call poll()/consume() with timeouts that trigger chunking
#    - Verify messages are returned correctly despite chunking
#    - Measure elapsed time to ensure wakeable pattern doesn't delay delivery
#
# 2. Testing Methodology:
#    - Setup: Create topics, produce messages, create consumers with proper config
#    - Execution: Call poll()/consume() with timeouts >= 200ms (triggers chunking)
#    - Verification: Check messages are returned, values are correct, timing is reasonable
#    - Cleanup: Close consumers and verify no resource leaks
#
# 3. What We Verify:
#    - Messages are correctly returned when available (wakeable pattern doesn't block delivery)
#    - Message values and metadata are preserved through chunking
#    - Timing remains reasonable (messages return quickly when available)
#    - Consumer state remains consistent after operations complete


def test_poll_message_delivery_with_wakeable_pattern(kafka_cluster):
    """Test that poll() correctly returns messages when available.

    This integration test verifies that the wakeable poll pattern doesn't
    interfere with normal message delivery.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-poll-message-delivery')

    # Produce a test message (use cimpl_producer for raw bytes)
    producer = kafka_cluster.cimpl_producer()
    producer.produce(topic, value=b'test-message')
    producer.flush(timeout=1.0)

    # Create consumer with wakeable poll pattern settings
    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': 'test-poll-message-available',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    # Wait for subscription and message availability
    time.sleep(2.0)

    # Poll for message - should return immediately when available
    start = time.time()
    msg = consumer.poll(timeout=2.0)
    elapsed = time.time() - start

    # Verify message was returned correctly
    assert msg is not None, "Expected message, got None"
    assert not msg.error(), f"Message has error: {msg.error()}"
    # Allow more time for initial consumer setup, but once ready, should return quickly
    assert elapsed < 2.5, f"Message available but took {elapsed:.2f}s, expected < 2.5s"
    assert msg.value() == b'test-message', "Message value mismatch"

    consumer.close()


def test_consume_message_delivery_with_wakeable_pattern(kafka_cluster):
    """Test that consume() correctly returns messages when available.

    This integration test verifies that the wakeable poll pattern doesn't
    interfere with normal batch message delivery.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-consume-message-delivery')

    # Produce multiple test messages (use cimpl_producer for raw bytes)
    producer = kafka_cluster.cimpl_producer()
    for i in range(3):
        producer.produce(topic, value=f'test-message-{i}'.encode())
    producer.flush(timeout=1.0)

    # Create consumer with wakeable poll pattern settings
    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': 'test-consume-messages-available',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    # Wait for subscription and message availability
    time.sleep(2.0)

    # Consume messages - should return immediately when available
    start = time.time()
    msglist = consumer.consume(num_messages=5, timeout=2.0)
    elapsed = time.time() - start

    # Verify messages were returned correctly
    assert len(msglist) > 0, "Expected messages, got empty list"
    assert len(msglist) <= 5, f"Should return at most 5 messages, got {len(msglist)}"
    # Allow more time for initial consumer setup, but once ready, should return quickly
    assert elapsed < 2.5, f"Messages available but took {elapsed:.2f}s, expected < 2.5s"

    # Verify message values
    for i, msg in enumerate(msglist):
        assert not msg.error(), f"Message {i} has error: {msg.error()}"
        assert msg.value() is not None, f"Message {i} has no value"
        # Verify we got the expected messages
        expected_value = f'test-message-{i}'.encode()
        expected_msg = f"Message {i} value mismatch: expected {expected_value}, " f"got {msg.value()}"
        assert msg.value() == expected_value, expected_msg

    consumer.close()


def test_consume_accumulates_messages_across_chunks(kafka_cluster):
    """Test that consume() accumulates messages across 200ms chunks.
    This verifies that consume() doesn't return early on the first chunk of messages
    when using the wakeable pattern.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-consume-accumulate-chunks')

    # Produce 10 messages
    producer = kafka_cluster.cimpl_producer()
    num_produced = 10
    for i in range(num_produced):
        producer.produce(topic, value=f'msg-{i}'.encode())
    producer.flush(timeout=5.0)

    # Create consumer
    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': 'test-consume-accumulate',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    # Wait for subscription and partition assignment
    time.sleep(2.0)

    # Consume with num_messages=10 and a generous timeout.
    # Before the fix: would return < 10 (whatever arrived in the first 200ms chunk)
    # After the fix: accumulates across chunks until 10 are collected
    msglist = consumer.consume(num_messages=num_produced, timeout=10.0)

    assert len(msglist) == num_produced, (
        f"Expected {num_produced} messages but got {len(msglist)}. " f"consume() may not be accumulating across chunks."
    )

    for i, msg in enumerate(msglist):
        assert not msg.error(), f"Message {i} has error: {msg.error()}"

    consumer.close()


def test_consume_returns_partial_on_timeout(kafka_cluster):
    """Test that consume() returns partial results when timeout expires
    before num_messages is reached."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-consume-partial-timeout')

    # Produce only 3 messages, but request 100
    producer = kafka_cluster.cimpl_producer()
    num_produced = 3
    for i in range(num_produced):
        producer.produce(topic, value=f'partial-{i}'.encode())
    producer.flush(timeout=5.0)

    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': 'test-consume-partial',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    time.sleep(2.0)

    # Request 100 messages but only 3 exist — should return 3 after timeout
    start = time.time()
    msglist = consumer.consume(num_messages=100, timeout=3.0)
    elapsed = time.time() - start

    assert len(msglist) == num_produced, f"Expected {num_produced} messages (partial), got {len(msglist)}"
    # Should have waited close to the full timeout since num_messages wasn't reached
    assert elapsed >= 2.0, f"Should wait near full timeout for more messages, but returned in {elapsed:.2f}s"

    for i, msg in enumerate(msglist):
        assert not msg.error(), f"Message {i} has error: {msg.error()}"
        assert msg.value() == f'partial-{i}'.encode()

    consumer.close()


def test_consume_accumulates_messages_produced_in_waves(kafka_cluster):
    """Test that consume() accumulates messages that arrive in multiple waves.
    This verifies that consume() doesn't return early on the first wave of messages
    when using the wakeable pattern.
    """
    import threading

    topic = kafka_cluster.create_topic_and_wait_propogation('test-consume-waves')

    producer = kafka_cluster.cimpl_producer()

    def produce_in_waves():
        """Produce 3 waves of 4 messages each, with 1s gaps."""
        for wave in range(3):
            time.sleep(1.0)
            for i in range(4):
                msg_num = wave * 4 + i
                producer.produce(topic, value=f'wave-{msg_num}'.encode())
            producer.flush(timeout=5.0)

    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': 'test-consume-waves',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    time.sleep(2.0)

    # Start producing in background
    producer_thread = threading.Thread(target=produce_in_waves, daemon=True)
    producer_thread.start()

    # Request 10 messages with a long timeout (waves take ~3s to complete)
    msglist = consumer.consume(num_messages=10, timeout=10.0)

    # Should have accumulated messages across multiple waves
    assert len(msglist) == 10, (
        f"Expected exactly 10 messages accumulated across waves, got {len(msglist)}. "
        f"consume() may be returning early on the first wave."
    )

    for msg in msglist:
        assert not msg.error(), f"Message has error: {msg.error()}"

    producer_thread.join(timeout=5.0)
    consumer.close()
