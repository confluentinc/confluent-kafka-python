#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for wakeable poll/flush/consume functionality.

These tests verify the interruptibility of blocking operations (poll, flush, consume)
using the wakeable pattern with signal checking between chunks.

Includes:
- Utility function tests (calculate_chunk_timeout, check_signals_between_chunks)
- Producer wakeable tests (poll, flush)
- Consumer wakeable tests (poll, consume)
"""

import threading
import time

import pytest

from confluent_kafka import Producer
from tests.common import TestConsumer, TestUtils

# Timing constants for wakeable poll/flush/consume pattern tests
# For timeouts < 200ms, the wakeable pattern is NOT used (see Producer.c/Consumer.c),
# so those timeouts can complete faster. For timeouts >= 200ms, chunking is used.
CHUNK_TIMEOUT_MS = 200  # Chunk size in milliseconds
WAKEABLE_POLL_TIMEOUT_MIN = 0.2  # Minimum timeout for chunked operations (seconds)
WAKEABLE_POLL_TIMEOUT_MAX = 2.0  # Maximum timeout (seconds)


# ============================================================================
# Approach to Wakeability Testing
# ============================================================================
#
# The wakeable pattern is implemented using shared C utility functions that are
# used by both Producer and Consumer. Our testing strategy mirrors this architecture:
#
# High level Wakeability Implementation:
# ------------
# Shared Utilities (confluent_kafka.h):
#   - calculate_chunk_timeout(): Splits long timeouts into 200ms chunks
#   - check_signals_between_chunks(): Re-acquires GIL, checks signals, handles cleanup
#
# Producer Implementation (Producer.c):
#   - Producer.poll() uses wakeable pattern for timeouts >= 200ms
#   - Producer.flush() uses wakeable pattern for timeouts >= 200ms
#
# Consumer Implementation (Consumer.c):
#   - Consumer.poll() uses wakeable pattern for timeouts >= 200ms
#   - Consumer.consume() uses wakeable pattern for timeouts >= 200ms
#
# How We Test Wakeability:
# ------------------------
# Since Producer and Consumer share the same C utility functions but have different
# Python APIs, we test them using a layered approach:
#
# 1. Testing Producer Wakeability:
#    - Create Producer instances and call poll()/flush() with various timeouts
#    - Inject signals at different times (immediate, after chunks, during finite timeout)
#    - Verify KeyboardInterrupt is raised and Producer-specific behavior (return types, cleanup)
#    - Test both infinite and finite timeouts to cover all code paths
#
# 2. Testing Consumer Wakeability:
#    - Create Consumer instances and call poll()/consume() with various timeouts
#    - Inject signals at different times using the same pattern as Producer tests
#    - Verify KeyboardInterrupt is raised and Consumer-specific behavior (return types, message handling)
#    - Test both infinite and finite timeouts, including edge cases like num_messages=0
#
# 3. Testing Methodology:
#    - Signal Injection: Use TestUtils.send_sigint_after_delay() in a background thread
#      to simulate KeyboardInterrupt at specific times during blocking operations
#    - Chunking Verification: Measure elapsed time to verify >= 200ms timeouts use chunking
#      (multiple 200ms intervals) while < 200ms timeouts bypass chunking entirely
#    - Interruptibility Verification: Wrap blocking calls in try/except to catch
#      KeyboardInterrupt and verify operations abort cleanly
#    - State Verification: Check that objects are properly cleaned up (closed state,
#      no resource leaks) after interrupted operations


# ============================================================================
# Producer wakeable tests
# ============================================================================


def test_producer_wakeable_poll_utility_functions_interaction():
    """Test interaction between calculate_chunk_timeout() and check_signals_between_chunks()."""
    # Assert: Chunk calculation and signal check work together
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.4))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer1.poll(timeout=1.0)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer1.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: Multiple chunks before signal detection
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer2.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer2.close()

    assert interrupted, "Should have raised KeyboardInterrupt"


def test_producer_wakeable_poll_interruptibility_and_messages():
    """Test poll() interruptibility and message handling."""
    # Assert: Infinite timeout can be interrupted
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.1))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer1.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer1.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: Finite timeout can be interrupted before timeout expires
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer2.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer2.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: Signal sent after multiple chunks still interrupts
    producer3 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer3.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer3.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: No signal - timeout works normally
    producer4 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer4.poll(timeout=0.5)
    elapsed = time.time() - start

    assert isinstance(result, int), "poll() should return int"
    assert (
        WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Timeout took {elapsed:.2f}s, expected ~0.5s"
    producer4.close()


def test_producer_wakeable_poll_edge_cases():
    """Test poll() edge cases."""
    # Assert: Zero timeout returns immediately
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer1.poll(timeout=0.0)
    elapsed = time.time() - start

    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Zero timeout took {elapsed:.2f}s"
    assert isinstance(result, int)
    producer1.close()

    # Assert: Closed producer raises RuntimeError
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})
    producer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        producer2.poll(timeout=0.1)
    assert 'Producer has been closed' in str(exc_info.value)

    # Assert: Short timeout works correctly
    producer3 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer3.poll(timeout=0.1)
    elapsed = time.time() - start

    assert isinstance(result, int)
    # Short timeouts don't use chunking
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, f"Short timeout took {elapsed:.2f}s"
    producer3.close()

    # Assert: Very short timeout works
    producer4 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer4.poll(timeout=0.05)
    elapsed = time.time() - start

    assert isinstance(result, int)
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Very short timeout took {elapsed:.2f}s"
    producer4.close()


def test_producer_wakeable_flush_interruptibility_and_messages():
    """Test flush() interruptibility and message handling."""
    # Assert: Infinite timeout can be interrupted
    producer1 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 60000,
            'message.timeout.ms': 30000,
            'acks': 'all',
            'batch.num.messages': 100,
            'linger.ms': 100,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 104857600,
            'max.in.flight.requests.per.connection': 1,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 30000,
        }
    )

    messages_produced = False
    stop_producing = threading.Event()
    production_stats = {'count': 0, 'errors': 0}

    def continuous_producer():
        message_num = 0
        while not stop_producing.is_set():
            try:
                producer1.produce(
                    'test-topic', value=f'continuous-{message_num}'.encode(), key=f'key-{message_num}'.encode()
                )
                production_stats['count'] += 1
                message_num += 1
            except Exception as e:
                production_stats['errors'] += 1
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.001)
                else:
                    time.sleep(0.01)

    try:
        for i in range(1000):
            try:
                producer1.produce('test-topic', value=f'initial-{i}'.encode())
                messages_produced = True
            except Exception as e:
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.01)
                    continue
                break

        if not messages_produced:
            producer1.close()
            pytest.skip("Broker not available, cannot test flush() interruptibility")

        poll_start = time.time()
        while time.time() - poll_start < 0.5:
            producer1.poll(timeout=0.1)

        producer_thread = threading.Thread(target=continuous_producer, daemon=True)
        producer_thread.start()
        time.sleep(0.1)

        interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.1))
        interrupt_thread.daemon = True
        interrupt_thread.start()

        interrupted = False
        try:
            producer1.flush()
        except KeyboardInterrupt:
            interrupted = True
        finally:
            stop_producing.set()
            time.sleep(0.1)
            producer1.close()

        assert interrupted, "Should have raised KeyboardInterrupt"
    except Exception:
        stop_producing.set()
        producer1.close()
        raise

    # Assert: Finite timeout can be interrupted before timeout expires
    producer2 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 60000,
            'message.timeout.ms': 30000,
            'acks': 'all',
            'batch.num.messages': 100,
            'linger.ms': 100,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 104857600,
            'max.in.flight.requests.per.connection': 1,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 30000,
        }
    )

    stop_producing2 = threading.Event()
    production_stats2 = {'count': 0, 'errors': 0}

    def continuous_producer2():
        message_num = 0
        while not stop_producing2.is_set():
            try:
                producer2.produce(
                    'test-topic', value=f'continuous2-{message_num}'.encode(), key=f'key2-{message_num}'.encode()
                )
                production_stats2['count'] += 1
                message_num += 1
            except Exception as e:
                production_stats2['errors'] += 1
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.001)
                else:
                    time.sleep(0.01)

    try:
        for i in range(1000):
            try:
                producer2.produce('test-topic', value=f'initial2-{i}'.encode())
            except Exception as e:
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.01)
                    continue
                break

        poll_start = time.time()
        while time.time() - poll_start < 0.5:
            producer2.poll(timeout=0.1)

        producer_thread2 = threading.Thread(target=continuous_producer2, daemon=True)
        producer_thread2.start()
        time.sleep(0.1)

        interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
        interrupt_thread.daemon = True
        interrupt_thread.start()

        interrupted = False
        try:
            producer2.flush(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
        except KeyboardInterrupt:
            interrupted = True
        finally:
            stop_producing2.set()
            time.sleep(0.1)
            producer2.close()

        assert interrupted, "Should have raised KeyboardInterrupt"
    except Exception:
        stop_producing2.set()
        producer2.close()
        raise

    # Assert: Signal sent after multiple chunks still interrupts
    producer3 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 60000,
            'message.timeout.ms': 30000,
            'acks': 'all',
            'batch.num.messages': 100,
            'linger.ms': 100,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 104857600,
            'max.in.flight.requests.per.connection': 1,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 30000,
        }
    )

    stop_producing3 = threading.Event()
    production_stats3 = {'count': 0, 'errors': 0}

    def continuous_producer3():
        message_num = 0
        while not stop_producing3.is_set():
            try:
                producer3.produce(
                    'test-topic', value=f'continuous3-{message_num}'.encode(), key=f'key3-{message_num}'.encode()
                )
                production_stats3['count'] += 1
                message_num += 1
            except Exception as e:
                production_stats3['errors'] += 1
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.001)
                else:
                    time.sleep(0.01)

    try:
        for i in range(1000):
            try:
                producer3.produce('test-topic', value=f'initial3-{i}'.encode())
            except Exception as e:
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.01)
                    continue
                break

        poll_start = time.time()
        while time.time() - poll_start < 0.5:
            producer3.poll(timeout=0.1)

        producer_thread3 = threading.Thread(target=continuous_producer3, daemon=True)
        producer_thread3.start()
        time.sleep(0.1)

        interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
        interrupt_thread.daemon = True
        interrupt_thread.start()

        interrupted = False
        try:
            producer3.flush()
        except KeyboardInterrupt:
            interrupted = True
        finally:
            stop_producing3.set()
            time.sleep(0.1)
            producer3.close()

        assert interrupted, "Should have raised KeyboardInterrupt"
    except Exception:
        stop_producing3.set()
        producer3.close()
        raise

    # Assert: No signal - timeout works normally
    producer4 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10,
            'acks': 'all',
            'max.in.flight.requests.per.connection': 1,
        }
    )

    try:
        for i in range(100):
            producer4.produce('test-topic', value=f'timeout-test-{i}'.encode())
    except Exception:
        pass

    start = time.time()
    qlen = producer4.flush(timeout=0.5)
    elapsed = time.time() - start

    assert isinstance(qlen, int), "flush() should return int"
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, f"Timeout took {elapsed:.2f}s"
    producer4.close()


def test_producer_wakeable_flush_edge_cases():
    """Test flush() edge cases."""
    # Assert: Zero timeout returns immediately
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer1.flush(timeout=0.0)
    elapsed = time.time() - start

    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Zero timeout took {elapsed:.2f}s"
    assert isinstance(qlen, int)
    producer1.close()

    # Assert: Closed producer raises RuntimeError
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})
    producer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        producer2.flush(timeout=0.1)
    assert 'Producer has been closed' in str(exc_info.value)

    # Assert: Short timeout works correctly
    producer3 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer3.flush(timeout=0.1)
    elapsed = time.time() - start

    assert isinstance(qlen, int)
    # Short timeouts don't use chunking
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, f"Short timeout took {elapsed:.2f}s"
    producer3.close()

    # Assert: Very short timeout works
    producer4 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer4.flush(timeout=0.05)
    elapsed = time.time() - start

    assert isinstance(qlen, int)
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Very short timeout took {elapsed:.2f}s"
    producer4.close()

    # Assert: Empty queue flush returns immediately
    producer5 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer5.flush(timeout=1.0)
    elapsed = time.time() - start

    assert qlen == 0
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Empty flush took {elapsed:.2f}s"
    producer5.close()


# ============================================================================
# Consumer wakeable tests
# ============================================================================


def test_consumer_wakeable_poll_utility_functions_interaction():
    """Test interaction between calculate_chunk_timeout() and check_signals_between_chunks()."""
    # Assertion 1: Both functions work together - chunk calculation + signal check
    consumer1 = TestConsumer(
        {
            'group.id': 'test-interaction-chunk-signal',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer1.subscribe(['test-topic'])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.4))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        consumer1.poll(timeout=1.0)  # 1 second timeout, interrupt after 0.4s
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer1.close()

    assert interrupted, "Assertion 1 failed: Should have raised KeyboardInterrupt"

    # Assertion 2: Multiple chunks before signal - both functions work over multiple iterations
    consumer2 = TestConsumer(
        {
            'group.id': 'test-interaction-multiple-chunks',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer2.subscribe(['test-topic'])

    # Send signal after 0.6 seconds (3 chunks should have passed: 0.2s, 0.4s, 0.6s)
    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        consumer2.poll()  # Infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer2.close()

    assert interrupted, "Assertion 2 failed: Should have raised KeyboardInterrupt"


def test_consumer_wakeable_poll_interruptibility_and_messages():
    """Test poll() interruptibility (main fix) and message handling."""
    topic = 'test-poll-interrupt-topic'

    # Assertion 1: Infinite timeout can be interrupted immediately
    consumer1 = TestConsumer(
        {
            'group.id': 'test-poll-infinite-immediate',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer1.subscribe([topic])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.1))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        consumer1.poll()  # Infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer1.close()

    assert interrupted, "Assertion 1 failed: Should have raised KeyboardInterrupt"

    # Assertion 2: Finite timeout can be interrupted before timeout expires
    consumer2 = TestConsumer(
        {
            'group.id': 'test-poll-finite-interrupt',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer2.subscribe([topic])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    timeout_value = WAKEABLE_POLL_TIMEOUT_MAX  # Use constant instead of hardcoded 2.0
    try:
        consumer2.poll(timeout=timeout_value)  # Use constant for timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer2.close()

    assert interrupted, "Assertion 2 failed: Should have raised KeyboardInterrupt"

    # Assertion 3: Signal sent after multiple chunks still interrupts quickly
    consumer3 = TestConsumer(
        {
            'group.id': 'test-poll-multiple-chunks',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer3.subscribe([topic])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        consumer3.poll()  # Infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer3.close()

    assert interrupted, "Assertion 3 failed: Should have raised KeyboardInterrupt"

    # Assertion 4: No signal - timeout works normally
    consumer4 = TestConsumer(
        {
            'group.id': 'test-poll-timeout-normal',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer4.subscribe([topic])

    start = time.time()
    msg = consumer4.poll(timeout=0.5)  # 500ms, no signal
    elapsed = time.time() - start

    assert msg is None, "Assertion 4 failed: Expected None (timeout), no signal should not interrupt"
    assert (
        WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 4 failed: Normal timeout took {elapsed:.2f}s, expected ~0.5s"
    consumer4.close()


def test_consumer_wakeable_poll_edge_cases():
    """Test poll() edge cases."""
    topic = 'test-poll-edge-topic'

    # Assertion 1: Zero timeout returns immediately (non-blocking)
    consumer1 = TestConsumer(
        {
            'group.id': 'test-poll-zero-timeout',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer1.subscribe([topic])

    start = time.time()
    msg = consumer1.poll(timeout=0.0)  # Zero timeout
    elapsed = time.time() - start

    assert (
        elapsed < WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 1 failed: Zero timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    assert msg is None, "Assertion 1 failed: Zero timeout with no messages should return None"
    consumer1.close()

    # Assertion 2: Closed consumer raises RuntimeError
    consumer2 = TestConsumer({'group.id': 'test-poll-closed', 'socket.timeout.ms': 100, 'session.timeout.ms': 1000})
    consumer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        consumer2.poll(timeout=0.1)
    msg = f"Assertion 2 failed: Expected 'Consumer closed' error, " f"got: {exc_info.value}"
    assert 'Consumer closed' in str(exc_info.value), msg

    # Assertion 3: Short timeout works correctly (no signal)
    consumer3 = TestConsumer(
        {
            'group.id': 'test-poll-short-timeout',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer3.subscribe([topic])

    start = time.time()
    msg = consumer3.poll(timeout=0.1)  # 100ms timeout
    elapsed = time.time() - start

    assert msg is None, "Assertion 3 failed: Short timeout with no messages should return None"
    # Short timeouts (< 200ms) don't use chunking, so they can complete faster than WAKEABLE_POLL_TIMEOUT_MIN
    # Only check upper bound to allow for actual timeout duration
    assert (
        elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 3 failed: Short timeout took {elapsed:.2f}s, expected <= {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer3.close()

    # Assertion 4: Very short timeout (less than chunk size) works
    consumer4 = TestConsumer(
        {
            'group.id': 'test-poll-very-short',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer4.subscribe([topic])

    start = time.time()
    msg = consumer4.poll(timeout=0.05)  # 50ms timeout (less than 200ms chunk)
    elapsed = time.time() - start

    assert msg is None, "Assertion 4 failed: Very short timeout should return None"
    assert (
        elapsed < WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 4 failed: Very short timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer4.close()


def test_consumer_wakeable_consume_interruptibility_and_messages():
    """Test consume() interruptibility (main fix) and message handling."""
    topic = 'test-consume-interrupt-topic'

    # Assertion 1: Infinite timeout can be interrupted immediately
    consumer1 = TestConsumer(
        {
            'group.id': 'test-consume-infinite-immediate',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer1.subscribe([topic])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.1))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        consumer1.consume()  # Infinite timeout, default num_messages=1
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer1.close()

    assert interrupted, "Assertion 1 failed: Should have raised KeyboardInterrupt"

    # Assertion 2: Finite timeout can be interrupted before timeout expires
    consumer2 = TestConsumer(
        {
            'group.id': 'test-consume-finite-interrupt',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer2.subscribe([topic])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    timeout_value = WAKEABLE_POLL_TIMEOUT_MAX  # Use constant instead of hardcoded 2.0
    try:
        consumer2.consume(num_messages=10, timeout=timeout_value)  # Use constant for timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer2.close()

    assert interrupted, "Assertion 2 failed: Should have raised KeyboardInterrupt"

    # Assertion 3: Signal sent after multiple chunks still interrupts quickly
    consumer3 = TestConsumer(
        {
            'group.id': 'test-consume-multiple-chunks',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer3.subscribe([topic])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        consumer3.consume(num_messages=5)  # Infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        consumer3.close()

    assert interrupted, "Assertion 3 failed: Should have raised KeyboardInterrupt"

    # Assertion 4: No signal - timeout works normally, returns empty list
    consumer4 = TestConsumer(
        {
            'group.id': 'test-consume-timeout-normal',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer4.subscribe([topic])

    start = time.time()
    msglist = consumer4.consume(num_messages=10, timeout=0.5)  # 500ms, no signal
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 4 failed: consume() should return a list"
    assert len(msglist) == 0, f"Assertion 4 failed: Expected empty list (timeout), got {len(msglist)} messages"
    assert (
        WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 4 failed: Normal timeout took {elapsed:.2f}s, expected ~0.5s"
    consumer4.close()

    # Assertion 5: num_messages=0 returns empty list immediately
    consumer5 = TestConsumer(
        {
            'group.id': 'test-consume-zero-messages',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer5.subscribe([topic])

    start = time.time()
    msglist = consumer5.consume(num_messages=0, timeout=1.0)
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 5 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 5 failed: num_messages=0 should return empty list"
    assert (
        elapsed < WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 5 failed: num_messages=0 took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer5.close()


def test_consumer_wakeable_consume_edge_cases():
    """Test consume() wakeable edge cases."""
    topic = 'test-consume-edge-topic'

    # Assertion 1: Zero timeout returns immediately (non-blocking)
    consumer1 = TestConsumer(
        {
            'group.id': 'test-consume-zero-timeout',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer1.subscribe([topic])

    start = time.time()
    msglist = consumer1.consume(num_messages=10, timeout=0.0)  # Zero timeout
    elapsed = time.time() - start

    assert (
        elapsed < WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 1 failed: Zero timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    assert isinstance(msglist, list), "Assertion 1 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 1 failed: Zero timeout with no messages should return empty list"
    consumer1.close()

    # Assertion 2: Closed consumer raises RuntimeError
    consumer2 = TestConsumer({'group.id': 'test-consume-closed', 'socket.timeout.ms': 100, 'session.timeout.ms': 1000})
    consumer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        consumer2.consume(num_messages=10, timeout=0.1)
    msg = f"Assertion 2 failed: Expected 'Consumer closed' error, " f"got: {exc_info.value}"
    assert 'Consumer closed' in str(exc_info.value), msg

    # Assertion 3: Invalid num_messages (negative) raises ValueError
    consumer3 = TestConsumer(
        {
            'group.id': 'test-consume-invalid-negative',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer3.subscribe([topic])

    with pytest.raises(ValueError) as exc_info:
        consumer3.consume(num_messages=-1, timeout=0.1)
    msg = f"Assertion 3 failed: Expected num_messages range error, " f"got: {exc_info.value}"
    assert 'num_messages must be between 0 and 1000000' in str(exc_info.value), msg
    consumer3.close()

    # Assertion 4: Invalid num_messages (too large) raises ValueError
    consumer4 = TestConsumer(
        {
            'group.id': 'test-consume-invalid-large',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer4.subscribe([topic])

    with pytest.raises(ValueError) as exc_info:
        consumer4.consume(num_messages=1000001, timeout=0.1)
    msg = f"Assertion 4 failed: Expected num_messages range error, " f"got: {exc_info.value}"
    assert 'num_messages must be between 0 and 1000000' in str(exc_info.value), msg
    consumer4.close()

    # Assertion 5: Short timeout works correctly (no signal)
    consumer5 = TestConsumer(
        {
            'group.id': 'test-consume-short-timeout',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer5.subscribe([topic])

    start = time.time()
    msglist = consumer5.consume(num_messages=10, timeout=0.1)  # 100ms timeout
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 5 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 5 failed: Short timeout with no messages should return empty list"
    # Only check upper bound to allow for actual timeout duration
    assert (
        elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 5 failed: Short timeout took {elapsed:.2f}s, expected <= {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer5.close()

    # Assertion 6: Very short timeout (less than chunk size) works
    consumer6 = TestConsumer(
        {
            'group.id': 'test-consume-very-short',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'auto.offset.reset': 'latest',
        }
    )
    consumer6.subscribe([topic])

    start = time.time()
    msglist = consumer6.consume(num_messages=5, timeout=0.05)  # 50ms timeout (less than 200ms chunk)
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 6 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 6 failed: Very short timeout should return empty list"
    assert (
        elapsed < WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 6 failed: Very short timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer6.close()


# ============================================================================
# Utility function tests
# ============================================================================


@pytest.mark.parametrize("api_type", ["producer", "consumer"])
def test_calculate_chunk_timeout_utility_function(api_type):
    """Comprehensive test of calculate_chunk_timeout() utility function through poll() API.

    Tests all timeout scenarios: infinite, exact multiple, not multiple, very short,
    zero timeout, large timeout, and interruption during finite timeout.
    """

    # Helper to create API object and blocking call
    def create_api_obj(group_id_suffix=""):
        if api_type == "producer":
            obj = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})
            return obj, lambda t=None: obj.poll() if t is None else obj.poll(timeout=t)
        else:
            group_id = f'test-chunk-{group_id_suffix}' if group_id_suffix else 'test-chunk'
            obj = TestConsumer(
                {
                    'group.id': group_id,
                    'socket.timeout.ms': 100,
                    'session.timeout.ms': 1000,
                    'auto.offset.reset': 'latest',
                }
            )
            obj.subscribe(['test-topic'])
            return obj, lambda t=None: obj.poll() if t is None else obj.poll(timeout=t)

    # Assertion 1: Infinite timeout chunks forever with 200ms intervals
    obj1, blocking_call1 = create_api_obj("infinite")
    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        blocking_call1()  # Infinite timeout - should chunk every 200ms
    except KeyboardInterrupt:
        interrupted = True
    finally:
        obj1.close()

    assert interrupted, "Assertion 1 failed: Should have raised KeyboardInterrupt"

    # Assertion 2: Finite timeout exact multiple (1.0s = 5 chunks of 200ms)
    obj2, blocking_call2 = create_api_obj("exact-multiple")
    start = time.time()
    result = blocking_call2(1.0)  # Exactly 1000ms (5 chunks)
    elapsed = time.time() - start

    if api_type == "producer":
        assert isinstance(result, int), "Assertion 2 failed: poll() should return int"
    else:
        assert result is None, "Assertion 2 failed: Expected None (timeout)"
    assert (
        WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 2 failed: Timeout took {elapsed:.2f}s, expected ~1.0s"
    obj2.close()

    # Assertion 3: Finite timeout not multiple (0.35s = 1 chunk + 150ms partial)
    obj3, blocking_call3 = create_api_obj("not-multiple")
    start = time.time()
    result = blocking_call3(0.35)  # 350ms (1 full chunk + 150ms partial)
    elapsed = time.time() - start

    if api_type == "producer":
        assert isinstance(result, int), "Assertion 3 failed: poll() should return int"
    else:
        assert result is None, "Assertion 3 failed: Expected None (timeout)"
    assert (
        WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 3 failed: Timeout took {elapsed:.2f}s, expected ~0.35s"
    obj3.close()

    # Assertion 4: Very short timeout (< 200ms chunk size)
    obj4, blocking_call4 = create_api_obj("very-short")
    start = time.time()
    result = blocking_call4(0.05)  # 50ms (less than 200ms chunk)
    elapsed = time.time() - start

    if api_type == "producer":
        assert isinstance(result, int), "Assertion 4 failed: poll() should return int"
    else:
        assert result is None, "Assertion 4 failed: Expected None (timeout)"
    # Short timeouts don't use chunking, so only check upper bound
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, (
        f"Assertion 4 failed: Very short timeout took {elapsed:.2f}s, " f"expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    )
    obj4.close()

    # Assertion 5: Zero timeout (non-blocking)
    obj5, blocking_call5 = create_api_obj("zero")
    start = time.time()
    result = blocking_call5(0.0)  # Non-blocking
    elapsed = time.time() - start

    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, (
        f"Assertion 5 failed: Zero timeout took {elapsed:.2f}s, " f"expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    )
    obj5.close()

    # Assertion 6: Large finite timeout (10s = 50 chunks)
    obj6, blocking_call6 = create_api_obj("large")
    start = time.time()
    result = blocking_call6(10.0)  # 10 seconds (50 chunks)
    elapsed = time.time() - start

    if api_type == "producer":
        assert isinstance(result, int), "Assertion 6 failed: poll() should return int"
    else:
        assert result is None, "Assertion 6 failed: Expected None (timeout)"
    # Use constants for bounds - verify timeout happened (loose bounds for large timeout)
    assert elapsed >= WAKEABLE_POLL_TIMEOUT_MIN, (
        f"Assertion 6 failed: Timeout took {elapsed:.2f}s, " f"expected >= {WAKEABLE_POLL_TIMEOUT_MIN}s"
    )
    assert elapsed <= 10.0 * 2.0, (
        f"Assertion 6 failed: Timeout took {elapsed:.2f}s, " f"expected <= {10.0 * 2.0}s (relative check)"
    )
    obj6.close()

    # Assertion 7: Finite timeout with interruption (chunk calculation continues correctly)
    obj7, blocking_call7 = create_api_obj("interrupt")
    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.4))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    timeout_value = WAKEABLE_POLL_TIMEOUT_MAX  # Use constant instead of hardcoded 1.0
    try:
        blocking_call7(timeout_value)  # Use constant for timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        obj7.close()

    assert interrupted, "Assertion 7 failed: Should have raised KeyboardInterrupt"


@pytest.mark.parametrize("api_type", ["producer", "consumer"])
def test_check_signals_between_chunks_utility_function(api_type):
    """Comprehensive test of check_signals_between_chunks() utility function through poll() API.

    Tests signal detection on first chunk, later chunk, no signal case, every chunk check,
    and signal during finite timeout.
    """

    # Helper to create API object and blocking call
    def create_api_obj(group_id_suffix=""):
        if api_type == "producer":
            obj = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})
            return obj, lambda t=None: obj.poll() if t is None else obj.poll(timeout=t)
        else:
            group_id = f'test-signal-{group_id_suffix}' if group_id_suffix else 'test-signal'
            obj = TestConsumer(
                {
                    'group.id': group_id,
                    'socket.timeout.ms': 100,
                    'session.timeout.ms': 1000,
                    'auto.offset.reset': 'latest',
                }
            )
            obj.subscribe(['test-topic'])
            return obj, lambda t=None: obj.poll() if t is None else obj.poll(timeout=t)

    # Assertion 1: Signal detected on first chunk check
    obj1, blocking_call1 = create_api_obj("first-chunk")
    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.05))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        blocking_call1()  # Infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        obj1.close()

    assert interrupted, "Assertion 1 failed: Should have raised KeyboardInterrupt"

    # Assertion 2: Signal detected on later chunk check
    obj2, blocking_call2 = create_api_obj("later-chunk")
    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.5))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        blocking_call2()  # Infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        obj2.close()

    assert interrupted, "Assertion 2 failed: Should have raised KeyboardInterrupt"

    # Assertion 3: No signal - continues polling
    obj3, blocking_call3 = create_api_obj("no-signal")
    start = time.time()
    result = blocking_call3(0.5)  # 500ms, no signal
    elapsed = time.time() - start

    if api_type == "producer":
        assert isinstance(result, int), "Assertion 3 failed: poll() should return int"
    else:
        assert result is None, "Assertion 3 failed: Expected None (timeout), no signal should not interrupt"
    assert (
        WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Assertion 3 failed: No signal timeout took {elapsed:.2f}s, expected ~0.5s"
    obj3.close()

    # Assertion 4: Signal checked every chunk (not just once)
    obj4, blocking_call4 = create_api_obj("every-chunk")
    # Send signal after 0.6 seconds (3 chunks should have passed)
    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        blocking_call4()  # Infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        obj4.close()

    assert interrupted, "Assertion 4 failed: Should have raised KeyboardInterrupt"

    # Assertion 5: Signal check works during finite timeout
    obj5, blocking_call5 = create_api_obj("finite-timeout")
    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    timeout_value = WAKEABLE_POLL_TIMEOUT_MAX  # Use constant instead of hardcoded 2.0
    try:
        blocking_call5(timeout_value)  # Use constant for timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        obj5.close()

    assert interrupted, "Assertion 5 failed: Should have raised KeyboardInterrupt"


@pytest.mark.parametrize("api_type", ["producer", "consumer"])
def test_utilities_interaction(api_type):
    """Test that chunking and signal checking work together."""
    if api_type == "producer":
        obj = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

        def blocking_call(t):
            return obj.poll(timeout=t)

    else:
        obj = TestConsumer(
            {
                'group.id': 'test-interaction',
                'socket.timeout.ms': 100,
                'session.timeout.ms': 1000,
                'auto.offset.reset': 'latest',
            }
        )
        obj.subscribe(['test-topic'])

        def blocking_call(t):
            return obj.poll(timeout=t)

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.4))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    timeout_value = WAKEABLE_POLL_TIMEOUT_MAX  # Use constant instead of hardcoded 1.0
    try:
        blocking_call(timeout_value)  # Use constant for timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        time.sleep(0.5)  # Wait for signal thread
        obj.close()

    # Key assertion: interrupted before full timeout
    assert interrupted, "Should have been interrupted"


@pytest.mark.parametrize(
    "api_type,method",
    [
        ("producer", "poll"),
        ("producer", "flush"),
        ("consumer", "poll"),
        ("consumer", "consume"),
    ],
)
def test_can_be_interrupted(api_type, method):
    """Test that blocking operations can be interrupted."""
    if api_type == "producer":
        obj = Producer({'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 100, 'message.timeout.ms': 10})
        if method == "poll":

            def blocking_call():
                return obj.poll()

        else:  # flush
            obj.produce('test-topic', value='test', callback=lambda err, msg: None)

            def blocking_call():
                return obj.flush()

    else:  # consumer
        obj = TestConsumer(
            {
                'group.id': 'test-interrupt',
                'socket.timeout.ms': 100,
                'session.timeout.ms': 1000,
                'auto.offset.reset': 'latest',
            }
        )
        obj.subscribe(['test-topic'])
        if method == "poll":

            def blocking_call():
                return obj.poll()

        else:  # consume

            def blocking_call():
                return obj.consume()

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.1))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        blocking_call()
    except KeyboardInterrupt:
        interrupted = True
    finally:
        # Wait for signal thread to complete
        time.sleep(0.2)
        obj.close()

    # Key assertion: operation was interruptible
    assert interrupted, f"{api_type}.{method}() should be interruptible"


@pytest.mark.parametrize(
    "api_type,method",
    [
        ("producer", "poll"),
        ("consumer", "poll"),
        ("consumer", "consume"),
    ],
)
def test_short_timeout_not_chunked(api_type, method):
    """Test that short timeouts use non-chunked path."""
    if api_type == "producer":
        obj = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

        def blocking_call(t):
            return obj.poll(timeout=t)

    else:
        obj = TestConsumer(
            {
                'group.id': 'test-short',
                'socket.timeout.ms': 100,
                'session.timeout.ms': 1000,
                'auto.offset.reset': 'latest',
            }
        )
        obj.subscribe(['test-topic'])
        if method == "poll":

            def blocking_call(t):
                return obj.poll(timeout=t)

        else:  # consume

            def blocking_call(t):
                return obj.consume(timeout=t)

    start = time.time()
    if method == "consume":
        result = blocking_call(0.1)
        assert isinstance(result, list)
    else:
        result = blocking_call(0.1)
    elapsed = time.time() - start

    obj.close()

    # Key assertion: short timeout completes quickly (use constant)
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Short timeout should complete quickly, took {elapsed:.2f}s"


def test_flush_empty_queue_returns_immediately():
    """Test that flush() with no messages returns immediately."""
    producer = Producer({'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer.flush(timeout=0.5)
    elapsed = time.time() - start

    producer.close()

    # Key assertion: empty flush is fast
    assert qlen == 0, "Empty queue should return 0"
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Empty flush should return quickly, took {elapsed:.2f}s"
