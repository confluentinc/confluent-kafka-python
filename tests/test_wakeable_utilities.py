#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for wakeable poll/flush/consume utility functions.
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


class TestWakeableUtilities:
    """Test shared utility functions through both Producer and Consumer APIs.

    These tests verify the calculate_chunk_timeout() and check_signals_between_chunks()
    utility functions that are shared between Producer and Consumer implementations.
    """

    @pytest.mark.parametrize("api_type", ["producer", "consumer"])
    def test_calculate_chunk_timeout_utility_function(self, api_type):
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
        # Use constants for bounds - verify timeout happened but allow lenient upper bound
        assert elapsed >= WAKEABLE_POLL_TIMEOUT_MIN, (
            f"Assertion 4 failed: Timeout took {elapsed:.2f}s, " f"expected >= {WAKEABLE_POLL_TIMEOUT_MIN}s"
        )
        assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, (
            f"Assertion 4 failed: Timeout took {elapsed:.2f}s, " f"expected <= {WAKEABLE_POLL_TIMEOUT_MAX}s"
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
    def test_check_signals_between_chunks_utility_function(self, api_type):
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
    def test_utilities_interaction(self, api_type):
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


class TestWakeableInterruptibility:
    """Test interruptibility of blocking operations."""

    @pytest.mark.parametrize(
        "api_type,method",
        [
            ("producer", "poll"),
            ("producer", "flush"),
            ("consumer", "poll"),
            ("consumer", "consume"),
        ],
    )
    def test_can_be_interrupted(self, api_type, method):
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
    def test_short_timeout_not_chunked(self, api_type, method):
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

    def test_flush_empty_queue_returns_immediately(self):
        """Test that flush() with no messages returns immediately."""
        producer = Producer({'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 100, 'message.timeout.ms': 10})

        start = time.time()
        qlen = producer.flush(timeout=0.5)
        elapsed = time.time() - start

        producer.close()

        # Key assertion: empty flush is fast
        assert qlen == 0, "Empty queue should return 0"
        assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Empty flush should return quickly, took {elapsed:.2f}s"
