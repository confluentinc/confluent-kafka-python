#!/usr/bin/env python

import threading
import time

import pytest

from confluent_kafka import (Consumer, TopicPartition, KafkaError,
                             KafkaException, TIMESTAMP_NOT_AVAILABLE,
                             OFFSET_INVALID)
from tests.common import TestConsumer, TestUtils

from tests.test_wakeable_utilities import WAKEABLE_POLL_TIMEOUT_MIN, WAKEABLE_POLL_TIMEOUT_MAX


def test_basic_api():
    """Basic API tests, these wont really do anything since there is no
    broker configured."""

    with pytest.raises(TypeError) as ex:
        kc = TestConsumer()
    assert ex.match('expected configuration dict')

    def dummy_commit_cb(err, partitions):
        pass

    kc = TestConsumer(
        {
            'group.id': 'test',
            'socket.timeout.ms': '100',
            'session.timeout.ms': 1000,  # Avoid close() blocking too long
            'on_commit': dummy_commit_cb,
        }
    )

    kc.subscribe(["test"])
    kc.unsubscribe()

    def dummy_assign_revoke(consumer, partitions):
        pass

    kc.subscribe(["test"], on_assign=dummy_assign_revoke, on_revoke=dummy_assign_revoke)
    kc.unsubscribe()

    msg = kc.poll(timeout=0.001)
    if msg is None:
        print('OK: poll() timeout')
    elif msg.error():
        print('OK: consumer error: %s' % msg.error().str())
    else:
        print('OK: consumed message')

    if msg is not None:
        assert msg.timestamp() == (TIMESTAMP_NOT_AVAILABLE, -1)

    msglist = kc.consume(num_messages=10, timeout=0.001)
    assert len(msglist) == 0, "expected 0 messages, not %d" % len(msglist)

    with pytest.raises(ValueError) as ex:
        kc.consume(-100)
    assert 'num_messages must be between 0 and 1000000 (1M)' == str(ex.value)

    with pytest.raises(ValueError) as ex:
        kc.consume(1000001)
    assert 'num_messages must be between 0 and 1000000 (1M)' == str(ex.value)

    partitions = list(map(lambda part: TopicPartition("test", part), range(0, 100, 3)))
    kc.assign(partitions)

    with pytest.raises(KafkaException) as ex:
        kc.seek(TopicPartition("test", 0, 123))
    assert 'Erroneous state' in str(ex.value)

    # Verify assignment
    assignment = kc.assignment()
    assert partitions == assignment

    # Pause partitions
    kc.pause(partitions)

    # Resume partitions
    kc.resume(partitions)

    # Get cached watermarks, should all be invalid.
    lo, hi = kc.get_watermark_offsets(partitions[0], cached=True)
    assert lo == -1001 and hi == -1001
    assert lo == OFFSET_INVALID and hi == OFFSET_INVALID

    # Query broker for watermarks, should raise an exception.
    try:
        lo, hi = kc.get_watermark_offsets(partitions[0], timeout=0.5, cached=False)
    except KafkaException as e:
        assert e.args[0].code() in (
            KafkaError._TIMED_OUT,
            KafkaError._WAIT_COORD,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError._ALL_BROKERS_DOWN,
        )

    kc.unassign(partitions)

    kc.commit(asynchronous=True)

    try:
        kc.commit(asynchronous=False)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._NO_OFFSET)

    # Get current position, should all be invalid.
    kc.position(partitions)
    assert len([p for p in partitions if p.offset == OFFSET_INVALID]) == len(partitions)

    try:
        kc.committed(partitions, timeout=0.001)
    except KafkaException as e:
        assert e.args[0].code() == KafkaError._TIMED_OUT

    try:
        kc.list_topics(timeout=0.2)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT)

    try:
        kc.list_topics(topic="hi", timeout=0.1)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT)

    kc.close()


def test_store_offsets():
    """Basic store_offsets() tests"""

    c = TestConsumer(
        {
            'group.id': 'test',
            'enable.auto.commit': True,
            'enable.auto.offset.store': False,
            'socket.timeout.ms': 50,
            'session.timeout.ms': 100,
        }
    )

    c.subscribe(["test"])

    try:
        c.store_offsets(offsets=[TopicPartition("test", 0, 42)])
    except KafkaException as e:
        assert e.args[0].code() == KafkaError._UNKNOWN_PARTITION

    c.unsubscribe()
    c.close()


def test_on_commit():
    """Verify that on_commit is only called once per commit() (issue #71)"""

    class CommitState(object):
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition
            self.once = True

    def commit_cb(cs, err, ps):
        print('on_commit: err %s, partitions %s' % (err, ps))
        assert cs.once is True
        assert err == KafkaError._NO_OFFSET
        assert len(ps) == 1
        p = ps[0]
        assert p.topic == cs.topic
        assert p.partition == cs.partition
        cs.once = False

    cs = CommitState('test', 2)

    c = TestConsumer(
        {
            'group.id': 'x',
            'enable.auto.commit': False,
            'socket.timeout.ms': 50,
            'session.timeout.ms': 100,
            'on_commit': lambda err, ps: commit_cb(cs, err, ps),
        }
    )

    c.assign([TopicPartition(cs.topic, cs.partition)])

    for i in range(1, 3):
        c.poll(0.1)

        if cs.once:
            # Try commit once
            try:
                c.commit(asynchronous=False)
            except KafkaException as e:
                print('commit failed with %s (expected)' % e)
                assert e.args[0].code() == KafkaError._NO_OFFSET

    c.close()


def test_subclassing():
    class SubConsumer(Consumer):
        def poll(self, somearg):
            assert isinstance(somearg, str)
            super(SubConsumer, self).poll(timeout=0.0001)

    sc = SubConsumer({"group.id": "test", "session.timeout.ms": "90"})
    sc.poll("astring")
    sc.close()


def test_offsets_for_times():
    c = TestConsumer(
        {
            'group.id': 'test',
            'enable.auto.commit': True,
            'enable.auto.offset.store': False,
            'socket.timeout.ms': 50,
            'session.timeout.ms': 100,
        }
    )
    # Query broker for timestamps for partition
    try:
        test_topic_partition = TopicPartition("test", 0, 100)
        c.offsets_for_times([test_topic_partition], timeout=0.1)
    except KafkaException as e:
        assert e.args[0].code() in (
            KafkaError._TIMED_OUT,
            KafkaError._WAIT_COORD,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError._ALL_BROKERS_DOWN,
        )
    c.close()


def test_multiple_close_does_not_throw_exception():
    """Calling Consumer.close() multiple times should not throw Runtime Exception"""
    c = TestConsumer(
        {
            'group.id': 'test',
            'enable.auto.commit': True,
            'enable.auto.offset.store': False,
            'socket.timeout.ms': 50,
            'session.timeout.ms': 100,
        }
    )

    c.subscribe(["test"])

    c.unsubscribe()
    c.close()
    c.close()


def test_any_method_after_close_throws_exception():
    """Calling any consumer method after close should throw a RuntimeError"""
    c = TestConsumer(
        {
            'group.id': 'test',
            'enable.auto.commit': True,
            'enable.auto.offset.store': False,
            'socket.timeout.ms': 50,
            'session.timeout.ms': 100,
        }
    )

    c.subscribe(["test"])
    c.unsubscribe()
    c.close()

    with pytest.raises(RuntimeError) as ex:
        c.subscribe(['test'])
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.unsubscribe()
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.poll()
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.consume()
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.assign([TopicPartition('test', 0)])
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.unassign([TopicPartition('test', 0)])
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.assignment()
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.commit()
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.committed([TopicPartition("test", 0)])
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.position([TopicPartition("test", 0)])
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.seek(TopicPartition("test", 0, 0))
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        lo, hi = c.get_watermark_offsets(TopicPartition("test", 0))
    assert ex.match('Consumer closed')


def test_calling_store_offsets_after_close_throws_erro():
    """calling store_offset after close should throw RuntimeError"""

    c = TestConsumer(
        {
            'group.id': 'test',
            'enable.auto.commit': True,
            'enable.auto.offset.store': False,
            'socket.timeout.ms': 50,
            'session.timeout.ms': 100,
        }
    )

    c.subscribe(["test"])
    c.unsubscribe()
    c.close()

    with pytest.raises(RuntimeError) as ex:
        c.store_offsets(offsets=[TopicPartition("test", 0, 42)])
    assert ex.match('Consumer closed')

    with pytest.raises(RuntimeError) as ex:
        c.offsets_for_times([TopicPartition("test", 0)])
    assert ex.match('Consumer closed')


def test_consumer_without_groupid():
    """Consumer should raise exception if group.id is not set"""

    with pytest.raises(ValueError) as ex:
        TestConsumer({'bootstrap.servers': "mybroker:9092"})
    assert ex.match('group.id must be set')


def test_callback_exception_no_system_error():
    """Test all consumer callbacks exception handling with separate assertions for each callback"""

    # Test error_cb callback
    error_called = []

    def error_cb_that_raises(error):
        """Error callback that raises an exception"""
        error_called.append(error)
        raise RuntimeError("Test exception from error_cb")

    consumer1 = TestConsumer(
        {
            'group.id': 'test-error-callback',
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'error_cb': error_cb_that_raises,
        }
    )

    consumer1.subscribe(['test-topic'])

    # Test error_cb callback
    with pytest.raises(RuntimeError) as exc_info:
        consumer1.consume(timeout=0.1)

    # Verify error_cb was called and raised the expected exception
    assert "Test exception from error_cb" in str(exc_info.value)
    assert len(error_called) > 0
    consumer1.close()

    # Test stats_cb callback
    stats_called = []

    def stats_cb_that_raises(stats_json):
        """Stats callback that raises an exception"""
        stats_called.append(stats_json)
        raise RuntimeError("Test exception from stats_cb")

    consumer2 = TestConsumer(
        {
            'group.id': 'test-stats-callback',
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'statistics.interval.ms': 100,  # Enable stats callback
            'stats_cb': stats_cb_that_raises,
        }
    )

    consumer2.subscribe(['test-topic'])

    # Test stats_cb callback
    with pytest.raises(RuntimeError) as exc_info:
        consumer2.consume(timeout=0.2)  # Longer timeout to allow stats callback

    # Verify stats_cb was called and raised the expected exception
    assert "Test exception from stats_cb" in str(exc_info.value)
    assert len(stats_called) > 0
    consumer2.close()

    # Test throttle_cb callback (may not be triggered in this scenario)
    throttle_called = []

    def throttle_cb_that_raises(throttle_event):
        """Throttle callback that raises an exception"""
        throttle_called.append(throttle_event)
        raise RuntimeError("Test exception from throttle_cb")

    consumer3 = TestConsumer(
        {
            'group.id': 'test-throttle-callback',
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'throttle_cb': throttle_cb_that_raises,
        }
    )

    consumer3.subscribe(['test-topic'])

    # Test throttle_cb callback - may not be triggered, so we'll just verify it doesn't crash
    try:
        consumer3.consume(timeout=0.1)
        # If no exception is raised, that's also fine - throttle_cb may not be triggered
        print("Throttle callback not triggered in this scenario")
    except RuntimeError as exc_info:
        # If throttle_cb was triggered and raised an exception, verify it
        if "Test exception from throttle_cb" in str(exc_info.value):
            assert len(throttle_called) > 0
            print("Throttle callback was triggered and raised exception")

    consumer3.close()


def test_error_callback_exception_different_error_types():
    """Test error callback with different exception types"""

    def error_cb_kafka_exception(error):
        """Error callback that raises KafkaException"""
        raise KafkaException(error)

    def error_cb_value_error(error):
        """Error callback that raises ValueError"""
        raise ValueError(f"Custom error: {error}")

    def error_cb_runtime_error(error):
        """Error callback that raises RuntimeError"""
        raise RuntimeError(f"Runtime error: {error}")

    # Test with KafkaException
    consumer1 = TestConsumer(
        {
            'group.id': 'test-kafka-exception',
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'error_cb': error_cb_kafka_exception,
        }
    )
    consumer1.subscribe(['test-topic'])

    with pytest.raises(KafkaException):
        consumer1.consume(timeout=0.1)
    consumer1.close()

    # Test with ValueError
    consumer2 = TestConsumer(
        {
            'group.id': 'test-value-error',
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'error_cb': error_cb_value_error,
        }
    )
    consumer2.subscribe(['test-topic'])

    with pytest.raises(ValueError) as exc_info:
        consumer2.consume(timeout=0.1)
    assert "Custom error:" in str(exc_info.value)
    consumer2.close()

    # Test with RuntimeError
    consumer3 = TestConsumer(
        {
            'group.id': 'test-runtime-error',
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'session.timeout.ms': 1000,
            'error_cb': error_cb_runtime_error,
        }
    )
    consumer3.subscribe(['test-topic'])

    with pytest.raises(RuntimeError) as exc_info:
        consumer3.consume(timeout=0.1)
    assert "Runtime error:" in str(exc_info.value)
    consumer3.close()


def test_consumer_context_manager_basic():
    """Test basic Consumer context manager usage and return value"""
    config = {'group.id': 'test', 'socket.timeout.ms': 10, 'session.timeout.ms': 100}

    # Test __enter__ returns self
    consumer = Consumer(config)
    entered = consumer.__enter__()
    assert entered is consumer
    consumer.__exit__(None, None, None)  # Clean up

    # Test basic context manager usage
    with Consumer(config) as consumer:
        assert consumer is not None
        consumer.subscribe(['mytopic'])
        consumer.unsubscribe()

    # Consumer should be closed after exiting context
    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.subscribe(['mytopic'])


def test_consumer_context_manager_exception_propagation():
    """Test exceptions propagate and consumer is cleaned up"""
    config = {'group.id': 'test', 'socket.timeout.ms': 10, 'session.timeout.ms': 100}

    # Test exception propagation
    exception_caught = False
    try:
        with Consumer(config) as consumer:
            consumer.subscribe(['mytopic'])
            raise ValueError("Test exception")
    except ValueError as e:
        assert str(e) == "Test exception"
        exception_caught = True

    assert exception_caught, "Exception should have propagated"

    # Consumer should be closed even after exception
    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.subscribe(['mytopic'])


def test_consumer_context_manager_exit_with_exceptions():
    """Test __exit__ properly handles exception arguments"""
    config = {'group.id': 'test', 'socket.timeout.ms': 10, 'session.timeout.ms': 100}

    consumer = Consumer(config)
    consumer.subscribe(['mytopic'])

    # Simulate exception in with block
    exc_type = ValueError
    exc_value = ValueError("Test error")
    exc_traceback = None

    # __exit__ should cleanup and return None (propagate exception)
    result = consumer.__exit__(exc_type, exc_value, exc_traceback)
    assert result is None  # None means propagate exception

    # Consumer should be closed
    with pytest.raises(RuntimeError):
        consumer.subscribe(['mytopic'])


def test_consumer_context_manager_after_exit():
    """Test Consumer behavior after context manager exit"""
    config = {'group.id': 'test', 'socket.timeout.ms': 10, 'session.timeout.ms': 100}

    # Normal exit
    with Consumer(config) as consumer:
        consumer.subscribe(['mytopic'])
        consumer.unsubscribe()

    # All methods should fail after context exit
    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.subscribe(['mytopic'])

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.poll()

    # close() is idempotent - calling it on already-closed consumer should not raise
    consumer.close()  # Should succeed silently

    # Test already-closed consumer edge case
    # Using __enter__ and __exit__ directly on already-closed consumer
    entered = consumer.__enter__()
    assert entered is consumer

    # Operations should still fail
    with pytest.raises(RuntimeError):
        consumer.subscribe(['mytopic'])

    # __exit__ should handle already-closed gracefully
    result = consumer.__exit__(None, None, None)
    assert result is None


def test_consumer_context_manager_multiple_instances():
    """Test Consumer context manager with multiple instances"""
    config = {'group.id': 'test', 'socket.timeout.ms': 10, 'session.timeout.ms': 100}

    # Test multiple sequential instances
    with Consumer(config) as consumer1:
        consumer1.subscribe(['mytopic'])

    with Consumer(config) as consumer2:
        consumer2.subscribe(['mytopic'])
        # Both should be independent
        assert consumer1 is not consumer2

    # Both should be closed
    with pytest.raises(RuntimeError):
        consumer1.subscribe(['mytopic'])
    with pytest.raises(RuntimeError):
        consumer2.subscribe(['mytopic'])

    # Test nested context managers
    with Consumer(config) as consumer1:
        with Consumer(config) as consumer2:
            assert consumer1 is not consumer2
            consumer1.subscribe(['mytopic'])
            consumer2.subscribe(['mytopic2'])
        # consumer2 should be closed, consumer1 still open
        consumer1.unsubscribe()

    # Both should be closed now
    with pytest.raises(RuntimeError):
        consumer1.subscribe(['mytopic'])
    with pytest.raises(RuntimeError):
        consumer2.subscribe(['mytopic2'])


def test_uninitialized_consumer_methods():
    """Test that all Consumer methods raise RuntimeError when called on uninitialized instance."""

    class UninitializedConsumer(Consumer):
        def __init__(self, config):
            # Don't call super().__init__() - leaves self->rk as NULL
            pass

    consumer = UninitializedConsumer({'group.id': 'test'})

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.subscribe(['topic'])

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.unsubscribe()

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.poll()

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.consume()

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.assign([TopicPartition('topic', 0)])

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.unassign()

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.assignment()

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.commit()

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.committed([TopicPartition('topic', 0)])

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.position([TopicPartition('topic', 0)])

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.seek(TopicPartition('topic', 0, 0))

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.get_watermark_offsets(TopicPartition('topic', 0))

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.store_offsets([TopicPartition('topic', 0, 42)])

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.offsets_for_times([TopicPartition('topic', 0)])

    with pytest.raises(RuntimeError, match="Handle has been closed"):
        consumer.list_topics()

    consumer.close()  # Should succeed

    with pytest.raises(RuntimeError, match="Consumer closed"):
        consumer.consumer_group_metadata()


def test_wakeable_poll_utility_functions_interaction():
    """Test interaction between calculate_chunk_timeout() and check_signals_between_chunks().
    """
    # Assertion 1: Both functions work together - chunk calculation + signal check
    consumer1 = TestConsumer({
        'group.id': 'test-interaction-chunk-signal',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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
    consumer2 = TestConsumer({
        'group.id': 'test-interaction-multiple-chunks',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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


def test_wakeable_poll_interruptibility_and_messages():
    """Test poll() interruptibility (main fix) and message handling.
    """
    topic = 'test-poll-interrupt-topic'

    # Assertion 1: Infinite timeout can be interrupted immediately
    consumer1 = TestConsumer({
        'group.id': 'test-poll-infinite-immediate',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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
    consumer2 = TestConsumer({
        'group.id': 'test-poll-finite-interrupt',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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
    consumer3 = TestConsumer({
        'group.id': 'test-poll-multiple-chunks',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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
    consumer4 = TestConsumer({
        'group.id': 'test-poll-timeout-normal',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer4.subscribe([topic])

    start = time.time()
    msg = consumer4.poll(timeout=0.5)  # 500ms, no signal
    elapsed = time.time() - start

    assert msg is None, "Assertion 4 failed: Expected None (timeout), no signal should not interrupt"
    assert WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, \
        f"Assertion 4 failed: Normal timeout took {elapsed:.2f}s, expected ~0.5s"
    consumer4.close()


def test_wakeable_poll_edge_cases():
    """Test poll() edge cases.
    """
    topic = 'test-poll-edge-topic'

    # Assertion 1: Zero timeout returns immediately (non-blocking)
    consumer1 = TestConsumer({
        'group.id': 'test-poll-zero-timeout',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer1.subscribe([topic])

    start = time.time()
    msg = consumer1.poll(timeout=0.0)  # Zero timeout
    elapsed = time.time() - start

    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Assertion 1 failed: Zero timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    assert msg is None, "Assertion 1 failed: Zero timeout with no messages should return None"
    consumer1.close()

    # Assertion 2: Closed consumer raises RuntimeError
    consumer2 = TestConsumer({
        'group.id': 'test-poll-closed',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000
    })
    consumer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        consumer2.poll(timeout=0.1)
    msg = (f"Assertion 2 failed: Expected 'Consumer closed' error, "
           f"got: {exc_info.value}")
    assert 'Consumer closed' in str(exc_info.value), msg

    # Assertion 3: Short timeout works correctly (no signal)
    consumer3 = TestConsumer({
        'group.id': 'test-poll-short-timeout',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer3.subscribe([topic])

    start = time.time()
    msg = consumer3.poll(timeout=0.1)  # 100ms timeout
    elapsed = time.time() - start

    assert msg is None, "Assertion 3 failed: Short timeout with no messages should return None"
    # Short timeouts (< 200ms) don't use chunking, so they can complete faster than WAKEABLE_POLL_TIMEOUT_MIN
    # Only check upper bound to allow for actual timeout duration
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, \
        f"Assertion 3 failed: Short timeout took {elapsed:.2f}s, expected <= {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer3.close()

    # Assertion 4: Very short timeout (less than chunk size) works
    consumer4 = TestConsumer({
        'group.id': 'test-poll-very-short',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer4.subscribe([topic])

    start = time.time()
    msg = consumer4.poll(timeout=0.05)  # 50ms timeout (less than 200ms chunk)
    elapsed = time.time() - start

    assert msg is None, "Assertion 4 failed: Very short timeout should return None"
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Assertion 4 failed: Very short timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer4.close()


def test_wakeable_consume_interruptibility_and_messages():
    """Test consume() interruptibility (main fix) and message handling.
    """
    topic = 'test-consume-interrupt-topic'

    # Assertion 1: Infinite timeout can be interrupted immediately
    consumer1 = TestConsumer({
        'group.id': 'test-consume-infinite-immediate',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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
    consumer2 = TestConsumer({
        'group.id': 'test-consume-finite-interrupt',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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
    consumer3 = TestConsumer({
        'group.id': 'test-consume-multiple-chunks',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
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
    consumer4 = TestConsumer({
        'group.id': 'test-consume-timeout-normal',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer4.subscribe([topic])

    start = time.time()
    msglist = consumer4.consume(num_messages=10, timeout=0.5)  # 500ms, no signal
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 4 failed: consume() should return a list"
    assert len(msglist) == 0, f"Assertion 4 failed: Expected empty list (timeout), got {len(msglist)} messages"
    assert WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, \
        f"Assertion 4 failed: Normal timeout took {elapsed:.2f}s, expected ~0.5s"
    consumer4.close()

    # Assertion 5: num_messages=0 returns empty list immediately
    consumer5 = TestConsumer({
        'group.id': 'test-consume-zero-messages',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer5.subscribe([topic])

    start = time.time()
    msglist = consumer5.consume(num_messages=0, timeout=1.0)
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 5 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 5 failed: num_messages=0 should return empty list"
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Assertion 5 failed: num_messages=0 took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer5.close()


def test_wakeable_consume_edge_cases():
    """Test consume() wakeable edge cases.
    """
    topic = 'test-consume-edge-topic'

    # Assertion 1: Zero timeout returns immediately (non-blocking)
    consumer1 = TestConsumer({
        'group.id': 'test-consume-zero-timeout',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer1.subscribe([topic])

    start = time.time()
    msglist = consumer1.consume(num_messages=10, timeout=0.0)  # Zero timeout
    elapsed = time.time() - start

    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Assertion 1 failed: Zero timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    assert isinstance(msglist, list), "Assertion 1 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 1 failed: Zero timeout with no messages should return empty list"
    consumer1.close()

    # Assertion 2: Closed consumer raises RuntimeError
    consumer2 = TestConsumer({
        'group.id': 'test-consume-closed',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000
    })
    consumer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        consumer2.consume(num_messages=10, timeout=0.1)
    msg = (f"Assertion 2 failed: Expected 'Consumer closed' error, "
           f"got: {exc_info.value}")
    assert 'Consumer closed' in str(exc_info.value), msg

    # Assertion 3: Invalid num_messages (negative) raises ValueError
    consumer3 = TestConsumer({
        'group.id': 'test-consume-invalid-negative',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer3.subscribe([topic])

    with pytest.raises(ValueError) as exc_info:
        consumer3.consume(num_messages=-1, timeout=0.1)
    msg = (f"Assertion 3 failed: Expected num_messages range error, "
           f"got: {exc_info.value}")
    assert 'num_messages must be between 0 and 1000000' in str(exc_info.value), msg
    consumer3.close()

    # Assertion 4: Invalid num_messages (too large) raises ValueError
    consumer4 = TestConsumer({
        'group.id': 'test-consume-invalid-large',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer4.subscribe([topic])

    with pytest.raises(ValueError) as exc_info:
        consumer4.consume(num_messages=1000001, timeout=0.1)
    msg = (f"Assertion 4 failed: Expected num_messages range error, "
           f"got: {exc_info.value}")
    assert 'num_messages must be between 0 and 1000000' in str(exc_info.value), msg
    consumer4.close()

    # Assertion 5: Short timeout works correctly (no signal)
    consumer5 = TestConsumer({
        'group.id': 'test-consume-short-timeout',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer5.subscribe([topic])

    start = time.time()
    msglist = consumer5.consume(num_messages=10, timeout=0.1)  # 100ms timeout
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 5 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 5 failed: Short timeout with no messages should return empty list"
    # Only check upper bound to allow for actual timeout duration
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, \
        f"Assertion 5 failed: Short timeout took {elapsed:.2f}s, expected <= {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer5.close()

    # Assertion 6: Very short timeout (less than chunk size) works
    consumer6 = TestConsumer({
        'group.id': 'test-consume-very-short',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'auto.offset.reset': 'latest'
    })
    consumer6.subscribe([topic])

    start = time.time()
    msglist = consumer6.consume(num_messages=5, timeout=0.05)  # 50ms timeout (less than 200ms chunk)
    elapsed = time.time() - start

    assert isinstance(msglist, list), "Assertion 6 failed: consume() should return a list"
    assert len(msglist) == 0, "Assertion 6 failed: Very short timeout should return empty list"
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Assertion 6 failed: Very short timeout took {elapsed:.2f}s, expected < {WAKEABLE_POLL_TIMEOUT_MAX}s"
    consumer6.close()
