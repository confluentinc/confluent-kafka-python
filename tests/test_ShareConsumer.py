#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for ShareConsumer class.
"""
import pytest

from confluent_kafka import ShareConsumer


def test_constructor_requires_config():
    """ShareConsumer constructor requires a configuration dict."""
    with pytest.raises(TypeError) as ex:
        ShareConsumer()
    assert ex.match('expected configuration dict')


def test_constructor_with_valid_config():
    """ShareConsumer can be created with valid configuration."""
    sc = ShareConsumer(
        {
            'group.id': 'test-share-group',
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )
    assert sc is not None
    sc.close()


def test_subscribe():
    """Test subscribe() method."""
    sc = ShareConsumer(
        {
            'group.id': 'test-share-group',
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.subscribe(['test-topic'])

    subscription = sc.subscription()
    assert subscription is not None
    assert 'test-topic' in subscription

    sc.close()


def test_unsubscribe():
    """Test unsubscribe() method."""
    sc = ShareConsumer(
        {
            'group.id': 'test-share-group',
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.subscribe(['test-topic'])
    sc.unsubscribe()

    subscription = sc.subscription()
    assert len(subscription) == 0

    sc.close()


def test_poll_no_broker():
    """Test poll() returns empty list when no broker available."""
    sc = ShareConsumer(
        {
            'group.id': 'test-share-group',
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.subscribe(['test-topic'])

    # Should timeout and return empty list
    messages = sc.poll(timeout=0.1)
    assert isinstance(messages, list)
    # May be empty or contain error messages

    sc.close()


def test_context_manager():
    """Test that ShareConsumer works as a context manager and closes on exit."""
    with ShareConsumer(
        {
            'group.id': 'test-share-group',
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    ) as sc:
        assert sc is not None
        sc.subscribe(['test-topic'])
        subscription = sc.subscription()
        assert 'test-topic' in subscription

    # After exiting the context manager, the consumer should be closed
    with pytest.raises(RuntimeError) as ex:
        sc.subscribe(['test-topic'])
    assert ex.match('Share consumer closed')


def test_close_idempotent():
    """Test that close() can be called multiple times."""
    sc = ShareConsumer(
        {
            'group.id': 'test-share-group',
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.close()
    sc.close()  # Should not raise


def test_any_method_after_close_throws_exception():
    """Test that all operations on a closed consumer raise RuntimeError."""
    sc = ShareConsumer(
        {
            'group.id': 'test-share-group',
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.subscribe(['test-topic'])
    sc.close()

    with pytest.raises(RuntimeError) as ex:
        sc.subscribe(['test'])
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.unsubscribe()
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.subscription()
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.poll(timeout=0.1)
    assert ex.match('Share consumer closed')


def test_required_group_id():
    """Test that group.id is required."""
    with pytest.raises(ValueError) as ex:
        ShareConsumer(
            {
                'bootstrap.servers': 'localhost:9092',
            }
        )
    assert ex.match('group.id must be set')


@pytest.mark.integration
def test_concurrent_consumers():
    """Test multiple consumers in same share group."""
    pytest.skip("Requires running Kafka broker with Share Consumer support")

    kafka_config = {
        'group.id': 'test-share-group-integration',
        'bootstrap.servers': 'localhost:9092',
        'socket.timeout.ms': 100,
    }

    sc1 = ShareConsumer(kafka_config)
    sc2 = ShareConsumer(kafka_config)

    try:
        sc1.subscribe(['test-topic'])
        sc2.subscribe(['test-topic'])

        messages1 = sc1.poll(timeout=2.0)
        messages2 = sc2.poll(timeout=2.0)

        # Verify no overlap (share group semantics)
        offsets1 = {(msg.topic(), msg.partition(), msg.offset()) for msg in messages1 if not msg.error()}
        offsets2 = {(msg.topic(), msg.partition(), msg.offset()) for msg in messages2 if not msg.error()}

        assert len(offsets1.intersection(offsets2)) == 0

    finally:
        sc1.close()
        sc2.close()


def test_error_cb():
    """Test that error_cb fires for ShareConsumer when broker is unreachable."""
    error_called = []

    def my_error_cb(error):
        error_called.append(error)

    sc = ShareConsumer(
        {
            'group.id': 'test-share-error-cb',
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': my_error_cb,
        }
    )

    sc.subscribe(['test-topic'])
    sc.poll(timeout=0.5)

    assert len(error_called) > 0, "error_cb should have been called"
    sc.close()


def test_error_cb_exception_propagates():
    """Test that an exception raised in error_cb propagates to poll."""
    error_called = []

    def error_cb_that_raises(error):
        error_called.append(error)
        raise RuntimeError("Test exception from error_cb")

    sc = ShareConsumer(
        {
            'group.id': 'test-share-error-cb-exc',
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': error_cb_that_raises,
        }
    )

    sc.subscribe(['test-topic'])

    with pytest.raises(RuntimeError) as exc_info:
        sc.poll(timeout=0.5)

    assert "Test exception from error_cb" in str(exc_info.value)
    assert len(error_called) > 0
    # close() may also drain pending callbacks and re-raise, so ignore
    try:
        sc.close()
    except RuntimeError:
        pass


def test_stats_cb():
    """Test that stats_cb fires for ShareConsumer."""
    stats_called = []

    def my_stats_cb(stats_json):
        stats_called.append(stats_json)

    sc = ShareConsumer(
        {
            'group.id': 'test-share-stats-cb',
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'statistics.interval.ms': 100,
            'stats_cb': my_stats_cb,
        }
    )

    sc.subscribe(['test-topic'])
    sc.poll(timeout=0.5)

    assert len(stats_called) > 0, "stats_cb should have been called"
    # Verify we got valid JSON string
    import json

    parsed = json.loads(stats_called[0])
    assert 'name' in parsed
    sc.close()


def test_stats_cb_exception_propagates():
    """Test that an exception raised in stats_cb propagates to poll."""
    stats_called = []

    def stats_cb_that_raises(stats_json):
        stats_called.append(stats_json)
        raise RuntimeError("Test exception from stats_cb")

    sc = ShareConsumer(
        {
            'group.id': 'test-share-stats-cb-exc',
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'statistics.interval.ms': 100,
            'stats_cb': stats_cb_that_raises,
        }
    )

    sc.subscribe(['test-topic'])

    with pytest.raises(RuntimeError) as exc_info:
        sc.poll(timeout=0.5)

    assert "Test exception from stats_cb" in str(exc_info.value)
    assert len(stats_called) > 0
    sc.close()


def test_throttle_cb():
    """Test that throttle_cb can be registered without crashing.

    throttle_cb requires broker-side throttling to fire, which can't be
    triggered in a unit test. We verify it can be set and doesn't crash.
    """
    throttle_called = []

    def my_throttle_cb(event):
        throttle_called.append(event)

    sc = ShareConsumer(
        {
            'group.id': 'test-share-throttle-cb',
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'throttle_cb': my_throttle_cb,
        }
    )

    sc.subscribe(['test-topic'])

    # throttle_cb won't fire without broker throttling — just verify no crash
    sc.poll(timeout=0.2)
    sc.close()
