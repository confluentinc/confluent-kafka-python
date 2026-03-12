#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for ShareConsumer class.
"""
import pytest


try:
    from confluent_kafka import ShareConsumer
    SHARE_CONSUMER_AVAILABLE = True
except ImportError:
    SHARE_CONSUMER_AVAILABLE = False


pytestmark = pytest.mark.skipif(
    not SHARE_CONSUMER_AVAILABLE,
    reason="ShareConsumer requires librdkafka with KIP-932 support"
)


def test_constructor_requires_config():
    """ShareConsumer constructor requires a configuration dict."""
    with pytest.raises(TypeError) as ex:
        ShareConsumer()
    assert ex.match('expected configuration dict')


def test_constructor_with_valid_config():
    """ShareConsumer can be created with valid configuration."""
    sc = ShareConsumer({
        'group.id': 'test-share-group',
        'bootstrap.servers': 'localhost:9092',
        'session.timeout.ms': 100,
    })
    assert sc is not None
    sc.close()


def test_subscribe():
    """Test subscribe() method."""
    sc = ShareConsumer({
        'group.id': 'test-share-group',
        'bootstrap.servers': 'localhost:9092',
        'session.timeout.ms': 100,
    })

    sc.subscribe(['test-topic'])

    subscription = sc.subscription()
    assert subscription is not None
    assert 'test-topic' in subscription

    sc.close()


def test_unsubscribe():
    """Test unsubscribe() method."""
    sc = ShareConsumer({
        'group.id': 'test-share-group',
        'bootstrap.servers': 'localhost:9092',
        'session.timeout.ms': 100,
    })

    sc.subscribe(['test-topic'])
    sc.unsubscribe()

    subscription = sc.subscription()
    assert len(subscription) == 0

    sc.close()


def test_consume_batch_no_broker():
    """Test consume_batch() returns empty list when no broker available."""
    sc = ShareConsumer({
        'group.id': 'test-share-group',
        'bootstrap.servers': 'localhost:9092',
        'socket.timeout.ms': '100',
        'session.timeout.ms': 100,
    })

    sc.subscribe(['test-topic'])

    # Should timeout and return empty list
    messages = sc.consume_batch(timeout=0.1)
    assert isinstance(messages, list)
    # May be empty or contain error messages

    sc.close()



def test_close_idempotent():
    """Test that close() can be called multiple times."""
    sc = ShareConsumer({
        'group.id': 'test-share-group',
        'bootstrap.servers': 'localhost:9092',
        'session.timeout.ms': 100,
    })

    sc.close()
    sc.close()  # Should not raise


def test_any_method_after_close_throws_exception():
    """Test that all operations on a closed consumer raise RuntimeError."""
    sc = ShareConsumer({
        'group.id': 'test-share-group',
        'bootstrap.servers': 'localhost:9092',
        'session.timeout.ms': 100,
    })

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
        sc.consume_batch(timeout=0.1)
    assert ex.match('Share consumer closed')


def test_required_group_id():
    """Test that group.id is required."""
    with pytest.raises(ValueError) as ex:
        ShareConsumer({
            'bootstrap.servers': 'localhost:9092',
        })
    assert ex.match('group.id must be set')


@pytest.mark.integration
def test_concurrent_consumers():
    """Test multiple consumers in same share group."""
    pytest.skip("Requires running Kafka broker with Share Consumer support")

    kafka_config = {
        'group.id': 'test-share-group-integration',
        'bootstrap.servers': 'localhost:9092',
        'session.timeout.ms': 100,
    }

    sc1 = ShareConsumer(kafka_config)
    sc2 = ShareConsumer(kafka_config)

    try:
        sc1.subscribe(['test-topic'])
        sc2.subscribe(['test-topic'])

        messages1 = sc1.consume_batch(timeout=2.0)
        messages2 = sc2.consume_batch(timeout=2.0)

        # Verify no overlap (share group semantics)
        offsets1 = {(msg.topic(), msg.partition(), msg.offset())
                    for msg in messages1 if not msg.error()}
        offsets2 = {(msg.topic(), msg.partition(), msg.offset())
                    for msg in messages2 if not msg.error()}

        assert len(offsets1.intersection(offsets2)) == 0

    finally:
        sc1.close()
        sc2.close()
