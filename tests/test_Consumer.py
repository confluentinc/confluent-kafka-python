#!/usr/bin/env python

import pytest

from confluent_kafka import (Consumer, TopicPartition, KafkaError,
                             KafkaException, TIMESTAMP_NOT_AVAILABLE,
                             OFFSET_INVALID)

from tests.common import TestConsumer


def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    with pytest.raises(TypeError) as ex:
        kc = TestConsumer()
    assert ex.match('expected configuration dict')

    def dummy_commit_cb(err, partitions):
        pass

    kc = TestConsumer({'group.id': 'test', 'socket.timeout.ms': '100',
                       'session.timeout.ms': 1000,  # Avoid close() blocking too long
                       'on_commit': dummy_commit_cb})

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
            KafkaError._TIMED_OUT, KafkaError._WAIT_COORD,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError._ALL_BROKERS_DOWN)

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
    """ Basic store_offsets() tests """

    c = TestConsumer({'group.id': 'test',
                      'enable.auto.commit': True,
                      'enable.auto.offset.store': False,
                      'socket.timeout.ms': 50,
                      'session.timeout.ms': 100})

    c.subscribe(["test"])

    try:
        c.store_offsets(offsets=[TopicPartition("test", 0, 42)])
    except KafkaException as e:
        assert e.args[0].code() == KafkaError._UNKNOWN_PARTITION

    c.unsubscribe()
    c.close()


def test_on_commit():
    """ Verify that on_commit is only called once per commit() (issue #71) """

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

    c = TestConsumer({'group.id': 'x',
                      'enable.auto.commit': False, 'socket.timeout.ms': 50,
                      'session.timeout.ms': 100,
                      'on_commit': lambda err, ps: commit_cb(cs, err, ps)})

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
    c = TestConsumer({'group.id': 'test',
                      'enable.auto.commit': True,
                      'enable.auto.offset.store': False,
                      'socket.timeout.ms': 50,
                      'session.timeout.ms': 100})
    # Query broker for timestamps for partition
    try:
        test_topic_partition = TopicPartition("test", 0, 100)
        c.offsets_for_times([test_topic_partition], timeout=0.1)
    except KafkaException as e:
        assert e.args[0].code() in (
            KafkaError._TIMED_OUT, KafkaError._WAIT_COORD,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError._ALL_BROKERS_DOWN)
    c.close()


def test_multiple_close_does_not_throw_exception():
    """ Calling Consumer.close() multiple times should not throw Runtime Exception
    """
    c = TestConsumer({'group.id': 'test',
                      'enable.auto.commit': True,
                      'enable.auto.offset.store': False,
                      'socket.timeout.ms': 50,
                      'session.timeout.ms': 100})

    c.subscribe(["test"])

    c.unsubscribe()
    c.close()
    c.close()


def test_any_method_after_close_throws_exception():
    """ Calling any consumer method after close should throw a RuntimeError
    """
    c = TestConsumer({'group.id': 'test',
                      'enable.auto.commit': True,
                      'enable.auto.offset.store': False,
                      'socket.timeout.ms': 50,
                      'session.timeout.ms': 100})

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
    """ calling store_offset after close should throw RuntimeError """

    c = TestConsumer({'group.id': 'test',
                      'enable.auto.commit': True,
                      'enable.auto.offset.store': False,
                      'socket.timeout.ms': 50,
                      'session.timeout.ms': 100})

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
    """ Consumer should raise exception if group.id is not set """

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

    consumer1 = TestConsumer({
        'group.id': 'test-error-callback',
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'error_cb': error_cb_that_raises
    })

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

    consumer2 = TestConsumer({
        'group.id': 'test-stats-callback',
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'statistics.interval.ms': 100,  # Enable stats callback
        'stats_cb': stats_cb_that_raises
    })

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

    consumer3 = TestConsumer({
        'group.id': 'test-throttle-callback',
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'throttle_cb': throttle_cb_that_raises
    })

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
    consumer1 = TestConsumer({
        'group.id': 'test-kafka-exception',
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'error_cb': error_cb_kafka_exception
    })
    consumer1.subscribe(['test-topic'])

    with pytest.raises(KafkaException):
        consumer1.consume(timeout=0.1)
    consumer1.close()

    # Test with ValueError
    consumer2 = TestConsumer({
        'group.id': 'test-value-error',
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'error_cb': error_cb_value_error
    })
    consumer2.subscribe(['test-topic'])

    with pytest.raises(ValueError) as exc_info:
        consumer2.consume(timeout=0.1)
    assert "Custom error:" in str(exc_info.value)
    consumer2.close()

    # Test with RuntimeError
    consumer3 = TestConsumer({
        'group.id': 'test-runtime-error',
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'session.timeout.ms': 1000,
        'error_cb': error_cb_runtime_error
    })
    consumer3.subscribe(['test-topic'])

    with pytest.raises(RuntimeError) as exc_info:
        consumer3.consume(timeout=0.1)
    assert "Runtime error:" in str(exc_info.value)
    consumer3.close()
