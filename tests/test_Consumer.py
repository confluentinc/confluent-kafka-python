#!/usr/bin/env python

from confluent_kafka import (Consumer, TopicPartition, KafkaError,
                             KafkaException, TIMESTAMP_NOT_AVAILABLE,
                             OFFSET_INVALID, libversion)
import pytest


def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    with pytest.raises(TypeError) as ex:
        kc = Consumer()
    assert ex.match('expected configuration dict')

    def dummy_commit_cb(err, partitions):
        pass

    kc = Consumer({'group.id': 'test', 'socket.timeout.ms': '100',
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

    kc.unassign()

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


@pytest.mark.skipif(libversion()[1] < 0x000b0000,
                    reason="requires librdkafka >=0.11.0")
def test_store_offsets():
    """ Basic store_offsets() tests """

    c = Consumer({'group.id': 'test',
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


# librdkafka <=0.9.2 has a race-issue where it will hang indefinately
# if a commit is issued when no coordinator is available.
@pytest.mark.skipif(libversion()[1] <= 0x000902ff,
                    reason="requires librdkafka >0.9.2")
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

    c = Consumer({'group.id': 'x',
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


@pytest.mark.skipif(libversion()[1] < 0x000b0000,
                    reason="requires librdkafka >=0.11.0")
def test_offsets_for_times():
    c = Consumer({'group.id': 'test',
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
    c = Consumer({'group.id': 'test',
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
    c = Consumer({'group.id': 'test',
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
        c.unassign()
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


@pytest.mark.skipif(libversion()[1] < 0x000b0000,
                    reason="requires librdkafka >=0.11.0")
def test_calling_store_offsets_after_close_throws_erro():
    """ calling store_offset after close should throw RuntimeError """

    c = Consumer({'group.id': 'test',
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
        Consumer({'bootstrap.servers': "mybroker:9092"})
    assert ex.match('group.id must be set')
