#!/usr/bin/env python

from confluent_kafka import (Consumer, TopicPartition, KafkaError, KafkaException, TIMESTAMP_NOT_AVAILABLE,
                             OFFSET_INVALID, libversion)
import pytest


def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    try:
        kc = Consumer()
    except TypeError as e:
        assert str(e) == "expected configuration dict"

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

    partitions = list(map(lambda part: TopicPartition("test", part), range(0, 100, 3)))
    kc.assign(partitions)

    # Verify assignment
    assignment = kc.assignment()
    assert partitions == assignment

    # Get cached watermarks, should all be invalid.
    lo, hi = kc.get_watermark_offsets(partitions[0], cached=True)
    assert lo == -1001 and hi == -1001
    assert lo == OFFSET_INVALID and hi == OFFSET_INVALID

    # Query broker for watermarks, should raise an exception.
    try:
        lo, hi = kc.get_watermark_offsets(partitions[0], timeout=0.5, cached=False)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._WAIT_COORD, KafkaError.LEADER_NOT_AVAILABLE),\
            str(e.args([0]))

    kc.unassign()

    kc.commit(async=True)

    try:
        kc.commit(async=False)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._NO_OFFSET)

    # Get current position, should all be invalid.
    kc.position(partitions)
    assert len([p for p in partitions if p.offset == OFFSET_INVALID]) == len(partitions)

    try:
        kc.committed(partitions, timeout=0.001)
    except KafkaException as e:
        assert e.args[0].code() == KafkaError._TIMED_OUT

    kc.close()


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
                c.commit(async=False)
            except KafkaException as e:
                print('commit failed with %s (expected)' % e)
                assert e.args[0].code() == KafkaError._NO_OFFSET

    c.close()


def test_subclassing():
    class SubConsumer(Consumer):
        def poll(self, somearg):
            assert type(somearg) == str
            super(SubConsumer, self).poll(timeout=0.0001)

    sc = SubConsumer({"group.id": "test", "session.timeout.ms": "90"})
    sc.poll("astring")
    sc.close()
