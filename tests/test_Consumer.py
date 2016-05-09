#!/usr/bin/env python

from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException


def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    try:
        kc = Consumer()
    except TypeError as e:
        assert str(e) == "expected configuration dict"

    def dummy_commit_cb (err, partitions):
        pass

    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100',
                   'session.timeout.ms': 1000, # Avoid close() blocking too long
                   'on_commit': dummy_commit_cb})

    kc.subscribe(["test"])
    kc.unsubscribe()

    def dummy_assign_revoke (consumer, partitions):
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

    partitions = list(map(lambda p: TopicPartition("test", p), range(0,100,3)))
    kc.assign(partitions)

    kc.unassign()

    kc.commit(async=True)

    try:
        kc.commit(async=False)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._NO_OFFSET)

    # Get current position, should all be invalid.
    kc.position(partitions)
    assert len([p for p in partitions if p.offset == -1001]) == len(partitions)

    try:
        offsets = kc.committed(partitions, timeout=0.001)
    except KafkaException as e:
        assert e.args[0].code() == KafkaError._TIMED_OUT


    kc.close()

