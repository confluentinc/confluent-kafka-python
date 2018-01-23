import time
import json
import uuid
import os
import pytest

from confluent_kafka import Consumer, KafkaError, Producer


def retry(assertion_callable, retry_time=10, wait_between_tries=0.1, exception_to_retry=AssertionError):
    start = time.time()
    while True:
        try:
            return assertion_callable()
        except exception_to_retry as e:
            if time.time() - start >= retry_time:
                raise e
            time.sleep(wait_between_tries)


@pytest.mark.parametrize("pause_broker", [False, True])
def test_broker(pause_broker):
    """
    Signal handlers do not support threads
    """
    source_topic_name = str(uuid.uuid4())

    producer = Producer(**{
        'bootstrap.servers': 'localhost:9094',
        'compression.codec': 'snappy',
        'queue.buffering.max.ms': 0,
        'metadata.request.timeout.ms': 100,
        'topic.metadata.refresh.interval.ms': 100,
        'socket.timeout.ms': 100,
    })

    n_msgs = 100
    for i in range(n_msgs):
        producer.produce(source_topic_name, json.dumps({'foo': i}))
    producer.flush()

    if pause_broker:
        os.system('docker-compose pause kafka1')

    def _error_callback(err):
        print("got err: {}".format(str(err)))

    new_consumer = Consumer(**{
        'bootstrap.servers': 'localhost:9094',
        'group.id': str(uuid.uuid4()),
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'enable.auto.commit': False,
        'metadata.request.timeout.ms': 100,
        'topic.metadata.refresh.interval.ms': 100,
        'socket.timeout.ms': 100,
        'error_cb': _error_callback,
    })
    new_consumer.subscribe([source_topic_name])
    msgs = []

    def _ensure_delivery():
        while True:
            raw_msg = new_consumer.poll(0)
            if raw_msg:
                msgs.append(raw_msg)
            else:
                break

        assert len(msgs) == n_msgs

    retry(_ensure_delivery, retry_time=30)
    new_consumer.close()
