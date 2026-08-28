#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import inspect
import sys
import time
from uuid import uuid1

from confluent_kafka import OFFSET_INVALID, KafkaError
from tests.common import TestConsumer


def called_by():
    if sys.version_info < (3, 5):
        return inspect.stack()[1][3]

    return inspect.stack()[1].function


def prefixed_error_cb(prefix):
    def error_cb(err):
        """Reports global/generic errors to aid in troubleshooting test failures."""
        print("[{}]: {}".format(prefix, err))

    return error_cb


def prefixed_delivery_cb(prefix):
    def delivery_err(err, msg):
        """Reports failed message delivery to aid in troubleshooting test failures."""
        if err:
            print("[{}]: Message delivery failed ({} [{}]): {}".format(prefix, msg.topic(), str(msg.partition()), err))
            return

    return delivery_err


def test_commit_transaction(kafka_cluster):
    output_topic = kafka_cluster.create_topic_and_wait_propogation("output_topic")

    producer = kafka_cluster.producer(
        {
            'transactional.id': 'example_transactional_id',
            'error_cb': prefixed_error_cb('test_commit_transaction'),
        }
    )

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)
    producer.commit_transaction()

    assert consume_committed(kafka_cluster.client_conf(), output_topic) == 100


def test_abort_transaction(kafka_cluster):
    output_topic = kafka_cluster.create_topic_and_wait_propogation("output_topic")

    producer = kafka_cluster.producer(
        {
            'transactional.id': 'example_transactional_id',
            'error_cb': prefixed_error_cb('test_abort_transaction'),
        }
    )

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)
    producer.abort_transaction()

    assert consume_committed(kafka_cluster.client_conf(), output_topic) == 0


def test_abort_retry_commit_transaction(kafka_cluster):
    output_topic = kafka_cluster.create_topic_and_wait_propogation("output_topic")

    producer = kafka_cluster.producer(
        {
            'transactional.id': 'example_transactional_id',
            'error_cb': prefixed_error_cb('test_abort_retry_commit_transaction'),
        }
    )

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)
    producer.abort_transaction()

    transactional_produce(producer, output_topic, 25)
    producer.commit_transaction()

    assert consume_committed(kafka_cluster.client_conf(), output_topic) == 25


def test_send_offsets_committed_transaction(kafka_cluster):
    input_topic = kafka_cluster.create_topic_and_wait_propogation("input_topic")
    output_topic = kafka_cluster.create_topic_and_wait_propogation("output_topic")
    error_cb = prefixed_error_cb('test_send_offsets_committed_transaction')
    producer = kafka_cluster.producer(
        {
            'client.id': 'producer1',
            'transactional.id': 'example_transactional_id',
            'error_cb': error_cb,
        }
    )

    consumer_conf = {
        'group.id': str(uuid1()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': True,
        'error_cb': error_cb,
    }
    consumer_conf.update(kafka_cluster.client_conf())
    consumer = TestConsumer(consumer_conf)

    kafka_cluster.seed_topic(input_topic)
    consumer.subscribe([input_topic])

    read_all_msgs(consumer)

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)

    consumer_position = consumer.position(consumer.assignment())
    group_metadata = consumer.consumer_group_metadata()
    print("=== Sending offsets {} to transaction ===".format(consumer_position))
    producer.send_offsets_to_transaction(consumer_position, group_metadata)
    producer.commit_transaction()

    producer2 = kafka_cluster.producer(
        {'client.id': 'producer2', 'transactional.id': 'example_transactional_id', 'error_cb': error_cb}
    )

    # ensure offset commits are visible prior to sending FetchOffsets request
    producer2.init_transactions()

    committed_offsets = consumer.committed(consumer.assignment())
    print("=== Committed offsets for {} ===".format(committed_offsets))

    assert [tp.offset for tp in committed_offsets] == [100]

    consumer.close()


def test_send_offsets_aborted_transaction(kafka_cluster):
    """Offsets passed to send_offsets_to_transaction() must not be
    committed if the transaction is aborted rather than committed."""
    input_topic = kafka_cluster.create_topic_and_wait_propogation("input_topic")
    output_topic = kafka_cluster.create_topic_and_wait_propogation("output_topic")
    error_cb = prefixed_error_cb('test_send_offsets_aborted_transaction')
    producer = kafka_cluster.producer(
        {
            'transactional.id': 'example_transactional_id',
            'error_cb': error_cb,
        }
    )

    consumer_conf = {
        'group.id': str(uuid1()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': True,
        'error_cb': error_cb,
    }
    consumer_conf.update(kafka_cluster.client_conf())
    consumer = TestConsumer(consumer_conf)

    kafka_cluster.seed_topic(input_topic)
    consumer.subscribe([input_topic])

    read_all_msgs(consumer)

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)

    consumer_position = consumer.position(consumer.assignment())
    group_metadata = consumer.consumer_group_metadata()
    print("=== Sending offsets {} to transaction (to be aborted) ===".format(consumer_position))
    producer.send_offsets_to_transaction(consumer_position, group_metadata)
    producer.abort_transaction()

    committed_offsets = consumer.committed(consumer.assignment())
    print("=== Committed offsets after abort: {} ===".format(committed_offsets))
    assert all(
        tp.offset == OFFSET_INVALID for tp in committed_offsets
    ), f"expected no committed offsets after an aborted transaction, got: {committed_offsets}"

    # The produced messages must not be visible either.
    assert consume_committed(kafka_cluster.client_conf(), output_topic) == 0

    consumer.close()


def test_close_resolves_open_transaction_promptly(kafka_cluster):
    """close() should actively abort an open transaction rather
    than leave it dangling on the broker. A dangling transaction blocks its
    partition's last-stable-offset (LSO) from advancing, so a read_committed
    consumer can't see any later message on that partition until the transaction
    resolves. Without an explicit abort, that only happens once transaction.timeout.ms
    elapses; with one, it should happen almost immediately.
    """
    output_topic = kafka_cluster.create_topic_and_wait_propogation("output_topic")

    txn_timeout_ms = 15000
    visibility_deadline_s = 7

    producer1 = kafka_cluster.producer(
        {
            'transactional.id': f'test_close_resolves_open_transaction_promptly-{uuid1()}',
            'transaction.timeout.ms': txn_timeout_ms,
            'error_cb': prefixed_error_cb('test_close_resolves_open_transaction_promptly-p1'),
        }
    )
    producer1.init_transactions()
    producer1.begin_transaction()
    producer1.produce(output_topic, value=b'from-open-txn')
    producer1.flush()

    # Deliberately close() without calling abort_transaction()
    assert producer1.close() is True

    # Now create a new non-transactional producer and produce one msg to the same topic
    producer2 = kafka_cluster.producer(
        {'error_cb': prefixed_error_cb('test_close_resolves_open_transaction_promptly-p2')}
    )
    producer2.produce(output_topic, value=b'plain-message-after-close')
    producer2.close()

    # Create a new consumer to try reading the msg produced by producer2
    consumer_conf = kafka_cluster.client_conf()
    consumer_conf.update(
        {
            'group.id': str(uuid1()),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
            'error_cb': prefixed_error_cb('test_close_resolves_open_transaction_promptly-consumer'),
        }
    )
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([output_topic])

    deadline = time.monotonic() + visibility_deadline_s
    msg = None
    while time.monotonic() < deadline:
        msg = consumer.poll(timeout=0.5)
        if msg is not None and msg.error() is None:
            break
        msg = None

    consumer.close()

    assert msg is not None, (
        f"plain message produced after close() was not visible to a read_committed "
        f"consumer within {visibility_deadline_s}s -- the transaction left open by "
        f"close() is blocking the partition's last-stable-offset from advancing"
    )
    assert msg.value() == b'plain-message-after-close'


def transactional_produce(producer, topic, num_messages):
    print("=== Producing {} transactional messages to topic {}. ===".format(num_messages, topic))

    producer.begin_transaction()

    for value in ['test-data{}'.format(i) for i in range(0, num_messages)]:
        producer.produce(topic, value, on_delivery=prefixed_delivery_cb(called_by()))
        producer.poll(0.0)

    producer.flush()


def read_all_msgs(consumer):
    """
    Consumes all messages in the consumer assignment.

    This method assumes the consumer has not already read all of the
    messages available in a partition.

    :param consumer:
    :returns: total messages read
    :rtype: int
    """
    msg_cnt = 0
    eof = {}
    print("=== Draining {} ===".format(consumer.assignment()))
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        topic, partition = msg.topic(), msg.partition()
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof[(topic, partition)] = True
                if len(eof) == len(consumer.assignment()):
                    break
            continue

        eof.pop((topic, partition), None)
        msg_cnt += 1

    return msg_cnt


def consume_committed(conf, topic):
    print("=== Consuming transactional messages from topic {}. ===".format(topic))

    consumer_conf = {
        'group.id': str(uuid1()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': True,
        'error_cb': prefixed_error_cb(called_by()),
    }

    consumer_conf.update(conf)
    consumer = TestConsumer(consumer_conf)
    consumer.subscribe([topic])

    msg_cnt = read_all_msgs(consumer)

    consumer.close()

    return msg_cnt
