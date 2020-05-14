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
# limit
#
import inspect
import sys
from uuid import uuid1

from confluent_kafka import Consumer, KafkaError


def called_by():
    if sys.version_info < (3, 5):
        return inspect.stack()[1][3]

    return inspect.stack()[1].function


def prefixed_error_cb(prefix):
    def error_cb(err):
        """ Reports global/generic errors to aid in troubleshooting test failures. """
        print("[{}]: {}".format(prefix, err))

    return error_cb


def prefixed_delivery_cb(prefix):
    def delivery_err(err, msg):
        """ Reports failed message delivery to aid in troubleshooting test failures. """
        if err:
            print("[{}]: Message delivery failed ({} [{}]): {}".format(
                prefix, msg.topic(), str(msg.partition()), err))
            return

    return delivery_err


def test_commit_transaction(kafka_cluster):
    output_topic = kafka_cluster.create_topic("output_topic")

    producer = kafka_cluster.producer({
        'transactional.id': 'example_transactional_id',
        'error_cb': prefixed_error_cb('test_commit_transaction'),
    })

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)
    producer.commit_transaction()

    assert consume_committed(kafka_cluster.client_conf(), output_topic) == 100


def test_abort_transaction(kafka_cluster):
    output_topic = kafka_cluster.create_topic("output_topic")

    producer = kafka_cluster.producer({
        'transactional.id': 'example_transactional_id',
        'error_cb': prefixed_error_cb('test_abort_transaction'),
    })

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)
    producer.abort_transaction()

    assert consume_committed(kafka_cluster.client_conf(), output_topic) == 0


def test_abort_retry_commit_transaction(kafka_cluster):
    output_topic = kafka_cluster.create_topic("output_topic")

    producer = kafka_cluster.producer({
        'transactional.id': 'example_transactional_id',
        'error_cb': prefixed_error_cb('test_abort_retry_commit_transaction'),
    })

    producer.init_transactions()
    transactional_produce(producer, output_topic, 100)
    producer.abort_transaction()

    transactional_produce(producer, output_topic, 25)
    producer.commit_transaction()

    assert consume_committed(kafka_cluster.client_conf(), output_topic) == 25


def test_send_offsets_committed_transaction(kafka_cluster):
    input_topic = kafka_cluster.create_topic("input_topic")
    output_topic = kafka_cluster.create_topic("output_topic")
    error_cb = prefixed_error_cb('test_send_offsets_committed_transaction')
    producer = kafka_cluster.producer({
        'client.id': 'producer1',
        'transactional.id': 'example_transactional_id',
        'error_cb': error_cb,
    })

    consumer_conf = {
        'group.id': str(uuid1()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': True,
        'error_cb': error_cb
    }
    consumer_conf.update(kafka_cluster.client_conf())
    consumer = Consumer(consumer_conf)

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

    producer2 = kafka_cluster.producer({
        'client.id': 'producer2',
        'transactional.id': 'example_transactional_id',
        'error_cb': error_cb
    })

    # ensure offset commits are visible prior to sending FetchOffsets request
    producer2.init_transactions()

    committed_offsets = consumer.committed(consumer.assignment())
    print("=== Committed offsets for {} ===".format(committed_offsets))

    assert [tp.offset for tp in committed_offsets] == [100]

    consumer.close()


def transactional_produce(producer, topic, num_messages):
    print("=== Producing {} transactional messages to topic {}. ===".format(
        num_messages, topic))

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
    while (True):
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

    consumer_conf = {'group.id': str(uuid1()),
                     'auto.offset.reset': 'earliest',
                     'enable.auto.commit': False,
                     'enable.partition.eof': True,
                     'error_cb': prefixed_error_cb(called_by()), }

    consumer_conf.update(conf)
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    msg_cnt = read_all_msgs(consumer)

    consumer.close()

    return msg_cnt
