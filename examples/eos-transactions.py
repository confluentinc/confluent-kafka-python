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


# This is a simple example of a `read-process-write` application
# using Apache Kafka's transactional API.
#
# See the following blog for additional information:
# https://www.confluent.io/blog/transactions-apache-kafka/
#


"""
The following example demonstrates how to perform a consume-transform-produce
loop with exactly-once semantics.

In order to achieve exactly-once semantics we use the transactional producer
and a single transaction aware consumer.

The following assumptions apply to the source data (input_topic below):
    1. There are no duplicates in the input topic.

##  A quick note about exactly-once-processing guarantees and Kafka. ##

The exactly once, and idempotence, guarantees start after the producer has been
provided a record. There is no way for a producer to identify a record as a
duplicate in isolation. Instead it is the application's job to ensure that only
a single copy of any record is passed to the producer.

Special care needs to be taken when expanding the consumer group to multiple
members.
Review KIP-447 for complete details.
"""

import argparse
from base64 import b64encode
from uuid import uuid4

from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition


def process_input(msg):
    """
    Base64 encodes msg key/value contents
    :param msg:
    :returns: transformed key, value
    :rtype: tuple
    """

    key, value = None, None
    if msg.key() is not None:
        key = b64encode(msg.key())
    if msg.value() is not None:
        value = b64encode(msg.value())

    return key, value


def delivery_report(err, msg):
    """
    Reports message delivery status; success or failure
    :param KafkaError err: reason for delivery failure
    :param Message msg:
    :returns: None
    """
    if err:
        print('Message delivery failed ({} [{}]): {}'.format(
            msg.topic(), str(msg.partition()), err))


def main(args):
    brokers = args.brokers
    group_id = args.group_id
    input_topic = args.input_topic
    input_partition = args.input_partition
    output_topic = args.output_topic

    consumer = Consumer({
        'bootstrap.servers': brokers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        # Do not advance committed offsets outside of the transaction.
        # Consumer offsets are committed along with the transaction
        # using the producer's send_offsets_to_transaction() API.
        'enable.auto.commit': False,
        'enable.partition.eof': True,
    })

    # Prior to KIP-447 being supported each input partition requires
    # its own transactional producer, so in this example we use
    # assign() to a single partition rather than subscribe().
    # A more complex alternative is to dynamically create a producer per
    # partition in subscribe's rebalance callback.
    consumer.assign([TopicPartition(input_topic, input_partition)])

    producer = Producer({
        'bootstrap.servers': brokers,
        'transactional.id': 'eos-transactions.py'
    })

    # Initialize producer transaction.
    producer.init_transactions()
    # Start producer transaction.
    producer.begin_transaction()

    eof = {}
    msg_cnt = 0
    print("=== Starting Consume-Transform-Process loop ===")
    while True:
        # serve delivery reports from previous produce()s
        producer.poll(0)

        # read message from input_topic
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        topic, partition = msg.topic(), msg.partition()
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof[(topic, partition)] = True
                print("=== Reached the end of {} [{}] at {}====".format(
                    topic, partition, msg.offset()))

                if len(eof) == len(consumer.assignment()):
                    print("=== Reached end of input ===")
                    break
            continue
        # clear EOF if a new message has been received
        eof.pop((topic, partition), None)

        msg_cnt += 1

        # process message
        processed_key, processed_value = process_input(msg)

        # produce transformed message to output topic
        producer.produce(output_topic, processed_value, processed_key,
                         on_delivery=delivery_report)

        if msg_cnt % 100 == 0:
            print("=== Committing transaction with {} messages at input offset {} ===".format(
                msg_cnt, msg.offset()))
            # Send the consumer's position to transaction to commit
            # them along with the transaction, committing both
            # input and outputs in the same transaction is what provides EOS.
            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata())

            # Commit the transaction
            producer.commit_transaction()

            # Begin new transaction
            producer.begin_transaction()
            msg_cnt = 0

    print("=== Committing final transaction with {} messages ===".format(msg_cnt))
    # commit processed message offsets to the transaction
    producer.send_offsets_to_transaction(
        consumer.position(consumer.assignment()),
        consumer.consumer_group_metadata())

    # commit transaction
    producer.commit_transaction()

    consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Exactly Once Semantics (EOS) example")
    parser.add_argument('-b', dest="brokers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="input_topic", required=True,
                        help="Input topic to consume from, an external " +
                        "producer needs to produce messages to this topic")
    parser.add_argument('-o', dest="output_topic", default="output_topic",
                        help="Output topic")
    parser.add_argument('-p', dest="input_partition", default=0, type=int,
                        help="Input partition to consume from")
    parser.add_argument('-g', dest="group_id",
                        default="eos_example_" + str(uuid4()),
                        help="Consumer group")

    main(parser.parse_args())
