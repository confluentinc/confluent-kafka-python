#!/usr/bin/env python
#
# Copyright 2025 Confluent Inc.
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

#
# Example Consumer using cooperative incremental rebalancing.
#
# Unlike the eager (default) rebalancing strategy, cooperative rebalancing
# allows consumers to continue processing messages from partitions that are
# not being reassigned during a rebalance. Only the partitions that need to
# move between consumers are revoked and reassigned.
#
# Usage:
#   python cooperative_consumer.py <bootstrap-brokers> <group> <topic1> [<topic2> ..]
#

import logging
import sys

from confluent_kafka import Consumer, KafkaException


def on_assign(consumer, partitions):
    """Called when partitions are incrementally assigned to this consumer."""
    print('Partitions assigned: {}'.format(
        ['{} [{}]'.format(p.topic, p.partition) for p in partitions]))


def on_revoke(consumer, partitions):
    """Called when partitions are incrementally revoked from this consumer.

    With cooperative rebalancing, only the partitions that are moving to
    another consumer are revoked. The consumer continues to own and process
    all other partitions without interruption.

    Commit offsets here for at-least-once delivery semantics.
    """
    print('Partitions revoked: {}'.format(
        ['{} [{}]'.format(p.topic, p.partition) for p in partitions]))
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        print('Commit failed during revoke: {}'.format(e))


def on_lost(consumer, partitions):
    """Called when partitions are lost (e.g., session timeout exceeded).

    Unlike revoke, lost partitions cannot be committed because the consumer
    is no longer part of the group. Use this callback to clean up any
    partition-specific local state.
    """
    print('Partitions lost: {}'.format(
        ['{} [{}]'.format(p.topic, p.partition) for p in partitions]))


if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.stderr.write(
            'Usage: {} <bootstrap-brokers> <group> <topic1> [<topic2> ..]\n'.format(sys.argv[0]))
        sys.exit(1)

    broker = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]

    # Consumer configuration with cooperative-sticky assignor.
    # See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        'partition.assignment.strategy': 'cooperative-sticky',
        'auto.offset.reset': 'earliest',
        'enable.auto.offset.store': False,
    }

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    c = Consumer(conf, logger=logger)

    # Subscribe to topics with cooperative rebalance callbacks.
    # The on_lost callback is important: if you commit on revoke but don't
    # set on_lost, a lost-partitions event will be routed to on_revoke
    # which may attempt to commit offsets that the broker will reject.
    c.subscribe(topics,
                on_assign=on_assign,
                on_revoke=on_revoke,
                on_lost=on_lost)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                sys.stderr.write(
                    '%% %s [%d] at offset %d with key %s:\n'
                    % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                )
                print(msg.value())
                # Store the offset associated with msg to a local cache.
                # Stored offsets are committed to Kafka by a background
                # thread every 'auto.commit.interval.ms'.
                # Explicitly storing offsets after processing gives
                # at-least once semantics.
                c.store_offsets(msg)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()
