#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2019 Confluent Inc.
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
import time
from uuid import uuid1

import pytest

from confluent_kafka import Consumer, KafkaException


@pytest.mark.timeout(80)
def test_consumer_performance(cluster_fixture,
                              error_cb_fixture):
    """ Verify Consumer performance """

    conf = cluster_fixture.client_conf

    conf.update({'group.id': uuid1(),
                 'session.timeout.ms': 6000,
                 'error_cb': error_cb_fixture,
                 'auto.offset.reset': 'earliest'})

    consumer = Consumer(conf)
    topic = cluster_fixture.topic

    def my_on_assign(consumer, partitions):
        print('on_assign:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.assign(partitions)

    def my_on_revoke(consumer, partitions):
        print('on_revoke:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.unassign()

    consumer.subscribe([topic], on_assign=my_on_assign, on_revoke=my_on_revoke)

    max_msgcnt = 1000000
    bytecnt = 0
    msgcnt = 0

    cluster_fixture.produce(max_msgcnt)
    print('Will now consume %d messages' % max_msgcnt)
    while True:
        # Consume until EOF or error

        msg = consumer.poll(timeout=20.0)
        if msg is None:
            raise Exception('Stalled at %d/%d message, no new messages for 20s' %
                            (msgcnt, max_msgcnt))

        if msg.error():
            raise KafkaException(msg.error())

        bytecnt += len(msg)
        msgcnt += 1

        if msgcnt == 1:
            t_first_msg = time.time()
        if msgcnt >= max_msgcnt:
            break

    if msgcnt > 0:
        t_spent = time.time() - t_first_msg
        print('%d messages (%.2fMb) consumed in %.3fs: %d msgs/s, %.2f Mb/s' %
              (msgcnt, bytecnt / (1024 * 1024), t_spent, msgcnt / t_spent,
               (bytecnt / t_spent) / (1024 * 1024)))

    print('closing consumer')
    consumer.close()


def test_batch_consumer_performance(cluster_fixture,
                                    error_cb_fixture):
    """ Verify batch Consumer performance """

    conf = cluster_fixture.client_conf
    conf.update({'group.id': uuid1(),
                 'session.timeout.ms': 6000,
                 'error_cb': error_cb_fixture,
                 'auto.offset.reset': 'earliest'})

    consumer = Consumer(conf)
    topic = cluster_fixture.topic

    def my_on_assign(consumer, partitions):
        print('on_assign:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.assign(partitions)

    def my_on_revoke(consumer, partitions):
        print('on_revoke:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.unassign()

    consumer.subscribe([topic], on_assign=my_on_assign, on_revoke=my_on_revoke)

    max_msgcnt = 1000000
    bytecnt = 0
    msgcnt = 0
    batch_size = 1000

    cluster_fixture.produce(max_msgcnt)
    print('Will now consume %d messages' % max_msgcnt)
    while msgcnt < max_msgcnt:
        # Consume until we hit max_msgcnt

        msglist = consumer.consume(num_messages=batch_size, timeout=20.0)

        for msg in msglist:
            if msg.error():
                raise KafkaException(msg.error())

            bytecnt += len(msg)
            msgcnt += 1

            if msgcnt == 1:
                t_first_msg = time.time()

    if msgcnt > 0:
        t_spent = time.time() - t_first_msg
        print('%d messages (%.2fMb) consumed in %.3fs: %d msgs/s, %.2f Mb/s' %
              (msgcnt, bytecnt / (1024 * 1024), t_spent, msgcnt / t_spent,
               (bytecnt / t_spent) / (1024 * 1024)))

    print('closing consumer')
    consumer.close()
