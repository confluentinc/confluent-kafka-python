#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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

from confluent_kafka import Producer

import pytest


@pytest.mark.parametrize("with_dr_cb", [True, False])
def test_producer_performance(kafka_cluster_fixture, topic_fixture, delivery_report_fixture, error_cb_fixture,
                              with_dr_cb):
    """ Time how long it takes to produce and delivery X messages """
    conf = kafka_cluster_fixture.client_conf()

    conf.update({'linger.ms': 500,
                 'error_cb': error_cb_fixture})

    p = Producer(conf)

    msgcnt = 1000000
    msgsize = 100
    msg_pattern = 'test.py performance'
    msg_payload = (msg_pattern * int(msgsize / len(msg_pattern)))[0:msgsize]

    dr = delivery_report_fixture(silent=True)

    t_produce_start = time.time()
    msgs_produced = 0
    msgs_backpressure = 0
    print('# producing %d messages to topic %s' % (msgcnt, topic_fixture))

    for i in range(0, msgcnt):
        while True:
            try:
                if with_dr_cb:
                    p.produce(topic_fixture, value=msg_payload, callback=dr.delivery)
                else:
                    p.produce(topic_fixture, value=msg_payload)
                break
            except BufferError:
                # Local queue is full (slow broker connection?)
                msgs_backpressure += 1
                p.poll(100)
            continue

        msgs_produced += 1
        p.poll(0)

    t_produce_spent = time.time() - t_produce_start

    bytecnt = msgs_produced * msgsize

    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' %
          (msgs_produced, bytecnt / (1024 * 1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt / t_produce_spent) / (1024 * 1024)))
    print('# %d temporary produce() failures due to backpressure (local queue full)' % msgs_backpressure)

    print('waiting for %d/%d deliveries' % (len(p), msgs_produced))
    # Wait for deliveries
    p.flush()
    t_delivery_spent = time.time() - t_produce_start

    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' %
          (msgs_produced, bytecnt / (1024 * 1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt / t_produce_spent) / (1024 * 1024)))

    # Fake numbers if not using a dr_cb
    if not with_dr_cb:
        print('# not using dr_cb')
        dr.msgs_delivered = msgs_produced
        dr.bytes_delivered = bytecnt

    print('# delivering %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' %
          (dr.msgs_delivered, dr.bytes_delivered / (1024 * 1024), t_delivery_spent,
           dr.msgs_delivered / t_delivery_spent,
           (dr.bytes_delivered / t_delivery_spent) / (1024 * 1024)))
    print('# post-produce delivery wait took %.3fs' %
          (t_delivery_spent - t_produce_spent))
