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

import json
import time
from uuid import uuid1

from confluent_kafka import Consumer, KafkaException

good_stats_cb_result = False


def test_stats_cb(kafka_cluster_fixture, cluster_producer_fixture, topic_fixture, error_cb_fixture):
    """ Verify stats_cb """
    topic = topic_fixture

    def stats_cb(stats_json_str):
        global good_stats_cb_result
        stats_json = json.loads(stats_json_str)
        if topic in stats_json['topics']:
            app_offset = stats_json['topics'][topic]['partitions']['0']['app_offset']
            if app_offset > 0:
                print("# app_offset stats for topic %s partition 0: %d" %
                      (topic, app_offset))
                good_stats_cb_result = True

    conf = kafka_cluster_fixture.client_conf()
    conf.update({'group.id': uuid1(),
                 'session.timeout.ms': 6000,
                 'error_cb': error_cb_fixture,
                 'stats_cb': stats_cb,
                 'statistics.interval.ms': 200,
                 'auto.offset.reset': 'earliest'})

    c = Consumer(conf)
    c.subscribe([topic])

    max_msgcnt = 1000000
    bytecnt = 0
    msgcnt = 0

    cluster_producer_fixture(max_msgcnt)
    print('Will now consume %d messages' % max_msgcnt)
    while not good_stats_cb_result:
        # Consume until EOF or error

        msg = c.poll(timeout=20.0)
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
    c.close()
