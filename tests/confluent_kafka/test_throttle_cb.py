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
# limit
#

import pytest

from confluent_kafka import Producer

throttled_requests = 0


@pytest.mark.xfail
def test_throttle_cb(kafka_cluster_fixture, topic_fixture):
    """ Verify throttle_cb is invoked
        This test requires client quotas be configured.
        See tests/README.md for more information

        Prior to running this tests configure the broker
        to aggressively throttle requests.

        example(docker for mac 18.03+:
            docker run --network=host confluentinc/cp-kafka:5.3.1 sh -c " \
                /usr/bin/kafka-configs  --zookeeper host.docker.internal:60523 \
                --alter --add-config 'producer_byte_rate=1,consumer_byte_rate=1,request_percentage=001' \
                --entity-name throttled_client --entity-type clients"
    """

    conf = kafka_cluster_fixture.client_conf()
    conf.update({'linger.ms': 500,
                 'client.id': 'throttled_client',
                 'throttle_cb': throttle_cb})

    p = Producer(conf)

    topic = topic_fixture

    msgcnt = 1000
    msgsize = 100
    msg_pattern = 'test.py throttled client'
    msg_payload = (msg_pattern * int(msgsize / len(msg_pattern)))[0:msgsize]

    msgs_produced = 0
    msgs_backpressure = 0
    print('# producing %d messages to topic %s' % (msgcnt, topic))

    for i in range(0, msgcnt):
        while True:
            try:
                p.produce(topic, value=msg_payload)
                break
            except BufferError:
                # Local queue is full (slow broker connection?)
                msgs_backpressure += 1
                p.poll(10)
            continue
        msgs_produced += 1
        p.poll(0)

    p.flush()
    print('# {} of {} produce requests were throttled'.format(throttled_requests, msgs_produced))
    assert throttled_requests >= 1


def throttle_cb(throttle_event):
    # validate argument type
    assert isinstance(throttle_event.broker_name, str)
    assert isinstance(throttle_event.broker_id, int)
    assert isinstance(throttle_event.throttle_time, float)

    global throttled_requests
    throttled_requests += 1

    print(throttle_event)


def __init__():
    print("inited")
