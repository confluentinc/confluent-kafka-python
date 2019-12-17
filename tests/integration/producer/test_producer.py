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

import gc

from confluent_kafka import Producer, libversion

DrOnlyTestSuccess_gced = 0


def test_producer(cluster_fixture,
                  produce_headers_fixture, delivery_report_fixture):
    """ Verify basic Producer functionality """

    # Create producer
    producer = Producer(cluster_fixture.client_conf)
    topic = cluster_fixture.topic

    headers = produce_headers_fixture

    mydr = delivery_report_fixture()

    # Produce some messages
    producer.produce(topic, 'Hello Python!', headers=headers)
    producer.produce(topic, key='Just a key and headers', headers=headers)
    producer.produce(topic, key='Just a key')
    producer.produce(topic, partition=1, value='Strictly for partition 1',
                     key='mykey', headers=headers)

    # Produce more messages, now with delivery report callbacks in various forms.
    producer.produce(topic, value='This one has a dr callback',
                     callback=mydr.delivery)
    producer.produce(topic, value='This one has a lambda',
                     callback=lambda err, msg: mydr._delivery(err, msg))
    producer.produce(topic, value='This one has neither')

    # Try producing with a timestamp
    try:
        producer.produce(topic, value='with a timestamp', timestamp=123456789000)
    except NotImplementedError:
        if libversion()[1] >= 0x00090400:
            raise

    # Produce even more messages
    for i in range(0, 10):
        producer.produce(topic, value='Message #%d' % i, key=str(i),
                         callback=mydr.delivery)
        producer.poll(0)

    print('Waiting for %d messages to be delivered' % len(producer))

    # Block until all messages are delivered/failed
    producer.flush()


def test_producer_dr_only_error(cluster_fixture):
    """
    The C delivery.report.only.error configuration property
    can't be used with the Python client since the Python client
    allocates a msgstate for each produced message that has a callback,
    and on success (with delivery.report.only.error=true) the delivery report
    will not be called and the msgstate will thus never be freed.

    Since a proper broker is required for messages to be succesfully sent
    this test must be run from the integration tests rather than
    the unit tests.
    """

    conf = cluster_fixture.client_conf
    conf.update({'delivery.report.only.error': True})

    producer = Producer(conf)
    topic = cluster_fixture.topic

    class DrOnlyTestErr(object):
        def __init__(self):
            self.remaining = 1

        def handle_err(self, err, msg):
            """ This delivery handler should only get called for errored msgs """
            assert "BAD:" in msg.value().decode('utf-8')
            assert err is not None
            self.remaining -= 1

    class DrOnlyTestSuccess(object):
        def handle_success(self, err, msg):
            """ This delivery handler should never get called """
            # FIXME: Can we verify that it is actually garbage collected?
            assert "GOOD:" in msg.value().decode('utf-8')
            assert err is None
            assert False, "should never come here"

        def __del__(self):
            # Indicate that gc has hit this object.
            global DrOnlyTestSuccess_gced
            DrOnlyTestSuccess_gced = 1

    print('only.error: Verifying delivery.report.only.error')

    state = DrOnlyTestErr()
    producer.produce(topic, "BAD: This message will make not make it".encode('utf-8'),
                     partition=99, on_delivery=state.handle_err)

    not_called_state = DrOnlyTestSuccess()
    producer.produce(topic, "GOOD: This message will make make it".encode('utf-8'),
                     on_delivery=not_called_state.handle_success)

    # Garbage collection should not kick in yet for not_called_state
    # since there is a on_delivery reference to it.
    not_called_state = None
    gc.collect()
    global DrOnlyTestSuccess_gced
    assert DrOnlyTestSuccess_gced == 0

    print('only.error: Waiting for flush of %d messages' % len(producer))
    producer.flush(10000)

    print('only.error: Remaining messages now %d' % state.remaining)
    assert state.remaining == 0

    # Now with all messages flushed the reference to not_called_state should be gone.
    gc.collect()
    assert DrOnlyTestSuccess_gced == 1
