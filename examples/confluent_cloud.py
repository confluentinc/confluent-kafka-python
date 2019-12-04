#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
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


# This is a simple example demonstrating how to produce a message to
# Confluent Cloud then read it back again.
#
# https://www.confluent.io/confluent-cloud/
#
# Auto-creation of topics is disabled in Confluent Cloud. You will need to
# use the ccloud cli to create the python-test-topic topic before running this
# example.
#
# $ ccloud topic create python-test-topic
#
# The <ccloud bootstrap servers>, <ccloud key> and <ccloud secret> parameters
# are available via the Confluent Cloud web interface. For more information,
# refer to the quick-start:
#
# https://docs.confluent.io/current/cloud-quickstart.html
#
# to execute using Python 2.7:
# $ virtualenv ccloud_example
# $ source ccloud_example/bin/activate
# $ pip install confluent_kafka
# $ python confluent_cloud.py
# $ deactivate
#
# to execute using Python 3.x:
# $ python -m venv ccloud_example
# $ source ccloud_example/bin/activate
# $ pip install confluent_kafka
# $ python confluent_cloud.py
# $ deactivate

import uuid

from confluent_kafka import Producer, Consumer


p = Producer({
    'bootstrap.servers': '<ccloud bootstrap servers>',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '<ccloud key>',
    'sasl.password': '<ccloud secret>'
})


def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
        print("failed to deliver message: {}".format(err.str()))
    else:
        print("produced to: {} [{}] @ {}".format(msg.topic(), msg.partition(), msg.offset()))


p.produce('python-test-topic', value='python test value', callback=acked)

# flush() is typically called when the producer is done sending messages to wait
# for outstanding messages to be transmitted to the broker and delivery report
# callbacks to get called. For continous producing you should call p.poll(0)
# after each produce() call to trigger delivery report callbacks.
p.flush(10)

c = Consumer({
    'bootstrap.servers': '<ccloud bootstrap servers>',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '<ccloud key>',
    'sasl.password': '<ccloud secret>',
    'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
    'auto.offset.reset': 'earliest'
})

c.subscribe(['python-test-topic'])

try:
    while True:
        msg = c.poll(0.1)  # Wait for message or event/error
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to `session.timeout.ms` for
            #   the group to rebalance and start consuming.
            continue
        if msg.error():
            # Errors are typically temporary, print error and continue.
            print("Consumer error: {}".format(msg.error()))
            continue

        print('consumed: {}'.format(msg.value()))

except KeyboardInterrupt:
    pass

finally:
    # Leave group and commit final offsets
    c.close()
