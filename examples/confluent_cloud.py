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
# Confluent Cloud does not auto-create topics. You will need to use the ccloud
# cli to create the python-test-topic topic before running this example. The
# <ccloud bootstrap servers>, <ccloud key> and <ccloud secret> parameters are
# available via the confluent cloud web interface. For more information,
# refer to the quick-start:
#
# https://docs.confluent.io/current/cloud-quickstart.html
#
# to execute using Python 2.x:
#   virtualenv ccloud_example
#   source ccloud_example/bin/activate
#   pip install confluent_kafka
#   python example.py
#   deactivate
#
# to execute using Python 3.x: 
#   python -m venv ccloud_example
#   source ccloud_example/bin/activate
#   pip install confluent_kafka
#   python example.py
#   deactivate

import uuid
from confluent_kafka import Producer, Consumer, KafkaError

p = Producer({
    'bootstrap.servers': '<bootstrap servers>',
    'api.version.request': True,
    'broker.version.fallback': '0.10.0.0',
    'api.version.fallback.ms': 0,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': '/usr/local/etc/openssl/cert.pem',
    'sasl.username': '<key>',
    'sasl.password': '<secret>'
})

def acked(err, msg):
    if err is not None:
        print("failed to deliver message: {0}".format(err.str()))
    else:
        print("produced to: {0}/{1}/{2}".format(msg.topic(), msg.partition(), msg.offset()))

p.produce('python-test-topic', value='python test value', callback=acked)
p.flush(10)

c = Consumer({
    'bootstrap.servers': '<bootstrap servers>',
    'api.version.request': True,
    'broker.version.fallback': '0.10.0.0',
    'api.version.fallback.ms': 0,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': '/usr/local/etc/openssl/cert.pem',
    'sasl.username': '<key>',
    'sasl.password': '<secret>',
    'group.id': str(uuid.uuid1()),
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['python-test-topic'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print('consumed: {0}'.format(msg.value()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('end of partition: {0}/{1}'.format(msg.topic(), msg.partition()))
        else:
            print('error: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()
