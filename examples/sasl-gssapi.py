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
# This is a simple example demonstrating how to configure clients to communicate with a Kerberized Kafka cluster.
# Wire encryption(TLS) is also covered in this example.
#
# See `Configuring GSSAPI` in the confluent docs for instructions on setting up a Kerberized Kafka Cluster.
#
# https://docs.confluent.io/current/kafka/authentication_sasl/authentication_sasl_gssapi.html
#
# See also `Encryption with SSL` for instructions on setting up TLS communication with a Kafka Cluster.
#
# https://docs.confluent.io/current/kafka/encryption.html
#

import uuid

import certifi

from confluent_kafka import Producer, Consumer


def errored(err):
    """Global/Generic error report callback served upon calling `flush()` or `poll()`."""
    print('Error: %s' % err)


p = Producer({
    'bootstrap.servers': 'localhost:9093',
    'sasl.mechanisms': 'GSSAPI',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': certifi.where(),
    'sasl.kerberos.service.name': 'kafka',  # Broker principal without the hostname
    'sasl.kerberos.principal': 'client@REALM.COM',  # *Note* Always login user on Windows
    'sasl.kerberos.keytab': '/etc/security/keytabs/client.keytab',  # *Note* Not supported on Windows
    'error_cb': errored
})


def acked(err, msg):
    """Delivery report callback served upon calling  flush() or poll().
       Reports successful or failed delivery of the message."""
    if err is not None:
        print("failed to deliver message: {}".format(err.str()))
    else:
        print("produced to: {} [{}] @ {}".format(msg.topic(), msg.partition(), msg.offset()))


p.produce('python-gssapi-test-topic', value='python gssapi test value', on_delivery=acked)

# flush() is typically called when the producer is done sending messages to wait
# for outstanding messages to be transmitted to the broker and delivery report
# callbacks to get called. For continuous producing you should call p.poll(0)
# after each produce() call to trigger delivery report callbacks.
p.flush(10)

c = Consumer({
    'bootstrap.servers': 'localhost:9093',
    'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
    'auto.offset.reset': 'earliest',
    'sasl.mechanisms': 'GSSAPI',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': certifi.where(),
    'sasl.kerberos.service.name': 'kafka',  # Broker principal without the hostname
    'sasl.kerberos.principal': 'client@REALM.COM',  # *Note* Always login user on Windows
    'sasl.kerberos.keytab': '/etc/security/keytabs/client.keytab',  # *Note* Not supported on Windows
    'error_cb': errored
})

c.subscribe(['python-gssapi-test-topic'])

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
