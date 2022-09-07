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


# This is a simple example demonstrating SASL authentication.

import argparse

from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def sasl_conf(args):
    sasl_mechanism = args.sasl_mechanism.upper()

    sasl_conf = {'sasl.mechanism': sasl_mechanism,
                 # Set to SASL_SSL to enable TLS support.
                 'security.protocol': 'SASL_PLAINTEXT'}

    if sasl_mechanism != 'GSSAPI':
        sasl_conf.update({'sasl.username': args.user_principal,
                          'sasl.password': args.user_secret})

    if sasl_mechanism == 'GSSAPI':
        sasl_conf.update({'sasl.kerberos.service.name', args.broker_principal,
                          # Keytabs are not supported on Windows. Instead the
                          # the logged on user's credentials are used to
                          # authenticate.
                          'sasl.kerberos.principal', args.user_principal,
                          'sasl.kerberos.keytab', args.user_secret})
    return sasl_conf


def main(args):
    topic = args.topic
    delimiter = args.delimiter
    producer_conf = {'bootstrap.servers': args.bootstrap_servers}
    producer_conf.update(sasl_conf(args))
    producer = Producer(producer_conf)
    serializer = StringSerializer('utf_8')

    print("Producing records to topic {}. ^C to exit.".format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            msg_data = input(">")
            msg = msg_data.split(delimiter)
            if len(msg) == 2:
                producer.produce(topic=topic,
                                 key=serializer(msg[0]),
                                 value=serializer(msg[1]),
                                 on_delivery=delivery_report)
            else:
                producer.produce(topic=topic,
                                 value=serializer(msg[0]),
                                 on_delivery=delivery_report)
        except KeyboardInterrupt:
            break

    print("\nFlushing {} records...".format(len(producer)))
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SASL Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="example_producer_sasl",
                        help="Topic name")
    parser.add_argument('-d', dest="delimiter", default="|",
                        help="Key-Value delimiter. Defaults to '|'"),
    parser.add_argument('-m', dest="sasl_mechanism", default='PLAIN',
                        choices=['GSSAPI', 'PLAIN',
                                 'SCRAM-SHA-512', 'SCRAM-SHA-256'],
                        help="SASL mechanism to use for authentication."
                             "Defaults to PLAIN")
    parser.add_argument('--tls', dest="enab_tls", default=False)
    parser.add_argument('-u', dest="user_principal", required=True,
                        help="Username")
    parser.add_argument('-s', dest="user_secret", required=True,
                        help="Password for PLAIN and SCRAM, or path to"
                             " keytab (ignored on Windows) if GSSAPI.")

    main(parser.parse_args())
