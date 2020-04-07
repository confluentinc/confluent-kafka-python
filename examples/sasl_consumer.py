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
#

#
# This is a simple example of the DeserializingConsumer using SASL authentication.
#
import argparse

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer


def sasl_conf(args):
    """
    Generates a SASL configuration dict from a Namespace instance

    Args:
        args (argparse.Namespace): parsed args

    Returns:
         dict: SASL configuration dict

    """
    sasl_mechanism = args.sasl_mechanism

    sasl_conf = {'sasl.mechanism': sasl_mechanism,
                 'security.protocol': 'SASL_PLAINTEXT'}

    if sasl_mechanism != 'GSSAPI':
        sasl_conf.update({'sasl.username': args.user_principal,
                          'sasl.password': args.user_secret})
    else:
        sasl_conf.update({'sasl.kerberos.service.name', args.broker_principal,
                          # On Windows the configured user principal is ignored.
                          # The logged on user's credentials are used instead.
                          'sasl.kerberos.principal', args.user_principal,
                          'sasl.kerberos.keytab', args.user_secrent})

    return sasl_conf


def main(args):
    topic = args.topic
    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': 'earliest',
                     'key.deserializer': StringDeserializer('utf_8'),
                     'value.deserializer': StringDeserializer('utf_8')}

    consumer_conf.update(sasl_conf(args))

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = msg.value()
            if user is not None:
                print("key {}: value: {}".format(msg.key(), msg.value()))
        except KeyboardInterrupt:
            break
    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="DeserializingConsumer"
                                                 " SASL Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="example_sasl",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_sasl",
                        help="Consumer group")
    parser.add_argument('-m', dest="sasl_mechanism", default='PLAIN',
                        help="SASL mechanism to use for authentication."
                             "Defaults to PLAIN")
    parser.add_argument('-u', dest="user_principal", required=True,
                        help="username")
    parser.add_argument('-s', dest="user_secret", required=True,
                        help="password; path to keytab if using"
                             "SASL Mechanism GSSAPI")

    main(parser.parse_args())
