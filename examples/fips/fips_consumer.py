#!/usr/bin/env python
#
# Copyright 2023 Confluent Inc.
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
# Example FIPS Compliant Consumer
#
from confluent_kafka import Consumer, KafkaException
import sys


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print_usage_and_exit(sys.argv[0])

    broker = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]
    conf = {'bootstrap.servers': broker,
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'broker',
            'sasl.password': 'broker-secret',
            # pkc12 keystores are not FIPS compliant and hence you will need to use
            # path to key and certificate separately in FIPS mode
            # 'ssl.keystore.location': './docker/secrets/client.keystore.p12',
            # 'ssl.keystore.password': '111111',
            'ssl.key.location': './docker/secrets/localhost_client.key',
            'ssl.key.password': '111111',
            'ssl.certificate.location': './docker/secrets/localhost_client.crt',
            'ssl.ca.location': './docker/secrets/ca-root.crt',
            'ssl.providers': 'fips,base'
            }

    # Create Consumer instance
    c = Consumer(conf)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()
