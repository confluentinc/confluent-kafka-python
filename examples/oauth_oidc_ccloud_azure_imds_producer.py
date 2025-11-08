#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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


# This example uses Azure IMDS for credential-less authentication
# to Kafka on Confluent Cloud

import logging
import argparse
from confluent_kafka import Producer
from confluent_kafka.serialization import (StringSerializer)


def producer_config(args):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    params = {
        'bootstrap.servers': args.bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.method': 'oidc',
        'sasl.oauthbearer.metadata.authentication.type': 'azure_imds',
        'sasl.oauthbearer.config': f'query={args.query}'
    }
    # These two parameters are only applicable when producing to
    # Confluent Cloud where some sasl extensions are required.
    if args.logical_cluster and args.identity_pool_id:
        params['sasl.oauthbearer.extensions'] = 'logicalCluster=' + args.logical_cluster + \
            ',identityPoolId=' + args.identity_pool_id

    return params


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred, or None on success.

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
        print('Delivery failed for User record {}: {}'.format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    producer_conf = producer_config(args)
    producer = Producer(producer_conf)
    string_serializer = StringSerializer('utf_8')

    print('Producing records to topic {}. ^C to exit.'.format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            name = input(">")
            producer.produce(topic=topic,
                             key=string_serializer(name),
                             value=string_serializer(name),
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break

    print('\nFlushing {} records...'.format(len(producer)))
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="OAuth/OIDC example using Azure IMDS metadata-based authentication")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="example_producer_oauth",
                        help="Topic name")
    parser.add_argument('--query', dest="query", required=True,
                        help="Query parameters for Azure IMDS token endpoint")
    parser.add_argument('--logical-cluster', dest="logical_cluster", required=False, help="Logical Cluster.")
    parser.add_argument('--identity-pool-id', dest="identity_pool_id", required=False, help="Identity Pool ID.")

    main(parser.parse_args())
