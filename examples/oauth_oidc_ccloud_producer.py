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


# This uses OAuth client credentials grant:
# https://www.oauth.com/oauth2-servers/access-tokens/client-credentials/
# where client_id and client_secret are passed as HTTP Authorization header

import logging
import argparse
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer


def producer_config(args):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    params = {
        'bootstrap.servers': args.bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.method': 'oidc',
        'sasl.oauthbearer.client.id': args.client_id,
        'sasl.oauthbearer.client.secret': args.client_secret,
        'sasl.oauthbearer.token.endpoint.url': args.token_url,
        'sasl.oauthbearer.scope': ' '.join(args.scopes)
    }
    # These two parameters are only applicable when producing to
    # confluent cloud where some sasl extensions are required.
    if args.logical_cluster and args.identity_pool_id:
        params['sasl.oauthbearer.extensions'] = 'logicalCluster=' + args.logical_cluster + \
                            ',identityPoolId=' + args.identity_pool_id

    return params

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
        print('Delivery failed for User record {}: {}'.format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    delimiter = args.delimiter
    producer_conf = producer_config(args)
    producer = Producer(producer_conf)
    serializer = StringSerializer('utf_8')

    print('Producing records to topic {}. ^C to exit.'.format(topic))
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

    print('\nFlushing {} records...'.format(len(producer)))
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="OAUTH example with client credentials grant")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="example_producer_oauth",
                        help="Topic name")
    parser.add_argument('-d', dest="delimiter", default="|",
                        help="Key-Value delimiter. Defaults to '|'"),
    parser.add_argument('--client', dest="client_id", required=True,
                        help="Client ID for client credentials flow")
    parser.add_argument('--secret', dest="client_secret", required=True,
                        help="Client secret for client credentials flow.")
    parser.add_argument('--token-url', dest="token_url", required=True,
                        help="Token URL.")
    parser.add_argument('--scopes', dest="scopes", required=True, nargs='+',
                        help="Scopes requested from OAuth server.")
    parser.add_argument('--logical-cluster', dest="logical_cluster", required=False, help="Logical Cluster.")
    parser.add_argument('--identity-pool-id', dest="identity_pool_id", required=False, help="Identity Pool ID.")
  
    main(parser.parse_args())
