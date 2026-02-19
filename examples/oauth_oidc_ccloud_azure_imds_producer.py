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


# This example use Azure IMDS for credential-less authentication
# through to Schema Registry on Confluent Cloud

import argparse
import logging

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential
    """

    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # address should not be serialized, see user_to_dict()
        self._address = address


def user_to_dict(user, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return dict(name=user.name, favorite_number=user.favorite_number, favorite_color=user.favorite_color)


def producer_config(args):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    params = {
        'bootstrap.servers': args.bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.method': 'oidc',
        'sasl.oauthbearer.metadata.authentication.type': 'azure_imds',
        'sasl.oauthbearer.config': f'query={args.query}',
    }
    # These two parameters are only applicable when producing to
    # confluent cloud where some sasl extensions are required.
    if args.logical_cluster and args.identity_pool_id:
        params['sasl.oauthbearer.extensions'] = (
            'logicalCluster=' + args.logical_cluster + ',identityPoolId=' + args.identity_pool_id
        )

    return params


def schema_registry_config(args):
    params = {
        'url': args.schema_registry,
        'bearer.auth.credentials.source': 'OAUTHBEARER_AZURE_IMDS',
        'bearer.auth.issuer.endpoint.query': args.query,
    }
    # These two parameters are only applicable when producing to
    # confluent cloud where some sasl extensions are required.
    if args.logical_schema_registry_cluster and args.identity_pool_id:
        params['bearer.auth.logical.cluster'] = args.logical_schema_registry_cluster
        params['bearer.auth.identity.pool.id'] = args.identity_pool_id

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
    print(
        'User record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


def main(args):
    topic = args.topic
    producer_conf = producer_config(args)
    producer = Producer(producer_conf)
    string_serializer = StringSerializer('utf_8')
    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "User",
      "description": "A Confluent Kafka Python User",
      "type": "object",
      "properties": {
        "name": {
          "description": "User's name",
          "type": "string"
        },
        "favorite_number": {
          "description": "User's favorite number",
          "type": "number",
          "exclusiveMinimum": 0
        },
        "favorite_color": {
          "description": "User's favorite color",
          "type": "string"
        }
      },
      "required": [ "name", "favorite_number", "favorite_color" ]
    }
    """
    schema_registry_conf = schema_registry_config(args)
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client, user_to_dict)

    print('Producing records to topic {}. ^C to exit.'.format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            name = input(">")
            user = User(name=name, address="NA", favorite_color="blue", favorite_number=7)
            serialized_user = json_serializer(user, SerializationContext(topic, MessageField.VALUE))
            producer.produce(
                topic=topic, key=string_serializer(name), value=serialized_user, on_delivery=delivery_report
            )
        except KeyboardInterrupt:
            break

    print('\nFlushing {} records...'.format(len(producer)))
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="OAUTH example with client credentials grant")
    parser.add_argument('-b', dest="bootstrap_servers", required=True, help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="example_producer_oauth", help="Topic name")
    parser.add_argument('-s', dest="schema_registry", required=True, help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('--query', dest="query", required=True, help="Query parameters for Azure IMDS token endpoint")
    parser.add_argument('--logical-cluster', dest="logical_cluster", required=False, help="Logical Cluster.")
    parser.add_argument(
        '--logical-schema-registry-cluster',
        dest="logical_schema_registry_cluster",
        required=False,
        help="Logical Schema Registry Cluster.",
    )
    parser.add_argument('--identity-pool-id', dest="identity_pool_id", required=False, help="Identity Pool ID.")

    main(parser.parse_args())
