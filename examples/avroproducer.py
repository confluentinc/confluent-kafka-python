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
# This is a simple example of the SerializingProducer using Avro.
#
import argparse
from uuid import uuid4

from six.moves import input

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential

    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "favorite_number", "favorite_color", "_address"]

    def __init__(self, name=None, favorite_number=None, favorite_color=None,
                 address=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # do not serialize
        self._address = address


def from_user(user, ctx):
    """
    Converts a User instance to a dict.

    Args:
        user (User): User instance

        ctx (SerializationContext: Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.

    """
    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred.

        msg (Message): The message that was sent.

    Note:
        Message.key() and Message.value() will return contents as they were
        produce not as they were received. Meaning the contents will be in
        bytes not the original instance.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic

    schema_str = """
    {
        "namespace": "confluent.io.examples.serialization.avro",
        "name": "User",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": "int"},
            {"name": "favorite_color", "type": "string"}
        ]
    }
    """
    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_serializer = AvroSerializer(schema_registry_client, schema_str, from_user)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    while True:
        user = User()
        try:
            user.name = input("Enter name: ")
            user._address = input("Enter address: ")
            user.favorite_number = int(input("Enter favorite number: "))
            user.favorite_color = input("Enter favorite color: ")
            producer.produce(topic=topic, key=str(uuid4()), value=user,
                             on_delivery=delivery_report)
            # Serve on_delivery callbacks from previous asynchronous produce()
            producer.poll(0.0)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SerializingProducer Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")

    main(parser.parse_args())
