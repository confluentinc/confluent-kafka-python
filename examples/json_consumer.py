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


# A simple example demonstrating use of JSONDeserializer.

import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


class User(object):
    """
    User record

    Args:
        name (str): User's name
        favorite_number (int): User's favorite number
        favorite_color (str): User's favorite color
    """

    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        obj (dict): Object literal(dict)
    """

    if obj is None:
        return None

    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


def main(args):
    topic = args.topic

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
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=dict_to_user)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if user is not None:
                print("User record {}: name: {}\n"
                      "\tfavorite_number: {}\n"
                      "\tfavorite_color: {}\n"
                      .format(msg.key(), user.name,
                              user.favorite_number,
                              user.favorite_color))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="JSONDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_json",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_json",
                        help="Consumer group")

    main(parser.parse_args())
