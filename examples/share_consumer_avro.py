#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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


# Example KIP-932 DeserializingShareConsumer reading Avro via Schema Registry.
#
# The key/value deserializers go in the config dict and run inside poll(). A
# record that can't be deserialized isn't raised — it comes back with its raw
# bytes and a _KEY/_VALUE_DESERIALIZATION error, so you can REJECT it and move
# on.

import argparse
import sys

from confluent_kafka import AcknowledgeType, DeserializingShareConsumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


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
    Converts an Avro-decoded object literal (dict) into a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    return User(name=obj['name'], favorite_number=obj['favorite_number'], favorite_color=obj['favorite_color'])


def main(args):
    schema_str = """
    {
      "namespace": "confluent.io.examples.serialization.avro",
      "name": "User",
      "type": "record",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number", "type": "long"},
        {"name": "favorite_color", "type": "string"}
      ]
    }
    """

    schema_registry_client = SchemaRegistryClient({'url': args.schema_registry})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str, dict_to_user)

    # Deserializers go in the config dict.
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group,
        'share.acknowledgement.mode': 'explicit',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
    }

    sc = DeserializingShareConsumer(conf)
    sc.subscribe([args.topic])

    try:
        while True:
            try:
                messages = sc.poll(timeout=1.0)  # a list, possibly empty
            except KafkaException as e:
                # Re-raise fatal errors; otherwise log and keep going.
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue

            for msg in messages:
                err = msg.error()
                if err is not None:
                    if err.code() in (KafkaError._KEY_DESERIALIZATION, KafkaError._VALUE_DESERIALIZATION):
                        # A record we received but can't decode. In explicit
                        # mode we still have to ack it — REJECT it as poison so
                        # it isn't redelivered.
                        sc.acknowledge(msg, AcknowledgeType.REJECT)
                    else:
                        # Any other flagged record: the library already handles
                        # it. Acking it yourself is redundant and can turn a
                        # permanent discard into a retry. Just log it.
                        sys.stderr.write('%% Error: %s\n' % err)
                    continue

                # value is already deserialized.
                user = msg.value()
                if user is not None:
                    print(
                        "User record {}: name: {}, favorite_number: {}, favorite_color: {}".format(
                            msg.key(), user.name, user.favorite_number, user.favorite_color
                        )
                    )
                sc.acknowledge(msg, AcknowledgeType.ACCEPT)

            # Flush the acks before the next poll().
            sc.commit_async()
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        sc.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="DeserializingShareConsumer Avro example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True, help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True, help="Schema Registry (http(s)://host[:port])")
    parser.add_argument('-t', dest="topic", default="example_serde_avro", help="Topic name")
    parser.add_argument('-g', dest="group", default="example_share_serde_avro", help="Share group")

    main(parser.parse_args())
