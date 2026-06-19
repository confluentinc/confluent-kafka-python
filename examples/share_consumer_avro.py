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


# Example KIP-932 DeserializingShareConsumer reading Avro data via Schema
# Registry.
#
# DeserializingShareConsumer takes the key/value deserializers in its config
# dict and applies them to every record inside poll() — there is no
# SerializationContext to build by hand. A record that fails to deserialize is
# not raised: it stays in the returned batch with its raw bytes and a
# _KEY/_VALUE_DESERIALIZATION error set, so the rest of the batch keeps
# flowing and the application can REJECT the poison record.

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

    # The deserializers live in the config dict and are applied to every
    # record inside poll().
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
                messages = sc.poll(timeout=1.0)  # returns a list (possibly empty)
            except KafkaException as e:
                # Poll-level error. Check err.fatal(): re-raise fatal errors,
                # otherwise treat the error as retriable and keep polling
                # (regardless of err.retriable()).
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue

            for msg in messages:
                err = msg.error()
                if err is not None:
                    if err.code() in (KafkaError._KEY_DESERIALIZATION, KafkaError._VALUE_DESERIALIZATION):
                        # Poison record: it will never decode. REJECT so the
                        # broker archives it instead of redelivering.
                        sc.acknowledge(msg, AcknowledgeType.REJECT)
                    else:
                        # Broker/transport error: RELEASE so it can be retried.
                        sc.acknowledge(msg, AcknowledgeType.RELEASE)
                    continue

                # key and value are already deserialized objects, not bytes.
                user = msg.value()
                if user is not None:
                    print(
                        "User record {}: name: {}, favorite_number: {}, favorite_color: {}".format(
                            msg.key(), user.name, user.favorite_number, user.favorite_color
                        )
                    )
                sc.acknowledge(msg, AcknowledgeType.ACCEPT)

            # Flush the batch's acks before the next poll() (required in
            # explicit mode).
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
