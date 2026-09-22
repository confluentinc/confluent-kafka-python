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

# Demonstrates Schema Registry topic-to-subject *associations* together with
# the serde builders.
#
# An association binds a topic of a given Kafka cluster to a subject, so that
# the subject need not follow the `<topic>-value` naming convention. Serdes
# using the (default) associated subject name strategy look the association up
# by the id of the cluster they are connected to. When the serde is built by a
# SerializingProducer / DeserializingConsumer through a builder, the client
# hands it that cluster id, so the application has nothing to configure: this
# example
#
#   1. asks the admin client for the cluster id and creates an association
#      from the topic to a subject of its choosing under that id,
#   2. produces User records through an AvroSerializerBuilder, whose
#      serializer registers the schema under the associated subject rather
#      than under `<topic>-value`,
#   3. consumes them back through an AvroDeserializerBuilder,
#   4. removes the association again.
#
# Requires a Schema Registry supporting associations (Confluent Platform 8.1
# / Confluent Cloud).

import argparse
from uuid import uuid4

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import (
    AssociationCreateOrUpdateInfo,
    AssociationCreateOrUpdateRequest,
    Schema,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import AvroDeserializerBuilder, AvroSerializerBuilder
from confluent_kafka.serialization import StringDeserializer, StringSerializer

SCHEMA_STR = """
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


class User(object):
    def __init__(self, name, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color

    def __repr__(self):
        return "User(name={!r}, favorite_number={!r}, favorite_color={!r})".format(
            self.name, self.favorite_number, self.favorite_color
        )


def user_to_dict(user, ctx):
    return dict(name=user.name, favorite_number=user.favorite_number, favorite_color=user.favorite_color)


def dict_to_user(obj, ctx):
    if obj is None:
        return None
    return User(obj['name'], obj['favorite_number'], obj['favorite_color'])


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print("Record {} produced to {} [{}] at offset {}".format(msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    subject = args.subject
    client_conf = {'bootstrap.servers': args.bootstrap_servers}
    schema_registry_conf = {'url': args.schema_registry}
    if args.sr_api_key and args.sr_api_secret:
        schema_registry_conf['basic.auth.user.info'] = "{}:{}".format(args.sr_api_key, args.sr_api_secret)

    # 1. Associate the topic, on this very cluster, with the subject.
    admin = AdminClient(client_conf)
    cluster_id = admin.cluster_id(timeout=30)
    print("Kafka cluster id: {}".format(cluster_id))

    schema_registry = SchemaRegistryClient(schema_registry_conf)
    resource_id = "{}:{}".format(cluster_id, topic)
    # Register the schema under the subject first: an association that is
    # not frozen (the only kind accepting a subject name unrelated to the
    # topic) cannot carry the schema itself.
    schema_registry.register_schema(subject, Schema(SCHEMA_STR, "AVRO"))
    schema_registry.create_association(
        AssociationCreateOrUpdateRequest(
            resource_name=topic,
            resource_namespace=cluster_id,
            resource_id=resource_id,
            resource_type="topic",
            associations=[
                AssociationCreateOrUpdateInfo(
                    subject=subject,
                    association_type="value",
                    lifecycle="STRONG",
                    frozen=False,
                )
            ],
        )
    )
    print("Associated topic {} with subject {}".format(topic, subject))

    try:
        # 2. Produce. No cluster id is configured on the serializer: the
        #    producer resolves it on the first message and the serializer finds
        #    the association created above, so the schema goes to `subject`
        #    and `<topic>-value` is never created.
        producer_conf = dict(
            client_conf,
            **{
                'key.serializer': StringSerializer('utf_8'),
                'value.serializer.builder': AvroSerializerBuilder(
                    schema_registry_config=schema_registry_conf,
                    schema=SCHEMA_STR,
                    to_dict=user_to_dict,
                    serializer_config={'auto.register.schemas': False, 'use.latest.version': True},
                ),
            }
        )
        with SerializingProducer(producer_conf) as producer:
            for i in range(3):
                user = User("user-{}".format(i), i, "blue")
                producer.produce(topic, key=str(uuid4()), value=user, on_delivery=delivery_report)
            # exiting the block flushes, then closes the serializer and the
            # Schema Registry client it created

        # 3. Consume through a deserializer built the same way.
        consumer_conf = dict(
            client_conf,
            **{
                'group.id': "avro-association-{}".format(uuid4()),
                'auto.offset.reset': 'earliest',
                'key.deserializer': StringDeserializer('utf_8'),
                'value.deserializer.builder': AvroDeserializerBuilder(
                    schema_registry_config=schema_registry_conf,
                    from_dict=dict_to_user,
                ),
            }
        )
        with DeserializingConsumer(consumer_conf) as consumer:
            consumer.subscribe([topic])
            received = 0
            while received < 3:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                print("Consumed {} -> {}".format(msg.deserialized_key(), msg.deserialized_value()))
                received += 1

        subjects = schema_registry.get_subjects()
        print("Subject {} registered: {}".format(subject, subject in subjects))
        print("Subject {}-value registered: {}".format(topic, "{}-value".format(topic) in subjects))
    finally:
        # 4. Clean up the association (and, with cascade, the subject).
        schema_registry.delete_associations(resource_id=resource_id, cascade_lifecycle=True)
        print("Removed the association of topic {}".format(topic))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Schema Registry association example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True, help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True, help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_avro_association", help="Topic name")
    parser.add_argument('--subject', dest="subject", default="example_avro_association_subject", help="Subject name")
    parser.add_argument('--sr-api-key', dest="sr_api_key", default=None, help="Confluent Cloud SR API key (optional)")
    parser.add_argument(
        '--sr-api-secret', dest="sr_api_secret", default=None, help="Confluent Cloud SR API secret (optional)"
    )

    main(parser.parse_args())
