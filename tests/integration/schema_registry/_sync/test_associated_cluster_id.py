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
#
"""
End-to-end: serde builders resolving subjects through the associated subject
name strategy with the Kafka cluster id supplied by the client.

Nothing configures ``subject.name.strategy.kafka.cluster.id``: the
serializing producer / deserializing consumer hand their serdes a resolver
for the id of the cluster they are connected to, which the serdes invoke on
their first subject lookup.
"""

import json
from uuid import uuid4

import pytest

from confluent_kafka.schema_registry import (
    AssociationCreateOrUpdateInfo,
    AssociationCreateOrUpdateRequest,
    Schema,
)
from confluent_kafka.schema_registry.avro import AvroDeserializerBuilder, AvroSerializerBuilder
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import StringDeserializer, StringSerializer

SCHEMA = {
    'type': 'record',
    'name': 'Counter',
    'fields': [{'name': 'count', 'type': 'int'}],
}
SCHEMA_STR = json.dumps(SCHEMA)
RECORDS = [{'count': 1}, {'count': 2}]
CLUSTER_ID_TIMEOUT = 60.0


def _associate(sr, topic, namespace, subject, resource_id):
    """
    Register the schema under ``subject`` and associate the topic with it.

    The association is not frozen: a frozen one pins the subject to the
    topic-derived name, while the test wants a subject the fallback strategy
    cannot produce. Skips when the Schema Registry has no associations API.
    """
    request = AssociationCreateOrUpdateRequest(
        resource_name=topic,
        resource_namespace=namespace,
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
    try:
        sr.register_schema(subject, Schema(SCHEMA_STR, "AVRO"))
        sr.create_association(request)
    except SchemaRegistryError as e:
        if e.http_status_code in (404, 405, 501):
            pytest.skip("Schema Registry does not support associations: {}".format(e))
        raise


def _spy_cluster_id(monkeypatch, client):
    """Record the ``cluster_id()`` calls a client makes on behalf of its serdes."""
    calls = []
    original = type(client).cluster_id

    def spy(self, *args, **kwargs):
        calls.append(kwargs.get('timeout'))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(type(client), 'cluster_id', spy)
    return calls


def _consume(consumer, topic, count):
    consumer.subscribe([topic])
    received = []
    for _ in range(120):
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        received.append(msg.deserialized_value())
        if len(received) == count:
            break
    return received


def test_builder_serdes_use_the_association_of_the_connected_cluster(kafka_cluster, monkeypatch):
    topic = kafka_cluster.create_topic_and_wait_propogation("assoc_cluster_id")
    sr = kafka_cluster.schema_registry()
    cluster_id = kafka_cluster.admin().cluster_id(timeout=30)
    subject = "assoc-subject-{}".format(uuid4())
    resource_id = "{}:{}".format(cluster_id, topic)

    _associate(sr, topic, cluster_id, subject, resource_id)

    try:
        producer = kafka_cluster.builder_producer(
            {
                'key.serializer': StringSerializer('utf_8'),
                # the builder creates its own Schema Registry client, owned by
                # the serializer and closed with the producer
                'value.serializer.builder': AvroSerializerBuilder(
                    schema_registry_config=kafka_cluster.schema_registry_conf(),
                    schema=SCHEMA_STR,
                    serializer_config={'auto.register.schemas': False, 'use.latest.version': True},
                ),
            }
        )
        calls = _spy_cluster_id(monkeypatch, producer)

        # construction did not wait on the broker
        assert calls == []

        producer.produce(topic, key='k1', value=RECORDS[0])
        # the first message resolved the id (subject cache miss) ...
        assert calls == [CLUSTER_ID_TIMEOUT]
        producer.produce(topic, key='k2', value=RECORDS[1])
        # ... and the second one used the cached subject
        assert calls == [CLUSTER_ID_TIMEOUT]
        producer.flush()
        producer.close()

        # the association was honoured: no <topic>-value subject was created
        subjects = sr.get_subjects()
        assert subject in subjects
        assert topic + "-value" not in subjects

        consumer = kafka_cluster.builder_consumer(
            {
                'key.deserializer': StringDeserializer('utf_8'),
                'value.deserializer.builder': AvroDeserializerBuilder(schema_registry_client=sr),
            }
        )
        try:
            assert _consume(consumer, topic, len(RECORDS)) == RECORDS
        finally:
            consumer.close()
    finally:
        sr.delete_associations(resource_id=resource_id, cascade_lifecycle=True)


def test_association_under_wildcard_namespace_is_not_seen_by_a_connected_client(kafka_cluster):
    # An association registered under the "-" namespace is only found when no
    # cluster id is known. A serde built by the producer knows the id, so it
    # falls back to the <topic>-value subject instead.
    topic = kafka_cluster.create_topic_and_wait_propogation("assoc_wildcard")
    sr = kafka_cluster.schema_registry()
    subject = "wildcard-subject-{}".format(uuid4())
    resource_id = "wildcard:{}".format(topic)

    _associate(sr, topic, "-", subject, resource_id)

    try:
        producer = kafka_cluster.builder_producer(
            {
                'value.serializer.builder': AvroSerializerBuilder(
                    schema_registry_client=sr,
                    schema=SCHEMA_STR,
                ),
            }
        )
        try:
            producer.produce(topic, value=RECORDS[0])
            producer.flush()
        finally:
            producer.close()

        assert topic + "-value" in sr.get_subjects()
    finally:
        sr.delete_associations(resource_id=resource_id, cascade_lifecycle=True)
        # a subject has to be soft-deleted before it can be deleted permanently
        sr.delete_subject(topic + "-value")
        sr.delete_subject(topic + "-value", permanent=True)
