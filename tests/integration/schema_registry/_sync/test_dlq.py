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
import json

import pytest

from confluent_kafka import TopicPartition
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.dlq.dlq_action import DlqAction
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, RuleSet
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

_SCHEMA = {
    'type': 'record',
    'name': 'test',
    'fields': [
        {'name': 'intField', 'type': 'int'},
        {'name': 'stringField', 'type': 'string'},
    ],
}


def test_dlq_read_failure_and_replay(kafka_cluster):
    """
    A failing READ rule with on_failure=DLQ tees the original record to the DLQ
    topic and still raises; the teed record is replayable because the
    __rule.name header skips the previously failed rule.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
    """
    CelExecutor.register()

    topic = kafka_cluster.create_topic_and_wait_propogation("dlq-source")
    dlq_topic = kafka_cluster.create_topic_and_wait_propogation("dlq-dead-letter")
    sr = kafka_cluster.schema_registry()

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.READ,
        "CEL",
        None,
        None,
        "message.stringField != 'hi'",
        None,
        "DLQ",
        False,
    )
    sr.register_schema(topic + "-value", Schema(json.dumps(_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    client_conf = kafka_cluster.client_conf()
    DlqAction.register(
        {
            'dlq.topic': dlq_topic,
            'dlq.auto.flush': True,
            'bootstrap.servers': client_conf['bootstrap.servers'],
        }
    )

    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    ser = AvroSerializer(sr, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(topic, MessageField.VALUE)
    obj = {'intField': 123, 'stringField': 'hi'}
    obj_bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(sr)
    with pytest.raises(SerializationError, match="Rule failed: test-cel"):
        deser(obj_bytes, ser_ctx)

    consumer = kafka_cluster.consumer()
    consumer.assign([TopicPartition(dlq_topic, 0)])
    msg = consumer.poll()
    # the original wire bytes were teed verbatim
    assert msg.value() == obj_bytes
    headers = dict(msg.headers())
    assert headers[DlqAction.RULE_NAME] == b'test-cel'
    assert headers[DlqAction.RULE_MODE] == b'READ'
    assert headers[DlqAction.RULE_SUBJECT] == (topic + "-value").encode('utf-8')
    assert headers[DlqAction.RULE_TOPIC] == topic.encode('utf-8')
    assert DlqAction.RULE_EXCEPTION in headers

    # replaying the DLQ record does not fail again on the same rule
    replay_ctx = SerializationContext(topic, MessageField.VALUE, msg.headers())
    obj2 = deser(msg.value(), replay_ctx)
    assert obj2 == obj
