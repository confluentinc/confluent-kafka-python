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

import pytest
from enum import Enum
from confluent_kafka import ConsumerGroupType, KafkaException
from tests.common import TestUtils

topic_prefix = "test_consumer_upgrade_downgrade_"
number_of_partitions = 10


def get_group_protocol_type(a, group_id):
    futureMap = a.describe_consumer_groups([group_id])
    try:
        future = futureMap[group_id]
        g = future.result()
        return g.type
    except KafkaException as e:
        print("Error while describing group id '{}': {}".format(group_id, e))
    except Exception:
        raise


def check_consumer(kafka_cluster, consumers, admin_client, group_id, topic, expected_protocol):
    no_of_messages = 100
    total_msg_read = 0
    expected_partitions_per_consumer = number_of_partitions // len(consumers)
    while len(consumers[-1].assignment()) != expected_partitions_per_consumer:
        for consumer in consumers:
            consumer.poll(0.1)

    all_assignments = set()
    for consumer in consumers:
        assignment = consumer.assignment()
        all_assignments.update(assignment)
        assert len(assignment) == expected_partitions_per_consumer
    assert len(all_assignments) == number_of_partitions

    assert get_group_protocol_type(admin_client, group_id) == expected_protocol

    # Produce some messages to the topic
    test_data = ['test-data{}'.format(i) for i in range(0, no_of_messages)]
    test_keys = ['test-key{}'.format(i) for i in range(0, no_of_messages)]  # we want each partition to have data
    kafka_cluster.seed_topic(topic, test_data, test_keys)

    while total_msg_read < no_of_messages:
        for consumer in consumers:
            # Poll for messages
            msg = consumer.poll(0.1)
            if msg is not None:
                total_msg_read += 1

    assert total_msg_read == no_of_messages, f"Expected to read {no_of_messages} messages, but read {total_msg_read}"


class Operation(Enum):
    ADD = 0
    REMOVE = 1


def perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(
        kafka_cluster, partition_assignment_strategy):
    """
    Test consumer upgrade and downgrade.
    """
    topic_name_prefix = f"{topic_prefix}_{partition_assignment_strategy}"
    topic = kafka_cluster.create_topic_and_wait_propagation(topic_name_prefix,
                                                            {
                                                                "num_partitions": number_of_partitions
                                                            })
    admin_client = kafka_cluster.admin()

    consumer_conf = {'group.id': topic,
                     'auto.offset.reset': 'earliest'}
    consumer_conf_classic = {
        'group.protocol': 'classic',
        'partition.assignment.strategy': partition_assignment_strategy,
        **consumer_conf
    }
    consumer_conf_consumer = {
        'group.protocol': 'consumer',
        **consumer_conf
    }

    test_scenarios = [(Operation.ADD, consumer_conf_classic, ConsumerGroupType.CLASSIC),
                      (Operation.ADD, consumer_conf_consumer, ConsumerGroupType.CONSUMER),
                      (Operation.REMOVE, None, ConsumerGroupType.CONSUMER),
                      (Operation.ADD, consumer_conf_classic, ConsumerGroupType.CONSUMER),
                      (Operation.REMOVE, None, ConsumerGroupType.CLASSIC)]
    consumers = []

    for operation, conf, expected_protocol in test_scenarios:
        if operation == Operation.ADD:
            consumer = kafka_cluster.consumer(conf)
            assert consumer is not None
            consumer.subscribe([topic])
            consumers.append(consumer)
        elif operation == Operation.REMOVE:
            consumer_to_remove = consumers.pop(0)
            consumer_to_remove.close()
        check_consumer(kafka_cluster, consumers, admin_client, topic, topic, expected_protocol)

    assert len(consumers) == 1
    consumers[0].close()
    kafka_cluster.delete_topic(topic)


@pytest.mark.skipif(not TestUtils.use_group_protocol_consumer(),
                    reason="Skipping test as group protocol consumer is not enabled")
def test_consumer_upgrade_downgrade(kafka_cluster):
    perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(kafka_cluster, 'roundrobin')
    perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(kafka_cluster, 'range')
    perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(kafka_cluster, 'cooperative-sticky')
