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
from confluent_kafka import ConsumerGroupType, IsolationLevel, KafkaException, TopicPartition
from confluent_kafka.admin import OffsetSpec
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


def list_offsets(a, topic, no_of_partitions):
    topic_partition_offsets = {}
    for partition in range(no_of_partitions):
        topic_partition = TopicPartition(topic, partition)
        topic_partition_offsets[topic_partition] = OffsetSpec.latest()
    futmap = a.list_offsets(topic_partition_offsets, isolation_level=IsolationLevel.READ_COMMITTED, request_timeout=30)
    for partition, fut in futmap.items():
        try:
            result = fut.result()
            print("Topicname : {} Partition_Index : {} Offset : {} Timestamp : {}"
                  .format(partition.topic, partition.partition, result.offset,
                          result.timestamp))
        except KafkaException as e:
            print("Topicname : {} Partition_Index : {} Error : {}"
                  .format(partition.topic, partition.partition, e))


def check_consumer(kafka_cluster, consumers, admin_client, topic, expected_protocol):
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

    assert get_group_protocol_type(admin_client, topic) == expected_protocol

    # Produce some messages to the topic
    kafka_cluster.seed_topic(topic)
    list_offsets(admin_client, topic, number_of_partitions)

    while total_msg_read < 100:
        for consumer in consumers:
            # Poll for messages
            msg = consumer.poll(0.1)
            if msg is not None:
                total_msg_read += 1
    
    assert total_msg_read == 100, "Expected to read 100 messages, but read {}".format(total_msg_read)


def perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(kafka_cluster, partition_assignment_strategy):
    """
    Test consumer upgrade and downgrade.
    """
    topic_name_prefix = f"{topic_prefix}_{partition_assignment_strategy}"
    topic = kafka_cluster.create_topic_and_wait_propogation(topic_name_prefix,
                                                                    {
                                                                        "num_partitions": number_of_partitions
                                                                    })
    admin_client = kafka_cluster.admin()

    # Create a consumer with the latest version
    consumer_conf = {'group.id': topic,
                     'auto.offset.reset': 'earliest',
                     'group.protocol': 'classic'}
    consumer_conf['partition.assignment.strategy'] = partition_assignment_strategy
    consumer = kafka_cluster.consumer(consumer_conf)
    assert consumer is not None
    consumer.subscribe([topic])
    check_consumer(kafka_cluster, [consumer], admin_client, topic, ConsumerGroupType.CLASSIC)
    del consumer_conf['partition.assignment.strategy']

    # Now simulate an upgrade by creating a new consumer with 'consumer' protocol
    consumer_conf['group.protocol'] = 'consumer'
    consumer2 = kafka_cluster.consumer(consumer_conf)
    assert consumer2 is not None
    consumer2.subscribe([topic])
    check_consumer(kafka_cluster, [consumer, consumer2], admin_client, topic, ConsumerGroupType.CONSUMER)

    # Now simulate a downgrade by deleting the second consumer and keeping only 'classic' consumer
    consumer2.close()
    check_consumer(kafka_cluster, [consumer], admin_client, topic, ConsumerGroupType.CLASSIC)

    consumer.close()
    kafka_cluster.delete_topic(topic)


@pytest.mark.skipif(not TestUtils.use_group_protocol_consumer(),
                    reason="Skipping test as group protocol consumer is not enabled")
def test_consumer_upgrade_downgrade(kafka_cluster):
    perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(kafka_cluster, 'roundrobin')
    perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(kafka_cluster, 'range')
    perform_consumer_upgrade_downgrade_test_with_partition_assignment_strategy(kafka_cluster, 'cooperative-sticky')
