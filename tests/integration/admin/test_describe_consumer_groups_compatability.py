# -*- coding: utf-8 -*-
# Copyright 2024 Confluent Inc.
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

from confluent_kafka import ConsumerGroupState, ConsumerGroupType, TopicPartition
import uuid

from tests.common import TestUtils

topic_prefix = "test-topic"


def create_consumers(kafka_cluster, topic, group_id, client_id, Protocol):
    conf = {'group.id': group_id,
            'client.id': client_id,
            'group.protocol': Protocol,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
            'debug': 'all'}
    consumer = kafka_cluster.consumer(conf)
    consumer.subscribe([topic])
    consumer.poll(10)
    return consumer


def verify_describe_consumer_groups(kafka_cluster, admin_client, topic):

    group_id_new1 = f"test-group_new1-{uuid.uuid4()}"
    group_id_new2 = f"test-group_new2-{uuid.uuid4()}"
    group_id_old1 = f"test-group_old1-{uuid.uuid4()}"
    group_id_old2 = f"test-group_old2-{uuid.uuid4()}"

    client_id1 = "test-client1"
    client_id2 = "test-client2"
    client_id3 = "test-client3"
    client_id4 = "test-client4"

    consumers = []

    # Create two groups with new group protocol
    consumers.append(create_consumers(kafka_cluster, topic, group_id_new1, client_id1, "consumer"))
    consumers.append(create_consumers(kafka_cluster, topic, group_id_new2, client_id2, "consumer"))

    # Create two groups with old group protocol
    consumers.append(create_consumers(kafka_cluster, topic, group_id_old1, client_id3, "classic"))
    consumers.append(create_consumers(kafka_cluster, topic, group_id_old2, client_id4, "classic"))

    partition = [TopicPartition(topic, 0)]

    # We will pass 3 requests, one containing the two groups created with new
    # group protocol and the other containing the two groups created with old
    # group protocol and the third containing all the groups and verify the results.
    fs1 = admin_client.describe_consumer_groups(group_ids=[group_id_new1, group_id_new2])
    for group_id, f in fs1.items():
        result = f.result()
        assert result.group_id in [group_id_new1, group_id_new2]
        assert result.is_simple_consumer_group is False
        assert result.state == ConsumerGroupState.STABLE
        assert result.type == ConsumerGroupType.CONSUMER
        assert len(result.members) == 1
        for member in result.members:
            assert member.client_id in [client_id1, client_id2]
            assert member.assignment.topic_partitions == partition

    fs2 = admin_client.describe_consumer_groups(group_ids=[group_id_old1, group_id_old2])
    for group_id, f in fs2.items():
        result = f.result()
        assert result.group_id in [group_id_old1, group_id_old2]
        assert result.is_simple_consumer_group is False
        assert result.state == ConsumerGroupState.STABLE
        assert result.type == ConsumerGroupType.CLASSIC
        assert len(result.members) == 1
        for member in result.members:
            assert member.client_id in [client_id3, client_id4]
            assert member.assignment.topic_partitions == partition

    fs3 = admin_client.describe_consumer_groups(group_ids=[group_id_new1, group_id_new2, group_id_old1, group_id_old2])
    for group_id, f in fs3.items():
        result = f.result()
        assert result.group_id in [group_id_new1, group_id_new2, group_id_old1, group_id_old2]
        assert result.is_simple_consumer_group is False
        assert result.state == ConsumerGroupState.STABLE
        if result.group_id in [group_id_new1, group_id_new2]:
            assert result.type == ConsumerGroupType.CONSUMER
        else:
            assert result.type == ConsumerGroupType.CLASSIC
        assert len(result.members) == 1
        for member in result.members:
            if result.group_id in [group_id_new1, group_id_new2]:
                assert member.client_id in [client_id1, client_id2]
            else:
                assert member.client_id in [client_id3, client_id4]
            assert member.assignment.topic_partitions == partition

    for consumer in consumers:
        consumer.close()


def test_describe_consumer_groups_compatability(kafka_cluster):

    admin_client = kafka_cluster.admin()

    # Create Topic
    topic_config = {"compression.type": "gzip"}
    our_topic = kafka_cluster.create_topic_and_wait_propogation(topic_prefix,
                                                               {
                                                                   "num_partitions": 1,
                                                                   "config": topic_config,
                                                                   "replication_factor": 1,
                                                               },
                                                               validate_only=False
                                                               )

    # Delete created topic
    fs = admin_client.delete_topics([our_topic])
    for topic, f in fs.items():
        f.result()
