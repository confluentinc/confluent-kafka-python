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

topic_prefix = "test-topic"


def create_consumers(kafka_cluster, topic, group_id, client_id, group_protocol):
    conf = {'group.id': group_id,
            'client.id': client_id,
            'group.protocol': group_protocol,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    consumer = kafka_cluster.consumer(conf)
    consumer.subscribe([topic])
    consumer.poll(10)
    return consumer


def verify_describe_consumer_groups(kafka_cluster, admin_client, topic):
    def create_consumer_groups():
        """Create consumer groups with new and old protocols."""
        group_ids = {
            "new1": f"test-group_new1-{uuid.uuid4()}",
            "new2": f"test-group_new2-{uuid.uuid4()}",
            "old1": f"test-group_old1-{uuid.uuid4()}",
            "old2": f"test-group_old2-{uuid.uuid4()}",
        }
        client_ids = {
            "new": ["test-client1", "test-client2"],
            "old": ["test-client3", "test-client4"],
        }

        consumers = [
            create_consumers(kafka_cluster, topic, group_ids["new1"], client_ids["new"][0], "consumer"),
            create_consumers(kafka_cluster, topic, group_ids["new2"], client_ids["new"][1], "consumer"),
            create_consumers(kafka_cluster, topic, group_ids["old1"], client_ids["old"][0], "classic"),
            create_consumers(kafka_cluster, topic, group_ids["old2"], client_ids["old"][1], "classic"),
        ]
        return group_ids, client_ids, consumers

    def verify_consumer_group_results(fs, expected_group_ids, expected_type, expected_clients):
        """Verify the results of consumer group descriptions."""
        for group_id, f in fs.items():
            result = f.result()
            assert result.group_id in expected_group_ids
            assert result.is_simple_consumer_group is False
            assert result.state == ConsumerGroupState.STABLE
            assert result.type == expected_type
            assert len(result.members) == 1
            for member in result.members:
                assert member.client_id in expected_clients
                assert member.assignment.topic_partitions == partition

    # Create consumer groups
    group_ids, client_ids, consumers = create_consumer_groups()
    partition = [TopicPartition(topic, 0)]

    # Describe and verify new group protocol consumer groups
    fs_new = admin_client.describe_consumer_groups([group_ids["new1"], group_ids["new2"]])
    verify_consumer_group_results(fs_new, [group_ids["new1"], group_ids["new2"]],
                                  ConsumerGroupType.CONSUMER, client_ids["new"])

    # Describe and verify old group protocol consumer groups
    fs_old = admin_client.describe_consumer_groups([group_ids["old1"], group_ids["old2"]])
    verify_consumer_group_results(fs_old, [group_ids["old1"], group_ids["old2"]],
                                  ConsumerGroupType.CLASSIC, client_ids["old"])

    # Describe and verify all consumer groups
    fs_all = admin_client.describe_consumer_groups(list(group_ids.values()))
    for group_id, f in fs_all.items():
        result = f.result()
        assert result.group_id in group_ids.values()
        assert result.is_simple_consumer_group is False
        assert result.state == ConsumerGroupState.STABLE
        if result.group_id in [group_ids["new1"], group_ids["new2"]]:
            assert result.type == ConsumerGroupType.CONSUMER
            assert result.members[0].client_id in client_ids["new"]
        else:
            assert result.type == ConsumerGroupType.CLASSIC
            assert result.members[0].client_id in client_ids["old"]
        assert result.members[0].assignment.topic_partitions == partition

    # Close all consumers
    for consumer in consumers:
        consumer.close()


def test_describe_consumer_groups_compatibility(kafka_cluster):

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
