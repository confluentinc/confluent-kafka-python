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

from confluent_kafka.admin import OffsetSpec, DeletedRecords
from confluent_kafka import TopicPartition


def test_delete_records(kafka_cluster):
    """
    Test delete_records, delete the records upto the specified offset
    in that particular partition of the specified topic.
    """
    admin_client = kafka_cluster.admin()

    # Create a topic with a single partition
    topic = kafka_cluster.create_topic_and_wait_propogation("test-del-records",
                                                            {
                                                                "num_partitions": 1,
                                                                "replication_factor": 1,
                                                            })

    # Create Producer instance
    p = kafka_cluster.producer()
    p.produce(topic, "Message-1")
    p.produce(topic, "Message-2")
    p.produce(topic, "Message-3")
    p.flush()

    topic_partition = TopicPartition(topic, 0)
    requests = {topic_partition: OffsetSpec.earliest()}

    # Check if the earliest avilable offset for this topic partition is 0
    fs = admin_client.list_offsets(requests)
    result = list(fs.values())[0].result()
    assert (result.offset == 0)

    topic_partition_offset = TopicPartition(topic, 0, 2)

    # Delete the records
    fs1 = admin_client.delete_records([topic_partition_offset])

    # Find the earliest available offset for that specific topic partition after deletion has been done
    fs2 = admin_client.list_offsets(requests)

    # Check if the earliest available offset is equal to the offset passed to the delete records function
    res = list(fs1.values())[0].result()
    assert isinstance(res, DeletedRecords)
    assert (res.low_watermark == list(fs2.values())[0].result().offset)

    # Delete created topic
    fs = admin_client.delete_topics([topic])
    for topic, f in fs.items():
        f.result()


def test_delete_records_multiple_topics_and_partitions(kafka_cluster):
    """
    Test delete_records, delete the records upto the specified offset
    in that particular partition of the specified topic.
    """
    admin_client = kafka_cluster.admin()
    num_partitions = 3
    # Create two topics with a single partition
    topic = kafka_cluster.create_topic_and_wait_propogation("test-del-records",
                                                            {
                                                                "num_partitions": num_partitions,
                                                                "replication_factor": 1,
                                                            })
    topic2 = kafka_cluster.create_topic_and_wait_propogation("test-del-records2",
                                                             {
                                                                 "num_partitions": num_partitions,
                                                                 "replication_factor": 1,
                                                             })

    topics = [topic, topic2]
    partitions = list(range(num_partitions))
    # Create Producer instance
    p = kafka_cluster.producer()
    for t in topics:
        for partition in partitions:
            p.produce(t, "Message-1", partition=partition)
            p.produce(t, "Message-2", partition=partition)
            p.produce(t, "Message-3", partition=partition)
    p.flush()
    requests = dict(
        [
            (TopicPartition(t, partition), OffsetSpec.earliest())
            for t in topics
            for partition in partitions
        ]
    )
    # Check if the earliest available offset for this topic partition is 0
    fs = admin_client.list_offsets(requests)
    assert all([p.result().offset == 0 for p in fs.values()])
    delete_index = 0
    # Delete the records
    for delete_partitions in [
        # Single partition no deletion
        [TopicPartition(topic, 0, 0)],
        # Single topic, two partitions, single record deleted
        [TopicPartition(topic, 0, 1), TopicPartition(topic, 1, 1)],
        # Two topics, four partitions, two records deleted
        [TopicPartition(topic, 2, 2), TopicPartition(topic2, 0, 2),
         TopicPartition(topic2, 1, 2), TopicPartition(topic2, 2, 2)],
    ]:
        list_offsets_requests = dict([
            (part, OffsetSpec.earliest()) for part in delete_partitions
        ])
        futmap_delete = admin_client.delete_records(delete_partitions)
        delete_results = [(part, fut.result())
                          for part, fut in futmap_delete.items()]
        futmap_list = admin_client.list_offsets(list_offsets_requests)
        list_results = dict([(part, fut.result())
                            for part, fut in futmap_list.items()])
        for part, delete_result in delete_results:
            list_result = list_results[part]
            assert isinstance(delete_result, DeletedRecords)
            assert delete_result.low_watermark == list_result.offset
            assert delete_result.low_watermark == delete_index
        delete_index += 1

    # Delete created topics
    fs = admin_client.delete_topics(topics)
    for topic, f in fs.items():
        f.result()
