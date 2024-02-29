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

from confluent_kafka.admin import ListOffsetsResultInfo, OffsetSpec
from confluent_kafka import TopicPartition, IsolationLevel


def test_delete_records(kafka_cluster):
    """
    Test delete_records, delete the records upto the specified offset
    in that particular partition of the specified topic.
    """
    admin_client = kafka_cluster.admin()

    # Create a topic with a single partition
    topic = kafka_cluster.create_topic("test-del-records",
                                       {
                                           "num_partitions": 1,
                                           "replication_factor": 1,
                                       })

    # Create Producer instance
    p = kafka_cluster.producer()
    p.produce(topic, "Message-1",)
    p.produce(topic, "Message-2")
    p.produce(topic, "Message-3")
    p.flush()

    topic_partition = TopicPartition(topic, 0)
    requests = {topic_partition: OffsetSpec.earliest()}
    topic_partition_offset = TopicPartition(topic, 0, 2)

    kwargs = {"isolation_level": IsolationLevel.READ_UNCOMMITTED}

    # Delete the records
    fs1 = admin_client.delete_records([topic_partition_offset])
    earliest_offset_available = 0

    # Find the earliest available offset for that specific topic partition
    fs2 = admin_client.list_offsets(requests, **kwargs)
    for _, fut in fs2.items():
        result = fut.result()
        assert isinstance(result, ListOffsetsResultInfo)
        earliest_offset_available = result.offset

    # Check if the earliest available offset is equal to the offset passed to the delete records function
    for _, fut in fs1.items():
        result = fut.result()
        assert isinstance(result, TopicPartition)
        assert (result.offset == earliest_offset_available)

    # Delete created topic
    fs = admin_client.delete_topics([topic])
    for topic, f in fs.items():
        f.result()
