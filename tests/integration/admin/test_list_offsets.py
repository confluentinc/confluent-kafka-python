# -*- coding: utf-8 -*-
# Copyright 2023 Confluent Inc.
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


def test_list_offsets(kafka_cluster):
    """
    Test list offsets results when asking for the earliest offset (the first one),
    the latest offset (last one), max timestamp (second one)
    or after a specific timestamp (second one).
    """

    admin_client = kafka_cluster.admin()

    # Create a topic with a single partition
    topic = kafka_cluster.create_topic("test-topic-verify-list-offsets",
                                       {
                                           "num_partitions": 1,
                                           "replication_factor": 1,
                                       })

    # Create Producer instance
    p = kafka_cluster.producer()
    base_timestamp = 1000000000
    p.produce(topic, "Message-1", timestamp=(base_timestamp + 100))
    p.produce(topic, "Message-2", timestamp=(base_timestamp + 400))
    p.produce(topic, "Message-3", timestamp=(base_timestamp + 200))
    p.flush()

    topic_partition = TopicPartition(topic, 0)

    for kwargs in [{},
                   {"isolation_level": IsolationLevel.READ_UNCOMMITTED},
                   {"request_timeout": 30},
                   {"isolation_level": IsolationLevel.READ_COMMITTED, "request_timeout": 30}]:

        requests = {topic_partition: OffsetSpec.earliest()}
        futmap = admin_client.list_offsets(requests, **kwargs)
        for _, fut in futmap.items():
            result = fut.result()
            assert isinstance(result, ListOffsetsResultInfo)
            assert (result.offset == 0)

        requests = {topic_partition: OffsetSpec.latest()}
        futmap = admin_client.list_offsets(requests, **kwargs)
        for _, fut in futmap.items():
            result = fut.result()
            assert isinstance(result, ListOffsetsResultInfo)
            assert (result.offset == 3)

        requests = {topic_partition: OffsetSpec.max_timestamp()}
        futmap = admin_client.list_offsets(requests, **kwargs)
        for _, fut in futmap.items():
            result = fut.result()
            assert isinstance(result, ListOffsetsResultInfo)
            assert (result.offset == 1)

        requests = {topic_partition: OffsetSpec.for_timestamp(base_timestamp + 150)}
        futmap = admin_client.list_offsets(requests, **kwargs)
        for _, fut in futmap.items():
            result = fut.result()
            assert isinstance(result, ListOffsetsResultInfo)
            assert (result.offset == 1)

    # Delete created topic
    fs = admin_client.delete_topics([topic])
    for topic, f in fs.items():
        f.result()
