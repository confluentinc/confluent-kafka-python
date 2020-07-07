#!/usr/bin/env python3
#
# Copyright 2020 Confluent Inc.
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

#
# Show committed offsets and current lag for group and topic(s).
#


import sys
import confluent_kafka


if len(sys.argv) < 4:
    sys.stderr.write("Usage: {} <brokers> <group.id> <topic> <topic2..>\n".format(sys.argv[0]))
    sys.exit(1)

brokers, group = sys.argv[1:3]

# Create consumer.
# This consumer will not join the group, but the group.id is required by
# committed() to know which group to get offsets for.
consumer = confluent_kafka.Consumer({'bootstrap.servers': brokers,
                                     'group.id': group})


print("%-50s  %9s  %9s" % ("Topic [Partition]", "Committed", "Lag"))
print("=" * 72)

for topic in sys.argv[3:]:
    # Get the topic's partitions
    metadata = consumer.list_topics(topic, timeout=10)
    if metadata.topics[topic].error is not None:
        raise confluent_kafka.KafkaException(metadata.topics[topic].error)

    # Construct TopicPartition list of partitions to query
    partitions = [confluent_kafka.TopicPartition(topic, p) for p in metadata.topics[topic].partitions]

    # Query committed offsets for this group and the given partitions
    committed = consumer.committed(partitions, timeout=10)

    for partition in committed:
        # Get the partitions low and high watermark offsets.
        (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

        if partition.offset == confluent_kafka.OFFSET_INVALID:
            offset = "-"
        else:
            offset = "%d" % (partition.offset)

        if hi < 0:
            lag = "no hwmark"  # Unlikely
        elif partition.offset < 0:
            # No committed offset, show total message count as lag.
            # The actual message count may be lower due to compaction
            # and record deletions.
            lag = "%d" % (hi - lo)
        else:
            lag = "%d" % (hi - partition.offset)

        print("%-50s  %9s  %9s" % (
            "{} [{}]".format(partition.topic, partition.partition), offset, lag))


consumer.close()
