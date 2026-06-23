#!/usr/bin/env python
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

import sys

#
# Example KIP-932 ShareConsumer: explicit ack mode with synchronous commits.
#
# In explicit mode you must acknowledge every record before the next poll():
#   ACCEPT  - done; never redeliver it.
#   RELEASE - put it back so it can be retried, maybe by another consumer.
#   REJECT  - give up on it; the broker archives it.
#
# commit_sync() blocks for the broker reply and returns a per-partition
# {TopicPartition: error-or-None} dict, so you can see what succeeded.
#
from confluent_kafka import AcknowledgeType, KafkaException, ShareConsumer


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print_usage_and_exit(sys.argv[0])

    broker = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]

    # ShareConsumer configuration.
    # See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        'share.acknowledgement.mode': 'explicit',
    }

    sc = ShareConsumer(conf)
    sc.subscribe(topics)

    try:
        while True:
            try:
                batch = sc.poll(timeout=1.0)  # a list, possibly empty
            except KafkaException as e:
                # Re-raise fatal errors; otherwise log and keep going.
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue

            for msg in batch:
                if msg.error():
                    # No need to acknowledge a flagged record — the library
                    # already handles it. Acking it yourself is redundant and
                    # can override a permanent discard with a retry. Just log it.
                    sys.stderr.write('%% Error: %s\n' % msg.error())
                    continue

                try:
                    # Your processing goes here.
                    print(msg.value())
                except Exception as e:
                    # Couldn't process it — RELEASE so it can be retried.
                    sys.stderr.write('%% Processing failed: %s\n' % e)
                    sc.acknowledge(msg, AcknowledgeType.RELEASE)
                    continue

                sc.acknowledge(msg, AcknowledgeType.ACCEPT)

            if not batch:
                continue

            # Flush the acks before the next poll().
            results = sc.commit_sync(timeout=10.0)
            for topic_partition, err in results.items():
                if err is not None:
                    sys.stderr.write(
                        '%% Commit failed for %s [%d]: %s\n' % (topic_partition.topic, topic_partition.partition, err)
                    )
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        sc.close()
