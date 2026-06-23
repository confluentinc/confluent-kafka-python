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
# Example KIP-932 ShareConsumer: explicit ack mode with asynchronous commits.
#
# Same ACCEPT / RELEASE / REJECT acking as share_consumer_commit_sync.py, but
# commit_async() doesn't block — the broker's reply shows up later in the
# acknowledgement-commit callback below.
#
from confluent_kafka import AcknowledgeType, KafkaException, ShareConsumer


def acknowledgement_commit_callback(offsets, exception):
    # commit_async() reports its result only here. offsets maps each
    # TopicPartition to the set of acked offsets; exception is set on failure.
    if exception is not None:
        sys.stderr.write('%% Acknowledgement commit failed: %s\n' % exception)
        return
    for topic_partition, acked_offsets in offsets.items():
        sys.stderr.write(
            '%% Committed %s [%d]: %s\n' % (topic_partition.topic, topic_partition.partition, sorted(acked_offsets))
        )


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
    sc.set_acknowledgement_commit_callback(acknowledgement_commit_callback)
    sc.subscribe(topics)

    try:
        while True:
            try:
                messages = sc.poll(timeout=1.0)  # a list, possibly empty
            except KafkaException as e:
                # Re-raise fatal errors; otherwise log and keep going.
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue

            for msg in messages:
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

            # Flush the acks before the next poll(). commit_async() doesn't
            # block; results land in the callback.
            sc.commit_async()
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # close() doesn't flush in-flight async acks — commit_sync() first if
        # you need them to stick.
        sc.close()
