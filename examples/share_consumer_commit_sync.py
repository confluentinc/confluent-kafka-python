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
# Example KIP-932 ShareConsumer using explicit acknowledgement and synchronous
# commits.
#
# In explicit mode the application must acknowledge every record returned by
# poll() before the next poll(). Each record is acked as one of:
#   ACCEPT  - processed successfully; the broker never redelivers it.
#   RELEASE - return it to the share group so it can be retried, possibly by
#             another consumer.
#   REJECT  - give up on it; the broker archives it (and, once available, it
#             can be routed to a dead-letter queue).
#
# commit_sync() blocks until the broker replies and returns a
# {TopicPartition: KafkaError-or-None} dict, so the application can inspect
# each partition's outcome — a commit can partially succeed.
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
                batch = sc.poll(timeout=1.0)  # returns a list (possibly empty)
            except KafkaException as e:
                # Poll-level error. Check err.fatal(): re-raise fatal errors,
                # otherwise treat the error as retriable and keep polling
                # (regardless of err.retriable()).
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue

            for msg in batch:
                if msg.error():
                    # A record that arrived with an error carries no payload to
                    # process. RELEASE lets the share group retry it; switch to
                    # REJECT for a record you never want redelivered.
                    sc.acknowledge(msg, AcknowledgeType.RELEASE)
                    continue

                try:
                    # Replace this with your real processing.
                    print(msg.value())
                except Exception as e:
                    # Couldn't process it now. RELEASE so it can be retried
                    # (use REJECT for a record you know is bad).
                    sys.stderr.write('%% Processing failed: %s\n' % e)
                    sc.acknowledge(msg, AcknowledgeType.RELEASE)
                    continue

                # Processed cleanly: ACCEPT so the broker never redelivers it.
                sc.acknowledge(msg, AcknowledgeType.ACCEPT)

            if not batch:
                continue

            # Block until the broker replies (or 10s elapse), then inspect each
            # partition's outcome.
            results = sc.commit_sync(timeout=10.0)
            for topic_partition, err in results.items():
                if err is not None:
                    sys.stderr.write(
                        '%% Commit failed for %s [%d]: %s\n' % (topic_partition.topic, topic_partition.partition, err)
                    )
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # commit_sync() above already flushed each batch, so close() has
        # nothing pending to lose here.
        sc.close()
