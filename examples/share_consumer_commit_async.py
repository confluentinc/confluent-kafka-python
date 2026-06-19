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
# Example KIP-932 ShareConsumer using explicit acknowledgement and
# asynchronous commits.
#
# As in the commit_sync example, in explicit mode every record returned by
# poll() must be acknowledged before the next poll(), as one of:
#   ACCEPT  - processed successfully; the broker never redelivers it.
#   RELEASE - return it to the share group so it can be retried, possibly by
#             another consumer.
#   REJECT  - give up on it; the broker archives it (and, once available, it
#             can be routed to a dead-letter queue).
#
# commit_async() flushes the buffered acks without blocking. The broker's
# response is delivered later — on a subsequent poll() / commit_sync() /
# close() — through the acknowledgement-commit callback registered below.
#
from confluent_kafka import AcknowledgeType, KafkaException, ShareConsumer


def acknowledgement_commit_callback(offsets, exception):
    # commit_async() surfaces broker-side outcomes only through this callback.
    # offsets is a {TopicPartition: set(offsets)} of the acknowledged records;
    # exception is a KafkaException on failure or None on success.
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
                messages = sc.poll(timeout=1.0)  # returns a list (possibly empty)
            except KafkaException as e:
                # Poll-level error. Check err.fatal(): re-raise fatal errors,
                # otherwise treat the error as retriable and keep polling
                # (regardless of err.retriable()).
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue

            for msg in messages:
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

            # Explicit mode requires every record from the batch above to be
            # acknowledged before the next poll(). commit_async() flushes those
            # acks without blocking; outcomes arrive via the callback.
            sc.commit_async()
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # close() leaves the share group and frees the handle, but does NOT
        # flush pending acks. Async acks still in flight may be redelivered;
        # call commit_sync() before close() if you need them to persist.
        sc.close()
