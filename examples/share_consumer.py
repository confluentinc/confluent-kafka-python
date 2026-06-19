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
# Example KIP-932 ShareConsumer.
#
# A share consumer reads from one or more topics like a queue: many consumers
# in the same share group can read the same partition, and each record is
# acknowledged implicitly.
#
from confluent_kafka import KafkaException, ShareConsumer


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
    }

    sc = ShareConsumer(conf)
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
                    # Message-level error. In implicit mode you can't ACCEPT,
                    # RELEASE or REJECT a record by hand (acknowledge() is only
                    # valid in explicit mode), so just log it; the record is
                    # auto-accepted on the next poll() and the loop keeps making
                    # progress. Use explicit mode (see
                    # share_consumer_commit_sync.py) to RELEASE or REJECT a bad
                    # record.
                    sys.stderr.write('%% Error: %s\n' % msg.error())
                    continue
                sys.stderr.write(
                    '%% %s [%d] at offset %d with key %s:\n'
                    % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                )
                print(msg.value())
                # Implicit ack: the next poll() acknowledges this message.
                # If we crash before the next poll, the broker will
                # redeliver this record to another consumer in the share
                # group after the acquisition lock expires.
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        sc.close()
