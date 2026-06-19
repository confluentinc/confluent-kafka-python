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
# Example KIP-932 ShareConsumer in the default (implicit) ack mode.
#
# Consumers in a share group share partitions like a queue. In implicit mode
# each record is acknowledged for you on the next poll().
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
                messages = sc.poll(timeout=1.0)  # a list, possibly empty
            except KafkaException as e:
                # Re-raise fatal errors; otherwise log and keep going.
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue
            for msg in messages:
                if msg.error():
                    # A bad record. In implicit mode you can't ack it by hand
                    # (acknowledge() is rejected); the library automatically
                    # retries it (temporary errors) or discards it (permanent
                    # errors) on the next poll — it is never accepted. Just log it.
                    sys.stderr.write('%% Error: %s\n' % msg.error())
                    continue
                sys.stderr.write(
                    '%% %s [%d] at offset %d with key %s:\n'
                    % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                )
                print(msg.value())
                # No ack needed — the next poll() accepts this record. If we
                # crash first, another consumer picks it up later.
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        sc.close()
