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
from confluent_kafka import ConcurrentModificationException, IllegalStateException, KafkaException, ShareConsumer


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print_usage_and_exit(sys.argv[0])

    broker = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]

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

                for msg in messages:
                    if msg.error():
                        # The records with msg.error() field set will be acknowledged
                        # internally with RELEASE for temporary errors and REJECT for
                        # permanent errors. Check KIP for more details.
                        sys.stderr.write('%% Error: %s\n' % msg.error())
                        continue
                    print(
                        '%% %s [%d] at offset %d with key %s:'
                        % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                    )
                    print(msg.value())
                    # No ack needed — the next poll() accepts this record. If we
                    # crash first, another consumer picks it up later.
            except KafkaException as e:
                # The consumer should stop consuming after fatal error.
                if e.args[0].fatal():
                    raise
                sys.stderr.write('%% Consumer error: %s\n' % e)
                continue
            except (IllegalStateException, ConcurrentModificationException) as e:
                # These signal misuse (polling when not subscribed/closed, or
                # from more than one thread), not a transient hiccup — no point
                # looping, so bail out.
                sys.stderr.write('%% Fatal: %s\n' % e)
                raise
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        sc.close()
