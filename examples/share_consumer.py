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

import os
import sys
import time

#
# Example KIP-932 ShareConsumer.
#
# A share consumer reads from one or more topics like a queue: many consumers
# in the same share group can read the same partition, and each record is
# acknowledged implicitly.
#
from confluent_kafka import ShareConsumer


def print_usage_and_exit(program_name):
    sys.stderr.write(
        'Usage: %s <bootstrap-brokers|config-file> <group> <topic1> <topic2> ..\n'
        % program_name)
    sys.exit(1)


def read_properties(path):
    """Read a key=value properties file (same format used by librdkafka
    -X file= and Java --command-config)."""
    cfg = {}
    with open(path) as fp:
        for line in fp:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
            cfg[k.strip()] = v.strip()
    return cfg


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print_usage_and_exit(sys.argv[0])

    first = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]

    # ShareConsumer configuration.
    # Match Java ShareConsumerPerformance defaults so all three perf tools
    # measure share consumers under the same client settings.
    # See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'group.id': group,
        'client.id': 'py-share-consumer',
        'fetch.max.bytes': 1048576,              # 1 MiB
        'socket.receive.buffer.bytes': 2097152,  # 2 MiB
    }
    if os.path.isfile(first):
        conf.update(read_properties(first))
    else:
        conf['bootstrap.servers'] = first

    sc = ShareConsumer(conf)
    sc.subscribe(topics)

    msg_count = 0
    byte_count = 0
    start = time.monotonic()
    last_report = start
    last_msg_count = 0
    try:
        while True:
            messages = sc.poll(timeout=0.1)  # returns a list (possibly empty)
            for msg in messages:
                if msg.error():
                    continue
                msg_count += 1
                byte_count += len(msg.key() or b'') + len(msg.value() or b'')
                # Implicit ack: the next poll() acknowledges this message.
            now = time.monotonic()
            if now - last_report >= 5.0:
                elapsed = now - start
                rate = msg_count / elapsed if elapsed else 0
                sys.stderr.write('%% %d msgs (%d bytes) | %.0f msg/s cumulative\n'
                                 % (msg_count, byte_count, rate))
                last_report = now
                last_msg_count = msg_count
    except KeyboardInterrupt:
        elapsed = time.monotonic() - start
        avg = msg_count / elapsed if elapsed else 0
        sys.stderr.write('%% total: %d msgs (%d bytes) in %.1fs | avg %.0f msg/s\n'
                         % (msg_count, byte_count, elapsed, avg))
        sys.stderr.write('%% Aborted by user\n')
    finally:
        sc.close()
