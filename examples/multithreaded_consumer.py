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

# Example demonstrating the recommended multithreaded Consumer pattern (One consumer per thread).

import sys
import threading

from confluent_kafka import Consumer, KafkaException

running = threading.Event()
running.set()


def consume_messages(broker, group, topic):
    thread_id = threading.current_thread().name
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        'auto.offset.reset': 'earliest',
        'enable.auto.offset.store': True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while running.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            sys.stderr.write(
                f'%% {thread_id}: {msg.topic()} [{msg.partition()}] ' f'at offset {msg.offset()} with key {msg.key()}\n'
            )
            print(msg.value())
    finally:
        consumer.close()


def main():
    if len(sys.argv) < 4:
        sys.stderr.write(f'Usage: {sys.argv[0]} <bootstrap-brokers> <group> <topic> [num-threads]\n')
        sys.exit(1)

    broker = sys.argv[1]
    group = sys.argv[2]
    topic = sys.argv[3]
    num_threads = int(sys.argv[4]) if len(sys.argv) > 4 else 3

    threads = [threading.Thread(target=consume_messages, args=(broker, group, topic)) for _ in range(num_threads)]

    for t in threads:
        t.start()

    try:
        # Join with a timeout so this loop keeps checking for
        # KeyboardInterrupt instead of blocking on a single thread forever.
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1.0)
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user, shutting down consumers\n')
        running.clear()
        for t in threads:
            t.join()


if __name__ == '__main__':
    main()
