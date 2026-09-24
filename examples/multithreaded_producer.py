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

# Example demonstrating one of the recommended multithreaded Producer pattern (One producer per thread).

import sys
import threading

from confluent_kafka import Producer

MESSAGES_PER_THREAD = 10


def produce_messages(broker, topic):
    thread_id = threading.current_thread().name
    conf = {'bootstrap.servers': broker}
    producer = Producer(conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write(f'%% {thread_id}: message failed delivery: {err}\n')
        else:
            sys.stderr.write(
                f'%% {thread_id}: message delivered to {msg.topic()} ' f'[{msg.partition()}] @ {msg.offset()}\n'
            )

    try:
        for i in range(MESSAGES_PER_THREAD):
            value = f'Message {i} from {thread_id}'
            try:
                producer.produce(topic, key=thread_id, value=value, callback=delivery_callback)
            except BufferError:
                sys.stderr.write(f'%% {thread_id}: local queue full, waiting for space\n')
                producer.poll(1)
                producer.produce(topic, key=thread_id, value=value, callback=delivery_callback)

            # Serve delivery callbacks for messages produced so far.
            producer.poll(0)
    finally:
        producer.close()


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(f'Usage: {sys.argv[0]} <bootstrap-brokers> <topic> [num-threads]\n')
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    num_threads = int(sys.argv[3]) if len(sys.argv) > 3 else 3

    threads = [threading.Thread(target=produce_messages, args=(broker, topic)) for _ in range(num_threads)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f'%% All {num_threads} threads finished producing {MESSAGES_PER_THREAD} messages each')


if __name__ == '__main__':
    main()
