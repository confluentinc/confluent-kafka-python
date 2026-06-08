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

"""KIP-932 share-consumer demo -- producer half.

Run this in one terminal and share_consumer_ack_callback.py in another, pointed
at the same broker and topic. This side creates the topic and produces a steady
stream, printing the topic[partition]@offset the broker assigned to each record
so you can line it up against what the consumer's ack-commit callback reports.

    python examples/share_consumer_ack_producer.py localhost:9092 share-demo
"""

import argparse
import sys
import time

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topic(bootstrap, topic, partitions):
    admin = AdminClient({'bootstrap.servers': bootstrap})
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic]
    try:
        fut.result()
        print(f"[producer] created topic '{topic}' with {partitions} partitions")
    except KafkaException as e:
        if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            print(f"[producer] topic '{topic}' already exists -- reusing it")
        else:
            raise
    # Metadata needs a beat to settle before the first produce, otherwise the
    # initial records bounce around waiting for partition leaders.
    time.sleep(1)


def on_delivery(err, msg):
    if err is not None:
        sys.stderr.write(f"[producer] delivery FAILED: {err}\n")
        return
    val = msg.value().decode() if msg.value() else None
    print(f"[producer] partition={msg.partition()}, offset={msg.offset()}, msg={val}")


def main():
    ap = argparse.ArgumentParser(description='KIP-932 share-consumer demo producer')
    ap.add_argument('bootstrap', help='bootstrap servers, e.g. localhost:9092')
    ap.add_argument('topic', help='topic to produce to')
    ap.add_argument('--partitions', type=int, default=3, help='partitions to create the topic with (default 3)')
    ap.add_argument('--rate', type=float, default=2.0, help='messages per second (default 2)')
    ap.add_argument('--count', type=int, default=0, help='stop after N messages (0 = run until Ctrl-C)')
    args = ap.parse_args()

    ensure_topic(args.bootstrap, args.topic, args.partitions)

    producer = Producer({'bootstrap.servers': args.bootstrap})
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    i = 0
    try:
        while args.count == 0 or i < args.count:
            producer.produce(args.topic, key=f'k{i}'.encode(), value=f'message-{i}'.encode(),
                             on_delivery=on_delivery)
            # Serve delivery callbacks as we go so offsets print in near-real time.
            producer.poll(0)
            i += 1
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print('\n[producer] stopping')
    finally:
        producer.flush(10)


if __name__ == '__main__':
    main()
