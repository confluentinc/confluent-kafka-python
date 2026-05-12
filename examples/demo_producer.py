#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Producer for the KIP-932 share-consumer demo.

Recreates the 'orders' topic, bumps the group's share.record.lock.duration.ms
to 30s, then streams tasks at 400ms intervals. Every 10th record is a poison
(REJECT'd by the consumer); every 10th offset is flaky (RELEASE'd by the
consumer).

Pair with demo_share_consumer.py (or its commit_sync/commit_async variants).
"""

import sys
import time

from confluent_kafka import Producer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)

TOPIC = 'orders'
GROUP = 'orders-share-consumers'
BOOTSTRAP = 'localhost:9092'
LOCK_DURATION_MS = '30000'
NUM_PARTITIONS = 3
PRODUCE_INTERVAL_S = 0.4



def setup_topic_and_group():
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    p = Producer({'bootstrap.servers': BOOTSTRAP})

    # Recreate the topic so each run starts from offset 0 on every partition.
    for fut in admin.delete_topics([TOPIC]).values():
        try:
            fut.result(timeout=10)
        except Exception:
            pass  # topic didn't exist
    _wait_until(lambda: TOPIC not in p.list_topics(timeout=2).topics, 10)

    admin.create_topics(
        [NewTopic(TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)]
    )[TOPIC].result(timeout=10)
    _wait_until(
        lambda: len(p.list_topics(TOPIC, timeout=2).topics[TOPIC].partitions) >= NUM_PARTITIONS,
        10,
    )

    cfg = ConfigResource(ConfigResource.Type.GROUP, GROUP)
    cfg.add_incremental_config(ConfigEntry(
        'share.record.lock.duration.ms', LOCK_DURATION_MS,
        incremental_operation=AlterConfigOpType.SET))
    admin.incremental_alter_configs([cfg])[cfg].result(timeout=10)


def _wait_until(predicate, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise TimeoutError(f'condition not met within {timeout_s}s')


# 10 sequential messages, you get 8 accepts, 1 release, 1 reject
def message_name(i):
    if i % 10 == 7:
        return f'msg-reject-{i}'
    if i % 10 == 2:
        return f'msg-release-{i}'
    return f'msg-accept-{i}'


def produce():
    # Initialize producer with delivery callback to track when messages are confirmed.
    p = Producer({'bootstrap.servers': BOOTSTRAP})
    confirmed = 0
    def on_delivery(err, msg):
        nonlocal confirmed
        if err is not None:
            print(f'[producer] !! delivery failed: {err} value={msg.value()!r}', flush=True)
            return
        confirmed += 1
        print(f'[producer] -> {msg.value().decode():<16s} '
              f'partition={msg.partition()} offset={msg.offset():<5d}', flush=True)

    enqueued = 0
    try:
        while True:
            p.produce(TOPIC, value=message_name(enqueued).encode(), callback=on_delivery)
            p.poll(0)
            enqueued += 1
            time.sleep(PRODUCE_INTERVAL_S)
    except KeyboardInterrupt:
        print('\n[producer] flushing buffered records...', flush=True)
        p.flush(10)
        print(f'[producer] done. enqueued={enqueued} confirmed={confirmed}', flush=True)


def main():
    setup_topic_and_group()
    print('=' * 60)
    print(f'[producer] topic={TOPIC}  group={GROUP}  ready')
    print('[producer] start consumers in other terminals, e.g.:')
    print('    python examples/demo_share_consumer.py A')
    print('    python examples/demo_share_consumer.py B')
    print('[producer] press Enter here to start streaming.')
    print('=' * 60)
    try:
        input()
    except (EOFError, KeyboardInterrupt):
        sys.exit(0)
    produce()


if __name__ == '__main__':
    main()
