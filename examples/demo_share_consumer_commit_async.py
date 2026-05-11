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

"""KIP-932 share consumer demo, commit_async variant.

Same processing as demo_share_consumer.py, but commits asynchronously after
each batch (returns immediately; results land via the
share_acknowledgement_commit_cb callback if configured). On shutdown we
switch to commit_sync so we know pending acks landed before close.

Usage: python examples/demo_share_consumer_commit_async.py <NAME>
"""

import sys
import time

from confluent_kafka import AcknowledgeType, ShareConsumer

TOPIC = 'orders'
GROUP = 'orders-share-consumers'
BOOTSTRAP = 'localhost:9092'
WORK_DELAY_S = 0.15


def handle(name, sc, m, counters):
    if m.error():
        return

    value = m.value().decode()
    time.sleep(WORK_DELAY_S)
    where = (f'partition={m.partition()} offset={m.offset():<5d} '
             f'dc={m.delivery_count()}')

    if value.startswith('poison'):
        sc.acknowledge(m, AcknowledgeType.REJECT)
        counters['rejected'] += 1
        print(f'[{name}] {value:<12s}  {where}  REJECT  (archived)', flush=True)
    elif value.startswith('flaky'):
        sc.acknowledge(m, AcknowledgeType.RELEASE)
        counters['released'] += 1
        print(f'[{name}] {value:<12s}  {where}  RELEASE', flush=True)
    else:
        sc.acknowledge(m, AcknowledgeType.ACCEPT)
        counters['accepted'] += 1
        print(f'[{name}] {value:<12s}  {where}  ACCEPT', flush=True)


def main():
    name = sys.argv[1] if len(sys.argv) > 1 else 'X'

    sc = ShareConsumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': GROUP,
        'share.acknowledgement.mode': 'explicit',
    })
    sc.subscribe([TOPIC])
    counters = {'accepted': 0, 'rejected': 0, 'released': 0}

    print(f'[{name}] subscribed; commit_async per batch (Ctrl+C to stop)', flush=True)
    try:
        while True:
            for m in sc.poll(timeout=1.0):
                handle(name, sc, m, counters)
            sc.commit_async()
    except KeyboardInterrupt:
        print(f'\n[{name}] final commit_sync to confirm durability...', flush=True)
        try:
            result = sc.commit_sync(timeout=5.0)
            print(f'[{name}] commit_sync result: {dict(result)}', flush=True)
        except Exception as e:
            print(f'[{name}] commit_sync error: {e}', flush=True)
        print(f'[{name}] tally: {counters}', flush=True)
    finally:
        sc.close()


if __name__ == '__main__':
    main()
