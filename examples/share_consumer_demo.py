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

"""
KIP-932 Share Consumer  --  Acknowledge & Commit demo
=====================================================

Walks through the share consumer ack / commit workflow against a live
Kafka 4.1+ broker with KIP-932 enabled (default: localhost:9092).

  1. Implicit mode + commit_sync
        the default: poll, commit, broker accepts the batch
  2. Explicit mode + ACCEPT + commit_sync
        per-record control; same outcome but app-driven
  3. Explicit mode + RELEASE causes redelivery
        the queue effect: app says "try again later" and the record returns
  4. Explicit mode + REJECT permanently archives
        poison-pill handling: tell the broker "never deliver this again"
  5. Two consumers share one topic
        queue-style distribution: each record goes to exactly one consumer
  6. commit_async returns immediately
        non-blocking commit for high-throughput pipelines

Usage:
    python share_consumer_demo.py                  # run all scenarios
    python share_consumer_demo.py 3                # only scenario 3
    python share_consumer_demo.py 1 2 6            # cherry-pick
    python share_consumer_demo.py --bootstrap host:port [scenarios...]
"""

import argparse
import sys
import time
import uuid

from confluent_kafka import (
    AcknowledgeType,
    KafkaException,
    Producer,
    ShareConsumer,
)


BOOTSTRAP = 'localhost:9092'
POLL_TIMEOUT = 10.0


# ---------------------------------------------------------------------------
# Presentation helpers -- keep the demo output legible from across the room.
# ---------------------------------------------------------------------------

WIDTH = 76


def banner(title):
    print()
    print('=' * WIDTH)
    print(' ' + title)
    print('=' * WIDTH)


def section(title):
    print()
    print('-- ' + title + ' ' + '-' * max(0, WIDTH - len(title) - 4))


def step(text):
    print('  >>> ' + text)


def note(text):
    print('      ' + text)


def show_msg(msg, tag):
    val = msg.value().decode('utf-8', errors='replace') if msg.value() else ''
    print('      [%-8s] partition=%d offset=%-4d value=%r' % (tag, msg.partition(), msg.offset(), val))


def show_commit_result(result):
    if not result:
        print('      commit_sync -> {}    (no acks were pending)')
        return
    print('      commit_sync -> dict of %d partition result(s):' % len(result))
    for tp, err in result.items():
        status = 'None       (success)' if err is None else 'KafkaError: ' + str(err)
        print('         %s[%d]  ->  %s' % (tp.topic, tp.partition, status))


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def _uid():
    return uuid.uuid4().hex[:8]


def make_topic(name):
    """Force auto-creation and wait for metadata so subscribe() doesn't race."""
    p = Producer({'bootstrap.servers': BOOTSTRAP})
    p.produce(name, value=b'__init__')
    p.flush(10)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        md = p.list_topics(name, timeout=2.0)
        if name in md.topics and md.topics[name].partitions:
            return name
        time.sleep(0.1)
    raise RuntimeError('topic %r did not appear in metadata' % name)


def produce(topic, n, prefix='msg'):
    p = Producer({'bootstrap.servers': BOOTSTRAP})
    for i in range(n):
        p.produce(topic, value=('%s-%d' % (prefix, i)).encode())
    p.flush(10)


def share_consumer(group, mode='implicit'):
    return ShareConsumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': group,
        'share.acknowledgement.mode': mode,
    })


def warmup(sc, seconds=6.0):
    """SPSO is initialised to `latest` once the share session is assigned,
    so anything produced before the session stabilises is skipped by the
    broker. Drive empty polls until the session is up."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        sc.poll(timeout=1.0)


def collect(sc, n, timeout, ack_type=None):
    """Poll until n records arrive or timeout elapses. In explicit mode
    callers MUST pass ack_type so each record is acked inline -- otherwise
    the next poll raises __STATE with 'not acknowledged'."""
    deadline = time.monotonic() + timeout
    out = []
    while len(out) < n and time.monotonic() < deadline:
        for m in sc.poll(timeout=2.0):
            if m.error() is None:
                out.append(m)
                if ack_type is not None:
                    sc.acknowledge(m, ack_type)
    return out


# ---------------------------------------------------------------------------
# Scenario 1 -- implicit mode + commit_sync
# ---------------------------------------------------------------------------


def scenario_1():
    banner('Scenario 1   Implicit mode + commit_sync')
    print('  The default mode. The app calls poll() and commit_sync();')
    print('  the broker auto-converts every polled record to ACCEPT.')

    topic = 'demo_s1_' + _uid()
    group = 'g_s1_' + _uid()
    make_topic(topic)

    step('ShareConsumer(group=%s, share.acknowledgement.mode=implicit)' % group)
    sc = share_consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        note('subscribe([%s])' % topic)
        warmup(sc)

        step('Producing 3 messages')
        produce(topic, 3)

        step('Polling for records')
        msgs = collect(sc, 3, timeout=POLL_TIMEOUT)
        for m in msgs:
            show_msg(m, 'POLLED')

        step('commit_sync(timeout=10.0)')
        result = sc.commit_sync(timeout=10.0)
        show_commit_result(result)

        note('Behind the scenes: librdkafka turned every record into ACCEPT')
        note('and the broker advanced the SPSO past offset %d.' % msgs[-1].offset())
    finally:
        sc.close()


# ---------------------------------------------------------------------------
# Scenario 2 -- explicit mode + ACCEPT
# ---------------------------------------------------------------------------


def scenario_2():
    banner('Scenario 2   Explicit mode + ACCEPT + commit_sync')
    print('  Explicit mode requires the app to ack every polled record before')
    print('  the next poll. ACCEPT is the "processed cleanly" outcome.')

    topic = 'demo_s2_' + _uid()
    group = 'g_s2_' + _uid()
    make_topic(topic)

    step('ShareConsumer(group=%s, share.acknowledgement.mode=explicit)' % group)
    sc = share_consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        warmup(sc)
        step('Producing 4 messages')
        produce(topic, 4)

        step('Polling, then ACCEPT each record')
        msgs = collect(sc, 4, timeout=POLL_TIMEOUT, ack_type=AcknowledgeType.ACCEPT)
        for m in msgs:
            show_msg(m, 'ACCEPT')

        step('commit_sync(timeout=10.0)')
        result = sc.commit_sync(timeout=10.0)
        show_commit_result(result)
    finally:
        sc.close()


# ---------------------------------------------------------------------------
# Scenario 3 -- RELEASE -> redelivery
# ---------------------------------------------------------------------------


def scenario_3():
    banner('Scenario 3   RELEASE causes redelivery (the queue effect)')
    print('  When the app cannot process a record right now (transient error,')
    print('  back-pressure, downstream timeout) it RELEASEs the record. The')
    print('  broker returns it to the share-partition and redelivers it later.')

    topic = 'demo_s3_' + _uid()
    group = 'g_s3_' + _uid()
    make_topic(topic)

    sc = share_consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        warmup(sc)
        step('Producing 1 message')
        produce(topic, 1, prefix='task')

        step('First poll -- RELEASE the record')
        first = collect(sc, 1, timeout=POLL_TIMEOUT)[0]
        show_msg(first, 'POLLED')
        coords = (first.topic(), first.partition(), first.offset())
        sc.acknowledge(first, AcknowledgeType.RELEASE)
        sc.commit_sync(timeout=5.0)
        note('record released back to share-partition; SPSO does NOT advance')

        step('Polling again -- expect the same record to return')
        deadline = time.monotonic() + 15.0
        redelivered = None
        while not redelivered and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None and (m.topic(), m.partition(), m.offset()) == coords:
                    redelivered = m
                    break
                if m.error() is None:
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
        if redelivered:
            show_msg(redelivered, 'REDELIVERED')
            note('Same offset, but the broker tracks delivery_count internally.')
            sc.acknowledge(redelivered, AcknowledgeType.ACCEPT)
            sc.commit_sync(timeout=5.0)
        else:
            note('record did not come back within 15s -- check broker config')
    finally:
        sc.close()


# ---------------------------------------------------------------------------
# Scenario 4 -- REJECT -> permanent archive
# ---------------------------------------------------------------------------


def scenario_4():
    banner('Scenario 4   REJECT permanently archives (poison-pill handling)')
    print('  When the app knows a record will never succeed (bad payload,')
    print('  schema mismatch, business-rule violation) it REJECTs the record.')
    print('  The broker archives it and never redelivers it.')

    topic = 'demo_s4_' + _uid()
    group = 'g_s4_' + _uid()
    make_topic(topic)

    sc1 = share_consumer(group, mode='explicit')
    try:
        sc1.subscribe([topic])
        warmup(sc1)
        step('Producing 1 poison message')
        produce(topic, 1, prefix='poison')

        step('Polling -- REJECT the record')
        msgs = collect(sc1, 1, timeout=POLL_TIMEOUT, ack_type=AcknowledgeType.REJECT)
        for m in msgs:
            show_msg(m, 'REJECT')
        sc1.commit_sync(timeout=5.0)
        note('Broker moved the record to the Archived state.')
    finally:
        sc1.close()

    step('Starting a fresh consumer in the same group')
    sc2 = share_consumer(group, mode='explicit')
    try:
        sc2.subscribe([topic])
        warmup(sc2, seconds=4.0)
        step('Polling for 5s -- expecting nothing (record was archived)')
        seen = collect(sc2, 1, timeout=5.0, ack_type=AcknowledgeType.ACCEPT)
        if seen:
            note('WARNING: REJECTed record was redelivered -- broker bug?')
            for m in seen:
                show_msg(m, 'UNEXPECTED')
        else:
            note('No records redelivered. REJECT worked.')
    finally:
        sc2.close()


# ---------------------------------------------------------------------------
# Scenario 5 -- two consumers split one topic
# ---------------------------------------------------------------------------


def scenario_5():
    banner('Scenario 5   Two consumers cooperatively drain one topic')
    print('  This is the headline KIP-932 capability: one partition served by')
    print('  multiple consumers, each record delivered to exactly one of them.')

    topic = 'demo_s5_' + _uid()
    group = 'g_s5_' + _uid()
    n = 20
    make_topic(topic)

    sc1 = share_consumer(group, mode='explicit')
    sc2 = share_consumer(group, mode='explicit')
    try:
        sc1.subscribe([topic])
        sc2.subscribe([topic])
        step('Two ShareConsumers, same group=%s, same topic' % group)
        warmup(sc1)
        warmup(sc2)

        step('Producing %d messages' % n)
        produce(topic, n, prefix='item')

        step('Both consumers poll concurrently, ACCEPT what they receive')
        seen1, seen2 = [], []
        deadline = time.monotonic() + 30.0
        while len(seen1) + len(seen2) < n and time.monotonic() < deadline:
            for sc, bucket in ((sc1, seen1), (sc2, seen2)):
                for m in sc.poll(timeout=1.0):
                    if m.error() is None:
                        bucket.append(m.value().decode())
                        sc.acknowledge(m, AcknowledgeType.ACCEPT)

        sc1.commit_sync(timeout=5.0)
        sc2.commit_sync(timeout=5.0)

        s1, s2 = set(seen1), set(seen2)
        print()
        note('Consumer A received %d records: %s' % (len(seen1), sorted(seen1)))
        note('Consumer B received %d records: %s' % (len(seen2), sorted(seen2)))
        print()
        note('Total unique records: %d (produced %d)' % (len(s1 | s2), n))
        note('Overlap (should be empty): %s' % (s1 & s2))
    finally:
        sc1.close()
        sc2.close()


# ---------------------------------------------------------------------------
# Scenario 6 -- commit_async is non-blocking
# ---------------------------------------------------------------------------


def scenario_6():
    banner('Scenario 6   commit_async returns immediately')
    print('  commit_async() initiates the commit and returns right away.')
    print('  For high-throughput pipelines this keeps the poll loop hot.')

    topic = 'demo_s6_' + _uid()
    group = 'g_s6_' + _uid()
    make_topic(topic)

    sc = share_consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        warmup(sc)
        step('Producing 5 messages')
        produce(topic, 5)
        msgs = collect(sc, 5, timeout=POLL_TIMEOUT)
        for m in msgs:
            show_msg(m, 'POLLED')

        step('commit_sync() -- measure how long it blocks')
        t0 = time.monotonic()
        sync_result = sc.commit_sync(timeout=10.0)
        sync_elapsed = time.monotonic() - t0
        note('commit_sync returned after %.3fs (%d partition result(s))' %
             (sync_elapsed, len(sync_result) if sync_result else 0))

        step('Produce + poll another batch, then call commit_async()')
        produce(topic, 5, prefix='asyncbatch')
        collect(sc, 5, timeout=POLL_TIMEOUT)

        t0 = time.monotonic()
        async_result = sc.commit_async()
        async_elapsed = time.monotonic() - t0
        note('commit_async returned after %.3fs, value=%r' % (async_elapsed, async_result))
    finally:
        sc.close()


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


SCENARIOS = {
    1: scenario_1,
    2: scenario_2,
    3: scenario_3,
    4: scenario_4,
    5: scenario_5,
    6: scenario_6,
}


def broker_reachable():
    try:
        p = Producer({'bootstrap.servers': BOOTSTRAP, 'socket.timeout.ms': 2000})
        return len(p.list_topics(timeout=2.0).brokers) > 0
    except Exception:
        return False


def main():
    global BOOTSTRAP
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap', default=BOOTSTRAP,
                        help='Kafka bootstrap servers (default: %s)' % BOOTSTRAP)
    parser.add_argument('scenarios', nargs='*', type=int,
                        help='Scenario numbers to run (default: all)')
    args = parser.parse_args()

    BOOTSTRAP = args.bootstrap

    print()
    print('=' * WIDTH)
    print('  confluent-kafka-python  --  KIP-932 Share Consumer demo')
    print('  broker: %s' % BOOTSTRAP)
    print('=' * WIDTH)

    if not broker_reachable():
        print()
        print('ERROR: cannot reach broker at %s' % BOOTSTRAP)
        print('       Start a Kafka 4.1+ cluster with share groups enabled,')
        print('       or pass --bootstrap host:port')
        sys.exit(2)

    to_run = args.scenarios or sorted(SCENARIOS.keys())
    for n in to_run:
        fn = SCENARIOS.get(n)
        if not fn:
            print('skip: unknown scenario %d' % n)
            continue
        try:
            fn()
        except KeyboardInterrupt:
            print()
            print('interrupted')
            sys.exit(130)
        except KafkaException as e:
            print()
            print('scenario %d failed: %s' % (n, e))

    print()
    print('=' * WIDTH)
    print('  demo complete')
    print('=' * WIDTH)
    print()


if __name__ == '__main__':
    main()
