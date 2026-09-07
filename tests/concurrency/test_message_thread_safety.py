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

"""
Concurrency tests for a single confluent_kafka.Message shared across threads.
"""

import itertools
import pickle
import threading
import time

from confluent_kafka import Consumer, KafkaError, Message, Producer
from tests.concurrency._subprocess_isolation import subprocess_isolated

_RACE_DURATION_S = 2.0
_THREADS_PER_ROLE = 2

_NUM_MESSAGES = 200
_NUM_READERS = 8
_HEADERS = [('h-1', b'v-1'), ('h-2', b'v-2'), ('h-none', None)]


###############################################################################
# Helpers
###############################################################################


def _run_for(workers, duration_s):
    """
    Run each callable in `workers` in a tight loop on its own thread for
    `duration_s`, all released together through a Barrier. Stops early if any
    worker raises. Returns the list of exceptions raised.
    """
    stop = threading.Event()
    errors = []
    barrier = threading.Barrier(len(workers))

    def run(work):
        try:
            barrier.wait()
            while not stop.is_set():
                work()
        except Exception as e:  # noqa: BLE001 - any failure is a test failure
            errors.append(e)
            stop.set()

    threads = [threading.Thread(target=run, args=(w,), daemon=True) for w in workers]
    for t in threads:
        t.start()
    time.sleep(duration_s)
    stop.set()
    for t in threads:
        t.join(timeout=30)
    assert all(not t.is_alive() for t in threads), "a worker thread did not finish"
    return errors


def _race_readers(msg, read, num_readers):
    """
    Release `num_readers` threads simultaneously into `read(msg)` and return
    their results in thread order. Any exception fails the test.
    """
    barrier = threading.Barrier(num_readers)
    results = [None] * num_readers
    errors = []

    def run(i):
        try:
            barrier.wait()
            results[i] = read(msg)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(num_readers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert all(not t.is_alive() for t in threads), "a reader thread did not finish"
    assert not errors, f"reader threads raised: {errors}"
    return results


def _consume_messages_with_headers(count):
    """
    Produce `count` messages carrying _HEADERS to librdkafka's in-process mock
    cluster and consume them back. The returned Messages still hold their
    headers in undecoded C form, exactly as a real consumer would hand them
    to an application.
    """
    producer = Producer({'test.mock.num.brokers': 1})
    brokers = producer.list_topics(timeout=10).brokers.values()
    bootstrap = ','.join(f'{b.host}:{b.port}' for b in brokers)

    for i in range(count):
        producer.produce('topic', value=b'm-%d' % i, headers=_HEADERS)
    assert producer.flush(10) == 0, "not every message was delivered to the mock cluster"

    consumer = Consumer({'bootstrap.servers': bootstrap, 'group.id': 'g', 'auto.offset.reset': 'earliest'})
    consumer.subscribe(['topic'])
    messages = []
    deadline = time.time() + 30
    while len(messages) < count and time.time() < deadline:
        for msg in consumer.consume(num_messages=count - len(messages), timeout=1.0):
            assert msg.error() is None, msg.error()
            messages.append(msg)
    consumer.close()

    assert len(messages) == count, f"consumed {len(messages)} of {count} messages"
    return messages


###############################################################################
# Getter vs setter on the same field
###############################################################################

# (field, make(i) -> a fresh value to set, valid(v) -> is v a value we set)
_FIELDS = [
    ('value', lambda i: b'v-%d' % i, lambda v: isinstance(v, bytes) and v.startswith(b'v-')),
    ('key', lambda i: b'k-%d' % i, lambda v: isinstance(v, bytes) and v.startswith(b'k-')),
    ('topic', lambda i: 't-%d' % i, lambda v: isinstance(v, str) and v.startswith('t-')),
    ('headers', lambda i: [('h', b'%d' % i)], lambda v: isinstance(v, list) and len(v) == 1 and v[0][0] == 'h'),
    (
        'error',
        lambda i: KafkaError(KafkaError._PARTITION_EOF, 'e-%d' % i),
        lambda v: isinstance(v, KafkaError) and v.str().startswith('e-'),
    ),
]


def _setter(msg, field, make):
    set_field = getattr(msg, 'set_' + field)
    counter = itertools.count(1)

    def work():
        # A fresh object every time, so each set_*() drops the last reference
        # to the previous value -- that is what a racing getter must survive.
        set_field(make(next(counter)))

    return work


def _getter(msg, field, valid):
    get_field = getattr(msg, field)

    def work():
        v = get_field()
        assert valid(v), f"{field}() returned {v!r}"

    return work


@subprocess_isolated
def test_concurrent_get_and_set_same_field():
    """For every mutable field, setters and getters hammering one shared
    Message must never crash and getters must only ever see values that were
    actually set."""
    msg = Message(
        topic='t-0',
        partition=0,
        offset=0,
        key=b'k-0',
        value=b'v-0',
        headers=[('h', b'0')],
        error=KafkaError(KafkaError._PARTITION_EOF, 'e-0'),
    )

    workers = []
    for field, make, valid in _FIELDS:
        workers += [_setter(msg, field, make) for _ in range(_THREADS_PER_ROLE)]
        workers += [_getter(msg, field, valid) for _ in range(_THREADS_PER_ROLE)]

    errors = _run_for(workers, _RACE_DURATION_S)
    assert not errors, f"worker threads raised: {errors}"


###############################################################################
# Lazy header decode on a consumed Message
###############################################################################


@subprocess_isolated
def test_concurrent_headers_decode_on_consumed_message():
    """Simultaneous headers() calls on one consumed Message must decode the C
    headers exactly once and all return the same list."""
    for msg in _consume_messages_with_headers(_NUM_MESSAGES):
        results = _race_readers(msg, lambda m: m.headers(), _NUM_READERS)
        assert results == [_HEADERS] * _NUM_READERS, results


@subprocess_isolated
def test_concurrent_pickle_of_consumed_message():
    """Same as above through __reduce__: simultaneous pickling of one consumed
    Message decodes the C headers exactly once."""

    def pickle_roundtrip_headers(m):
        return pickle.loads(pickle.dumps(m)).headers()

    for msg in _consume_messages_with_headers(_NUM_MESSAGES):
        results = _race_readers(msg, pickle_roundtrip_headers, _NUM_READERS)
        assert results == [_HEADERS] * _NUM_READERS, results
