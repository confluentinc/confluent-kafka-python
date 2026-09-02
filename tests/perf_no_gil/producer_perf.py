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
Producer free-threading perf runs. See README.md for the full run matrix.

This script implements the three producer *topologies* (single / shared /
per-thread); which CPython build (3.14 vs 3.14t) you're comparing is just a
matter of which interpreter you invoke it with. Each invocation creates a
fresh, randomly-named scratch topic with --partitions partitions and
produces into it using key-hash partitioning, so messages spread evenly
across partitions regardless of topology. Each mode produces for a fixed
wall-clock --duration rather than a fixed message count, then reports how
many messages it managed to get fully delivered in that time.

Examples
--------
Run 1 (single producer, 3.14):
    python3.14  producer_perf.py -b localhost:9092 --mode single -d 10
Run 2 (single producer, 3.14t):
    python3.14t producer_perf.py -b localhost:9092 --mode single -d 10
Run 3 (one producer shared across threads, 3.14):
    python3.14  producer_perf.py -b localhost:9092 --mode shared -d 10 -t 8
Run 4 (one producer shared across threads, 3.14t):
    python3.14t producer_perf.py -b localhost:9092 --mode shared -d 10 -t 8
Run 5 (one producer per thread, 3.14t):
    python3.14t producer_perf.py -b localhost:9092 --mode per-thread -d 10 -t 8
"""

import argparse
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import (  # noqa: E402
    DeliveryTracker,
    add_common_args,
    build_conf,
    create_scratch_topic,
    make_payload,
    report_result,
    warn_if_unflushed,
)

from confluent_kafka import Producer  # noqa: E402


def produce_until(
    producer: Producer, topic: str, deadline: float, message_size: int, tracker: DeliveryTracker, key_prefix: str = ''
) -> int:
    """Produce as fast as possible until `deadline` (a time.monotonic() value), returning how many
    produce() calls succeeded.

    key_prefix distinguishes this caller's keys from any other concurrent caller's (e.g. another
    thread sharing the same producer). Without it, threads whose local counters are in lockstep --
    the common case, since they all start at 0 and run at similar speed -- would keep producing the
    *same* key at the *same* time, i.e. hammering one partition's internal queue lock at a time
    instead of spreading load across partitions.

    The payload is created *here*, inside the function each thread runs, rather than once by the
    caller and passed in -- profiling with samply showed that sharing one payload object across
    threads (e.g. built once and handed to every worker) makes CPython 3.14's free-threaded
    biased reference counting take its slow atomic path (_Py_DecRefShared) on every produce()
    call, since only the object's *creating* thread gets the fast thread-local path. Each thread
    building its own payload eliminated that entirely and measurably increased throughput.

    `topic` gets the same treatment: it's the caller's string object, shared by every thread the
    same way the old payload was. `str(topic)`/`topic[:]` would both just return the identical
    object here (CPython short-circuits those for an exact str), so round-tripping through bytes
    is used to force a genuinely distinct, thread-owned string with the same content. `deadline`
    gets its own thread-owned copy too (`+ 0.0` always allocates, unlike `float(x)`, which can
    short-circuit to `x`): it's compared against on every loop iteration, so it's touched just as
    often as payload/topic were.

    `tracker.on_delivery` is bound once here rather than re-accessed every iteration: each access
    allocates a fresh bound-method object, which INCREFs both `tracker` (thread-owned, fine) and
    the underlying `DeliveryTracker.on_delivery` function object -- a single object shared by
    every instance and thread, defined once at module-import time on the main thread. Binding
    once means that shared function object's refcount is touched once per thread instead of once
    per message.
    """
    topic = topic.encode().decode()
    payload = make_payload(message_size)
    deadline = deadline + 0.0
    on_delivery = tracker.on_delivery
    count = 0
    while time.monotonic() < deadline:
        key = f'{key_prefix}{count}'.encode()
        try:
            # Positional args (topic, value, key, partition, callback) skip produce()'s
            # keyword-argument parsing entirely -- profiling showed that parsing (hashing and
            # dict-looking-up "key"/"value"/"on_delivery" on every call) dominates produce()'s
            # own cost, well above librdkafka's actual message construction. -1 here is
            # RD_KAFKA_PARTITION_UA, the same "let the partitioner decide" default partition
            # already gets when omitted.
            producer.produce(topic, payload, key, -1, on_delivery)
        except BufferError:
            producer.poll(0.1)
            continue
        count += 1
        producer.poll(0)
    return count


def run_single(conf: dict, topic: str, args: argparse.Namespace) -> None:
    producer = Producer(conf)
    tracker = DeliveryTracker(num_threads=1)
    tracker.bind(0)

    start = time.monotonic()
    count = produce_until(producer, topic, start + args.duration, args.message_size, tracker)
    remaining = producer.flush(args.flush_timeout)
    elapsed = time.monotonic() - start

    warn_if_unflushed(remaining)
    report_result(
        'producer', 'single', count, elapsed, errors=tracker.errors, drain_seconds=max(0.0, elapsed - args.duration)
    )


def run_shared(conf: dict, topic: str, args: argparse.Namespace) -> None:
    producer = Producer(conf)
    tracker = DeliveryTracker(num_threads=args.num_threads)
    counts = [0] * args.num_threads

    def worker(idx: int, deadline: float) -> None:
        tracker.bind(idx)
        counts[idx] = produce_until(producer, topic, deadline, args.message_size, tracker, key_prefix=f'{idx}-')

    start = time.monotonic()
    deadline = start + args.duration
    threads = [threading.Thread(target=worker, args=(i, deadline)) for i in range(args.num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    remaining = producer.flush(args.flush_timeout)
    elapsed = time.monotonic() - start

    warn_if_unflushed(remaining)
    report_result(
        'producer',
        'shared',
        sum(counts),
        elapsed,
        num_threads=args.num_threads,
        errors=tracker.errors,
        drain_seconds=max(0.0, elapsed - args.duration),
    )


def run_per_thread(conf: dict, topic: str, args: argparse.Namespace) -> None:
    counts = [0] * args.num_threads
    remaining_counts = [0] * args.num_threads
    errors_by_thread = [0] * args.num_threads

    def worker(idx: int, deadline: float) -> None:
        # Producer and DeliveryTracker are built *here*, on the thread that uses them, for the
        # same reason produce_until builds its own payload/topic: an object built by the main
        # thread and handed to a worker is "owned" by the main thread under CPython 3.14's
        # free-threaded biased reference counting, so every refcount touch from the worker (e.g.
        # the bound-method allocation for tracker.on_delivery on every produce() call) takes the
        # slow shared/atomic path instead of the fast thread-local one.
        producer = Producer(conf)
        tracker = DeliveryTracker(num_threads=1)
        tracker.bind(0)
        counts[idx] = produce_until(producer, topic, deadline, args.message_size, tracker, key_prefix=f'{idx}-')
        remaining_counts[idx] = producer.flush(args.flush_timeout)
        errors_by_thread[idx] = tracker.errors

    start = time.monotonic()
    deadline = start + args.duration
    threads = [threading.Thread(target=worker, args=(i, deadline)) for i in range(args.num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    warn_if_unflushed(sum(remaining_counts))
    errors = sum(errors_by_thread)
    report_result(
        'producer',
        'per-thread',
        sum(counts),
        elapsed,
        num_threads=args.num_threads,
        errors=errors,
        drain_seconds=max(0.0, elapsed - args.duration),
    )


MODES = {
    'single': run_single,
    'shared': run_shared,
    'per-thread': run_per_thread,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Producer free-threading perf runs')
    add_common_args(parser)
    parser.add_argument('--mode', choices=sorted(MODES), default='single')
    parser.add_argument('--duration', '-d', type=float, default=10.0, help='How long to produce for, in seconds')
    parser.add_argument('--message-size', type=int, default=128, help='Payload size in bytes')
    parser.add_argument('--num-threads', '-t', type=int, default=4, help='Thread count for shared/per-thread modes')
    parser.add_argument('--partitions', '-p', type=int, default=3, help='Partition count for the scratch topic')
    parser.add_argument('--replication-factor', type=int, default=1)
    parser.add_argument('--flush-timeout', type=float, default=120.0)
    parser.add_argument('--admin-timeout', type=float, default=30.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conf = build_conf(args, **{'linger.ms': 5, 'partitioner': 'consistent_random'})
    topic = create_scratch_topic(
        conf, 'perf-no-gil-producer', args.partitions, args.replication_factor, args.admin_timeout
    )
    print(f'Created topic {topic!r} with {args.partitions} partitions.')
    MODES[args.mode](conf, topic, args)


if __name__ == '__main__':
    main()
