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
Consumer free-threading perf runs. See README.md for the full run matrix.

The topic must already contain messages spread across the partitions you want to test with --
this script never produces anything itself. Each run
picks a fresh random group.id by default so it always re-reads from the beginning, regardless
of prior runs. Like producer_perf.py, each mode consumes for a fixed wall-clock --duration
rather than a fixed message count, then reports how many messages it actually got in that time.

Examples
--------
Run 1 (single consumer, 3.14):
    python3.14  consumer_perf.py -b localhost:9092 --topic perf-no-gil-consumer --mode single -d 10
Run 2 (single consumer, 3.14t):
    python3.14t consumer_perf.py -b localhost:9092 --topic perf-no-gil-consumer --mode single -d 10
Run 3 (one consumer per thread, sharing a group, 3.14):
    python3.14  consumer_perf.py -b localhost:9092 --topic perf-no-gil-consumer --mode per-thread -d 10 -t 6
Run 4 (same as Run 3, 3.14t):
    python3.14t consumer_perf.py -b localhost:9092 --topic perf-no-gil-consumer --mode per-thread -d 10 -t 6

Pass --consume-batch-size N (N > 1) to use consume() instead of poll() -- see its help text.
"""

import argparse
import os
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import add_common_args, build_conf, report_result  # noqa: E402

from confluent_kafka import Consumer, KafkaError  # noqa: E402


def wait_for_assignment(consumer: Consumer, topic: str, warmup_timeout: float) -> None:
    """Subscribe and drive the initial rebalance to completion, so the timed section below
    only measures steady-state polling. `topic` gets a thread-owned copy for the same reason
    producer_perf.py's produce_until does (see its docstring) -- it's the caller's string
    object, and touching it from a non-owning thread would take CPython 3.14 free-threading's
    slow shared-refcount path. Called once per thread, so the win here is small, but free and
    consistent with drain()'s treatment of poll_timeout below.
    """
    topic = topic.encode().decode()
    assigned = threading.Event()

    def on_assign(_consumer, _partitions):
        assigned.set()

    consumer.subscribe([topic], on_assign=on_assign)

    deadline = time.monotonic() + warmup_timeout
    while not assigned.is_set():
        if time.monotonic() > deadline:
            raise SystemExit(f'Timed out waiting for partition assignment on {topic!r}')
        consumer.poll(0.1)


def drain(consumer: Consumer, deadline: float, poll_timeout: float, consume_batch_size: int) -> int:
    """Poll/consume as fast as possible until `deadline` (a time.monotonic() value), returning
    how many messages were received. `deadline`/`poll_timeout` get thread-owned copies (`+ 0.0`
    always allocates, unlike float(x), which can short-circuit to return x itself): both are
    floats read off the shared `args` Namespace or computed once by the main thread, and
    poll_timeout in particular is passed into poll()/consume() on every single loop iteration,
    so touching the main thread's copy there would repeatedly hit CPython 3.14 free-threading's
    slow shared-refcount path -- the same class of cost producer_perf.py's investigation found
    and fixed for payload/topic/deadline.

    consume_batch_size <= 1 uses poll() (one C-level call per message); > 1 uses
    consume() (one C-level call per up-to-consume_batch_size messages) -- the sync-side
    equivalent of aio_consumer_perf.py's --consume-batch-size.
    """
    deadline = deadline + 0.0
    poll_timeout = poll_timeout + 0.0
    count = 0
    if consume_batch_size <= 1:
        while time.monotonic() < deadline:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise SystemExit(f'Consumer error: {msg.error()}')
            count += 1
    else:
        while time.monotonic() < deadline:
            msgs = consumer.consume(consume_batch_size, poll_timeout)
            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise SystemExit(f'Consumer error: {msg.error()}')
                count += 1
    return count


def warn_if_empty(count: int) -> None:
    if count == 0:
        print('WARNING: consumed 0 messages -- is the topic seeded?')


def run_single(conf: dict, args: argparse.Namespace) -> None:
    consumer = Consumer(conf)
    wait_for_assignment(consumer, args.topic, args.warmup_timeout)

    start = time.monotonic()
    count = drain(consumer, start + args.duration, args.poll_timeout, args.consume_batch_size)
    elapsed = time.monotonic() - start

    consumer.close()
    warn_if_empty(count)
    report_result('consumer', 'single', count, elapsed)


def run_per_thread(conf: dict, args: argparse.Namespace) -> None:
    counts = [0] * args.num_threads
    timing = {}

    def on_release() -> None:
        # Runs exactly once, on whichever thread is last to reach the barrier, before any
        # thread is released past it -- so every worker sees the same start/deadline, computed
        # only once all of them have finished the (variable-length) rebalance wait.
        now = time.monotonic()
        timing['start'] = now
        timing['deadline'] = now + args.duration

    barrier = threading.Barrier(args.num_threads, action=on_release)

    def worker(idx: int) -> None:
        # Consumer built here, on the thread that uses it, for the same reason
        # producer_perf.py's per-thread worker builds its own Producer rather than using one
        # built by the main thread's list comprehension: an object built by the main thread
        # and handed to a worker is "owned" by the main thread under CPython 3.14's
        # free-threaded biased reference counting, so every method call from the worker
        # (poll(), close(), ...) would take the slow shared/atomic refcount path instead of
        # the fast thread-local one.
        consumer = Consumer(conf)
        wait_for_assignment(consumer, args.topic, args.warmup_timeout)
        barrier.wait()
        counts[idx] = drain(consumer, timing['deadline'], args.poll_timeout, args.consume_batch_size)
        consumer.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - timing['start']

    warn_if_empty(sum(counts))
    report_result('consumer', 'per-thread', sum(counts), elapsed, num_threads=args.num_threads)


MODES = {
    'single': run_single,
    'per-thread': run_per_thread,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Consumer free-threading perf runs')
    add_common_args(parser)
    parser.add_argument('--topic', required=True, help='Topic pre-seeded with messages')
    parser.add_argument('--mode', choices=sorted(MODES), default='single')
    parser.add_argument('--duration', '-d', type=float, default=10.0, help='How long to consume for, in seconds')
    parser.add_argument('--num-threads', '-t', type=int, default=4, help='Consumer count for per-thread mode')
    parser.add_argument(
        '--consume-batch-size',
        type=int,
        default=1,
        help='Default 1 uses poll() (one C-level call per message); >1 uses consume(num_messages=N) '
        'instead (one C-level call per up-to-N messages). Worth sweeping -- see aio_consumer_perf.py '
        '--consume-batch-size for the equivalent knob on the AIOConsumer side.',
    )
    parser.add_argument(
        '--group-id',
        default=None,
        help='Consumer group.id (default: a fresh random one per run, so every invocation re-reads from the beginning)',
    )
    parser.add_argument('--poll-timeout', type=float, default=1.0)
    parser.add_argument(
        '--warmup-timeout', type=float, default=30.0, help='Max seconds to wait for the initial partition assignment'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    group_id = args.group_id or f'perf-no-gil-{args.mode}-{uuid.uuid4().hex[:8]}'
    conf = build_conf(
        args,
        **{
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        },
    )
    MODES[args.mode](conf, args)


if __name__ == '__main__':
    main()
