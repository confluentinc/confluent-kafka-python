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
AIOProducer free-threading perf runs. See README.md for the full run matrix.

AIOProducer's concurrency model isn't like the sync Producer's: produce()
just buffers a message in-memory and returns almost immediately, dispatching
to librdkafka (via a background ThreadPoolExecutor) only once --batch-size
messages have queued up or --buffer-timeout elapses. There's also no
supported pattern for calling an AIOProducer's methods from a thread other
than the one running its event loop, so there's no "shared across threads"
mode the way the sync Producer has one -- the idiomatic way to add
concurrency is more coroutines on the *same* event loop, not more threads.

Modes
-----
single:     one coroutine, one event loop, sequential produce()+await.
concurrent: -t concurrent asyncio tasks all calling produce() on *one*
            AIOProducer instance within *one* event loop (one OS thread) --
            the idiomatic way real async code scales. --executor-workers
            controls the actual parallelism available to it (the
            ThreadPoolExecutor that runs librdkafka's blocking calls).
per-loop:   -t independent OS threads, each running its own event loop with
            its own AIOProducer instance -- genuine OS-level parallelism
            across fully independent instances, the direct analog of the
            sync Producer's per-thread mode.

Examples
--------
Run 1 (single coroutine, 3.14):
    python3.14  aio_producer_perf.py -b localhost:9092 --mode single -d 10
Run 2 (single coroutine, 3.14t):
    python3.14t aio_producer_perf.py -b localhost:9092 --mode single -d 10
Run 3 (concurrent tasks on one instance, 3.14):
    python3.14  aio_producer_perf.py -b localhost:9092 --mode concurrent -d 10 -t 8
Run 4 (concurrent tasks on one instance, 3.14t):
    python3.14t aio_producer_perf.py -b localhost:9092 --mode concurrent -d 10 -t 8
Run 5 (independent instance per event loop/thread, 3.14t):
    python3.14t aio_producer_perf.py -b localhost:9092 --mode per-loop -d 10 -t 8
"""

import argparse
import asyncio
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import (  # noqa: E402
    add_common_args,
    build_conf,
    create_scratch_topic,
    make_payload,
    report_result,
    warn_if_unflushed,
)

from confluent_kafka.aio import AIOProducer  # noqa: E402


def make_tracker() -> tuple[dict, callable]:
    """A done-callback based delivery tracker for AIOProducer's per-message futures.

    No lock needed: asyncio schedules a Future's done-callbacks on its own event loop's
    thread (via call_soon/call_soon_threadsafe internally), so within a single event loop --
    which is all any one of these trackers ever sees, even when many coroutines share one
    AIOProducer in `concurrent` mode -- callbacks only ever run one at a time, on that loop's
    own thread.

    `pending` exists because `fut.set_result()`/`set_exception()` *schedule* done-callbacks
    rather than running them synchronously -- so even after `flush()` confirms librdkafka has
    delivered every message, this callback may not have run yet for all of them. Counting
    `pending` down to 0 (see wait_for_pending) is what actually proves every future's outcome
    has been observed, rather than assuming flush()'s return implies it.
    """
    state = {'errors': 0, 'pending': 0}

    def on_done(fut: 'asyncio.Future') -> None:
        state['pending'] -= 1
        if fut.exception() is not None:
            state['errors'] += 1

    return state, on_done


async def wait_for_pending(state: dict, timeout: float) -> int:
    """Block until every produce() future's on_done callback has actually run (state['pending']
    reaches 0), or `timeout` elapses. Returns the still-pending count (0 on success)."""
    deadline = time.monotonic() + timeout
    while state['pending'] > 0 and time.monotonic() < deadline:
        await asyncio.sleep(0)
    return state['pending']


async def produce_until(
    producer: AIOProducer,
    topic: str,
    deadline: float,
    payload: bytes,
    state: dict,
    on_done: callable,
    key_prefix: str = '',
) -> int:
    """Produce as fast as possible until `deadline` (a time.monotonic() value), returning how many
    produce() calls succeeded. Each returned future gets `on_done` attached to catch delivery
    errors; the future itself is otherwise left alone (not awaited, not stored) since
    AIOProducer's own internal batch state -- not our reference to the future -- is what keeps
    it alive until delivery. `state['pending']` is incremented here so wait_for_pending() knows
    how many outcomes are still outstanding.
    """
    count = 0
    while time.monotonic() < deadline:
        key = f'{key_prefix}{count}'.encode()
        fut = await producer.produce(topic, value=payload, key=key)
        state['pending'] += 1
        fut.add_done_callback(on_done)
        count += 1
    return count


async def run_single_async(conf: dict, topic: str, args: argparse.Namespace) -> None:
    producer = AIOProducer(conf, max_workers=args.executor_workers, batch_size=args.batch_size)
    payload = make_payload(args.message_size)
    state, on_done = make_tracker()

    start = time.monotonic()
    count = await produce_until(producer, topic, start + args.duration, payload, state, on_done)
    remaining = await producer.flush(args.flush_timeout)
    still_pending = await wait_for_pending(state, args.flush_timeout)
    await producer.close()
    elapsed = time.monotonic() - start

    warn_if_unflushed((remaining or 0) + still_pending)
    report_result(
        'aio_producer',
        'single',
        count,
        elapsed,
        errors=state['errors'],
        drain_seconds=max(0.0, elapsed - args.duration),
    )


async def run_concurrent_async(conf: dict, topic: str, args: argparse.Namespace) -> None:
    producer = AIOProducer(conf, max_workers=args.executor_workers, batch_size=args.batch_size)
    payload = make_payload(args.message_size)
    state, on_done = make_tracker()
    counts = [0] * args.num_threads

    async def task_worker(idx: int, deadline: float) -> None:
        counts[idx] = await produce_until(producer, topic, deadline, payload, state, on_done, key_prefix=f'{idx}-')

    start = time.monotonic()
    deadline = start + args.duration
    await asyncio.gather(*(task_worker(i, deadline) for i in range(args.num_threads)))
    remaining = await producer.flush(args.flush_timeout)
    still_pending = await wait_for_pending(state, args.flush_timeout)
    await producer.close()
    elapsed = time.monotonic() - start

    warn_if_unflushed((remaining or 0) + still_pending)
    report_result(
        'aio_producer',
        'concurrent',
        sum(counts),
        elapsed,
        num_threads=args.num_threads,
        errors=state['errors'],
        drain_seconds=max(0.0, elapsed - args.duration),
    )


def run_per_loop(conf: dict, topic: str, args: argparse.Namespace) -> None:
    counts = [0] * args.num_threads
    remaining_counts = [0] * args.num_threads
    errors_by_thread = [0] * args.num_threads

    async def loop_main(idx: int, deadline: float) -> None:
        # Built here, inside the coroutine this thread's own event loop runs -- an
        # AIOProducer, its payload, and its topic string built by the *main* thread and
        # handed to a worker thread would be "owned" by the main thread under CPython 3.14's
        # free-threaded biased reference counting, making every refcount touch from the
        # worker take the slow shared/atomic path instead of the fast thread-local one. See
        # produce_until's docstring in producer_perf.py for the sync-Producer investigation
        # that found this.
        producer = AIOProducer(conf, max_workers=args.executor_workers, batch_size=args.batch_size)
        payload = make_payload(args.message_size)
        thread_topic = topic.encode().decode()
        state, on_done = make_tracker()

        counts[idx] = await produce_until(
            producer, thread_topic, deadline, payload, state, on_done, key_prefix=f'{idx}-'
        )
        remaining = (await producer.flush(args.flush_timeout)) or 0
        still_pending = await wait_for_pending(state, args.flush_timeout)
        remaining_counts[idx] = remaining + still_pending
        await producer.close()
        errors_by_thread[idx] = state['errors']

    def worker(idx: int, deadline: float) -> None:
        asyncio.run(loop_main(idx, deadline))

    start = time.monotonic()
    deadline = start + args.duration
    threads = [threading.Thread(target=worker, args=(i, deadline)) for i in range(args.num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    warn_if_unflushed(sum(remaining_counts))
    report_result(
        'aio_producer',
        'per-loop',
        sum(counts),
        elapsed,
        num_threads=args.num_threads,
        errors=sum(errors_by_thread),
        drain_seconds=max(0.0, elapsed - args.duration),
    )


def run_single(conf: dict, topic: str, args: argparse.Namespace) -> None:
    asyncio.run(run_single_async(conf, topic, args))


def run_concurrent(conf: dict, topic: str, args: argparse.Namespace) -> None:
    asyncio.run(run_concurrent_async(conf, topic, args))


MODES = {
    'single': run_single,
    'concurrent': run_concurrent,
    'per-loop': run_per_loop,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='AIOProducer free-threading perf runs')
    add_common_args(parser)
    parser.add_argument('--mode', choices=sorted(MODES), default='single')
    parser.add_argument('--duration', '-d', type=float, default=10.0, help='How long to produce for, in seconds')
    parser.add_argument('--message-size', type=int, default=128, help='Payload size in bytes')
    parser.add_argument(
        '--num-threads', '-t', type=int, default=4, help='Task/thread count for concurrent/per-loop modes'
    )
    parser.add_argument(
        '--executor-workers',
        type=int,
        default=100,
        help="AIOProducer's ThreadPoolExecutor size (default: %(default)s) -- the actual OS-thread "
        "parallelism available to its blocking librdkafka calls (AIOProducer's own default is 4).",
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='AIOProducer batch_size (default: %(default)s, matching its own default) -- how many '
        'buffered messages trigger a dispatch to librdkafka. Testing found a real optimum right '
        'around this default: below ~500 throughput collapses (too few messages per round trip to '
        "amortize the executor dispatch/cross-thread-wakeup cost -- 18x worse at batch_size=1); "
        'above it, throughput mildly declines as batch size grows further (more of '
        "produce_batch()'s per-message dict-lookup cost accumulates per round trip). Worth "
        'sweeping if your workload (message size, broker latency) differs from the default test.',
    )
    parser.add_argument('--partitions', '-p', type=int, default=3, help='Partition count for the scratch topic')
    parser.add_argument('--replication-factor', type=int, default=1)
    parser.add_argument('--flush-timeout', type=float, default=120.0)
    parser.add_argument('--admin-timeout', type=float, default=30.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conf = build_conf(args, **{'linger.ms': 5, 'partitioner': 'consistent_random'})
    topic = create_scratch_topic(
        conf, 'perf-no-gil-aio-producer', args.partitions, args.replication_factor, args.admin_timeout
    )
    print(f'Created topic {topic!r} with {args.partitions} partitions.')
    MODES[args.mode](conf, topic, args)


if __name__ == '__main__':
    main()
