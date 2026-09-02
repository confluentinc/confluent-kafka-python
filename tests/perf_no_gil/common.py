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
Shared helpers for the manual free-threading perf scripts in this directory.

These are standalone scripts, not pytest tests -- see README.md for how to
run them and how they map to the producer/consumer/admin perf runs used to
compare CPython 3.14 against 3.14t (free-threaded).
"""

import argparse
import sys
import sysconfig
import threading
import uuid

import confluent_kafka
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Flags shared by every script in this directory. Each script adds its
    own --topic (the sensible default/requiredness differs per script)."""
    parser.add_argument(
        '--bootstrap-servers',
        '-b',
        required=True,
        help=(
            'Kafka bootstrap.servers, passed straight through to librdkafka. Accepts a plain '
            'comma-separated host:port list (localhost:9092) or a scheme-prefixed one '
            '(PLAINTEXT://localhost:55920,PLAINTEXT://localhost:55925,PLAINTEXT://localhost:55930), '
            'e.g. as produced by a trivup KafkaCluster.'
        ),
    )
    parser.add_argument(
        '--extra-conf',
        action='append',
        default=[],
        metavar='KEY=VALUE',
        help='Extra librdkafka config, e.g. --extra-conf security.protocol=SASL_SSL. May be repeated.',
    )


def build_conf(args: argparse.Namespace, **defaults) -> dict:
    """Merge --bootstrap-servers, script-specific defaults, and --extra-conf
    (highest precedence) into a librdkafka config dict."""
    conf = dict(defaults)
    conf['bootstrap.servers'] = args.bootstrap_servers
    for item in args.extra_conf:
        if '=' not in item:
            raise SystemExit(f'--extra-conf must be KEY=VALUE, got: {item!r}')
        key, value = item.split('=', 1)
        conf[key] = value
    return conf


def make_payload(size: int) -> bytes:
    return b'x' * size


def create_scratch_topic(conf: dict, prefix: str, partitions: int, replication_factor: int, timeout: float) -> str:
    """Create a fresh, randomly-named topic so concurrent/repeated runs never collide."""
    topic = f'{prefix}-{uuid.uuid4().hex[:8]}'
    admin = AdminClient(conf)
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)])
    try:
        futures[topic].result(timeout)
    except KafkaException as e:
        raise SystemExit(f'Failed to create topic {topic!r}: {e}')
    return topic


def warn_if_unflushed(remaining: int) -> None:
    """`Producer`/`AIOProducer` flush()'s return value is the number of messages that still had
    no final delivery resolution (success or failure) when its timeout expired -- i.e. still
    sitting in the client buffer. It should always be 0 here; if it isn't, the run's counts
    can't be trusted (raise --flush-timeout)."""
    if remaining:
        print(f'WARNING: {remaining} messages still in the client buffer after the flush timeout')


def divide_work(total: int, n: int) -> list:
    """Split `total` into `n` near-equal integer shares."""
    base, remainder = divmod(total, n)
    return [base + (1 if i < remainder else 0) for i in range(n)]


def gil_status() -> str:
    if not hasattr(sys, '_is_gil_enabled'):
        return 'enabled (not a free-threaded build)'
    if not sys._is_gil_enabled():
        return 'disabled'
    if bool(sysconfig.get_config_var('Py_GIL_DISABLED')):
        return 'enabled (re-enabled at runtime on a free-threaded build)'
    return 'enabled'


def build_label() -> str:
    free_threaded = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))
    build = 'free-threaded build' if free_threaded else 'standard build'
    return f'CPython {sys.version.split()[0]} ({build}), GIL {gil_status()}'


class DeliveryTracker:
    """Delivery-error counter for producer callbacks, built to need no lock.

    One slot per caller thread; each thread calls bind() once, at startup, to record which
    slot is "its own" in thread-local storage. on_delivery() then always writes to the slot
    belonging to whichever OS thread is *currently executing the callback* -- which, in
    "shared" mode, may not be the thread that originally called produce() for that message,
    since librdkafka serves delivery reports to whichever thread happens to be inside
    poll()/flush() at the time. Keying by the executing thread (not the originating one) is
    what makes this safe without a lock: a single OS thread can only ever be running one
    piece of code at a time, so it's the only thread that could possibly write to its own
    slot at that instant -- no two threads ever touch the same slot concurrently. Summing
    happens only after every thread has joined, so that read is race-free too. The success
    path does no work at all (not even a counter increment), since nothing in this harness
    reads a "delivered" count -- only errors matter for correctness checking.
    """

    def __init__(self, num_threads: int):
        self._errors = [0] * num_threads
        self._slot = threading.local()

    def bind(self, idx: int) -> None:
        self._slot.idx = idx

    def on_delivery(self, err, msg) -> None:
        if err is not None:
            self._errors[self._slot.idx] += 1

    @property
    def errors(self) -> int:
        return sum(self._errors)


def report_result(
    component: str,
    mode: str,
    num_units: int,
    elapsed: float,
    num_threads: int = 1,
    errors: int = 0,
    drain_seconds: float = 0.0,
) -> None:
    """drain_seconds, when given, is how much of `elapsed` was spent in a post-window flush()/drain
    waiting for already-enqueued work to finish, rather than in the timed work itself -- surfaced
    separately since a big drain means the reported throughput is being pulled down by a downstream
    bottleneck (e.g. the broker's ingestion rate), not by the client-side code under test."""
    throughput = num_units / elapsed if elapsed > 0 else float('inf')
    print('-' * 70)
    print(f'component      : {component}')
    print(f'mode           : {mode}')
    print(f'python         : {build_label()}')
    print(f'client         : {confluent_kafka.version()}')
    print(f'threads        : {num_threads}')
    print(f'messages/calls : {num_units}')
    print(f'elapsed (s)    : {elapsed:.3f}')
    if drain_seconds > 0.05:
        print(f'  (of which drain: {drain_seconds:.3f}s spent flushing a backlog after the timed window closed)')
    print(f'throughput/s   : {throughput:,.1f}')
    if errors:
        print(f'errors         : {errors}')
    print('-' * 70)
    sys.stdout.flush()
