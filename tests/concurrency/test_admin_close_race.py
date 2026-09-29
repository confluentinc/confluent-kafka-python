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

import threading
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType
from tests.concurrency._subprocess_isolation import subprocess_isolated

###############################################################################
# Tests for races between AdminClient teardown and
# concurrent calls to other methods on the same AdminClient instance, and
# between context-manager exit and itself. AdminClient currently has none of
# the active_calls/closing protection Producer.close() has, so these races
# are expected to crash or misbehave until that protection is added.
###############################################################################

_ADMIN_CONF = {'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}
ITERATIONS = 10
NUM_WORKERS = 7


def _race_exit_against(workers, num_workers=NUM_WORKERS):
    """
    Run worker(s) on `num_workers` other threads while exiting the `with`
    block from the main thread, repeated `ITERATIONS` times against a fresh
    AdminClient each time.

    `workers` may be a single `worker(admin, stop_event)` callable, applied
    to every thread, or a list of `num_workers` different callables (one per
    thread) to race several different methods concurrently instead of
    hammering a single one.
    """
    worker_list = workers if isinstance(workers, list) else [workers] * num_workers
    assert len(worker_list) == num_workers
    errors = []

    for i in range(ITERATIONS):
        stop_event = threading.Event()
        start_barrier = threading.Barrier(num_workers + 1)

        with AdminClient(_ADMIN_CONF) as admin:

            def run_worker(worker):
                try:
                    start_barrier.wait()
                    worker(admin, stop_event)
                except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
                    errors.append((i, e))

            threads = [threading.Thread(target=run_worker, args=(w,)) for w in worker_list]
            for t in threads:
                t.start()

            start_barrier.wait()

        stop_event.set()
        for t in threads:
            t.join(timeout=10)
        assert all(
            not t.is_alive() for t in threads
        ), f"iteration {i}: a worker thread did not finish after exiting the with block"

    assert not errors, f"unexpected exceptions from worker threads: {errors}"


@subprocess_isolated
def test_exit_races_create_topics():
    """Exiting the `with` block concurrent with create_topics() on another thread."""

    def worker(admin, stop_event):
        while not stop_event.is_set():
            try:
                futmap = admin.create_topics([NewTopic('mytopic', num_partitions=1, replication_factor=1)])
                for f in futmap.values():
                    try:
                        f.result(timeout=1)
                    except Exception:  # noqa: BLE001 - broker-less errors expected, not the bug we're after
                        pass
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError racing teardown: {e}"
                break

    _race_exit_against(worker)


@subprocess_isolated
def test_exit_races_list_topics():
    """Exiting the `with` block concurrent with list_topics() on another thread."""

    def worker(admin, stop_event):
        while not stop_event.is_set():
            try:
                admin.list_topics(timeout=0.05)
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError racing teardown: {e}"
                break
            except Exception:  # noqa: BLE001 - librdkafka timeout/transport errors expected without a broker
                pass

    _race_exit_against(worker)


@subprocess_isolated
def test_exit_races_set_sasl_credentials():
    """Exiting the `with` block concurrent with set_sasl_credentials() on another thread."""

    def worker(admin, stop_event):
        while not stop_event.is_set():
            try:
                admin.set_sasl_credentials('user', 'password')
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError racing teardown: {e}"
                break
            except Exception:  # noqa: BLE001 - librdkafka errors possible without a SASL-configured broker
                pass

    _race_exit_against(worker)


@subprocess_isolated
def test_exit_races_exit():
    """Multiple threads exiting the `with` block on the same AdminClient at once."""

    def worker(admin, stop_event):
        with admin:
            pass

    _race_exit_against(worker)


@subprocess_isolated
def test_exit_races_multiple_methods():
    """Exiting the `with` block concurrent with several different Admin
    methods at once (rather than many threads hammering the same one) --
    closer to realistic usage, and exercises more of Admin.c's call sites
    in a single run."""

    def make_worker(call):
        def worker(admin, stop_event):
            while not stop_event.is_set():
                try:
                    call(admin)
                except RuntimeError as e:
                    assert 'closed' in str(e).lower(), f"unexpected RuntimeError racing teardown: {e}"
                    break
                except Exception:  # noqa: BLE001 - broker-less/timeout errors expected, not the bug we're after
                    pass

        return worker

    def call_create_topics(admin):
        futmap = admin.create_topics([NewTopic('mytopic', num_partitions=1, replication_factor=1)])
        for f in futmap.values():
            try:
                f.result(timeout=1)
            except Exception:  # noqa: BLE001 - broker-less errors expected, not the bug we're after
                pass

    def call_list_topics(admin):
        admin.list_topics(timeout=0.05)

    def call_describe_configs(admin):
        resource = ConfigResource(ResourceType.TOPIC, 'mytopic')
        futmap = admin.describe_configs([resource])
        for f in futmap.values():
            try:
                f.result(timeout=1)
            except Exception:  # noqa: BLE001 - broker-less errors expected, not the bug we're after
                pass

    def call_delete_topics(admin):
        futmap = admin.delete_topics(['mytopic'])
        for f in futmap.values():
            try:
                f.result(timeout=1)
            except Exception:  # noqa: BLE001 - broker-less errors expected, not the bug we're after
                pass

    workers = [
        make_worker(call_create_topics),
        make_worker(call_list_topics),
        make_worker(call_describe_configs),
        make_worker(call_delete_topics),
    ]

    _race_exit_against(workers, num_workers=len(workers))


def test_exit_waits_for_in_flight_call():
    """__exit__() blocks until an in-flight list_topics() call finishes."""
    admin = AdminClient(_ADMIN_CONF)
    list_started = threading.Event()
    list_finished_at = None

    def run_list_topics():
        nonlocal list_finished_at
        list_started.set()
        try:
            admin.list_topics(timeout=5)
        except KafkaException:
            pass  # unreachable broker -> transport error at the end, expected
        list_finished_at = time.monotonic()

    t = threading.Thread(target=run_list_topics)
    t.start()
    list_started.wait()
    time.sleep(1)  # Make sure list_topics() is genuinely in-flight (holding the gate).

    exit_start = time.monotonic()
    admin.__exit__(None, None, None)
    exit_end = time.monotonic()

    t.join(timeout=15)
    assert not t.is_alive(), "list_topics() thread did not finish after __exit__()"
    assert list_finished_at is not None, "list_topics() never finished"

    # __exit__() must not have returned before the in-flight list_topics()
    # released the gate.
    assert exit_end >= list_finished_at, (
        f"__exit__() returned at {exit_end:.2f} before in-flight list_topics() "
        f"finished at {list_finished_at:.2f} -- it did not wait"
    )
    exit_duration = exit_end - exit_start
    assert exit_duration >= 3.0, (
        f"__exit__() returned in {exit_duration:.2f}s -- too fast to have waited "
        f"for the in-flight list_topics() call"
    )
