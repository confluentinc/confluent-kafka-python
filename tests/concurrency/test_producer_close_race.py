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

import os
import signal
import threading
import time

from confluent_kafka import Consumer, Producer, TopicPartition
from tests.concurrency._subprocess_isolation import subprocess_isolated

_PRODUCER_CONF = {'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10, 'message.timeout.ms': 10}
_TXN_PRODUCER_CONF = dict(_PRODUCER_CONF, **{'transactional.id': 'test-producer-close-race-txn'})
ITERATIONS = 10


###############################################################################
# Tests for races between Producer.close() and concurrent calls to
# other methods on the same Producer instance.
###############################################################################


def _race_close_against(worker, conf=None, num_workers=1):
    """
    Run `worker(producer, stop_event)` on `num_workers` other threads while
    calling close() from the main thread, repeated `iterations` times
    against a fresh Producer each time. Every close() call (the main
    thread's, once per iteration) must return True.
    """
    errors = []
    close_results = []

    for i in range(ITERATIONS):
        producer = Producer(conf or _PRODUCER_CONF)
        stop_event = threading.Event()
        start_barrier = threading.Barrier(num_workers + 1)

        def run_worker():
            try:
                start_barrier.wait()
                worker(producer, stop_event)
            except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
                errors.append((i, e))

        threads = [threading.Thread(target=run_worker) for _ in range(num_workers)]
        for t in threads:
            t.start()

        start_barrier.wait()
        close_results.append(producer.close())

        stop_event.set()
        for t in threads:
            t.join(timeout=10)
        assert all(not t.is_alive() for t in threads), f"iteration {i}: a worker thread did not finish after close()"

    assert not errors, f"unexpected exceptions from worker threads: {errors}"
    assert all(close_results), f"not every close() call returned True: {close_results}"


def _worker_produce(producer, stop_event):
    while not stop_event.is_set():
        try:
            producer.produce('mytopic', value=b'x')
        except RuntimeError:
            # Expected once close() has fully completed on this thread's
            # view of self->rk; anything other than a clean RuntimeError
            # (e.g. a segfault) is the bug this test is trying to catch.
            break


def _worker_poll(producer, stop_event):
    while not stop_event.is_set():
        try:
            producer.poll(0)
        except RuntimeError:
            break


@subprocess_isolated
def test_close_races_produce():
    """close() concurrent with produce() on another thread."""
    _race_close_against(_worker_produce)


@subprocess_isolated
def test_close_races_multiple_producers_and_pollers():
    """
    close() concurrent with several threads calling produce()/poll() at
    once, not just one.
    """
    num_workers = 8
    _race_close_against(_worker_produce, num_workers=num_workers)
    _race_close_against(_worker_poll, num_workers=num_workers)


@subprocess_isolated
def test_close_races_poll():
    """close() concurrent with poll() on another thread."""
    _race_close_against(_worker_poll)


@subprocess_isolated
def test_close_races_flush():
    """close() concurrent with flush() on another thread."""

    def worker(producer, stop_event):
        while not stop_event.is_set():
            try:
                producer.flush(0.01)
            except RuntimeError:
                break

    _race_close_against(worker)


@subprocess_isolated
def test_close_races_produce_batch():
    """close() concurrent with produce_batch() on another thread."""

    def worker(producer, stop_event):
        messages = [{'value': b'x'}, {'value': b'y'}]
        while not stop_event.is_set():
            try:
                producer.produce_batch('mytopic', messages)
            except RuntimeError:
                break

    _race_close_against(worker)


@subprocess_isolated
def test_close_races_init_transactions():
    """close() concurrent with init_transactions() on another thread."""

    def worker(producer, stop_event):
        while not stop_event.is_set():
            try:
                producer.init_transactions(0.05)
            except RuntimeError:
                break
            except Exception:  # noqa: BLE001 - librdkafka state/timeout errors are expected without a broker
                pass

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_begin_transaction():
    """close() concurrent with begin_transaction() on another thread."""

    def worker(producer, stop_event):
        while not stop_event.is_set():
            try:
                producer.begin_transaction()
            except RuntimeError:
                break
            except Exception:  # noqa: BLE001 - librdkafka state errors are expected without a broker
                pass

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_commit_transaction():
    """close() concurrent with commit_transaction() on another thread."""

    def worker(producer, stop_event):
        while not stop_event.is_set():
            try:
                producer.commit_transaction(0.05)
            except RuntimeError:
                break
            except Exception:  # noqa: BLE001 - librdkafka state/timeout errors are expected without a broker
                pass

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_abort_transaction():
    """close() concurrent with abort_transaction() on another thread."""

    def worker(producer, stop_event):
        while not stop_event.is_set():
            try:
                producer.abort_transaction(0.05)
            except RuntimeError:
                break
            except Exception:  # noqa: BLE001 - librdkafka state/timeout errors are expected without a broker
                pass

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_send_offsets_to_transaction():
    """close() concurrent with send_offsets_to_transaction() on another thread."""

    def worker(producer, stop_event):
        # consumer_group_metadata() doesn't need a live broker connection.
        consumer = Consumer({'group.id': 'test-producer-close-race', 'socket.timeout.ms': 10})
        metadata = consumer.consumer_group_metadata()
        consumer.close()

        offsets = [TopicPartition('mytopic', 0, 1)]
        while not stop_event.is_set():
            try:
                producer.send_offsets_to_transaction(offsets, metadata, 0.05)
            except RuntimeError:
                break
            except Exception:  # noqa: BLE001 - librdkafka state/timeout errors are expected without a broker
                pass

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_purge():
    """close() concurrent with purge() on another thread."""

    def worker(producer, stop_event):
        while not stop_event.is_set():
            try:
                producer.purge()
            except RuntimeError:
                break

    _race_close_against(worker)


@subprocess_isolated
def test_close_races_close():
    """Multiple threads calling close() on the same Producer at once. The
    CAS winner (whichever thread's close() actually tears down self->rk)
    returns True; a losing thread returns False *unless* its own check
    happens to run after self->rk is already NULL (the winner fully
    completed first), in which case it also gets True."""
    num_workers = 7

    for i in range(ITERATIONS):
        producer = Producer(_PRODUCER_CONF)
        all_results = []
        start_barrier = threading.Barrier(num_workers + 1)

        def worker():
            start_barrier.wait()
            all_results.append(producer.close())

        threads = [threading.Thread(target=worker) for _ in range(num_workers)]
        for t in threads:
            t.start()

        start_barrier.wait()
        all_results.append(producer.close())

        for t in threads:
            t.join(timeout=10)
        assert all(not t.is_alive() for t in threads), f"iteration {i}: a close() thread did not finish"

        assert (
            len(all_results) == num_workers + 1
        ), f"iteration {i}: expected {num_workers + 1} results, got {all_results}"
        assert all(
            isinstance(r, bool) for r in all_results
        ), f"iteration {i}: every close() call must return True or False, got: {all_results}"
        assert any(all_results), f"iteration {i}: expected at least one close() call to return True, got: {all_results}"


###############################################################################


def test_close_completes_quickly_with_indefinite_poll_in_progress():
    """close() must not block indefinitely behind an in-flight poll(-1)
    call on another thread -- poll()'s chunk loop notices `closing` and
    exits early, so close() should complete within a small, bounded time
    instead of waiting for poll() to return on its own."""
    producer = Producer(_PRODUCER_CONF)
    poll_started = threading.Event()
    poll_finished_at = None

    def run_poll():
        nonlocal poll_finished_at
        poll_started.set()
        producer.poll(-1)
        poll_finished_at = time.monotonic()

    t = threading.Thread(target=run_poll)
    t.start()
    poll_started.wait()
    time.sleep(0.5)  # Make sure poll() is genuinely in-flight when close() fires.

    close_start = time.monotonic()
    producer.close()
    close_duration = time.monotonic() - close_start

    t.join(timeout=10)
    assert not t.is_alive(), "poll() thread did not finish after close()"
    assert poll_finished_at is not None, "poll() never finished"
    assert close_duration < 0.5, (
        f"close() took {close_duration:.2f}s to complete while poll(-1) was "
        f"in progress -- expected it to finish within 0.5s"
    )


def test_close_completes_quickly_with_indefinite_flush_in_progress():
    """close() must not block indefinitely behind an in-flight flush(-1)
    call on another thread -- flush()'s chunk loop notices `closing` and
    exits early, so close() should complete within a small, bounded time
    instead of waiting for flush() to return on its own."""
    producer = Producer(_PRODUCER_CONF)
    flush_started = threading.Event()
    flush_finished_at = None

    def run_flush():
        nonlocal flush_finished_at
        flush_started.set()
        producer.flush(-1)
        flush_finished_at = time.monotonic()

    t = threading.Thread(target=run_flush)
    t.start()
    flush_started.wait()
    time.sleep(0.5)  # Make sure flush() is genuinely in-flight when close() fires.

    close_start = time.monotonic()
    producer.close()
    close_duration = time.monotonic() - close_start

    t.join(timeout=10)
    assert not t.is_alive(), "flush() thread did not finish after close()"
    assert flush_finished_at is not None, "flush() never finished"
    assert close_duration < 0.5, (
        f"close() took {close_duration:.2f}s to complete while flush(-1) was "
        f"in progress -- expected it to finish within 0.5s"
    )


@subprocess_isolated
def test_close_propagates_signal_while_waiting_for_active_calls():
    """close() must raise KeyboardInterrupt if a signal arrives while its
    active_calls drain-wait loop is spinning"""
    producer = Producer(_PRODUCER_CONF)
    poll_started = threading.Event()

    def hold_active_call():
        poll_started.set()
        try:
            producer.poll(-1)
        except BaseException:  # noqa: BLE001 - just draining the thread, not asserting here
            pass

    t = threading.Thread(target=hold_active_call)
    t.start()
    poll_started.wait()

    def send_sigint_soon():
        time.sleep(0.05)
        os.kill(os.getpid(), signal.SIGINT)

    interrupt_thread = threading.Thread(target=send_sigint_soon)
    interrupt_thread.daemon = True
    interrupt_thread.start()

    try:
        producer.close()
        assert False, "close() returned normally instead of raising KeyboardInterrupt"
    except KeyboardInterrupt:
        assert True  # expected outcome: close() correctly propagated the signal
    finally:
        t.join(timeout=10)
        assert not t.is_alive(), "poll() thread did not finish after close() was interrupted"
