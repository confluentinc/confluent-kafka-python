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

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
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
    Run `worker(producer)` on `num_workers` other threads while calling
    close() from the main thread, repeated `iterations` times against a
    fresh Producer each time. Every close() call (the main thread's, once
    per iteration) must return True.
    """
    errors = []
    close_results = []

    for i in range(ITERATIONS):
        producer = Producer(conf or _PRODUCER_CONF)
        start_barrier = threading.Barrier(num_workers + 1)

        def run_worker():
            try:
                start_barrier.wait()
                worker(producer)
            except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
                errors.append((i, e))

        threads = [threading.Thread(target=run_worker, daemon=True) for _ in range(num_workers)]
        for t in threads:
            t.start()

        start_barrier.wait()
        close_results.append(producer.close())

        for t in threads:
            t.join(timeout=10)
        assert all(not t.is_alive() for t in threads), f"iteration {i}: a worker thread did not finish after close()"

    assert not errors, f"unexpected exceptions from worker threads: {errors}"
    assert all(close_results), f"not every close() call returned True: {close_results}"


def _worker_produce(producer):
    while True:
        try:
            producer.produce('mytopic', value=b'x')
        except RuntimeError as e:
            assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
            break


def _worker_poll(producer):
    while True:
        try:
            producer.poll(0)
        except RuntimeError as e:
            assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
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

    def worker(producer):
        while True:
            try:
                producer.flush(0.01)
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break

    _race_close_against(worker)


@subprocess_isolated
def test_close_races_produce_batch():
    """close() concurrent with produce_batch() on another thread."""

    def worker(producer):
        messages = [{'value': b'x'}, {'value': b'y'}]
        while True:
            try:
                producer.produce_batch('mytopic', messages)
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break

    _race_close_against(worker)


@subprocess_isolated
def test_close_races_init_transactions():
    """close() concurrent with init_transactions() on another thread."""

    def worker(producer):
        while True:
        # TODO NOGIL: move to tests/integration -- init_transactions() needs a
        # real transaction coordinator to reach the race being tested; against
        # the unreachable localhost:9092 used here it fails with a genuine
        # _TIMED_OUT KafkaException instead of the expected RuntimeError.
            try:
                producer.init_transactions(0.05)
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break
            except KafkaException as e:
                # init_transactions() needs a reachable coordinator,
                # so against localhost:9092 it can never succeed --
                # expected, unrelated to close(), ignore and keep
                # racing until closing wins.
                if e.args[0].code() != KafkaError._TIMED_OUT:
                    raise

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_begin_transaction():
    """close() concurrent with begin_transaction() on another thread."""

    def worker(producer):
        while True:
            try:
                producer.begin_transaction()
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break
            except KafkaException as e:
                # begin_transaction() can return a state error as we didn't
                # call init_transactions() -- expected, unrelated to close(),
                # ignore and keep racing until closing wins.
                if e.args[0].code() != KafkaError._STATE:
                    raise

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_commit_transaction():
    """close() concurrent with commit_transaction() on another thread."""

    def worker(producer):
        while True:
            try:
                producer.commit_transaction(2.0)
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break
            except KafkaException as e:
                # No open transaction exists -- expected, unrelated
                # to close(), ignore and keep racing until closing wins.
                if e.args[0].code() != KafkaError._STATE:
                    raise

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_abort_transaction():
    """close() concurrent with abort_transaction() on another thread."""

    def worker(producer):
        while True:
        # TODO NOGIL: move to tests/integration -- abort_transaction() needs a
        # real transaction coordinator to reach the race being tested; against
        # the unreachable localhost:9092 used here it fails with a genuine
        # _STATE KafkaException instead of the expected RuntimeError.
            try:
                producer.abort_transaction(2.0)
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break
            except KafkaException as e:
                # No open transaction exists -- expected, unrelated
                # to close(), ignore and keep racing until closing wins.
                if e.args[0].code() != KafkaError._STATE:
                    raise

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_send_offsets_to_transaction():
    """close() concurrent with send_offsets_to_transaction() on another thread."""

    def worker(producer):
        # consumer_group_metadata() doesn't need a live broker connection.
        consumer = Consumer({'group.id': 'test-producer-close-race', 'socket.timeout.ms': 10})
        metadata = consumer.consumer_group_metadata()
        consumer.close()

        offsets = [TopicPartition('mytopic', 0, 1)]
        while True:
            try:
                # A generous timeout keeps this a pure _STATE check: the
                # underlying check is local and near-instant, so 2s leaves
                # huge headroom against _TIMED_OUT ever firing instead,
                # even under CI scheduling delays.
                producer.send_offsets_to_transaction(offsets, metadata, 2.0)
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break
            except KafkaException as e:
                # No open transaction exists (init/begin never completed
                # against this unreachable broker) -- expected, unrelated
                # to close(), ignore and keep racing until closing wins.
                if e.args[0].code() != KafkaError._STATE:
                    raise

    _race_close_against(worker, conf=_TXN_PRODUCER_CONF)


@subprocess_isolated
def test_close_races_purge():
    """close() concurrent with purge() on another thread."""

    def worker(producer):
        while True:
            try:
                producer.purge()
            except RuntimeError as e:
                assert 'closed' in str(e).lower(), f"unexpected RuntimeError: {e}"
                break

    _race_close_against(worker)


@subprocess_isolated
def test_close_races_close():
    """Multiple threads calling close() on the same Producer at once. The
    CAS winner tears down self->rk itself; every losing thread blocks until
    the winner finishes and then returns True too, same as a normal close()."""
    num_workers = 7

    for i in range(ITERATIONS):
        producer = Producer(_PRODUCER_CONF)
        all_results = []
        start_barrier = threading.Barrier(num_workers + 1)

        def worker():
            start_barrier.wait()
            all_results.append(producer.close())

        # Daemon: if close() itself ever hangs (e.g. a deadlock), the
        # process must still be able to exit right after the assertion
        # below fires instead of hanging until the outer
        # subprocess_isolated timeout.
        threads = [threading.Thread(target=worker, daemon=True) for _ in range(num_workers)]
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
        assert all(all_results), f"iteration {i}: every concurrent close() call must return True, got: {all_results}"


###############################################################################
# End of tests for races between Producer.close() and concurrent calls
# on the same Producer instance.
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


def test_close_waits_for_in_flight_list_topics():
    """list_topics() goes through the rk-use gate (active_calls) and a close()
    that starts while list_topics() is in flight must wait for it to finish before
    tearing down self->rk, rather than racing it. Against the unreachable
    localhost:9092, list_topics(timeout=5) blocks its full timeout, giving
    close() a real in-flight call to wait on."""
    producer = Producer(_PRODUCER_CONF)
    list_started = threading.Event()
    list_finished_at = None

    def run_list_topics():
        nonlocal list_finished_at
        list_started.set()
        try:
            producer.list_topics(timeout=5)
        except KafkaException:
            pass  # unreachable broker -> transport error at the end, expected
        list_finished_at = time.monotonic()

    t = threading.Thread(target=run_list_topics)
    t.start()
    list_started.wait()
    time.sleep(1)  # Make sure list_topics() is genuinely in-flight (holding the gate).

    close_start = time.monotonic()
    result = producer.close()
    close_end = time.monotonic()

    t.join(timeout=15)
    assert not t.is_alive(), "list_topics() thread did not finish after close()"
    assert list_finished_at is not None, "list_topics() never finished"
    assert result is True, f"close() must return True, got {result!r}"

    close_duration = close_end - close_start
    # close() must not have returned before the in-flight list_topics()
    # released the gate ...
    assert close_end >= list_finished_at, (
        f"close() returned at {close_end:.2f} before in-flight list_topics() "
        f"finished at {list_finished_at:.2f} -- it did not wait"
    )
    assert close_duration >= 3.0, (
        f"close() returned in {close_duration:.2f}s -- too fast to have waited " f"for the in-flight list_topics() call"
    )


def test_close_is_idempotent():
    """Calling close() twice, with no concurrency at all, must return True
    both times."""
    producer = Producer(_PRODUCER_CONF)

    assert producer.close() is True, "first close() must return True"
    assert producer.close() is True, "second close() on an already-closed producer must also return True"
