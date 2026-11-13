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
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Unit tests for the Consumer reentrancy gate (Handle_gate_enter()/
Handle_gate_exit() in Consumer.c).
"""

import threading
import time

import pytest

from confluent_kafka import TopicPartition, cimpl
from tests.common import TestConsumer


def _new_consumer(conf=None):
    consumer_conf = {
        'group.id': 'test',
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 50,
        'session.timeout.ms': 100,
    }
    if conf:
        consumer_conf.update(conf)
    return TestConsumer(consumer_conf)


class _GateHolder:
    """Keeps a consumer's gate taken by another thread for as long as needed.

    Inside the with block, a thread presenting `identity` (or its own thread id)
    is inside poll(), blocked in the first stats_cb, so it holds the gate until
    the block ends. Tests can then check what other callers do meanwhile without
    relying on sleeps or on how fast threads get scheduled.
    """

    def __init__(self, identity=None):
        self._holding = threading.Event()
        self._release = threading.Event()
        self._identity = identity
        self.consumer = _new_consumer({'statistics.interval.ms': 100, 'stats_cb': self._stats_cb})
        self._thread = threading.Thread(target=self._poll)

    def _stats_cb(self, stats_json):
        if not self._holding.is_set():
            self._holding.set()
            self._release.wait()

    def _poll(self):
        if self._identity is not None:
            cimpl._reentry_identity_var.set(self._identity)
        while not self._release.is_set():
            self.consumer.poll(1.0)

    def __enter__(self):
        self._thread.start()
        if not self._holding.wait(30):
            self.__exit__()
            raise AssertionError("stats_cb never ran inside poll()")
        return self

    def __exit__(self, *exc_info):
        self._release.set()
        self._thread.join()


def _gated_call_in_thread(consumer, identity, results, name):
    """Call consumer.pause([]) from a new thread presenting `identity`. Returns the
    thread and an Event set once the call has returned or raised."""
    done = threading.Event()

    def call():
        cimpl._reentry_identity_var.set(identity)
        try:
            consumer.pause([])
            results[name] = 'ok'
        except Exception as e:  # noqa: BLE001 - recording whatever happens
            results[name] = e
        done.set()

    thread = threading.Thread(target=call)
    thread.start()
    return thread, done


def test_sequential_calls_from_same_thread_never_raise_spuriously():
    """Repeated gated calls from the same thread, one after another, must
    never raise ConcurrentModificationException -- the gate is
    only ever entered and exited within each call, so there is no
    legitimate contention here, only a sanity check that ordinary
    single-threaded usage is unaffected by the gate's bookkeeping."""
    c = _new_consumer()
    try:
        for _ in range(5):
            c.pause([])
            c.resume([])
    finally:
        c.close()


def test_gate_identity_matches_calling_thread():
    """For the sync Consumer, Handle_gate_enter() falls back to the calling
    thread's own PyThread_get_thread_ident() as the gate identity.
    Calls from two different real threads, made sequentially, must each succeed
    on their own -- ownership hands off cleanly since neither holds the gate across the
    call boundary. This is a sanity check, not a concurrency test."""
    c = _new_consumer()
    results = []

    def call_from_thread(label):
        try:
            c.pause([])
            results.append((label, 'ok'))
        except Exception as e:  # noqa: BLE001 - recording whatever happens
            results.append((label, e))

    try:
        t1 = threading.Thread(target=call_from_thread, args=('t1',))
        t1.start()
        t1.join()
        t2 = threading.Thread(target=call_from_thread, args=('t2',))
        t2.start()
        t2.join()

        assert results == [('t1', 'ok'), ('t2', 'ok')], results
    finally:
        c.close()


def test_reentrant_call_from_stats_cb_during_poll_succeeds():
    """A nested pause() call made from inside stats_cb, on the same thread that's
    still inside poll(), must be admitted as legitimate (gate_depth incremented)
    rather than rejected."""
    reentrant_results = []

    def stats_cb(json_str):
        try:
            c.pause([])
            reentrant_results.append('ok')
        except Exception as e:
            reentrant_results.append(e)

    c = _new_consumer({'statistics.interval.ms': 100, 'stats_cb': stats_cb})
    try:
        c.poll(0.5)
        print(f"reentrant_results={reentrant_results}")
        assert reentrant_results, "stats_cb never fired"
        assert all(r == 'ok' for r in reentrant_results), reentrant_results
    finally:
        c.close()


def test_reentrant_call_error_in_stats_cb_releases_gate():
    """A re-entrant call made from stats_cb that itself errors out must
    still release the gate -- gate_depth must correctly unwind back to the
    outer poll()'s own entry rather than getting stuck, which is exactly
    the failure mode the underflow assert in Handle_gate_exit() guards
    against."""
    nested_errors = []

    def stats_cb(json_str):
        if not nested_errors:
            try:
                c.pause('not-an-int')
            except Exception as e:
                nested_errors.append(e)

    c = _new_consumer({'statistics.interval.ms': 100, 'stats_cb': stats_cb})
    try:
        c.poll(0.5)
        print(f"nested_errors={nested_errors}")
        assert nested_errors, "stats_cb's nested call never ran"
        assert isinstance(nested_errors[0], TypeError), nested_errors

        c.pause([])
        c.resume([])
    finally:
        c.close()


def test_identity_match_admits_across_threads_mismatch_waits():
    """The gate's admission rule is identity-based, not thread-based.
    Two different OS threads presenting the SAME identity v are both admitted immediately (re-entrant),
    but a third thread presenting a different identity must wait for the gate to free up before proceeding.
    """
    results = {}
    gate = _GateHolder(identity=4242)
    try:
        with gate:
            same, same_done = _gated_call_in_thread(gate.consumer, 4242, results, 'same_identity')
            different, different_done = _gated_call_in_thread(gate.consumer, 9999, results, 'different_identity')
            # Re-entrant admission must not be queued behind the holder.
            assert same_done.wait(30), "same identity was not admitted while the holder held the gate"
            assert results['same_identity'] == 'ok', (
                f"same identity from a different thread should be admitted as "
                f"re-entrant, got: {results['same_identity']!r}"
            )
            assert not different_done.wait(
                0.5
            ), f"a different identity returned while the holder held the gate: {results['different_identity']!r}"
        same.join()
        different.join()
        assert results['different_identity'] == 'ok', (
            f"a different identity should wait for the gate rather than being "
            f"rejected, got: {results['different_identity']!r}"
        )
    finally:
        gate.consumer.close()


def test_zero_identity_falls_back_to_own_thread():
    """0 is documented as never a legitimate identity --
    it must be treated the same as "not set", falling back to the calling thread's own id.
    Two different threads both explicitly presenting identity 0 must still collide with each
    other, waiting rather than being rejected."""
    results = {}
    gate = _GateHolder(identity=0)
    try:
        with gate:
            other, other_done = _gated_call_in_thread(gate.consumer, 0, results, 'other_with_zero')
            assert not other_done.wait(
                0.5
            ), f"other_with_zero returned while the holder held the gate: {results['other_with_zero']!r}"
        other.join()
        assert results['other_with_zero'] == 'ok', (
            f"two threads both presenting identity 0 should each fall back "
            f"to their own (different) thread id, collide, and wait rather "
            f"than being rejected, got: {results['other_with_zero']!r}"
        )
    finally:
        gate.consumer.close()


def test_gate_released_after_error_return_path():
    """pause() with a malformed partitions argument raises TypeError deep
    inside Consumer_pause(). The gate must still be released there, rather than
    being rejected by a stuck gate_owner left behind by the failed call."""
    c = _new_consumer()
    results = {}

    def bad_call():
        try:
            c.pause('not-an-int')
            results['bad_call'] = 'no error?!'
        except Exception as e:
            results['bad_call'] = e

    def follow_up():
        try:
            c.pause([])
            results['follow_up'] = 'ok'
        except Exception as e:
            results['follow_up'] = e

    try:
        t1 = threading.Thread(target=bad_call)
        t1.start()
        t1.join()

        assert isinstance(
            results['bad_call'], TypeError
        ), f"expected pause('not-an-int') to raise TypeError, got: {results['bad_call']!r}"

        t2 = threading.Thread(target=follow_up)
        t2.start()
        t2.join()

        print(f"results={results}")
        assert (
            results['follow_up'] == 'ok'
        ), f"the gate must be released even though the previous call on another thread errored out, got: \
{results['follow_up']!r}"
    finally:
        c.close()


def _call_subscribe(c):
    c.subscribe(['test-topic'])


def _call_unsubscribe(c):
    c.subscribe(['test-topic'])
    c.unsubscribe()


def _call_assign(c):
    c.assign([])


def _call_unassign(c):
    c.unassign([])


def _call_incremental_assign(c):
    c.incremental_assign([])


def _call_incremental_unassign(c):
    c.incremental_unassign([])


def _call_store_offsets(c):
    c.store_offsets(offsets=[])


def _call_position(c):
    c.position([])


def _call_pause(c):
    c.pause([])


def _call_resume(c):
    c.resume([])


def _call_get_watermark_offsets_cached(c):
    c.get_watermark_offsets(TopicPartition('test-topic', 0), cached=True)


def _call_poll(c):
    c.poll(0.05)


def _call_consume(c):
    c.consume(num_messages=1, timeout=0.05)


_LOCAL_ONLY_METHODS = [
    pytest.param(_call_subscribe, id='subscribe'),
    pytest.param(_call_unsubscribe, id='unsubscribe'),
    pytest.param(_call_assign, id='assign'),
    pytest.param(_call_unassign, id='unassign'),
    pytest.param(_call_incremental_assign, id='incremental_assign'),
    pytest.param(_call_incremental_unassign, id='incremental_unassign'),
    pytest.param(_call_store_offsets, id='store_offsets'),
    pytest.param(_call_position, id='position'),
    pytest.param(_call_pause, id='pause'),
    pytest.param(_call_resume, id='resume'),
    pytest.param(_call_get_watermark_offsets_cached, id='get_watermark_offsets_cached'),
    pytest.param(_call_poll, id='poll'),
    pytest.param(_call_consume, id='consume'),
]


@pytest.mark.parametrize('call_method', _LOCAL_ONLY_METHODS)
def test_gate_released_after_every_local_method(call_method):
    """Sweep of every gated method that doesn't need a real broker to
    return: each is called on one thread, then a different thread must be
    able to immediately acquire the gate afterward. Every one of the ~20
    on every return path, not just the ones exercised elsewhere."""
    c = _new_consumer()
    results = {}

    def first_call():
        try:
            call_method(c)
            results['first_call'] = 'ok'
        except Exception as e:
            results['first_call'] = e

    def follow_up():
        try:
            c.pause([])
            results['follow_up'] = 'ok'
        except Exception as e:
            results['follow_up'] = e

    try:
        t1 = threading.Thread(target=first_call)
        t1.start()
        t1.join()

        t2 = threading.Thread(target=follow_up)
        t2.start()
        t2.join()

        print(f"results={results}")
        assert results['follow_up'] == 'ok', (
            f"gate was not released after {call_method.__name__} "
            f"(result: {results.get('first_call')!r}), got: {results['follow_up']!r}"
        )
    finally:
        c.close()


def test_concurrent_call_waits_while_poll_in_progress():
    """A call from a genuinely different thread while poll() holds the gate
    must wait for it rather than being rejected, then succeed once poll()
    returns and releases the gate."""
    results = {}
    gate = _GateHolder()
    try:
        with gate:
            other, done = _gated_call_in_thread(gate.consumer, 0, results, 'pause')
            assert not done.wait(0.5), f"pause() returned while poll() held the gate: {results['pause']!r}"
        other.join()
        assert (
            results['pause'] == 'ok'
        ), f"expected pause() to wait out poll()'s gate hold and then succeed, got: {results['pause']!r}"

        # The gate must be free again now that poll() has returned.
        gate.consumer.pause([])
        gate.consumer.resume([])
    finally:
        gate.consumer.close()


def test_many_waiters_all_eventually_succeed():
    """The gate serializes access, so under many-way contention every waiter
    must eventually get its turn. Each holder/waiter runs on its own thread
    and all stay alive for the test's duration, so their natural
    PyThread_get_thread_ident() fallback already gives every one of them a
    distinct identity -- no need to set cimpl._reentry_identity_var here."""
    c = _new_consumer()
    results = {}
    n_waiters = 20

    def holder():
        results['holder'] = c.poll(1.0)

    def waiter(idx):
        try:
            c.pause([])
            results[idx] = 'ok'
        except Exception as e:
            results[idx] = e

    try:
        t_holder = threading.Thread(target=holder)
        t_holder.start()
        time.sleep(0.1)  # let the holder comfortably enter the gate first

        waiters = [threading.Thread(target=waiter, args=(i,)) for i in range(n_waiters)]
        for t in waiters:
            t.start()

        t_holder.join(timeout=5)
        for t in waiters:
            t.join(timeout=5)

        assert results['holder'] is None
        failures = {i: results.get(i) for i in range(n_waiters) if results.get(i) != 'ok'}
        assert not failures, f"expected all {n_waiters} waiters to eventually succeed, got: {failures}"
    finally:
        c.close()
