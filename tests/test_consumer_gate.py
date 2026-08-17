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

from confluent_kafka import ConcurrentModificationException, TopicPartition, cimpl
from tests.common import TestConsumer
from tests.integration.conftest import consumer_gate_enabled

pytestmark = pytest.mark.skipif(
    not consumer_gate_enabled(),
    reason="Consumer gate is a no-op on Python versions <=3.14 GIL based; "
    "see CFL_CONSUMER_GATE_ENABLED in confluent_kafka.h",
)


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


def test_identity_match_admits_across_threads_mismatch_rejects():
    """The gate's admission rule is "identity. Confirmed
    here directly against the sync Consumer: two different OS threads
    presenting the SAME identity via cimpl._reentry_identity_var are both admitted,
    but a third thread presenting a different identity collides and is rejected."""
    c = _new_consumer()
    results = {}

    def holder():
        cimpl._reentry_identity_var.set(4242)
        results['holder'] = c.poll(1.0)

    def same_identity():
        # Give the holder time to enter the gate and be solidly inside its
        # 1-second poll() -- see the module docstring on why poll() against
        # an unreachable broker is a reliable, deterministic gate hold.
        time.sleep(0.2)
        cimpl._reentry_identity_var.set(4242)
        try:
            c.pause([])
            results['same_identity'] = 'ok'
        except Exception as e:
            results['same_identity'] = e

    def different_identity():
        time.sleep(0.2)
        cimpl._reentry_identity_var.set(9999)
        try:
            c.pause([])
            results['different_identity'] = 'ok'
        except Exception as e:
            results['different_identity'] = e

    try:
        threads = [
            threading.Thread(target=holder),
            threading.Thread(target=same_identity),
            threading.Thread(target=different_identity),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        print(f"results={results}")
        assert results['holder'] is None
        assert results['same_identity'] == 'ok', (
            f"same identity from a different thread should be admitted as "
            f"re-entrant, got: {results['same_identity']!r}"
        )
        assert isinstance(results['different_identity'], ConcurrentModificationException), (
            f"a different identity should collide with the gate's current "
            f"owner, got: {results['different_identity']!r}"
        )
    finally:
        c.close()


def test_zero_identity_falls_back_to_own_thread():
    """0 is documented as never a legitimate identity --
    it must be treated the same as "not set", falling back to the calling thread's own id.
    Two different threads both explicitly presenting identity 0 must still collide with each
    other."""
    c = _new_consumer()
    results = {}

    def holder():
        cimpl._reentry_identity_var.set(0)
        results['holder'] = c.poll(1.0)

    def other_with_zero():
        time.sleep(0.2)
        cimpl._reentry_identity_var.set(0)
        try:
            c.pause([])
            results['other_with_zero'] = 'ok'
        except Exception as e:
            results['other_with_zero'] = e

    try:
        t1 = threading.Thread(target=holder)
        t2 = threading.Thread(target=other_with_zero)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        print(f"results={results}")
        assert results['holder'] is None
        assert isinstance(results['other_with_zero'], ConcurrentModificationException), (
            f"two threads both presenting identity 0 should each fall back "
            f"to their own (different) thread id and collide, got: "
            f"{results['other_with_zero']!r}"
        )
    finally:
        c.close()


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


def test_concurrent_call_rejected_while_poll_in_progress():
    """A long poll() holds the gate for a known, deterministic duration.
    A call from a genuinely different thread launched partway through
    must be rejected; the consumer must remain usable once poll() returns
    and releases the gate."""
    c = _new_consumer()
    results = {}

    def do_poll():
        results['poll'] = c.poll(2)

    def do_pause():
        time.sleep(0.2)
        try:
            c.pause([])
            results['pause'] = 'ok'
        except Exception as e:
            results['pause'] = e

    try:
        t1 = threading.Thread(target=do_poll)
        t2 = threading.Thread(target=do_pause)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        print(f"results={results}")
        assert results['poll'] is None
        assert isinstance(
            results['pause'], ConcurrentModificationException
        ), f"expected pause() to collide with poll()'s gate hold, got: {results['pause']!r}"

        # The gate must be free again now that poll() has returned.
        c.pause([])
        c.resume([])
    finally:
        c.close()
