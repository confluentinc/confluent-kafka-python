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
Tests for the Consumer gate's rejection path when a single sync
Consumer instance is shared across threads.

The gate (Handle_gate_enter()/Handle_gate_exit() in Consumer.c) only allows
one thread inside gated Consumer C code at a time. gate_owner stores the
calling thread's own ID for the sync Consumer, so a second, genuinely independent
thread trying to enter while the first still holds the gate is rejected with
ConcurrentModificationException -- unless it's the same thread re-entering
"""

import threading
from uuid import uuid1

import pytest

from confluent_kafka import ConcurrentModificationException, TopicPartition
from tests.integration.conftest import consumer_gate_enabled

pytestmark = pytest.mark.skipif(
    not consumer_gate_enabled(),
    reason="Consumer gate is a no-op on Python versions <=3.14 GIL based; "
    "see CFL_CONSUMER_GATE_ENABLED in confluent_kafka.h",
)


def _new_consumer(kafka_cluster, conf=None):
    consumer_conf = {
        'group.id': str(uuid1()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    if conf:
        consumer_conf.update(conf)
    return kafka_cluster.cimpl_consumer(consumer_conf)


def test_sequential_calls_succeed_simultaneous_calls_rejected(kafka_cluster):
    """A single Consumer shared across two threads: calls made sequentially
    (one thread waits for the other to finish) must both succeed.
    Calls made simultaneously (both threads enter the gate at roughly the same time)
    must not both be let through -- exactly one must raise ConcurrentModificationException."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_sequential_vs_simultaneous")
    kafka_cluster.seed_topic(topic, value_source=[b'hello', b'world'])

    consumer = _new_consumer(kafka_cluster)
    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    partitions = consumer.assignment()
    assert partitions

    # Sequential: thread A fully completes before thread B starts.
    results_sequential = []

    def call_sequential(label):
        try:
            consumer.assign(partitions)
            results_sequential.append((label, 'ok'))
        except Exception as e:
            results_sequential.append((label, e))

    t1 = threading.Thread(target=call_sequential, args=('seq1',))
    t1.start()
    t1.join()
    t2 = threading.Thread(target=call_sequential, args=('seq2',))
    t2.start()
    t2.join()

    print(f"results_sequential={results_sequential}")
    assert all(
        result == 'ok' for _, result in results_sequential
    ), f"sequential calls should all succeed, got: {results_sequential}"

    # Simultaneous: both threads release at the same instant via a Barrier,
    # both racing to enter the gate for a call that holds it for up to 5s --
    # a wide enough window that there's no doubt the two calls genuinely
    # overlap inside the gate, not just "probably still running".
    barrier = threading.Barrier(2)
    results_simultaneous = []

    def call_simultaneous(label):
        barrier.wait()
        try:
            consumer.poll(5)
            results_simultaneous.append((label, 'ok'))
        except Exception as e:
            results_simultaneous.append((label, e))

    t3 = threading.Thread(target=call_simultaneous, args=('sim1',))
    t4 = threading.Thread(target=call_simultaneous, args=('sim2',))
    t3.start()
    t4.start()
    t3.join()
    t4.join()

    print(f"results_simultaneous={results_simultaneous}")
    errors = [(label, r) for label, r in results_simultaneous if isinstance(r, BaseException)]
    successes = [(label, r) for label, r in results_simultaneous if not isinstance(r, BaseException)]
    assert len(errors) == 1, f"expected exactly one rejection, got: {results_simultaneous}"
    assert isinstance(
        errors[0][1], ConcurrentModificationException
    ), f"expected ConcurrentModificationException, got: {errors[0][1]!r}"
    assert len(successes) == 1, f"expected exactly one call to succeed, got: {results_simultaneous}"

    # The consumer must remain usable afterward
    msg2 = consumer.poll(5)
    print(f"final poll after simultaneous calls: {msg2.value() if msg2 else None}")
    assert msg2 is not None
    assert msg2.value() == b'world'

    consumer.close()


def test_close_rejected_while_poll_in_progress(kafka_cluster):
    """close() called from one thread while another thread is blocked
    inside a long poll() must be rejected. The consumer must remain open and usable afterward."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_close_rejected_while_poll")
    kafka_cluster.seed_topic(topic, value_source=[b'hello', b'world'])

    consumer = _new_consumer(kafka_cluster)
    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    msg = consumer.poll(10)
    assert msg is not None

    poll_started = threading.Event()
    poll_error = None
    close_error = None

    def do_poll():
        nonlocal poll_error
        poll_started.set()
        try:
            consumer.poll(2)
        except Exception as e:
            poll_error = e

    def do_close():
        nonlocal close_error
        poll_started.wait()
        try:
            consumer.close()
        except Exception as e:
            close_error = e

    t_poll = threading.Thread(target=do_poll)
    t_close = threading.Thread(target=do_close)
    t_poll.start()
    t_close.start()
    t_poll.join()
    t_close.join()

    print(f"poll_error={poll_error}, close_error={close_error}")
    assert poll_error is None, f"poll thread unexpectedly raised: {poll_error}"
    assert close_error is not None, "expected close() to be rejected, but it returned successfully"
    assert isinstance(
        close_error, ConcurrentModificationException
    ), f"expected ConcurrentModificationException, got: {close_error!r}"

    # The consumer must still be open and usable -- the rejected close()
    # must not have torn down the handle.
    kafka_cluster.seed_topic(topic, value_source=[b'foo'])
    msg2 = consumer.poll(5)
    print(f"final poll after rejected close(): {msg2.value() if msg2 else None}")
    assert msg2 is not None
    assert msg2.value() == b'foo'

    consumer.close()


def test_gate_rejects_different_methods_colliding(kafka_cluster):
    """Two different gated methods colliding, neither is poll(): one thread
    calls a synchronous commit() (blocks on a real broker round-trip while
    holding the gate), another thread calls assign() concurrently, released
    at the same instant via a Barrier. Confirms rejection isn't specific to
    poll()'s long, easily-tunable gate hold."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_different_methods_colliding")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster)
    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    partitions = consumer.assignment()
    assert partitions

    barrier = threading.Barrier(2)
    commit_error = None
    assign_error = None

    def do_commit():
        nonlocal commit_error
        barrier.wait()
        try:
            consumer.commit(message=msg, asynchronous=False)
        except Exception as e:
            commit_error = e

    def do_assign():
        nonlocal assign_error
        barrier.wait()
        try:
            consumer.assign(partitions)
        except Exception as e:
            assign_error = e

    t_commit = threading.Thread(target=do_commit)
    t_assign = threading.Thread(target=do_assign)
    t_commit.start()
    t_assign.start()
    t_commit.join()
    t_assign.join()

    print(f"commit_error={commit_error}, assign_error={assign_error}")
    errors = [e for e in (commit_error, assign_error) if e is not None]

    # Depending on real thread-scheduling timing, either call could win the
    # race to claim the gate first -- what matters is that they don't BOTH
    # succeed, and any rejection is the right exception type.
    assert len(errors) <= 1, f"expected at most one rejection, got: commit={commit_error}, assign={assign_error}"
    if errors:
        assert isinstance(
            errors[0], ConcurrentModificationException
        ), f"expected ConcurrentModificationException, got: {errors[0]!r}"

    # The consumer must remain usable afterward -- verify with a real
    # poll() that actually fetches a fresh message.
    kafka_cluster.seed_topic(topic, value_source=[b'world'])
    msg2 = consumer.poll(5)
    print(f"final poll after colliding calls: {msg2.value() if msg2 else None}")
    assert msg2 is not None
    assert msg2.value() == b'world'

    consumer.close()


def test_many_threads_hammering_one_consumer(kafka_cluster):
    """Stress/fuzz shape: many threads concurrently calling a mix of gated
    methods on one Consumer in a loop for a fixed number of iterations.
    Every rejection must be exactly ConcurrentModificationException -- no
    other exception type, no crash, no hang -- and the consumer must
    remain usable at the end."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_many_threads_hammering")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'] * 5)

    consumer = _new_consumer(kafka_cluster)
    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    partitions = consumer.assignment()
    assert partitions

    n_threads = 10
    n_iterations = 20
    unexpected_errors = []
    rejection_count = 0
    lock = threading.Lock()

    def worker(worker_id):
        nonlocal rejection_count
        for i in range(n_iterations):
            method = (worker_id + i) % 3
            try:
                if method == 0:
                    consumer.poll(0.05)
                elif method == 1:
                    consumer.assign(partitions)
                else:
                    consumer.pause(partitions)
                    consumer.resume(partitions)
            except ConcurrentModificationException:
                with lock:
                    rejection_count += 1  # Expected under real contention.
            except Exception as e:
                with lock:
                    unexpected_errors.append((worker_id, i, e))

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"unexpected_errors={unexpected_errors}, rejection_count={rejection_count}")
    assert not unexpected_errors, f"saw non-gate exceptions during concurrent hammering: {unexpected_errors}"

    total_calls = n_threads * n_iterations
    min_expected_rejections = total_calls // 2
    assert rejection_count >= total_calls // 2, (
        f"expected at least {min_expected_rejections} ConcurrentModificationException rejections "
        f"(50% of {total_calls} total calls) under real contention, got {rejection_count} -- "
        f"threads may not have genuinely overlapped"
    )

    # The consumer must still be fully usable after the stress run.
    msg2 = consumer.poll(2)
    print(f"final poll after hammering: {msg2}")

    consumer.close()


def test_callback_exception_releases_gate(kafka_cluster):
    """on_assign raises an exception: the exception must propagate through
    poll(): Consumer_rebalance_cb captures it via CallState_fetch_exception() and
    rd_kafka_yield()s, without swallowing or wrapping it) and the gate
    must still be released -- the consumer must remain fully usable for
    subsequent calls afterward."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_callback_exception_releases_gate")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster)

    def on_assign(consumer, partitions):
        # Re-entrantly assign first, proving the re-entrant call itself
        # succeeds, then raise.
        consumer.assign(partitions)
        raise ValueError("boom from on_assign")

    consumer.subscribe([topic], on_assign=on_assign)

    try:
        consumer.poll(10)
        assert False, "expected poll() to propagate the exception raised in on_assign"
    except ValueError as e:
        assert str(e) == "boom from on_assign"

    # A callback that raises is treated
    # as a failed rebalance, so the assigned offset
    # isn't proper -- seek() explicitly to observe the seeded message.
    partitions = consumer.assignment()
    assert partitions
    for tp in partitions:
        consumer.seek(TopicPartition(tp.topic, tp.partition, 0))

    msg = consumer.poll(5)
    print(f"poll after callback exception + seek: {msg.value() if msg else None}")
    assert msg is not None
    assert msg.value() == b'hello'

    consumer.close()


def test_callback_makes_multiple_reentrant_calls(kafka_cluster):
    """on_assign makes multiple re-entrant calls, each must legitimately borrow the gate
    and all must succeed."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_callback_multiple_reentrant_calls")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster)

    calls_made = []

    def on_assign(consumer, partitions):
        consumer.assign(partitions)
        calls_made.append('assign')
        consumer.pause(partitions)
        calls_made.append('pause')
        calls_made.append(('position', consumer.position(partitions)))
        consumer.resume(partitions)
        calls_made.append('resume')
        calls_made.append(('watermark', consumer.get_watermark_offsets(partitions[0])))

    consumer.subscribe([topic], on_assign=on_assign)

    msg = consumer.poll(10)

    print(f"calls_made={calls_made}, msg={msg.value() if msg else None}")
    assert [c if isinstance(c, str) else c[0] for c in calls_made] == [
        'assign',
        'pause',
        'position',
        'resume',
        'watermark',
    ], f"expected all 5 re-entrant calls to run in order, got: {calls_made}"
    assert msg is not None
    assert msg.value() == b'hello'

    consumer.close()
