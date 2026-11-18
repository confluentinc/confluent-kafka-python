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
Tests for the Consumer gate's serialization behavior when a single sync
Consumer instance is shared across threads.

The gate (Handle_gate_enter()/Handle_gate_exit() in Consumer.c) only allows
one thread inside gated Consumer C code at a time. gate_owner stores the
calling thread's own ID for the sync Consumer, so a second, genuinely
independent thread trying to enter while the first still holds the gate
waits (GIL released) until the gate frees up, then proceeds -- unless it's
the same thread re-entering, which is admitted immediately.
"""

import random
import threading
import time
from uuid import uuid1

from confluent_kafka import TopicPartition


def _new_consumer(kafka_cluster, conf=None):
    consumer_conf = {
        'group.id': str(uuid1()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    if conf:
        consumer_conf.update(conf)
    return kafka_cluster.cimpl_consumer(consumer_conf)


def _subscribed_consumer(kafka_cluster, topic_name, messages):
    """Create and seed a topic, then return a Consumer that has subscribed,
    consumed the first message, and been assigned partitions."""
    topic = kafka_cluster.create_topic_and_wait_propogation(topic_name)
    kafka_cluster.seed_topic(topic, value_source=messages)

    consumer = _new_consumer(kafka_cluster)
    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    partitions = consumer.assignment()
    assert partitions
    return consumer, topic, msg, partitions


def test_simultaneous_calls_on_shared_consumer_serialize(kafka_cluster):
    """A single Consumer shared across two threads: calls made simultaneously
     must both succeed -- one just waits for the other to release the gate first.
    Both threads release at the same instant via a Barrier, both racing to
    enter the gate for a call that holds it for up to 2s."""
    consumer, topic, _msg, _partitions = _subscribed_consumer(
        kafka_cluster, "test_sequential_vs_simultaneous", [b'hello']
    )

    barrier = threading.Barrier(2)
    results_simultaneous = {}
    timings_simultaneous = {}

    def call_simultaneous(label):
        barrier.wait()
        t0 = time.time()
        try:
            consumer.poll(2)
            results_simultaneous[label] = 'ok'
        except Exception as e:
            results_simultaneous[label] = e
        timings_simultaneous[label] = time.time() - t0

    t3 = threading.Thread(target=call_simultaneous, args=('sim1',))
    t4 = threading.Thread(target=call_simultaneous, args=('sim2',))
    t3.start()
    t4.start()
    t3.join()
    t4.join()

    print(f"results_simultaneous={results_simultaneous}, timings_simultaneous={timings_simultaneous}")
    assert all(
        r == 'ok' for r in results_simultaneous.values()
    ), f"both simultaneous calls should eventually succeed, got: {results_simultaneous}"
    # One of the two was queued behind the other's gate hold; it must have
    # taken close to double the individual poll() timeout, not slipped
    # through immediately.
    assert max(timings_simultaneous.values()) >= 3.0, (
        f"expected one of the two calls to wait out the other's full gate "
        f"hold before proceeding, got timings: {timings_simultaneous}"
    )

    # The consumer must remain usable afterward -- verify with a real poll().
    kafka_cluster.seed_topic(topic, value_source=[b'final'])
    msg2 = consumer.poll(5)
    print(f"final poll after simultaneous calls: {msg2.value() if msg2 else None}")
    assert msg2 is not None
    assert msg2.value() == b'final'

    consumer.close()


def test_independent_consumers_do_not_block_each_other(kafka_cluster):
    """The gate is scoped per Consumer instance.
    Two unrelated Consumer objects must never contend for the same gate:
    one consumer holds its own gate for a long poll() while
    a completely independent consumer makes a quick, local call at the same
    instant. That second call must return immediately rather than queuing
    behind the first."""
    consumer_a, _topic_a, _msg_a, _partitions_a = _subscribed_consumer(
        kafka_cluster, "test_independent_consumers_a", [b'hello']
    )
    consumer_b, _topic_b, _msg_b, partitions_b = _subscribed_consumer(
        kafka_cluster, "test_independent_consumers_b", [b'hello']
    )

    barrier = threading.Barrier(2)
    result_b = None
    elapsed_b = None

    def hold_a():
        barrier.wait()
        consumer_a.poll(2)

    def quick_b():
        nonlocal result_b, elapsed_b
        barrier.wait()
        t0 = time.time()
        try:
            consumer_b.position(partitions_b)
            result_b = 'ok'
        except Exception as e:
            result_b = e
        elapsed_b = time.time() - t0

    t_a = threading.Thread(target=hold_a)
    t_b = threading.Thread(target=quick_b)
    t_a.start()
    t_b.start()
    t_a.join()
    t_b.join()

    print(f"result_b={result_b}, elapsed_b={elapsed_b}")
    assert result_b == 'ok', f"expected consumer_b's call to succeed, got: {result_b!r}"
    assert elapsed_b < 0.5, (
        f"consumer_b's call took {elapsed_b:.2f}s -- it should have returned "
        f"immediately rather than waiting out consumer_a's unrelated gate "
        f"hold, which would mean the gate leaked across independent "
        f"Consumer instances"
    )

    consumer_a.close()
    consumer_b.close()


def test_close_waits_for_poll_then_succeeds(kafka_cluster):
    """close() called from one thread while another thread is blocked
    inside a long poll() must wait for poll() to release the gate, then
    close() genuinely proceeds. The blocking poll() is configured with a
    stats_cb that fires well before poll()'s own timeout, proving the
    callback runs mid-poll -- and that its running doesn't release the gate
    early: close() must still wait out poll()'s full hold, not just until
    the first callback returns."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_close_waits_for_poll")
    kafka_cluster.seed_topic(topic, value_source=[b'hello', b'world'])

    poll_start = None
    cb_fired = threading.Event()
    cb_elapsed = None

    def stats_cb(json_str):
        nonlocal cb_elapsed
        if poll_start is not None and not cb_fired.is_set():
            cb_elapsed = time.time() - poll_start
            cb_fired.set()

    consumer = _new_consumer(kafka_cluster, {'statistics.interval.ms': 100, 'stats_cb': stats_cb})
    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    msg = consumer.poll(10)
    assert msg is not None

    poll_error = None
    close_error = None
    close_elapsed = None

    def do_poll():
        nonlocal poll_error, poll_start
        poll_start = time.time()
        try:
            consumer.poll(2)
        except Exception as e:
            poll_error = e

    def do_close():
        nonlocal close_error, close_elapsed
        # A short sleep so that poll() enters the gate first.
        time.sleep(0.2)
        t0 = time.time()
        try:
            consumer.close()
        except Exception as e:
            close_error = e
        close_elapsed = time.time() - t0

    t_poll = threading.Thread(target=do_poll)
    t_close = threading.Thread(target=do_close)
    t_poll.start()
    t_close.start()
    t_poll.join()
    t_close.join()

    print(
        f"poll_error={poll_error}, close_error={close_error}, "
        f"close_elapsed={close_elapsed}, cb_elapsed={cb_elapsed}"
    )
    assert cb_fired.is_set(), "stats_cb never fired during the blocking poll()"
    assert cb_elapsed < 1.0, (
        f"stats_cb fired {cb_elapsed:.2f}s into poll() -- expected well "
        f"before poll()'s own 2s timeout, proving it ran mid-poll rather "
        f"than only after poll() returned"
    )
    assert poll_error is None, f"poll thread unexpectedly raised: {poll_error}"
    assert close_error is None, f"expected close() to wait and then succeed, got: {close_error!r}"
    # close() started as soon as poll() began; poll() releases the gate at
    # ~2s. stats_cb firing mid-poll must not have released the gate early,
    # so close() must have actually waited out the full hold rather than
    # racing it.
    assert close_elapsed >= 1.0, (
        f"close() returned in {close_elapsed:.2f}s -- too fast to have " f"actually waited for poll()'s gate hold"
    )


def test_gate_serializes_different_methods_colliding(kafka_cluster):
    """Two different gated methods colliding: commit() and position()
    are released together via a Barrier. This test doesn't assume who
    wins the race to the gate; it only asserts that whichever call waits,
    it waits successfully rather than being rejected.
    """
    consumer, topic, msg, partitions = _subscribed_consumer(
        kafka_cluster, "test_different_methods_colliding", [b'hello']
    )

    barrier = threading.Barrier(2)
    commit_error = None
    position_error = None

    def do_commit():
        nonlocal commit_error
        barrier.wait()
        try:
            consumer.commit(message=msg, asynchronous=False)
        except Exception as e:
            commit_error = e

    def do_position():
        nonlocal position_error
        barrier.wait()
        try:
            consumer.position(partitions)
        except Exception as e:
            position_error = e

    t_commit = threading.Thread(target=do_commit)
    t_position = threading.Thread(target=do_position)
    t_commit.start()
    t_position.start()
    t_commit.join()
    t_position.join()

    print(f"commit_error={commit_error}, position_error={position_error}")
    assert commit_error is None, f"commit() unexpectedly failed: {commit_error!r}"
    assert position_error is None, f"expected position() to wait and then succeed, got: {position_error!r}"

    # The consumer must remain usable afterward.
    # Verify with a real poll() that actually fetches a message.
    kafka_cluster.seed_topic(topic, value_source=[b'world'])
    msg2 = consumer.poll(5)
    print(f"final poll after colliding calls: {msg2.value() if msg2 else None}")
    assert msg2 is not None
    assert msg2.value() == b'world'

    consumer.close()


def test_many_threads_hammering_one_consumer(kafka_cluster):
    """Stress/fuzz shape: many threads concurrently calling a mix of gated
    methods on one Consumer in a loop for a fixed number of iterations.
    Every single call across every thread and iteration must eventually succeed,
    with no crash and no thread left hanging."""
    consumer, topic, _msg, partitions = _subscribed_consumer(
        kafka_cluster, "test_many_threads_hammering", [b'hello'] * 5
    )

    n_threads = 10
    n_iterations = 20
    unexpected_errors = []
    completed_calls = 0
    lock = threading.Lock()

    def worker(worker_id):
        nonlocal completed_calls
        for i in range(n_iterations):
            method = (worker_id + i) % 3
            try:
                if method == 0:
                    consumer.poll(0.05)
                elif method == 1:
                    consumer.position(partitions)
                else:
                    consumer.pause(partitions)
                    consumer.resume(partitions)
                with lock:
                    completed_calls += 1
            except Exception as e:
                with lock:
                    unexpected_errors.append((worker_id, i, e))

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    still_alive = [t.name for t in threads if t.is_alive()]
    assert not still_alive, f"expected all worker threads to finish, still running: {still_alive}"

    print(f"unexpected_errors={unexpected_errors}, completed_calls={completed_calls}")
    assert not unexpected_errors, f"saw exceptions during concurrent hammering: {unexpected_errors}"

    total_calls = n_threads * n_iterations
    assert completed_calls == total_calls, (
        f"expected all {total_calls} calls across {n_threads} threads to "
        f"complete, got {completed_calls} -- some may have been starved "
        f"by the gate's fixed-interval retry"
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


def test_list_topics_racing_close(kafka_cluster):
    """consumer.list_topics() is a common API that routes through the dispatcher,
    which for a Consumer takes the serialize gate. A worker hammers
    list_topics() while the main thread closes the consumer at a randomized
    moment; across many rounds every call must return metadata or, once the
    consumer is closed, raise cleanly -- never crash or hang.

    The worker does time.sleep(0) each iteration to yield the GIL; without it a
    GIL-enabled run lets this tight loop re-grab the gate before the concurrent
    close() can, starving close() (with the GIL off, close() races the free
    window on another core and doesn't need the yield)."""
    rounds = 20
    errors = []

    for r in range(rounds):
        consumer = _new_consumer(kafka_cluster)
        start = threading.Barrier(2)
        stop = threading.Event()

        def worker(consumer=consumer, start=start, stop=stop, r=r):
            try:
                start.wait()
                while not stop.is_set():
                    try:
                        consumer.list_topics(timeout=1)
                        time.sleep(0)  # yield the GIL so close() isn't starved
                    except RuntimeError as e:
                        assert "Handle has been closed" in str(e), f"round {r}: unexpected RuntimeError: {e!r}"
                        break
            except Exception as e:  # noqa: BLE001
                errors.append((r, e))

        t = threading.Thread(target=worker)
        t.start()
        start.wait()
        time.sleep(random.uniform(0.0, 0.05))
        consumer.close()
        stop.set()
        t.join(timeout=10)
        assert not t.is_alive(), f"round {r}: worker thread hung after close()"

    assert not errors, f"unexpected errors racing list_topics() vs close(): {errors}"


def test_list_topics_serializes_with_poll(kafka_cluster):
    """list_topics() takes the same serialize gate as poll(), so the two
    colliding on one shared Consumer must serialize. poll() enters the gate
    first and holds it for its full 10s, list_topics() arrives second and
    must wait behind it. Both then succeed and the consumer stays usable."""
    consumer, topic, _msg, _partitions = _subscribed_consumer(kafka_cluster, "test_list_topics_vs_poll", [b'hello'])

    poll_error = None
    list_error = None
    list_result = None
    list_elapsed = None

    def do_poll():
        nonlocal poll_error
        try:
            consumer.poll(10)
        except Exception as e:  # noqa: BLE001
            poll_error = e

    def do_list_topics():
        nonlocal list_error, list_result, list_elapsed
        # Let poll() enter the gate first, so list_topics() must wait for it.
        time.sleep(0.5)
        t0 = time.time()
        try:
            list_result = consumer.list_topics(timeout=2)
        except Exception as e:  # noqa: BLE001
            list_error = e
        list_elapsed = time.time() - t0

    t_poll = threading.Thread(target=do_poll)
    t_list = threading.Thread(target=do_list_topics)
    t_poll.start()
    t_list.start()
    t_poll.join()
    t_list.join()

    print(f"poll_error={poll_error}, list_error={list_error}, list_elapsed={list_elapsed}")
    assert poll_error is None, f"poll() unexpectedly failed: {poll_error!r}"
    assert list_error is None, f"expected list_topics() to wait and then succeed, got: {list_error!r}"
    assert list_result is not None and topic in list_result.topics, "list_topics() did not return the expected metadata"
    # poll() holds the gate for ~10s; list_topics() (2s timeout) started right
    # after and had to wait behind it, so its wall time must far exceed its own
    # timeout -- proof the gate serialized them rather than letting them overlap.
    assert list_elapsed >= 5.0, (
        f"list_topics() returned in {list_elapsed:.2f}s -- too fast to have "
        f"waited behind poll()'s gate hold; the two did not serialize"
    )

    # The consumer must remain usable afterward -- verify with a real poll().
    kafka_cluster.seed_topic(topic, value_source=[b'world'])
    msg2 = consumer.poll(5)
    print(f"final poll after colliding list_topics()/poll(): {msg2.value() if msg2 else None}")
    assert msg2 is not None
    assert msg2.value() == b'world'

    consumer.close()
