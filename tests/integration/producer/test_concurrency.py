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

import inspect
import os
import signal
import threading
import time
from uuid import uuid1

from confluent_kafka import KafkaError, KafkaException
from tests.common import TestConsumer


def called_by():
    return inspect.stack()[1].function


def prefixed_error_cb(prefix):
    def error_cb(err):
        """Reports global/generic errors to aid in troubleshooting test failures."""
        print("[{}]: {}".format(prefix, err))

    return error_cb


class TestCloseRace:
    def test_close_delivers_in_flight_messages(self, kafka_cluster):
        """
        Messages produced just before/during a concurrent close() are genuinely delivered,
        not silently dropped.
        """
        topic = kafka_cluster.create_topic_and_wait_propogation("test_close_delivery")
        producer = kafka_cluster.producer({'error_cb': prefixed_error_cb('test_close_delivers_in_flight_messages')})

        delivered = []
        delivery_errors = []

        def on_delivery(err, msg):
            if err:
                delivery_errors.append(err)
            else:
                delivered.append(msg)

        produced_count = 0
        closed_runtime_error = None

        def produce_loop():
            nonlocal produced_count, closed_runtime_error
            while True:
                try:
                    producer.produce(topic, value=f'msg-{produced_count}'.encode(), on_delivery=on_delivery)
                    producer.poll(0)
                    produced_count += 1
                except RuntimeError as e:
                    closed_runtime_error = e
                    break

        t = threading.Thread(target=produce_loop)
        t.start()

        # Give the worker thread a moment to actually start producing before
        # racing close() against it.
        time.sleep(0.1)

        print(f"{called_by()}: calling close() while produce_loop is running")
        assert producer.close() is True

        t.join(timeout=30)
        assert not t.is_alive(), "producer thread did not finish after close()"
        print(
            f"{called_by()}: produced_count={produced_count}, "
            f"delivered={len(delivered)}, delivery_errors={len(delivery_errors)}"
        )

        # The worker thread must have hit the "Producer has been closed"
        # RuntimeError. This proves close() genuinely raced an
        # in-flight produce().
        assert closed_runtime_error is not None, "worker thread never hit the closed-producer RuntimeError"
        assert not delivery_errors, f"unexpected delivery errors: {delivery_errors}"
        assert produced_count > 0, "no messages were produced"
        assert (
            len(delivered) == produced_count
        ), f"expected all {produced_count} produced messages to be delivered, got {len(delivered)}"

    def test_close_internal_flush_delivers_all_undelivered_messages(self, kafka_cluster):
        """Produce a batch of messages with no poll() in between (so none
        of their delivery reports have been serviced yet), then call
        close() directly. close()'s own internal flush must drive
        delivery of every one of them."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_close_internal_flush_delivery")
        producer = kafka_cluster.producer(
            {'error_cb': prefixed_error_cb('test_close_internal_flush_delivers_all_undelivered_messages')}
        )

        num_messages = 200
        delivered = []
        delivery_errors = []

        def on_delivery(err, msg):
            if err:
                delivery_errors.append(err)
            else:
                delivered.append(msg)

        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode(), on_delivery=on_delivery)
        # Deliberately no poll() here
        assert len(producer) == num_messages, "messages must still be queued (undelivered) when close() is called"

        print(f"{called_by()}: calling close() to flush {num_messages} undelivered messages")
        result = producer.close()

        print(
            f"{called_by()}: close()={result}, delivered={len(delivered)}, " f"delivery_errors={len(delivery_errors)}"
        )
        assert result is True
        assert not delivery_errors, f"unexpected delivery errors: {delivery_errors}"
        assert len(delivered) == num_messages, f"expected all {num_messages} messages delivered, got {len(delivered)}"

    def test_concurrent_flush_from_multiple_threads(self, kafka_cluster):
        """
        Two threads calling flush() concurrently --
        confirms librdkafka's own atomic flush counter (rd_kafka_flush)
        correctly waits for all outstanding messages across both callers,
        not just its own, and neither returns early.
        """
        topic = kafka_cluster.create_topic_and_wait_propogation("test_concurrent_flush")
        producer = kafka_cluster.producer(
            {'error_cb': prefixed_error_cb('test_concurrent_flush_from_multiple_threads')}
        )

        num_messages = 500
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.poll(0)

        flush_results = []

        def flush_worker():
            remaining = producer.flush(30)
            flush_results.append(remaining)

        threads = [threading.Thread(target=flush_worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=35)

        print(f"{called_by()}: flush_results={flush_results}, len(producer)={len(producer)}")
        assert all(not t.is_alive() for t in threads), "a flush() thread did not finish"
        assert all(
            r == 0 for r in flush_results
        ), f"flush() returned early with messages still outstanding: {flush_results}"
        assert len(producer) == 0

    def test_close_propagates_signal_while_waiting_for_active_calls(self, kafka_cluster):
        """A signal during close()'s active_calls wait raises
        KeyboardInterrupt and resets `closing`, but does not unblock an
        already-held poll() -- it keeps running, unaware anything
        happened. Only a second, uninterrupted close() call actually
        finishes the job."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_close_signal_interruption")
        producer = kafka_cluster.producer(
            {'error_cb': prefixed_error_cb('test_close_propagates_signal_while_waiting_for_active_calls')}
        )

        callback_started = threading.Event()

        def slow_on_delivery(err, msg):
            callback_started.set()
            time.sleep(2)

        producer.produce(topic, value=b'msg', on_delivery=slow_on_delivery)

        poll_started = threading.Event()

        def hold_active_call():
            poll_started.set()
            try:
                producer.poll(10)
            except BaseException:  # noqa: BLE001 - just draining the thread, not asserting here
                pass

        t = threading.Thread(target=hold_active_call, daemon=True)
        t.start()
        poll_started.wait()
        callback_started.wait(timeout=10)
        assert callback_started.is_set(), "delivery callback never started"

        def send_sigint_soon():
            time.sleep(0.5)  # comfortably inside the callback's 2s sleep
            os.kill(os.getpid(), signal.SIGINT)

        interrupt_thread = threading.Thread(target=send_sigint_soon, daemon=True)
        interrupt_thread.start()

        try:
            producer.close()
            assert False, "close() returned normally instead of raising KeyboardInterrupt"
        except KeyboardInterrupt:
            pass

        # The interrupted attempt must NOT have unblocked the held poll():
        # `closing` was reset to 0 before poll() ever got a chance to see
        # it, so poll() has no reason to exit early and must still be
        # running its own call.
        assert t.is_alive(), "poll() thread must still be running right after the interrupted close() attempt"

        # A second, uninterrupted close() call is required to actually
        # finish the job -- it must succeed, and this time the held
        # poll() must notice `closing` and exit, letting the holder thread
        # finish.
        assert producer.close() is True, "close() retried after a signal-interrupted attempt must return True"

        t.join(timeout=10)
        assert not t.is_alive(), "poll() thread did not finish after the successful retry of close()"

    def test_close_races_close_losers_wait_for_slow_winner(self, kafka_cluster):
        """Losing close() calls must actually block until the winner
        finishes, not just happen to observe self->rk already NULL. A
        slow delivery callback forces the CAS winner and every loser waiting behind it
        to spin through the drain-wait loop."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_close_race_slow_winner")
        producer = kafka_cluster.producer(
            {'error_cb': prefixed_error_cb('test_close_races_close_losers_wait_for_slow_winner')}
        )

        callback_started = threading.Event()

        def slow_on_delivery(err, msg):
            callback_started.set()
            time.sleep(2)

        producer.produce(topic, value=b'msg', on_delivery=slow_on_delivery)

        poll_started = threading.Event()

        def hold_active_call():
            poll_started.set()
            try:
                producer.poll(10)
            except RuntimeError:
                pass

        holder = threading.Thread(target=hold_active_call, daemon=True)
        holder.start()
        poll_started.wait()
        callback_started.wait(timeout=10)
        assert callback_started.is_set(), "delivery callback never started"

        num_workers = 5
        all_results = []
        start_barrier = threading.Barrier(num_workers)

        def call_close():
            start_barrier.wait()
            all_results.append(producer.close())

        threads = [threading.Thread(target=call_close) for _ in range(num_workers)]
        for t in threads:
            t.start()

        # Watch continuously: as long as the holder is still alive (still
        # holding its active_calls slot via the slow callback), no
        # close() thread should have finished yet.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and holder.is_alive():
            finished_early = [t for t in threads if not t.is_alive()]
            assert not finished_early, (
                f"{len(finished_early)} close() call(s) returned while the slow delivery callback "
                f"was still holding active_calls"
            )
            time.sleep(0.01)

        for t in threads:
            t.join(timeout=10)
        holder.join(timeout=10)

        print(f"{called_by()}: all_results={all_results}")
        assert not holder.is_alive(), "poll() thread did not finish"
        assert all(not t.is_alive() for t in threads), "a close() thread did not finish"
        assert len(all_results) == num_workers, f"expected {num_workers} results, got: {all_results}"
        assert all(all_results), f"every concurrent close() call must return True, got: {all_results}"


class TestTransactionalProducerConcurrency:
    def test_concurrent_produce_during_open_transaction(self, kafka_cluster):
        """produce() is allowed concurrently from multiple threads while a
        transaction is open (only checks an atomic flag, not an exclusive
        mutex like the transaction-state APIs)."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_txn_concurrent_produce")
        producer = kafka_cluster.producer(
            {
                'transactional.id': f'test-txn-concurrent-produce-{uuid1()}',
                'error_cb': prefixed_error_cb('test_concurrent_produce_during_open_transaction'),
            }
        )

        producer.init_transactions()
        producer.begin_transaction()

        num_threads = 8
        messages_per_thread = 50
        errors = []

        def produce_worker(thread_id):
            try:
                for i in range(messages_per_thread):
                    producer.produce(topic, value=f'thread-{thread_id}-msg-{i}'.encode())
                    producer.poll(0)
            except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
                errors.append((thread_id, e))

        threads = [threading.Thread(target=produce_worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        print(f"{called_by()}: {num_threads} threads finished producing, errors={errors}")
        assert all(not t.is_alive() for t in threads), "a produce() thread did not finish"
        assert not errors, f"unexpected exceptions from concurrent produce() during open transaction: {errors}"

        producer.commit_transaction()

        consumer_conf = kafka_cluster.client_conf()
        consumer_conf.update(
            {
                'group.id': str(uuid1()),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'enable.partition.eof': True,
                'isolation.level': 'read_committed',
            }
        )
        consumer = TestConsumer(consumer_conf)
        consumer.subscribe([topic])

        msg_cnt = 0
        eof_reached = False
        while not eof_reached:
            msg = consumer.poll(timeout=10.0)
            assert msg is not None, "timed out waiting for messages"
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_reached = True
                    continue
                raise KafkaException(msg.error())
            msg_cnt += 1
        consumer.close()

        print(f"{called_by()}: consumed msg_cnt={msg_cnt}")
        assert msg_cnt == num_threads * messages_per_thread

    def test_concurrent_calls_to_same_transaction_api(self, kafka_cluster):
        """Two threads both calling commit_transaction() at once: exactly
        one succeeds, the other gets a clean _PREV_IN_PROGRESS error."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_txn_same_api_race")
        producer = kafka_cluster.producer(
            {
                'transactional.id': f'test-txn-same-api-race-{uuid1()}',
                'error_cb': prefixed_error_cb('test_concurrent_calls_to_same_transaction_api'),
            }
        )

        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(topic, value=b'msg')
        producer.flush()

        results = []
        barrier = threading.Barrier(2)

        def call_commit():
            barrier.wait()
            try:
                producer.commit_transaction()
                results.append(True)
            except KafkaException as e:
                results.append(e.args[0])

        threads = [threading.Thread(target=call_commit) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        print(f"{called_by()}: results={results}")
        assert all(not t.is_alive() for t in threads), "a commit_transaction() thread did not finish"
        assert len(results) == 2
        successes = [r for r in results if r is True]
        errors = [r for r in results if r is not True]
        assert len(successes) == 1, f"expected exactly one successful commit, got: {results}"
        assert len(errors) == 1, f"expected exactly one error result, got: {results}"

    def test_close_races_open_transaction(self, kafka_cluster):
        """close() concurrent with an open (uncommitted) transaction:
        close() must not crash or hang, and since the transaction was
        never committed, none of its messages should become visible to
        a read_committed consumer."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_txn_close_race")
        producer = kafka_cluster.producer(
            {
                'transactional.id': f'test-txn-close-race-{uuid1()}',
                'error_cb': prefixed_error_cb('test_close_races_open_transaction'),
            }
        )

        producer.init_transactions()
        producer.begin_transaction()

        produced_count = 0
        closed_runtime_error = None

        def produce_loop():
            nonlocal produced_count, closed_runtime_error
            while True:
                try:
                    producer.produce(topic, value=f'msg-{produced_count}'.encode())
                    producer.poll(0)
                    produced_count += 1
                except RuntimeError as e:
                    closed_runtime_error = e
                    break

        t = threading.Thread(target=produce_loop)
        t.start()

        time.sleep(0.1)
        print(f"{called_by()}: calling close() while a transaction is still open")
        assert producer.close() is True

        t.join(timeout=30)
        assert not t.is_alive(), "producer thread did not finish after close()"
        print(f"{called_by()}: produced_count={produced_count} before close() won the race")
        assert closed_runtime_error is not None, "worker thread never hit the closed-producer RuntimeError"
        assert produced_count > 0, "no messages were produced before close()"

        consumer_conf = kafka_cluster.client_conf()
        consumer_conf.update(
            {
                'group.id': str(uuid1()),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'enable.partition.eof': True,
                'isolation.level': 'read_committed',
            }
        )
        consumer = TestConsumer(consumer_conf)
        consumer.subscribe([topic])

        msg = consumer.poll(timeout=10.0)
        consumer.close()

        print(f"{called_by()}: consumer.poll() returned error={msg.error() if msg else None}")
        assert msg is not None, "timed out waiting for a message/EOF"
        assert msg.error() is not None and msg.error().code() == KafkaError._PARTITION_EOF, (
            "an uncommitted transaction's messages must not be visible to a " "read_committed consumer after close()"
        )

    def test_concurrent_calls_to_different_transaction_apis(self, kafka_cluster):
        """One thread calls commit_transaction() while another calls
        abort_transaction() at once: exactly one succeeds, the other gets
        a clean _CONFLICT error."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_txn_diff_api_race")
        producer = kafka_cluster.producer(
            {
                'transactional.id': f'test-txn-diff-api-race-{uuid1()}',
                'error_cb': prefixed_error_cb('test_concurrent_calls_to_different_transaction_apis'),
            }
        )

        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(topic, value=b'msg')
        producer.flush()

        results = {}
        barrier = threading.Barrier(2)

        def call_commit():
            barrier.wait()
            try:
                producer.commit_transaction()
                results['commit'] = True
            except KafkaException as e:
                results['commit'] = e.args[0]

        def call_abort():
            barrier.wait()
            try:
                producer.abort_transaction()
                results['abort'] = True
            except KafkaException as e:
                results['abort'] = e.args[0]

        t1 = threading.Thread(target=call_commit)
        t2 = threading.Thread(target=call_abort)
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        print(f"{called_by()}: results={results}")
        assert not t1.is_alive() and not t2.is_alive(), "a transaction-ending thread did not finish"
        successes = [k for k, v in results.items() if v is True]
        assert len(successes) == 1, f"expected exactly one of commit/abort to succeed, got: {results}"


class TestReentrantDeliveryCallback:
    """Delivery callbacks run synchronously inside poll()/flush() on
    whatever thread called them. These tests cover a callback calling back
    into the Producer it belongs to, from that same call chain.

    Note: calling close() from within a callback is NOT covered here and is
    NOT supported."""

    def test_delivery_callback_producing_another_message_gets_delivered(self, kafka_cluster):
        """A delivery callback calling produce() again must succeed and the newly produced message must
        itself be delivered"""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_reentrant_produce_from_callback")
        producer = kafka_cluster.producer(
            {'error_cb': prefixed_error_cb('test_delivery_callback_producing_another_message_gets_delivered')}
        )

        first_delivered = []
        second_delivered = []
        produce_again_error = []

        def on_second_delivery(err, msg):
            if err:
                produce_again_error.append(err)
            else:
                second_delivered.append(msg)

        def on_first_delivery(err, msg):
            if err:
                return
            first_delivered.append(msg)
            try:
                producer.produce(topic, value=b'reentrant-produce', on_delivery=on_second_delivery)
            except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
                produce_again_error.append(e)

        producer.produce(topic, value=b'original', on_delivery=on_first_delivery)
        producer.flush(30)

        print(
            f"{called_by()}: first_delivered={len(first_delivered)}, "
            f"second_delivered={len(second_delivered)}, errors={produce_again_error}"
        )
        assert len(first_delivered) == 1, "the original message must be delivered"
        assert not produce_again_error, f"reentrant produce() from the callback failed: {produce_again_error}"
        assert len(second_delivered) == 1, "the message produced from within the callback must itself be delivered"

        producer.close()

    def test_delivery_callback_calling_poll_succeeds_with_single_message_in_flight(self, kafka_cluster):
        """A delivery callback reentrantly calling poll() succeeds without
        raising, when there is only one message in flight.

        Deliberately scoped to a single message: reentrant poll()/flush()
        calls corrupt a shared per-thread TLS slot (CallState_resume never
        restores what CallState_get consumed), and a second, nested
        delivery dispatch on the same thread while that slot is stale hits
        a NULL pointer."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_reentrant_poll_from_callback")
        producer = kafka_cluster.producer(
            {
                'error_cb': prefixed_error_cb(
                    'test_delivery_callback_calling_poll_succeeds_with_single_message_in_flight'
                )
            }
        )

        delivered = []
        reentrant_poll_results = []

        def on_delivery(err, msg):
            if not err:
                delivered.append(msg)
            try:
                reentrant_poll_results.append(producer.poll(0))
            except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
                reentrant_poll_results.append(e)

        producer.produce(topic, value=b'original', on_delivery=on_delivery)
        producer.flush(30)

        print(f"{called_by()}: delivered={len(delivered)}, " f"reentrant_poll_results={reentrant_poll_results}")
        assert len(delivered) == 1, "the message must be delivered"
        assert len(reentrant_poll_results) == 1, "the delivery callback must have fired exactly once"
        assert not isinstance(
            reentrant_poll_results[0], Exception
        ), f"reentrant poll() from the callback raised: {reentrant_poll_results[0]}"

        producer.close()

    def test_multiple_threads_reentrantly_producing_from_callbacks(self, kafka_cluster):
        """Several threads, each producing on the same shared Producer and
        each individually reentering produce() from its own delivery
        callback"""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_reentrant_produce_multi_thread")
        producer = kafka_cluster.producer(
            {'error_cb': prefixed_error_cb('test_multiple_threads_reentrantly_producing_from_callbacks')}
        )

        num_threads = 8
        messages_per_thread = 20
        first_delivered = []
        second_delivered = []
        errors = []
        lock = threading.Lock()

        def make_second_callback():
            def on_second_delivery(err, msg):
                with lock:
                    if err:
                        errors.append(err)
                    else:
                        second_delivered.append(msg)

            return on_second_delivery

        def make_first_callback(thread_id, i):
            def on_first_delivery(err, msg):
                if err:
                    with lock:
                        errors.append(err)
                    return
                with lock:
                    first_delivered.append(msg)
                try:
                    producer.produce(
                        topic,
                        value=f'reentrant-{thread_id}-{i}'.encode(),
                        on_delivery=make_second_callback(),
                    )
                except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
                    with lock:
                        errors.append(e)

            return on_first_delivery

        def worker(thread_id):
            for i in range(messages_per_thread):
                producer.produce(
                    topic,
                    value=f'original-{thread_id}-{i}'.encode(),
                    on_delivery=make_first_callback(thread_id, i),
                )
                producer.poll(0)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        producer.flush(30)

        expected_total = num_threads * messages_per_thread
        print(
            f"{called_by()}: first_delivered={len(first_delivered)}, "
            f"second_delivered={len(second_delivered)}, errors={errors}"
        )
        assert all(not t.is_alive() for t in threads), "a producing thread did not finish"
        assert not errors, f"unexpected errors from concurrent reentrant produce(): {errors}"
        assert (
            len(first_delivered) == expected_total
        ), f"expected all {expected_total} original messages delivered, got {len(first_delivered)}"
        assert (
            len(second_delivered) == expected_total
        ), f"expected all {expected_total} reentrantly-produced messages delivered, got {len(second_delivered)}"

        producer.close()

    def test_delivery_callback_reentrant_produce_during_close_does_not_truncate_flush(self, kafka_cluster):
        """A delivery callback fired from close()'s own internal flush
        calls produce() again. Handle_enter_rk_use() must let
        this reentrant call through (it's the same thread that's running
        close()'s flush) instead of rejecting it."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_reentrant_produce_during_close")
        producer = kafka_cluster.producer(
            {'error_cb': prefixed_error_cb('test_delivery_callback_reentrant_produce_during_close')}
        )

        first_delivered = []
        second_delivered = []
        errors = []

        def on_second_delivery(err, msg):
            if err:
                errors.append(err)
            else:
                second_delivered.append(msg)

        def on_first_delivery(err, msg):
            if err:
                errors.append(err)
                return
            first_delivered.append(msg)
            producer.produce(topic, value=b'reentrant-during-close', on_delivery=on_second_delivery)

        producer.produce(topic, value=b'original', on_delivery=on_first_delivery)
        # Deliberately no poll()/flush() here -- close()'s own internal
        # flush must be what dispatches the delivery callback.
        assert len(producer) == 1, "the original message must still be queued when close() is called"

        result = producer.close()

        print(
            f"{called_by()}: close()={result}, first_delivered={len(first_delivered)}, "
            f"second_delivered={len(second_delivered)}, errors={errors}"
        )
        assert result is True, "close() must complete cleanly despite the reentrant produce() from its callback"
        assert not errors, f"unexpected delivery errors: {errors}"
        assert len(first_delivered) == 1, "the original message must be delivered"
        assert len(second_delivered) == 1, "the reentrantly-produced message must itself be delivered"
