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
Integration tests for AIOProducer.
"""

import asyncio
import inspect
import threading
from uuid import uuid1

import pytest

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.aio import AIOProducer
from tests.common import TestConsumer


def called_by():
    return inspect.stack()[1].function


async def _new_aio_producer(kafka_cluster, conf=None, **kwargs):
    producer_conf = kafka_cluster.client_conf(conf or {})
    kwargs.setdefault('buffer_timeout', 0)  # deterministic tests: no background auto-flush unless asked for
    return AIOProducer(producer_conf, **kwargs)


def _read_committed_consumer(kafka_cluster, topic):
    """A read_committed consumer, subscribed and ready to drain a topic."""
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
    return consumer


def _drain_committed(consumer):
    """Consume until EOF on every assigned partition, returning the count of
    non-error messages seen (i.e. records visible under read_committed)."""
    msg_cnt = 0
    eof = {}
    while True:
        msg = consumer.poll(timeout=10.0)
        assert msg is not None, "timed out waiting for messages"
        topic, partition = msg.topic(), msg.partition()
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof[(topic, partition)] = True
                if len(eof) >= len(consumer.assignment()):
                    break
                continue
            raise KafkaException(msg.error())
        eof.pop((topic, partition), None)
        msg_cnt += 1
    return msg_cnt


class TestBasicAsyncProduce:
    """Baseline coverage against a real broker."""

    async def test_batched_produce_delivers_and_resolves_futures(self, kafka_cluster):
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_batched_produce")
        producer = await _new_aio_producer(kafka_cluster)

        try:
            n = 50
            futures = [await producer.produce(topic, key=f'k{i}', value=f'v{i}'.encode()) for i in range(n)]
            await producer.flush()
            msgs = await asyncio.gather(*futures)

            print(f"{called_by()}: delivered={len(msgs)}")
            assert len(msgs) == n
            assert {m.value() for m in msgs} == {f'v{i}'.encode() for i in range(n)}
        finally:
            await producer.close()

    async def test_many_concurrent_produce_futures_all_resolve(self, kafka_cluster):
        """Stress the delivery-Future resolution path under real concurrent delivery
        across multiple topics/partitions and executor worker threads -- every future
        must resolve exactly once, none lost, none duplicated."""
        topics = [
            kafka_cluster.create_topic_and_wait_propogation(f"test_aio_producer_concurrent_futures_{i}")
            for i in range(3)
        ]
        producer = await _new_aio_producer(kafka_cluster, max_workers=8, batch_size=25)

        try:
            n = 300
            futures = [
                await producer.produce(topics[i % len(topics)], key=str(i), value=str(i).encode()) for i in range(n)
            ]
            await producer.flush()
            msgs = await asyncio.gather(*futures)

            print(f"{called_by()}: delivered={len(msgs)}")
            assert len(msgs) == n
            assert {m.value() for m in msgs} == {str(i).encode() for i in range(n)}
        finally:
            await producer.close()

    async def test_partial_batch_failure_resolves_only_failed_future(self, kafka_cluster):
        """produce_batch() rejects an oversized message immediately (client-side
        message.max.bytes check) rather than round-tripping to the broker.
        _handle_partial_failures() must resolve only that message's future with an
        error, leaving the rest of the batch to resolve normally."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_partial_batch_failure")
        producer = await _new_aio_producer(kafka_cluster, conf={'message.max.bytes': 2000})

        try:
            good_future = await producer.produce(topic, value=b'small-message')
            bad_future = await producer.produce(topic, value=b'x' * 10000)
            await producer.flush()

            good_msg = await good_future
            print(f"{called_by()}: good_msg={good_msg.value()}")
            assert good_msg.value() == b'small-message'

            with pytest.raises(KafkaException) as exc_info:
                await bad_future
            err = exc_info.value.args[0]
            print(f"{called_by()}: bad_future error={err}")
            assert err.code() == KafkaError.MSG_SIZE_TOO_LARGE, f"unexpected error for the oversized message: {err}"
        finally:
            await producer.close()

    async def test_async_delivery_failure_resolves_future_with_error(self, kafka_cluster):
        """An out-of-range partition number is NOT caught client-side. produce_batch()
        accepts the message with no '_error' key, and it only fails later through a
        real delivery report. This exercises simple_callback's `if err:` branch via the
        actual dr_cb path."""
        topic = kafka_cluster.create_topic_and_wait_propogation(
            "test_aio_producer_async_delivery_failure", conf={'num_partitions': 1}
        )
        producer = await _new_aio_producer(kafka_cluster, conf={'message.timeout.ms': 5000})

        try:
            good_future = await producer.produce(topic, value=b'good-message')
            bad_future = await producer.produce(topic, value=b'bad-partition', partition=5)
            await producer.flush(10)

            good_msg = await good_future
            print(f"{called_by()}: good_msg={good_msg.value()}")
            assert good_msg.value() == b'good-message'

            with pytest.raises(KafkaException) as exc_info:
                await bad_future
            err = exc_info.value.args[0]
            print(f"{called_by()}: bad_future error={err}")
            assert err.code() == KafkaError._UNKNOWN_PARTITION, f"unexpected error for the bad partition: {err}"
        finally:
            await producer.close()


class TestSharedProducerAcrossThreads:
    """The wrapped sync Producer is explicitly designed to be safely called from multiple
    OS threads concurrently. These tests exercise that contract through AIOProducer's own wrapped
    instance, not just the bare sync Producer."""

    async def test_direct_thread_produce_concurrent_with_async_produce(self, kafka_cluster):
        """Raw OS threads calling produce()/poll() directly on aio_producer._producer,
        concurrently with the event loop driving produce()/flush() through the executor
        on the SAME underlying rk. Both paths must deliver cleanly, no crash."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_shared_across_threads")
        producer = await _new_aio_producer(kafka_cluster)

        direct_delivered = []
        direct_errors = []
        num_threads = 4
        messages_per_thread = 25

        def direct_worker(thread_id):
            def on_delivery(err, msg):
                if err:
                    direct_errors.append(err)
                else:
                    direct_delivered.append(msg)

            for i in range(messages_per_thread):
                producer._producer.produce(topic, value=f'direct-{thread_id}-{i}'.encode(), on_delivery=on_delivery)
                producer._producer.poll(0)

        threads = [threading.Thread(target=direct_worker, args=(i,), daemon=True) for i in range(num_threads)]

        try:
            for t in threads:
                t.start()

            async_futures = [await producer.produce(topic, value=f'async-{i}'.encode()) for i in range(50)]
            await producer.flush()
            async_msgs = await asyncio.gather(*async_futures)

            for t in threads:
                t.join(timeout=30)
            assert all(not t.is_alive() for t in threads), "a direct-produce thread did not finish"

            # The direct threads' own poll(0) calls are non-blocking and won't have
            # drained every delivery report by the time they finish producing --
            # keep polling from the async side until the rest arrive.
            for _ in range(100):
                await producer.poll(0.1)
                if len(direct_delivered) + len(direct_errors) == num_threads * messages_per_thread:
                    break

            print(
                f"{called_by()}: async_delivered={len(async_msgs)}, "
                f"direct_delivered={len(direct_delivered)}, direct_errors={direct_errors}"
            )
            assert not direct_errors, f"unexpected errors from direct produce(): {direct_errors}"
            assert len(async_msgs) == 50
        finally:
            await producer.close()


class TestSyncProduceWithDeliveryCallback:
    """AIOProducer.produce() is batch-only and always attaches its own internal
    future-resolving callback. The escape hatch is the wrapped sync Producer itself:
    aio_producer._producer is a plain, unwrapped confluent_kafka.Producer, so on_delivery
    works exactly as it does for the sync client."""

    async def test_direct_produce_with_on_delivery_works(self, kafka_cluster):
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_direct_on_delivery")
        producer = await _new_aio_producer(kafka_cluster)

        delivered = []
        errors = []

        def on_delivery(err, msg):
            if err:
                errors.append(err)
            else:
                delivered.append(msg)

        try:
            producer._producer.produce(topic, value=b'direct-value', on_delivery=on_delivery)
            for _ in range(50):
                await producer.poll(0.1)
                if delivered or errors:
                    break

            print(f"{called_by()}: delivered={len(delivered)}, errors={errors}")
            assert not errors, f"unexpected delivery errors: {errors}"
            assert len(delivered) == 1
            assert delivered[0].value() == b'direct-value'
        finally:
            await producer.close()

    async def test_direct_on_delivery_reentrant_produce(self, kafka_cluster):
        """Mirrors the sync Producer's own reentrant-delivery-callback coverage
        (test_delivery_callback_producing_another_message_gets_delivered in
        test_concurrency.py), through AIOProducer's wrapped instance instead of a bare
        Producer -- confirms wrapping doesn't disturb the underlying reentrant-produce
        support."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_direct_on_delivery_reentrant")
        producer = await _new_aio_producer(kafka_cluster)

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
            producer._producer.produce(topic, value=b'reentrant', on_delivery=on_second_delivery)

        try:
            producer._producer.produce(topic, value=b'original', on_delivery=on_first_delivery)
            for _ in range(50):
                await producer.poll(0.1)
                if second_delivered or errors:
                    break

            print(f"{called_by()}: first={len(first_delivered)}, second={len(second_delivered)}, errors={errors}")
            assert not errors, f"unexpected delivery errors: {errors}"
            assert len(first_delivered) == 1
            assert len(second_delivered) == 1
        finally:
            await producer.close()

    async def test_direct_on_delivery_fires_on_unpredictable_worker_thread(self, kafka_cluster):
        """Delivery-report draining is global to the Producer instance, not tied to
        whichever call produced a message. A message produced directly (bypassing
        AIOProducer.produce()) can have its on_delivery callback fired incidentally by
        AIOProducer's own internal batch machinery, on whichever executor worker thread
        happens to be draining the queue."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_direct_on_delivery_thread")
        producer = await _new_aio_producer(kafka_cluster, max_workers=4)
        main_thread_id = threading.get_ident()

        delivery_thread_ids = []
        errors = []

        def on_delivery(err, msg):
            if err:
                errors.append(err)
            else:
                delivery_thread_ids.append(threading.get_ident())

        try:
            # Directly on the event loop thread -- produce() itself must never fire the
            # callback synchronously, it only enqueues.
            producer._producer.produce(topic, value=b'direct', on_delivery=on_delivery)
            assert not delivery_thread_ids, "produce() must not fire the delivery callback synchronously"

            # Drive delivery entirely through AIOProducer's own batched path.
            batched_futures = [await producer.produce(topic, value=f'batched-{i}'.encode()) for i in range(20)]
            await producer.flush()
            await asyncio.gather(*batched_futures)

            for _ in range(50):
                await producer.poll(0.1)
                if delivery_thread_ids or errors:
                    break

            print(f"{called_by()}: delivery_thread_ids={delivery_thread_ids}, main_thread_id={main_thread_id}")
            assert not errors, f"unexpected delivery errors: {errors}"
            assert len(delivery_thread_ids) == 1
            assert (
                delivery_thread_ids[0] != main_thread_id
            ), "the direct on_delivery callback fired on the event loop thread, not a worker thread"
        finally:
            await producer.close()

    async def test_direct_on_delivery_bridged_safely_resolves_asyncio_future(self, kafka_cluster):
        """Reference pattern for anyone using the on_delivery escape hatch and wanting to
        resolve their own asyncio.Future from it: bridge back to the event loop with
        call_soon_threadsafe, the same way tests/common/_async/producer.py's
        wrapped_on_delivery does. AIOProducer does not do this for you when you bypass its
        own produce()"""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_direct_on_delivery_bridged")
        producer = await _new_aio_producer(kafka_cluster)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def on_delivery(err, msg):
            if err:
                loop.call_soon_threadsafe(fut.set_exception, KafkaException(err))
            else:
                loop.call_soon_threadsafe(fut.set_result, msg)

        try:
            producer._producer.produce(topic, value=b'bridged', on_delivery=on_delivery)
            for _ in range(50):
                if fut.done():
                    break
                await producer.poll(0.1)

            msg = await asyncio.wait_for(fut, timeout=5)
            print(f"{called_by()}: msg={msg.value()}")
            assert msg.value() == b'bridged'
        finally:
            await producer.close()


class TestCallbackReentrancy:
    """error_cb/throttle_cb/stats_cb are bridged onto the event loop via wrap_callback's
    asyncio.run_coroutine_threadsafe(...).result(), which blocks the calling executor
    worker thread until the coroutine finishes. A reentrant AIOProducer call from inside
    one of these needs a *free* worker from the same bounded pool."""

    async def test_error_cb_reentrant_poll_completes_with_two_workers(self):
        """max_workers=2 is enough for one level of reentrancy: one worker is blocked
        bridging error_cb onto the event loop, the reentrant poll() dispatches to the
        second. Deliberately points at an unreachable broker -- doesn't need
        kafka_cluster, this is purely about the executor/callback interaction."""
        error_cb_called = []
        reentrant_results = []

        async def error_cb(err):
            error_cb_called.append(err)
            if not reentrant_results:
                result = await producer.poll(0)
                reentrant_results.append(result)

        producer = AIOProducer(
            {
                'bootstrap.servers': 'PLAINTEXT://127.0.0.1:1',
                'error_cb': error_cb,
                'reconnect.backoff.max.ms': 100,
            },
            max_workers=2,
        )

        try:

            async def wait_for_error_cb():
                for _ in range(100):
                    await producer.poll(0.2)
                    if error_cb_called and reentrant_results:
                        break

            await asyncio.wait_for(wait_for_error_cb(), timeout=20)

            print(f"{called_by()}: error_cb_called={len(error_cb_called)}, reentrant_results={reentrant_results}")
            assert error_cb_called, "error_cb was never invoked"
            assert reentrant_results, "the reentrant poll() call from inside error_cb never completed"
        finally:
            await producer.close()

    async def test_stats_cb_reentrant_produce_keeps_firing(self, kafka_cluster):
        """The async stats_cb reentrantly produces a message; stats_cb must keep firing
        on schedule afterward rather than stalling the bridge."""
        stats_seen = []
        reentrant_produce_results = []

        topic_holder = {}

        async def stats_cb(stats_json_str):
            stats_seen.append(stats_json_str)
            if len(stats_seen) == 1 and topic_holder:
                fut = await producer.produce(topic_holder['topic'], value=b'from-stats-cb')
                # An explicit flush() is needed for the future to resolve.
                await producer.flush()
                reentrant_produce_results.append(await fut)

        producer = await _new_aio_producer(
            kafka_cluster,
            conf={'stats_cb': stats_cb, 'statistics.interval.ms': 200},
            max_workers=2,
        )
        topic_holder['topic'] = kafka_cluster.create_topic_and_wait_propogation("test_aio_producer_stats_reentrant")

        try:
            for _ in range(100):
                await producer.poll(0.2)
                if len(stats_seen) >= 3:
                    break

            print(f"{called_by()}: stats_seen={len(stats_seen)}, reentrant_produce_results={reentrant_produce_results}")
            assert len(stats_seen) >= 3, "stats_cb stopped firing -- likely stuck on the reentrant call"
            assert reentrant_produce_results, "the reentrant produce() call from inside stats_cb never completed"
        finally:
            await producer.close()


class TestAsyncTransactions:
    """AIOProducer's transactional API related test cases"""

    async def test_basic_transaction_lifecycle(self, kafka_cluster):
        """Baseline happy path: init -> begin -> produce -> commit, verified
        with a real read_committed consumer."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_txn_basic")
        producer = await _new_aio_producer(kafka_cluster, {'transactional.id': f'test-aio-txn-basic-{uuid1()}'})

        try:
            await producer.init_transactions()
            await producer.begin_transaction()

            n = 20
            futures = [await producer.produce(topic, value=f'v{i}'.encode()) for i in range(n)]
            # commit_transaction() flushes internally before it actually commits.
            # Run it as a background task so that internal flush resolves the futures below before the
            # transaction is actually committed.
            commit_task = asyncio.ensure_future(producer.commit_transaction())
            msgs = await asyncio.gather(*futures)
            await commit_task

            consumer = _read_committed_consumer(kafka_cluster, topic)
            msg_cnt = _drain_committed(consumer)
            consumer.close()

            print(f"{called_by()}: delivered={len(msgs)}, visible_after_commit={msg_cnt}")
            assert len(msgs) == n
            assert msg_cnt == n
        finally:
            await producer.close()

    async def test_concurrent_calls_to_same_transaction_api(self, kafka_cluster):
        """Two coroutines both awaiting commit_transaction() at once: exactly
        one succeeds, the other must fail specifically with _PREV_IN_PROGRESS."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_txn_same_api_race")
        producer = await _new_aio_producer(kafka_cluster, {'transactional.id': f'test-aio-txn-same-api-{uuid1()}'})

        try:
            await producer.init_transactions()
            await producer.begin_transaction()
            await producer.produce(topic, value=b'msg')
            await producer.flush()

            results = await asyncio.gather(
                producer.commit_transaction(), producer.commit_transaction(), return_exceptions=True
            )

            print(f"{called_by()}: results={results}")
            successes = [r for r in results if not isinstance(r, Exception)]
            errors = [r for r in results if isinstance(r, Exception)]
            assert len(successes) == 1, f"expected exactly one successful commit, got: {results}"
            assert len(errors) == 1, f"expected exactly one error result, got: {results}"
            assert isinstance(errors[0], KafkaException), f"unexpected error type: {errors[0]!r}"
            assert errors[0].args[0].code() == KafkaError._PREV_IN_PROGRESS, (
                f"expected the losing call to fail with _PREV_IN_PROGRESS (proof the two calls "
                f"genuinely overlapped on separate threads), got: {errors[0]}"
            )
        finally:
            await producer.close()

    async def test_concurrent_calls_to_different_transaction_apis(self, kafka_cluster):
        """One coroutine commits while another aborts at the same time:
        exactly one of the two succeeds, and the loser must fail specifically
        with _CONFLICT."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_txn_diff_api_race")
        producer = await _new_aio_producer(kafka_cluster, {'transactional.id': f'test-aio-txn-diff-api-{uuid1()}'})

        try:
            await producer.init_transactions()
            await producer.begin_transaction()
            await producer.produce(topic, value=b'msg')
            await producer.flush()

            results = await asyncio.gather(
                producer.commit_transaction(), producer.abort_transaction(), return_exceptions=True
            )

            print(f"{called_by()}: results={results}")
            successes = [r for r in results if not isinstance(r, Exception)]
            errors = [r for r in results if isinstance(r, Exception)]
            assert len(successes) == 1, f"expected exactly one of commit/abort to succeed, got: {results}"
            assert len(errors) == 1, f"expected exactly one error result, got: {results}"
            assert isinstance(errors[0], KafkaException), f"unexpected error type: {errors[0]!r}"
            assert errors[0].args[0].code() == KafkaError._CONFLICT, (
                f"expected the losing call to fail with _CONFLICT (proof the two calls "
                f"genuinely overlapped on separate threads), got: {errors[0]}"
            )
        finally:
            await producer.close()

    async def test_concurrent_produce_into_shared_buffer_then_commit(self, kafka_cluster):
        """Multiple coroutines concurrently calling produce(), which queue
        into AIOProducer's own internal batch buffer, not directly into
        librdkafka. Every produced message's future must resolve to exactly
        one of: delivered and visible after commit, or a clean error -- never
        silently missing, never double-counted. There is no lock guarding the
        buffer (see _producer_batch_processor.py); this relies entirely on
        asyncio's cooperative scheduling to keep buffer bookkeeping race-free,
        which is exactly what the concurrent producers exercise.
        """
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_txn_buffer_boundary")
        producer = await _new_aio_producer(
            kafka_cluster, {'transactional.id': f'test-aio-txn-buffer-boundary-{uuid1()}'}, batch_size=10
        )

        try:
            await producer.init_transactions()
            await producer.begin_transaction()

            num_workers = 8
            messages_per_worker = 15

            async def produce_worker(worker_id):
                return [
                    await producer.produce(topic, value=f'w{worker_id}-m{i}'.encode())
                    for i in range(messages_per_worker)
                ]

            all_futures_by_worker = await asyncio.gather(*(produce_worker(i) for i in range(num_workers)))
            all_futures = [f for worker_futures in all_futures_by_worker for f in worker_futures]

            await producer.flush()
            results = await asyncio.gather(*all_futures, return_exceptions=True)
            succeeded = [r for r in results if not isinstance(r, Exception)]
            failed = [r for r in results if isinstance(r, Exception)]

            await producer.commit_transaction()

            consumer = _read_committed_consumer(kafka_cluster, topic)
            msg_cnt = _drain_committed(consumer)
            consumer.close()

            print(f"{called_by()}: succeeded={len(succeeded)}, failed={len(failed)}, visible_after_commit={msg_cnt}")
            assert msg_cnt == len(succeeded), (
                f"every future that resolved successfully must be visible after commit: "
                f"{msg_cnt} visible vs {len(succeeded)} succeeded futures"
            )
        finally:
            await producer.close()

    async def test_direct_thread_produce_during_open_transaction(self, kafka_cluster):
        """Raw OS threads producing directly on aio_producer._producer
        (bypassing AIOProducer's own buffer entirely) while the event loop
        drives the transaction lifecycle through the executor on the same
        underlying rk -- produce() must be safe from any thread while a
        transaction is open, exactly as for the bare sync Producer (see
        TestSharedProducerAcrossThreads above)."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_txn_direct_thread_produce")
        producer = await _new_aio_producer(kafka_cluster, {'transactional.id': f'test-aio-txn-direct-{uuid1()}'})

        direct_delivered = []
        direct_errors = []
        num_threads = 4
        messages_per_thread = 25

        def direct_worker(thread_id):
            def on_delivery(err, msg):
                if err:
                    direct_errors.append(err)
                else:
                    direct_delivered.append(msg)

            for i in range(messages_per_thread):
                producer._producer.produce(topic, value=f'direct-{thread_id}-{i}'.encode(), on_delivery=on_delivery)
                producer._producer.poll(0)

        threads = [threading.Thread(target=direct_worker, args=(i,), daemon=True) for i in range(num_threads)]

        try:
            await producer.init_transactions()
            await producer.begin_transaction()

            for t in threads:
                t.start()

            while any(t.is_alive() for t in threads):
                await asyncio.sleep(0.05)

            for t in threads:
                t.join(timeout=30)
            assert all(not t.is_alive() for t in threads), "a direct-produce thread did not finish"

            await producer.commit_transaction()

            for _ in range(100):
                await producer.poll(0.1)
                if len(direct_delivered) + len(direct_errors) == num_threads * messages_per_thread:
                    break

            print(f"{called_by()}: direct_delivered={len(direct_delivered)}, direct_errors={direct_errors}")
            assert (
                not direct_errors
            ), f"unexpected errors from direct produce() during open transaction: {direct_errors}"
            assert len(direct_delivered) == num_threads * messages_per_thread

            consumer = _read_committed_consumer(kafka_cluster, topic)
            msg_cnt = _drain_committed(consumer)
            consumer.close()

            print(f"{called_by()}: visible_after_commit={msg_cnt}")
            assert msg_cnt == num_threads * messages_per_thread
        finally:
            await producer.close()

    async def test_close_races_commit_transaction(self, kafka_cluster):
        """close() concurrent with commit_transaction() via the executor.
        Unlike the sync Producer, close() never calls the underlying Producer's
        own close() -- so the only real interaction between the two here is contention
        for the shared ThreadPoolExecutor. That means only two outcomes are legitimate:
        either commit_transaction()'s work gets submitted before
        executor.shutdown() takes effect and the commit genuinely
        completes (verified below via a real read_committed consumer), or
        it's submitted after and fails with the stdlib RuntimeError
        ThreadPoolExecutor raises for exactly that case."""
        topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_txn_close_races_commit")
        producer = await _new_aio_producer(kafka_cluster, {'transactional.id': f'test-aio-txn-close-commit-{uuid1()}'})

        await producer.init_transactions()
        await producer.begin_transaction()
        await producer.produce(topic, value=b'msg')

        commit_result, close_result = await asyncio.wait_for(
            asyncio.gather(producer.commit_transaction(), producer.close(), return_exceptions=True),
            timeout=30,
        )

        print(f"{called_by()}: commit_result={commit_result!r}, close_result={close_result!r}")

        assert not isinstance(close_result, Exception), f"close() unexpectedly raised: {close_result!r}"

        if isinstance(commit_result, Exception):
            assert isinstance(commit_result, RuntimeError) and 'shutdown' in str(commit_result), (
                f"commit_transaction() failed with an unexpected error -- only the executor's "
                f"post-shutdown RuntimeError is legitimate for this race, got: {commit_result!r}"
            )
        else:
            consumer = _read_committed_consumer(kafka_cluster, topic)
            msg_cnt = _drain_committed(consumer)
            consumer.close()
            assert msg_cnt == 1, f"commit_transaction() reported success but the message isn't visible: {msg_cnt}"
