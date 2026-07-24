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


class TestCloseRaceDelivery:
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
