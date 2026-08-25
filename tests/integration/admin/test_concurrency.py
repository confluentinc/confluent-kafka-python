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
from uuid import uuid1

import pytest

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType


def prefixed_error_cb(prefix):
    def error_cb(err):
        print("[{}]: {}".format(prefix, err))

    return error_cb


def _new_admin(kafka_cluster, conf=None):
    admin_conf = kafka_cluster.client_conf(conf)
    return AdminClient(admin_conf)


def _assert_result_or_clean_destroy(future, on_success=None, timeout=30):
    """Wait for `future` and, if it resolved successfully, call
    `on_success()` to verify the result further (e.g. that a created topic
    is really visible). If it resolved with a clean KafkaError._DESTROY,
    `on_success` is skipped -- there's nothing to verify.

    rd_kafka_destroy() does not wait for in-flight background operations to
    complete against the broker -- it cancels them with a clean
    KafkaError._DESTROY rather than crashing, hanging, or silently dropping
    them. Both outcomes prove the same thing (Admin_background_event_cb()
    doesn't depend on self->rk surviving), so callers racing __exit__()
    immediately after issuing a call should accept either.
    """
    try:
        future.result(timeout=timeout)
    except Exception as e:  # noqa: BLE001 - only _DESTROY is an accepted non-success outcome
        if getattr(e, 'args', None) and getattr(e.args[0], 'code', lambda: None)() == KafkaError._DESTROY:
            return
        raise
    if on_success:
        on_success()


def test_exit_before_create_topics_delivers_result(kafka_cluster):
    """The `with` block exits right after create_topics() returns, before
    the 10-partition topics' background events can realistically have been
    delivered yet. __exit__() must cancel every one of them with a clean
    KafkaError._DESTROY rather than crashing or hanging."""
    admin = _new_admin(kafka_cluster, {'error_cb': prefixed_error_cb('test_exit_before_create_topics_delivers_result')})
    topics = ["test_exit_races_create_topics-{}".format(uuid1()) for _ in range(5)]

    with admin:
        futmap = admin.create_topics([NewTopic(t, num_partitions=10, replication_factor=1) for t in topics])

    for topic in topics:
        with pytest.raises(KafkaException) as e:
            futmap[topic].result(timeout=10)
        assert e.value.args[0].code() == KafkaError._DESTROY


def test_admin_shared_across_threads_multiple_apis(kafka_cluster):
    """One AdminClient instance, several threads each calling a different
    API concurrently, in a loop. All calls must succeed
    with no crash, and every future obtained along the way must resolve
    cleanly."""
    iterations = 10
    expected_retention_ms = '123456789'
    topic = kafka_cluster.create_topic_and_wait_propogation(
        "test_admin_shared_across_threads", conf={'config': {'retention.ms': expected_retention_ms}}
    )

    group_id = "test_admin_shared_across_threads_group-{}".format(uuid1())
    c = kafka_cluster.consumer({'group.id': group_id, 'session.timeout.ms': 6000})
    c.subscribe([topic])
    c.poll(10)
    c.close()

    errors = []
    barrier = threading.Barrier(5)

    admin = _new_admin(kafka_cluster, {'error_cb': prefixed_error_cb('test_admin_shared_across_threads_multiple_apis')})

    def call_create_topics():
        try:
            futures = []
            created_topics = []
            for _ in range(iterations):
                barrier.wait()
                new_topic = "test_admin_shared_across_threads_new-{}".format(uuid1())
                futmap = admin.create_topics([NewTopic(new_topic, num_partitions=1, replication_factor=1)])
                futures.append(futmap[new_topic])
                created_topics.append(new_topic)
            for future in futures:
                future.result(timeout=30)
            time.sleep(5)  # wait for metadata propagation
            metadata = admin.list_topics(timeout=10)
            for created_topic in created_topics:
                assert created_topic in metadata.topics, "topic {} not found in list_topics()".format(created_topic)
        except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
            errors.append(("create_topics", e))

    def call_list_topics():
        try:
            for _ in range(iterations):
                barrier.wait()
                metadata = admin.list_topics(timeout=10)
                assert topic in metadata.topics
        except Exception as e:  # noqa: BLE001
            errors.append(("list_topics", e))

    def call_describe_configs():
        try:
            futures = []
            for _ in range(iterations):
                barrier.wait()
                resource = ConfigResource(ResourceType.TOPIC, topic)
                futmap = admin.describe_configs([resource])
                futures.append(futmap[resource])
            for future in futures:
                config = future.result(timeout=30)
                assert config['retention.ms'].value == expected_retention_ms, "expected retention.ms={}, got {}".format(
                    expected_retention_ms, config['retention.ms'].value
                )
        except Exception as e:  # noqa: BLE001
            errors.append(("describe_configs", e))

    def call_list_consumer_groups():
        try:
            futures = []
            for _ in range(iterations):
                barrier.wait()
                futures.append(admin.list_consumer_groups(request_timeout=10))
            for future in futures:
                result = future.result(timeout=30)
                group_ids = [group.group_id for group in result.valid]
                assert group_id in group_ids, "Consumer group {} not found".format(group_id)
        except Exception as e:  # noqa: BLE001
            errors.append(("list_consumer_groups", e))

    def call_set_sasl_credentials():
        try:
            for _ in range(iterations):
                barrier.wait()
                result = admin.set_sasl_credentials('username', 'password')
                assert result is None, f"set_sasl_credentials() unexpectedly returned {result!r}"
        except Exception as e:  # noqa: BLE001
            errors.append(("set_sasl_credentials", e))

    threads = [
        threading.Thread(target=call_create_topics),
        threading.Thread(target=call_list_topics),
        threading.Thread(target=call_describe_configs),
        threading.Thread(target=call_list_consumer_groups),
        threading.Thread(target=call_set_sasl_credentials),
    ]

    with admin:
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

    assert all(not t.is_alive() for t in threads), "an Admin API call thread did not finish"
    assert not errors, f"unexpected exceptions from concurrent Admin API calls: {errors}"


def test_exit_races_exit_with_real_in_flight_work(kafka_cluster):
    """One thread calling poll() with a long timeout (genuinely in-flight
    work), several other threads all calling __exit__() concurrently on
    the same AdminClient. Whichever thread wins the CAS tears down and the
    rest just wait for it, but every single __exit__() call -- winner or
    loser -- must not return until poll() has actually finished."""
    admin = _new_admin(kafka_cluster, {'error_cb': prefixed_error_cb('test_exit_races_exit_with_real_in_flight_work')})

    poll_started = threading.Event()
    poll_duration = 10
    poll_finished_at = None

    def run_poll():
        nonlocal poll_finished_at
        poll_started.set()
        admin.poll(poll_duration)
        poll_finished_at = time.monotonic()

    poll_thread = threading.Thread(target=run_poll)
    poll_thread.start()
    poll_started.wait(timeout=5)
    time.sleep(2)

    errors = []
    num_exit_workers = 5
    barrier = threading.Barrier(num_exit_workers)
    exit_started_at = [None] * num_exit_workers
    exit_finished_at = [None] * num_exit_workers

    def call_exit(index):
        try:
            barrier.wait()
            exit_started_at[index] = time.monotonic()
            admin.__exit__(None, None, None)
            exit_finished_at[index] = time.monotonic()
        except Exception as e:  # noqa: BLE001 - want to see any exception, not just crashes
            errors.append(e)

    exit_threads = [threading.Thread(target=call_exit, args=(i,)) for i in range(num_exit_workers)]
    for t in exit_threads:
        t.start()
    for t in exit_threads:
        t.join(timeout=30)
    poll_thread.join(timeout=30)

    assert not poll_thread.is_alive(), "poll() thread did not finish"
    assert all(not t.is_alive() for t in exit_threads), "an __exit__() thread did not finish"
    assert not errors, f"unexpected exceptions from concurrent __exit__() calls: {errors}"
    assert poll_finished_at is not None, "poll() never finished"

    for i, started_at in enumerate(exit_started_at):
        assert started_at is not None, "__exit__() thread {} did not record a start time".format(i)
        assert poll_finished_at > started_at, (
            "poll() finished at {:.2f} before __exit__() thread {} even started at {:.2f} -- "
            "the race wasn't exercised".format(poll_finished_at, i, started_at)
        )

    for i, finished_at in enumerate(exit_finished_at):
        assert finished_at is not None, "__exit__() thread {} did not record a finish time".format(i)
        assert (
            finished_at >= poll_finished_at
        ), "__exit__() thread {} returned at {:.2f} before poll() finished at {:.2f}".format(
            i, finished_at, poll_finished_at
        )
