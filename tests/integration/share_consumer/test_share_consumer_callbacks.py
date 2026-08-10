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

"""Integration tests for ShareConsumer callback dispatch.

These run against a real broker, unlike the unit tests in
tests/test_ShareConsumer_callbacks.py. A real broker is needed to confirm
that throttle_cb actually fires, not just that it registers without error.

throttle_cb needs a broker-side quota. The provision_client_quota fixture
sets one up and removes it afterwards using kafka-configs.sh, so these tests
run unattended as long as there's a trivup install or a KAFKA_HOME.
"""

import glob
import logging
import os
import shutil
import subprocess
import time

import pytest

from confluent_kafka import KafkaError, KafkaException, ThrottleEvent
from tests.common import unique_id


def _resolve_kafka_configs_sh():
    """Locate kafka-configs.sh: $KAFKA_HOME/bin, the trivup install, or PATH."""
    kafka_home = os.environ.get('KAFKA_HOME')
    if kafka_home:
        candidate = os.path.join(kafka_home, 'bin', 'kafka-configs.sh')
        if os.path.exists(candidate):
            return candidate
    # trivup extracts the broker distribution under <cwd>/tmp-KafkaCluster/.
    matches = glob.glob(os.path.join('tmp-KafkaCluster', '**', 'bin', 'kafka-configs.sh'), recursive=True)
    if matches:
        return matches[0]
    return shutil.which('kafka-configs.sh')


def _run_kafka_configs(kafka_configs_sh, bootstrap_servers, alter_args, client_id):
    """Run kafka-configs.sh against a clients entity (raises on failure)."""
    cmd = [
        kafka_configs_sh,
        '--bootstrap-server',
        bootstrap_servers,
        '--alter',
        *alter_args,
        '--entity-type',
        'clients',
        '--entity-name',
        client_id,
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=60)


@pytest.fixture
def provision_client_quota(kafka_cluster):
    """Set up a throttling client quota and tear it down afterwards.

    Returns a function -- provision('consumer_byte_rate=1024') -> client.id --
    that applies the quota with kafka-configs.sh (we shell out because
    librdkafka has no admin op for client quotas). Skips the test if
    kafka-configs.sh isn't found. Quotas are removed on teardown.
    """
    kafka_configs_sh = _resolve_kafka_configs_sh()
    bootstrap_servers = kafka_cluster.client_conf()['bootstrap.servers']
    provisioned = []

    def _provision(add_config):
        if not kafka_configs_sh:
            pytest.skip("kafka-configs.sh not found; set KAFKA_HOME or run via trivup")
        client_id = unique_id('throttled-client')
        _run_kafka_configs(kafka_configs_sh, bootstrap_servers, ['--add-config', add_config], client_id)
        config_keys = ','.join(entry.split('=', 1)[0] for entry in add_config.split(','))
        provisioned.append((client_id, config_keys))
        return client_id

    yield _provision

    for client_id, config_keys in provisioned:
        try:
            _run_kafka_configs(kafka_configs_sh, bootstrap_servers, ['--delete-config', config_keys], client_id)
        except Exception:
            pass  # best-effort cleanup; the broker may already be torn down


def _produce_burst(kafka_cluster, topic, msg_count, msg_size):
    """Produce msg_count messages of msg_size bytes and flush."""
    payload = b'x' * msg_size
    producer = kafka_cluster.cimpl_producer()
    for _ in range(msg_count):
        producer.produce(topic, value=payload)
        producer.poll(0)  # drain the delivery queue to avoid BufferError
    producer.flush(timeout=30.0)


def test_throttle_cb_fires_under_quota(kafka_cluster, provision_client_quota):
    """throttle_cb is invoked when the broker throttles a share consumer.

    Produces a large volume of data, then consumes it through a share
    consumer whose client.id has a low consumer_byte_rate quota. The
    broker's fetch responses carry a Throttle_Time that librdkafka delivers
    to throttle_cb from the rd_kafka_share_poll drain.

    The test checks that throttle_cb fires at least once, that each
    ThrottleEvent has the expected fields (broker_name, broker_id,
    throttle_time), and that at least one event reports a positive
    throttle_time, since a "cleared" event reports throttle_time=0.
    """
    client_id = provision_client_quota('consumer_byte_rate=1024')

    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-throttle')

    # Produce enough data that a 1 KB/s quota takes many fetch cycles to
    # drain (5000 messages of 1 KB each, roughly 5 MB).
    msg_count = 5000
    msg_size = 1024
    payload = b'x' * msg_size

    producer = kafka_cluster.cimpl_producer()
    for _ in range(msg_count):
        producer.produce(topic, value=payload)
        # Drain the producer queue periodically to avoid BufferError.
        producer.poll(0)
    producer.flush(timeout=30.0)

    throttle_events = []

    def my_throttle_cb(event):
        throttle_events.append(event)

    sc = kafka_cluster.share_consumer(
        {
            'client.id': client_id,
            'throttle_cb': my_throttle_cb,
            # Raise the receive buffer so the throttle delay reflects the
            # broker's rate limit rather than slow draining on our side.
            'fetch.max.bytes': msg_size * msg_count,
        }
    )

    try:
        sc.subscribe([topic])

        # Poll for up to 30s, stopping early once throttling is seen. The
        # quota is low enough that the first fetch response should already
        # carry a Throttle_Time.
        deadline = time.monotonic() + 30.0
        consumed = 0
        while time.monotonic() < deadline:
            batch = sc.poll(timeout=1.0)
            consumed += sum(1 for m in batch if m.error() is None)
            if throttle_events and consumed > 0:
                break

        assert throttle_events, (
            f"throttle_cb should have fired against the quota for "
            f"client.id={client_id!r} (consumed {consumed} messages "
            f"without a single throttle event; is the quota actually set?)"
        )

        for ev in throttle_events:
            assert isinstance(ev, ThrottleEvent)
            assert isinstance(ev.broker_name, str) and ev.broker_name
            assert isinstance(ev.broker_id, int)
            assert isinstance(ev.throttle_time, float)
            assert ev.throttle_time >= 0.0

        # Require at least one event with a positive throttle_time.
        # librdkafka also emits "cleared" events with throttle_time=0 when
        # the per-broker rate drops back below the quota, so a zero-only
        # result would not prove that throttling happened.
        assert any(ev.throttle_time > 0.0 for ev in throttle_events), (
            f"all {len(throttle_events)} throttle events reported "
            f"throttle_time=0; expected at least one with a positive delay"
        )
    finally:
        sc.close()


def test_log_cb_share_facility_tags_present(kafka_cluster):
    """log_cb surfaces the share-specific facility tags.

    There's no dedicated share debug context, so debug=cgrp,fetch is what
    turns on the share path's logging. With real produce/consume/commit
    traffic, some records lead with a share facility (SHAREFETCH, SHAREACK,
    SHARESESSION, SHAREHEARTBEAT); check at least one reaches the logger.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-log-facilities')
    _produce_burst(kafka_cluster, topic, msg_count=50, msg_size=64)

    records = []

    class _CollectingHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger('test-share-log-facilities')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(_CollectingHandler())

    sc = kafka_cluster.share_consumer({'debug': 'cgrp,fetch', 'logger': logger})
    try:
        sc.subscribe([topic])

        deadline = time.monotonic() + 30.0
        consumed = 0
        while time.monotonic() < deadline and consumed == 0:
            batch = sc.poll(timeout=1.0)
            consumed += sum(1 for m in batch if m.error() is None)
        # Commit drives ShareAcknowledge traffic (SHAREACK facility).
        sc.commit_sync(timeout=10.0)

        share_facilities = ('SHAREFETCH', 'SHAREACK', 'SHARESESSION', 'SHAREHEARTBEAT')
        seen = {fac for r in records for fac in share_facilities if r.getMessage().startswith(fac)}
        assert seen, (
            f"expected a share facility tag among log records (consumed {consumed}); "
            f"leading tokens seen: {sorted({r.getMessage().split(' ', 1)[0] for r in records})}"
        )
    finally:
        sc.close()


def test_throttle_cb_emits_cleared_zero_event(kafka_cluster, provision_client_quota):
    """After throttling, a cleared (throttle_time=0) event is delivered.

    librdkafka fires throttle_cb on each throttle, then once more once the
    rate drops back below the quota (throttle_time=0). Go over the quota to
    get a positive event, then keep polling with no new data so the rate
    decays and the cleared event shows up.
    """
    client_id = provision_client_quota('consumer_byte_rate=1024')
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-throttle-clear')
    msg_count, msg_size = 5000, 1024
    _produce_burst(kafka_cluster, topic, msg_count, msg_size)

    events = []
    sc = kafka_cluster.share_consumer(
        {
            'client.id': client_id,
            'throttle_cb': events.append,
            'fetch.max.bytes': msg_size * msg_count,
        }
    )
    try:
        sc.subscribe([topic])

        # Phase 1: consume the burst until a positive throttle is observed.
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline and not any(e.throttle_time > 0 for e in events):
            sc.poll(timeout=1.0)
        assert any(e.throttle_time > 0 for e in events), "expected a positive throttle_time event"

        # Phase 2: idle-poll so the per-broker rate decays below the quota and
        # librdkafka delivers the cleared (throttle_time=0) event.
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and not any(e.throttle_time == 0 for e in events):
            sc.poll(timeout=1.0)
        assert any(
            e.throttle_time == 0.0 for e in events
        ), "expected a cleared throttle event (throttle_time=0) after the rate decayed"
    finally:
        sc.close()


@pytest.mark.skip(
    reason="Calling the consumer from inside throttle_cb segfaults today, same root cause as "
    "error_cb/log_cb (see test_ShareConsumer_callbacks.py::test_error_cb_reentrant_call_raises_state): "
    "the reentrancy guard only covers the ack-commit callback. Un-skip and check for _STATE once "
    "it covers the other callbacks."
)
def test_throttle_cb_reentrant_call_raises_state(kafka_cluster, provision_client_quota):
    """Calling the consumer from inside throttle_cb (same thread) should raise _STATE.

    The behavior we want once the reentrancy guard is fixed (matches the
    ack-commit callback and Java's IllegalStateException). Skipped because it
    crashes today.
    """
    client_id = provision_client_quota('consumer_byte_rate=1024')
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-throttle-reentrant')
    msg_count, msg_size = 5000, 1024
    _produce_burst(kafka_cluster, topic, msg_count, msg_size)

    captured = []
    reenter = [True]
    holder = {}

    def throttle_cb(event):
        sc = holder.get('sc')
        if sc is not None and reenter[0]:
            reenter[0] = False
            try:
                sc.poll(timeout=0)
            except KafkaException as exc:
                captured.append(exc)

    sc = kafka_cluster.share_consumer(
        {
            'client.id': client_id,
            'throttle_cb': throttle_cb,
            'fetch.max.bytes': msg_size * msg_count,
        }
    )
    holder['sc'] = sc
    try:
        sc.subscribe([topic])
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline and not captured:
            sc.poll(timeout=1.0)
        assert captured and captured[0].args[0].code() == KafkaError._STATE
    finally:
        sc.close()
