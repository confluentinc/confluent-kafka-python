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

Unlike tests/test_ShareConsumer_callbacks.py (unit-style, hermetic, unreachable
broker), these tests exercise the callback contract against a real broker — the
only way to validate that throttle_cb actually fires (vs. just registers
cleanly).

throttle_cb in particular needs broker-side quota config: see the
SHARE_THROTTLE_QUOTA_CLIENT_ID environment-variable contract documented on the
test below. The pattern mirrors
tests/integration/integration_test.py::verify_throttle_cb, which has used the
same external-setup convention since before this binding was a pytest suite.
"""

import os
import time

import pytest

from confluent_kafka import ThrottleEvent

# Env var convention (mirrors the verify_throttle_cb test in
# integration_test.py): the test runner sets this to the client.id for which
# a strict consumer_byte_rate quota has been preconfigured on the broker.
# Setup recipe (run once against the cluster before the test):
#
#     ${KAFKA_HOME}/bin/kafka-configs.sh \
#         --bootstrap-server <broker> \
#         --alter \
#         --add-config 'consumer_byte_rate=1024' \
#         --entity-type clients \
#         --entity-name <client.id>
#
# Then export SHARE_THROTTLE_QUOTA_CLIENT_ID=<client.id> and run pytest.
_THROTTLE_CLIENT_ID_ENV = 'SHARE_THROTTLE_QUOTA_CLIENT_ID'


@pytest.mark.skipif(
    not os.environ.get(_THROTTLE_CLIENT_ID_ENV),
    reason=(
        f"requires {_THROTTLE_CLIENT_ID_ENV} env var pointing at a client.id "
        f"with a strict consumer_byte_rate quota preconfigured on the broker; "
        f"see module docstring for the kafka-configs.sh setup recipe"
    ),
)
def test_throttle_cb_fires_under_quota(kafka_cluster):
    """throttle_cb is invoked when the broker throttles a share consumer.

    Pumps a large volume of data into a topic, then consumes via a share
    consumer whose client.id has a strict consumer_byte_rate quota. The
    broker's fetch responses include a Throttle_Time field that librdkafka
    surfaces via the throttle_cb trampoline (which fires from
    rd_kafka_share_consume_batch's rk_rep drain — same path verified for
    error_cb in tests/test_ShareConsumer_callbacks.py).

    Asserts:
    - throttle_cb fires at least once during a bounded poll loop
    - each ThrottleEvent has the documented shape (broker_name str,
      broker_id int, throttle_time float >= 0)
    - at least one event reports throttle_time > 0 (i.e., actual broker-side
      throttling, not just a "cleared" event)
    """
    client_id = os.environ[_THROTTLE_CLIENT_ID_ENV]

    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-throttle')

    # Produce enough data that even a 1 KB/s consumer quota takes multiple
    # fetch cycles to drain. 5000 × 1 KB = 5 MB.
    msg_count = 5000
    msg_size = 1024
    payload = b'x' * msg_size

    producer = kafka_cluster.cimpl_producer()
    for _ in range(msg_count):
        producer.produce(topic, value=payload)
        # Drain producer queue periodically to keep BufferError at bay.
        producer.poll(0)
    producer.flush(timeout=30.0)

    throttle_events = []

    def my_throttle_cb(event):
        throttle_events.append(event)

    sc = kafka_cluster.share_consumer(
        {
            'client.id': client_id,
            'throttle_cb': my_throttle_cb,
            # Bump the receive buffer so the broker's throttle delay is the
            # actual rate-limit, not us being slow to drain.
            'fetch.max.bytes': msg_size * msg_count,
        }
    )

    try:
        sc.subscribe([topic])

        # Poll aggressively for up to 30s, breaking early once we've seen
        # throttling. The quota is set very low (1 KB/s default in the docs)
        # so the first fetch response should already carry a Throttle_Time.
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
            f"without a single throttle event — is the quota actually set?)"
        )

        for ev in throttle_events:
            assert isinstance(ev, ThrottleEvent)
            assert isinstance(ev.broker_name, str) and ev.broker_name
            assert isinstance(ev.broker_id, int)
            assert isinstance(ev.throttle_time, float)
            assert ev.throttle_time >= 0.0

        # At least one event with positive throttle_time — librdkafka also
        # emits "cleared" events with throttle_time=0 when the per-broker
        # rate falls back below the quota, so >=1 strictly-positive event
        # is what proves we actually got throttled.
        assert any(ev.throttle_time > 0.0 for ev in throttle_events), (
            f"all {len(throttle_events)} throttle events reported "
            f"throttle_time=0; expected at least one with a positive delay"
        )
    finally:
        sc.close()
