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

throttle_cb requires a broker-side quota to be configured first. See the
SHARE_THROTTLE_QUOTA_CLIENT_ID environment variable and the setup steps on
the test below. This follows the same convention as
tests/integration/integration_test.py::verify_throttle_cb.
"""

import os
import time

import pytest

from confluent_kafka import ThrottleEvent

# Set this to the client.id that has a low consumer_byte_rate quota
# configured on the broker (same approach as verify_throttle_cb in
# integration_test.py). Configure the quota once before running the test:
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

    Produces a large volume of data, then consumes it through a share
    consumer whose client.id has a low consumer_byte_rate quota. The
    broker's fetch responses carry a Throttle_Time that librdkafka delivers
    to throttle_cb from the rd_kafka_share_consume_batch drain.

    The test checks that throttle_cb fires at least once, that each
    ThrottleEvent has the expected fields (broker_name, broker_id,
    throttle_time), and that at least one event reports a positive
    throttle_time, since a "cleared" event reports throttle_time=0.
    """
    client_id = os.environ[_THROTTLE_CLIENT_ID_ENV]

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
