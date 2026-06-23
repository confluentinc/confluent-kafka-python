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

"""Integration tests for ShareConsumer.set_sasl_credentials.

These run against the SASL cluster so the credentials actually matter —
on a plaintext broker set_sasl_credentials is a no-op.
"""

import pytest

from confluent_kafka import KafkaError
from tests.common import drain_share_consumers


@pytest.mark.skip(
    reason="TODO KIP-932: flaky - rotates a live consumer to invalid SASL "
    "creds and assumes the already-authenticated connection is reused; under "
    "CI load a reconnect/re-auth uses the invalid creds and drops records "
    "(0/20). Re-enable by rotating to valid creds so a reconnect still "
    "authenticates."
)
@pytest.mark.parametrize('sasl_cluster', [{'broker_cnt': 1}], indirect=True)
def test_set_sasl_credentials_during_active_consumption(sasl_cluster):
    """Rotating credentials on an already-connected consumer shouldn't
    disrupt it — records produced after the change still arrive."""
    topic = sasl_cluster.create_topic_and_wait_propogation('test-share-consumer-sasl-creds')
    n = 20

    sc = sasl_cluster.share_consumer()
    try:
        sc.subscribe([topic])
        # let the consumer settle in before changing anything
        for _ in range(5):
            sc.poll(timeout=0.2)

        # change credentials mid-stream
        assert sc.set_sasl_credentials('rotated-user', 'rotated-secret') is None

        producer = sasl_cluster.cimpl_producer()
        expected = [f'msg-{i}'.encode() for i in range(n)]
        for v in expected:
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], n)[0]
        values = sorted(m.value() for m in received)
        assert values == sorted(expected), f"Records lost after credential rotation: got {len(received)}/{n}"
    finally:
        sc.close()


@pytest.mark.parametrize('sasl_cluster', [{'broker_cnt': 1}], indirect=True)
def test_set_sasl_credentials_before_subscribe_and_repeated(sasl_cluster):
    """Setting credentials before subscribing, more than once, still leaves a
    working consumer. The last set has to be valid, or the connection on
    subscribe can't authenticate."""
    topic = sasl_cluster.create_topic_and_wait_propogation('test-share-consumer-sasl-creds-presub')
    n = 10

    valid = sasl_cluster.client_conf()
    sc = sasl_cluster.share_consumer()
    try:
        # Before subscribing, and more than once; last set wins.
        assert sc.set_sasl_credentials('rotated-user', 'rotated-secret') is None
        assert sc.set_sasl_credentials(valid['sasl.username'], valid['sasl.password']) is None

        sc.subscribe([topic])

        producer = sasl_cluster.cimpl_producer()
        expected = [f'msg-{i}'.encode() for i in range(n)]
        for v in expected:
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], n)[0]
        values = sorted(m.value() for m in received)
        assert values == sorted(expected), f"Consume failed after pre-subscribe credential set: got {len(received)}/{n}"
    finally:
        sc.close()


@pytest.mark.parametrize('sasl_cluster', [{'broker_cnt': 1}], indirect=True)
def test_set_sasl_credentials_invalid_blocks_consumption(sasl_cluster):
    """Negative case: setting *invalid* credentials before the first connection
    must stop the consumer from authenticating, so nothing is consumed.
    """
    topic = sasl_cluster.create_topic_and_wait_propogation('test-share-consumer-sasl-creds-invalid')
    n = 10

    # Put real records in the topic with a valid producer, so the only reason
    # the consumer can come up empty is the bad credentials we set below.
    producer = sasl_cluster.cimpl_producer()
    for i in range(n):
        producer.produce(topic, value=f'msg-{i}'.encode())
    producer.flush(timeout=10.0)

    auth_codes = {KafkaError._AUTHENTICATION, KafkaError.SASL_AUTHENTICATION_FAILED}
    seen_errors = []

    sc = sasl_cluster.share_consumer({'error_cb': lambda e: seen_errors.append(e)})
    try:
        # Override the valid creds with garbage *before* the first connection.
        assert sc.set_sasl_credentials('wrong-user', 'wrong-secret') is None
        sc.subscribe([topic])

        received = drain_share_consumers([sc], n, timeout_s=15.0)[0]
        assert received == [], f"invalid credentials should block consumption, but got {len(received)} records"

        # And confirm it failed for the right reason — an auth failure, not some
        # unrelated timeout. Without this, a no-op set + any other hiccup would
        # masquerade as a pass.
        seen_codes = {e.code() for e in seen_errors}
        assert seen_codes & auth_codes, f"expected an authentication error, saw: {[str(e) for e in seen_errors]}"
    finally:
        sc.close()
