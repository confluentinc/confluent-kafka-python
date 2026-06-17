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

from tests.common import drain_share_consumers


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
