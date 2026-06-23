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

"""Fixtures for the ShareConsumer SASL credential tests.

These need the SASL cluster, so they sit in their own directory — the
cleanup fixture here binds to sasl_cluster instead of the plaintext
kafka_cluster the other share consumer tests use.
"""

import sys

import pytest


@pytest.fixture(scope='module', autouse=True)
def _delete_share_test_topics(sasl_cluster):
    """Delete share-consumer test topics once the module finishes. Tests
    create topics with the test-share-consumer-* prefix plus a UUID suffix;
    on a long-lived shared cluster those would otherwise pile up across runs.
    """
    yield
    try:
        topics = list(sasl_cluster.admin().list_topics(timeout=5).topics.keys())
    except Exception as exc:
        print(f"share-consumer cleanup: list_topics failed: {exc!r}", file=sys.stderr)
        return
    share_topics = [t for t in topics if t.startswith('test-share-consumer-')]
    if not share_topics:
        return
    try:
        sasl_cluster.admin().delete_topics(share_topics)
    except Exception as exc:
        print(
            f"share-consumer cleanup: delete_topics({len(share_topics)}) failed: {exc!r}",
            file=sys.stderr,
        )
