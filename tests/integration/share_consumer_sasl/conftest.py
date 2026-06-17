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
