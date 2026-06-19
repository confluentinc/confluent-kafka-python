"""Per-directory pytest fixtures for share consumer integration tests."""

import sys

import pytest


@pytest.fixture(scope='module', autouse=True)
def _delete_share_test_topics(kafka_cluster):
    """Cleanup of share-consumer test topics after the module
    finishes. Tests create topics with deterministic prefixes
    (test-share-consumer-*) plus a UUID suffix; on long-lived shared clusters
    those would otherwise accumulate across runs.
    """
    yield
    try:
        topics = list(kafka_cluster.admin().list_topics(timeout=5).topics.keys())
    except Exception as exc:
        # Cluster gone or unreachable — nothing to clean up. Surface to
        # stderr so a genuinely broken cluster doesn't hide behind a silent
        # pass.
        print(f"share-consumer cleanup: list_topics failed: {exc!r}", file=sys.stderr)
        return
    share_topics = [t for t in topics if t.startswith('test-share-consumer-')]
    if not share_topics:
        return
    try:
        kafka_cluster.admin().delete_topics(share_topics)
    except Exception as exc:
        # Still log so accumulated leftover topics across runs
        # have a visible cause.
        print(
            f"share-consumer cleanup: delete_topics({len(share_topics)}) failed: {exc!r}",
            file=sys.stderr,
        )
