"""Per-directory pytest fixtures for share consumer integration tests."""

import sys
import sysconfig
import warnings

import pytest

# test_share_consumer_deserialization.py imports fastavro.
# fastavro does ship a free-threaded wheel, but has not declared itself GIL-safe
# so it is not installed on free-threaded builds (see requirements-tests-install-nogil.txt)
FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

collect_ignore = []
if FREE_THREADED_BUILD:
    collect_ignore = [
        "test_share_consumer_deserialization.py",
    ]
    warnings.warn(
        "free-threaded build: skipping collection of {} share_consumer "
        "integration test module(s) requiring fastavro, which has not "
        "declared itself GIL-safe".format(len(collect_ignore)),
        RuntimeWarning,
    )


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
