"""Fixtures for ShareConsumer OAUTHBEARER integration tests.

Starts a single-broker trivup KRaft cluster with the unsecured
OAUTHBEARER listener so tests can run a real SASL handshake. This suite
lives outside tests/integration/share_consumer/ on purpose, to avoid
that directory's autouse _delete_share_test_topics fixture, which
depends on the ordinary kafka_cluster fixture.

Trivup's unsecured listener fixes the JAAS validator to
unsecuredLoginStringClaim_sub="admin" and
unsecuredValidatorRequiredScope="requiredScope". Tokens from
unsecured_token.make_unsecured_jwt default to matching values.

Trivup normally tells clients to use librdkafka's built-in unsecured-JWT
producer (enable.sasl.oauthbearer.unsecure.jwt=true plus
sasl.oauthbearer.config). These tests need the binding's oauth_cb path
instead, so oauth_share_consumer_conf strips those keys from the base
config dict.

Locally the fixture uses $KAFKA_HOME (or ~/projects/kafka). In CI,
source-package-verification.sh pre-stages a tarball under
tmp-KafkaCluster/.../kafka/<version>/ and trivup's version-based search
picks it up. The local-source branch sets version='trunk' so trivup's
deploy.sh symlinks instead of downloading.
"""

import os

import pytest

from tests.common import TestUtils
from tests.integration.conftest import create_trivup_cluster

# Trivup's broker_conf list is appended to server.properties.
# connections.max.reauth.ms=5000 forces SASL reauth every 5s, which the
# refresh_through_reauth test relies on.
_BROKER_CONF = TestUtils.broker_conf() + [
    'connections.max.reauth.ms=5000',
]


def _resolve_kafka_path():
    """Return (kafka_path, version) for trivup.

    If a local Kafka source tree is reachable, use it with version
    'trunk' and a symlink. Otherwise return (None,
    TestUtils.broker_version()) and let trivup use the standard
    version-based path (which CI pre-stages under tmp-KafkaCluster/).
    """
    candidate = os.environ.get('KAFKA_HOME', os.path.expanduser('~/projects/kafka'))
    if os.path.exists(os.path.join(candidate, 'bin', 'kafka-server-start.sh')):
        return candidate, 'trunk'
    return None, TestUtils.broker_version()


@pytest.fixture(scope="session")
def oauth_trivup_cluster():
    """Session-scoped trivup cluster, OAUTHBEARER unsecured."""
    kafka_path, version = _resolve_kafka_path()
    conf = {
        'sasl_mechanism': 'OAUTHBEARER',
        'broker_cnt': 1,
        'with_sr': False,
        'broker_conf': _BROKER_CONF,
        'version': version,
    }
    if kafka_path is not None:
        conf['kafka_path'] = kafka_path
    cluster = create_trivup_cluster(conf)
    yield cluster
    cluster.stop()


@pytest.fixture
def oauth_share_consumer_conf(oauth_trivup_cluster):
    """Return a baseline ShareConsumer config dict for OAUTHBEARER.

    The caller fills in group.id, oauth_cb, and optionally error_cb and
    other settings. Trivup's built-in unsecured-JWT client defaults are
    stripped so the binding's oauth_cb is what produces the token.
    """
    conf = oauth_trivup_cluster.client_conf()
    for k in ('enable.sasl.oauthbearer.unsecure.jwt', 'sasl.oauthbearer.config'):
        conf.pop(k, None)
    conf['socket.timeout.ms'] = 5000
    return conf
