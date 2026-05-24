"""Fixtures for ShareConsumer OAUTHBEARER integration tests.

Uses the local Apache Kafka broker fixture under tests/oauth/ — a single-node
KRaft cluster with SASL_PLAINTEXT/OAUTHBEARER and the unsecured validator
(KIP-255). Tests in this directory talk to this broker only; they do NOT use
the trivup-based ``kafka_cluster`` fixture from tests/integration/conftest.py
and intentionally live outside tests/integration/share_consumer/ so the
share-consumer autouse cleanup (which depends on ``kafka_cluster``) doesn't
fire.
"""

import os
import socket
import subprocess
import sys
import time

import pytest


OAUTH_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "oauth")
)
START_SCRIPT = os.path.join(OAUTH_DIR, "start_broker.sh")
STOP_SCRIPT = os.path.join(OAUTH_DIR, "stop_broker.sh")
BOOTSTRAP = "localhost:9094"
DEFAULT_KAFKA_HOME = os.path.expanduser("~/projects/kafka")
STARTUP_TIMEOUT_SEC = 20


def _wait_for_port(host, port, deadline):
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


@pytest.fixture(scope="session")
def oauth_broker():
    """Start the local OAUTHBEARER broker for the session; yield bootstrap.

    Skips if no usable Apache Kafka tree is found. Tears down (and wipes the
    log dir) at session end.
    """
    kafka_home = os.environ.get("KAFKA_HOME", DEFAULT_KAFKA_HOME)
    if not os.path.exists(os.path.join(kafka_home, "bin", "kafka-server-start.sh")):
        pytest.skip(
            f"Apache Kafka not found at KAFKA_HOME={kafka_home}; "
            "set KAFKA_HOME to a built Kafka tree to run OAUTHBEARER tests"
        )

    # Bail out early if something else already owns 9094 — avoids a confusing
    # "broker started but tests fail" mode where the broker silently failed
    # to bind and the connection actually goes to an unrelated process.
    try:
        with socket.create_connection(("localhost", 9094), timeout=0.5):
            pytest.fail("localhost:9094 is already in use; stop the other process first")
    except OSError:
        pass  # port free — good

    env = {**os.environ, "KAFKA_HOME": kafka_home}
    subprocess.run(
        [START_SCRIPT, "-daemon"],
        check=True,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    deadline = time.time() + STARTUP_TIMEOUT_SEC
    if not _wait_for_port("localhost", 9094, deadline):
        subprocess.run(
            [STOP_SCRIPT, "--clean"],
            check=False,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        pytest.fail(
            f"OAuth broker failed to open localhost:9094 within "
            f"{STARTUP_TIMEOUT_SEC}s"
        )

    # Broker is listening, but the SASL/KRaft layer takes another moment
    # before it'll accept connections cleanly. Small grace period avoids
    # flaky first-test failures.
    time.sleep(2.0)

    print(f"\n[oauth_broker] up on {BOOTSTRAP}", file=sys.stderr)
    try:
        yield BOOTSTRAP
    finally:
        subprocess.run(
            [STOP_SCRIPT, "--clean"],
            check=False,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print("[oauth_broker] stopped and cleaned", file=sys.stderr)
