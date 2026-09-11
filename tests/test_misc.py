#!/usr/bin/env python

import json
import os
import sys
import time

import pytest

import confluent_kafka
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from tests.common import TestConsumer


def test_version():
    print('Using confluent_kafka module version %s' % confluent_kafka.version())
    assert len(confluent_kafka.version()) > 0

    print('Using librdkafka version (%s, %i)' % confluent_kafka.libversion())
    assert len(confluent_kafka.libversion()[0]) > 0


def test_error_cb():
    """Tests error_cb."""
    seen_error_cb = False

    def error_cb(error_msg):
        nonlocal seen_error_cb
        seen_error_cb = True
        acceptable_error_codes = (confluent_kafka.KafkaError._TRANSPORT, confluent_kafka.KafkaError._ALL_BROKERS_DOWN)
        assert error_msg.code() in acceptable_error_codes

    conf = {
        'bootstrap.servers': 'localhost:65531',  # Purposely cause connection refused error
        'group.id': 'test',
        'session.timeout.ms': 1000,  # Avoid close() blocking too long
        'error_cb': error_cb,
    }

    kc = TestConsumer(conf)
    kc.subscribe(["test"])
    while not seen_error_cb:
        kc.poll(timeout=0.1)

    kc.close()


def test_stats_cb():
    """Tests stats_cb."""
    seen_stats_cb = False

    def stats_cb(stats_json_str):
        nonlocal seen_stats_cb
        seen_stats_cb = True
        stats_json = json.loads(stats_json_str)
        assert len(stats_json['name']) > 0

    conf = {
        'group.id': 'test',
        'session.timeout.ms': 1000,  # Avoid close() blocking too long
        'statistics.interval.ms': 200,
        'stats_cb': stats_cb,
    }

    kc = TestConsumer(conf)

    kc.subscribe(["test"])
    while not seen_stats_cb:
        kc.poll(timeout=0.1)
    kc.close()


def test_conf_none():
    """Issue #133
    Test that None can be passed for NULL by setting bootstrap.servers
    to None. If None would be converted to a string then a broker would
    show up in statistics. Verify that it doesnt."""
    seen_stats_cb_check_no_brokers = False

    def stats_cb_check_no_brokers(stats_json_str):
        """Make sure no brokers are reported in stats"""
        nonlocal seen_stats_cb_check_no_brokers
        stats = json.loads(stats_json_str)
        assert len(stats['brokers']) == 0, "expected no brokers in stats: %s" % stats_json_str
        seen_stats_cb_check_no_brokers = True

    conf = {
        'bootstrap.servers': None,  # overwrites previous value
        'statistics.interval.ms': 10,
        'stats_cb': stats_cb_check_no_brokers,
    }

    p = confluent_kafka.Producer(conf)
    p.poll(timeout=0.1)

    assert seen_stats_cb_check_no_brokers


def throttle_cb_instantiate_fail():
    """Ensure noncallables raise TypeError"""
    with pytest.raises(ValueError):
        confluent_kafka.Producer({'throttle_cb': 1})


def throttle_cb_instantiate():
    """Ensure we can configure a proper callback"""

    def throttle_cb(throttle_event):
        pass

    confluent_kafka.Producer({'throttle_cb': throttle_cb})


def test_throttle_event_types():
    throttle_event = confluent_kafka.ThrottleEvent("broker", 0, 10.0)
    assert isinstance(throttle_event.broker_name, str) and throttle_event.broker_name == "broker"
    assert isinstance(throttle_event.broker_id, int) and throttle_event.broker_id == 0
    assert isinstance(throttle_event.throttle_time, float) and throttle_event.throttle_time == 10.0
    assert str(throttle_event) == "broker/0 throttled for 10000 ms"


def skip_interceptors():
    # Run interceptor test if monitoring-interceptor is found
    for path in ["/usr/lib", "/usr/local/lib", "staging/libs", "."]:
        for ext in [".so", ".dylib", ".dll"]:
            f = os.path.join(path, "monitoring-interceptor" + ext)
            if os.path.exists(f):
                return False

    # Skip interceptor tests
    return True


@pytest.mark.xfail(sys.platform in ('linux2', 'linux'), reason="confluent-librdkafka-plugins packaging issues")
@pytest.mark.skipif(
    skip_interceptors(), reason="requires confluent-librdkafka-plugins be installed and copied to the current directory"
)
@pytest.mark.parametrize(
    "init_func",
    [
        Consumer,
        Producer,
        AdminClient,
    ],
)
def test_unordered_dict(init_func):
    """
    Interceptor configs can only be handled after the plugin has been loaded not before.
    """
    client = init_func(
        {
            'group.id': 'test-group',
            'confluent.monitoring.interceptor.publishMs': 1000,
            'confluent.monitoring.interceptor.sessionDurationMs': 1000,
            'plugin.library.paths': 'monitoring-interceptor',
            'confluent.monitoring.interceptor.topic': 'confluent-kafka-testing',
            'confluent.monitoring.interceptor.icdebug': False,
        }
    )

    client.poll(0)


def test_topic_config_update():
    seen_delivery_cb = False

    # *NOTE* default.topic.config has been deprecated.
    # This example remains to ensure backward-compatibility until its removal.
    confs = [
        {"message.timeout.ms": 600000, "default.topic.config": {"message.timeout.ms": 1000}},
        {"message.timeout.ms": 1000},
        {"default.topic.config": {"message.timeout.ms": 1000}},
    ]

    def on_delivery(err, msg):
        # Since there is no broker, produced messages should time out.
        nonlocal seen_delivery_cb
        seen_delivery_cb = True
        assert err.code() == confluent_kafka.KafkaError._MSG_TIMED_OUT

    for conf in confs:
        p = confluent_kafka.Producer(conf)

        start = time.time()

        timeout = start + 10.0

        p.produce('mytopic', value='somedata', key='a key', on_delivery=on_delivery)
        while time.time() < timeout:
            if seen_delivery_cb:
                return
            p.poll(1.0)

        if "CI" in os.environ:
            pytest.xfail("Timeout exceeded")
        pytest.fail("Timeout exceeded")


def test_set_sasl_credentials_api():
    clients = [AdminClient({}), TestConsumer({"group.id": "dummy"}), confluent_kafka.Producer({})]

    for c in clients:
        c.set_sasl_credentials('username', 'password')

        c.set_sasl_credentials('override', 'override')

        with pytest.raises(TypeError):
            c.set_sasl_credentials(None, 'password')

        with pytest.raises(TypeError):
            c.set_sasl_credentials('username', None)


def test_producer_config_property():
    """Issue #465
    The effective configuration of a Producer instance can be read back
    through the read-only `config` attribute."""
    conf = {
        'bootstrap.servers': 'localhost:65531',
        'client.id': 'test-client-id',
    }

    p = confluent_kafka.Producer(conf)

    config = p.config
    assert isinstance(config, dict)
    assert config['client.id'] == 'test-client-id'
    # Defaults are included in the dump.
    assert config['api.version.request'] == 'true'
    # Values are reported as strings by librdkafka's conf dump.
    assert config['socket.timeout.ms'] == str(
        confluent_kafka.Producer({'client.id': 'probe'}).config['socket.timeout.ms'])

    # The returned dict is a copy: mutating it must not affect the client.
    config['client.id'] = 'mutated'
    assert p.config['client.id'] == 'test-client-id'

    p.poll(timeout=0.1)


def test_consumer_config_property():
    """Issue #465
    The effective configuration of a Consumer instance can be read back
    through the read-only `config` attribute."""
    conf = {
        'bootstrap.servers': 'localhost:65531',
        'group.id': 'test-group',
        'client.id': 'test-consumer-id',
        'session.timeout.ms': 1000,
    }

    kc = TestConsumer(conf)

    config = kc.config
    assert isinstance(config, dict)
    assert config['group.id'] == 'test-group'
    assert config['client.id'] == 'test-consumer-id'
    assert config['session.timeout.ms'] == '1000'

    kc.close()


def test_config_property_is_read_only():
    """The `config` attribute has no setter."""
    p = confluent_kafka.Producer({'bootstrap.servers': 'localhost:65531'})
    with pytest.raises(AttributeError):
        p.config = {'client.id': 'nope'}
    p.poll(timeout=0.1)


def test_config_property_excludes_callbacks():
    """Issue #465
    Python callbacks passed in the config dict are not librdkafka
    properties and must not appear in the config dump."""
    def error_cb(error_msg):
        pass

    p = confluent_kafka.Producer({
        'bootstrap.servers': 'localhost:65531',
        'error_cb': error_cb,
    })

    config = p.config
    assert 'error_cb' not in config
    p.poll(timeout=0.1)
