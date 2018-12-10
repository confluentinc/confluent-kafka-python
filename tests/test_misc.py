#!/usr/bin/env python

import confluent_kafka
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
import json
import pytest
import os
import time
import sys


def test_version():
    print('Using confluent_kafka module version %s (0x%x)' % confluent_kafka.version())
    sver, iver = confluent_kafka.version()
    assert len(sver) > 0
    assert iver > 0

    print('Using librdkafka version %s (0x%x)' % confluent_kafka.libversion())
    sver, iver = confluent_kafka.libversion()
    assert len(sver) > 0
    assert iver > 0

    assert confluent_kafka.version()[0] == confluent_kafka.__version__


# global variable for error_cb call back function
seen_error_cb = False


def test_error_cb():
    """ Tests error_cb. """

    def error_cb(error_msg):
        global seen_error_cb
        seen_error_cb = True
        acceptable_error_codes = (confluent_kafka.KafkaError._TRANSPORT, confluent_kafka.KafkaError._ALL_BROKERS_DOWN)
        assert error_msg.code() in acceptable_error_codes

    conf = {'bootstrap.servers': 'localhost:65531',  # Purposely cause connection refused error
            'group.id': 'test',
            'socket.timeout.ms': '100',
            'session.timeout.ms': 1000,  # Avoid close() blocking too long
            'error_cb': error_cb
            }

    kc = confluent_kafka.Consumer(**conf)
    kc.subscribe(["test"])
    while not seen_error_cb:
        kc.poll(timeout=1)

    kc.close()


# global variable for stats_cb call back function
seen_stats_cb = False


def test_stats_cb():
    """ Tests stats_cb. """

    def stats_cb(stats_json_str):
        global seen_stats_cb
        seen_stats_cb = True
        stats_json = json.loads(stats_json_str)
        assert len(stats_json['name']) > 0

    conf = {'group.id': 'test',
            'socket.timeout.ms': '100',
            'session.timeout.ms': 1000,  # Avoid close() blocking too long
            'statistics.interval.ms': 200,
            'stats_cb': stats_cb
            }

    kc = confluent_kafka.Consumer(**conf)

    kc.subscribe(["test"])
    while not seen_stats_cb:
        kc.poll(timeout=1)
    kc.close()


seen_stats_cb_check_no_brokers = False


def test_conf_none():
    """ Issue #133
    Test that None can be passed for NULL by setting bootstrap.servers
    to None. If None would be converted to a string then a broker would
    show up in statistics. Verify that it doesnt. """

    def stats_cb_check_no_brokers(stats_json_str):
        """ Make sure no brokers are reported in stats """
        global seen_stats_cb_check_no_brokers
        stats = json.loads(stats_json_str)
        assert len(stats['brokers']) == 0, "expected no brokers in stats: %s" % stats_json_str
        seen_stats_cb_check_no_brokers = True

    conf = {'bootstrap.servers': None,  # overwrites previous value
            'statistics.interval.ms': 10,
            'stats_cb': stats_cb_check_no_brokers}

    p = confluent_kafka.Producer(conf)
    p.poll(timeout=1)

    global seen_stats_cb_check_no_brokers
    assert seen_stats_cb_check_no_brokers


def throttle_cb_instantiate_fail():
    """ Ensure noncallables raise TypeError"""
    with pytest.raises(ValueError):
        confluent_kafka.Producer({'throttle_cb': 1})


def throttle_cb_instantiate():
    """ Ensure we can configure a proper callback"""

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


@pytest.mark.xfail(sys.platform in ('linux2', 'linux'),
                   reason="confluent-librdkafka-plugins packaging issues")
@pytest.mark.skipif(skip_interceptors(),
                    reason="requires confluent-librdkafka-plugins be installed and copied to the current directory")
@pytest.mark.parametrize("init_func", [
    Consumer,
    Producer,
    AdminClient,
])
def test_unordered_dict(init_func):
    """
    Interceptor configs can only be handled after the plugin has been loaded not before.
    """
    client = init_func({'group.id': 'test-group',
                        'confluent.monitoring.interceptor.publishMs': 1000,
                        'confluent.monitoring.interceptor.sessionDurationMs': 1000,
                        'plugin.library.paths': 'monitoring-interceptor',
                        'confluent.monitoring.interceptor.topic': 'confluent-kafka-testing',
                        'confluent.monitoring.interceptor.icdebug': False})

    client.poll(0)


# global variable for on_delivery call back function
seen_delivery_cb = False


def test_topic_config_update():
    # *NOTE* default.topic.config has been deprecated.
    # This example remains to ensure backward-compatibility until its removal.
    confs = [{"message.timeout.ms": 600000, "default.topic.config": {"message.timeout.ms": 1000}},
             {"message.timeout.ms": 1000},
             {"default.topic.config": {"message.timeout.ms": 1000}}]

    def on_delivery(err, msg):
        # Since there is no broker, produced messages should time out.
        global seen_delivery_cb
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
