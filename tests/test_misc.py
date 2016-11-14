#!/usr/bin/env python

import confluent_kafka
import json
import time
from pprint import pprint

def test_version():
    print('Using confluent_kafka module version %s (0x%x)' % confluent_kafka.version())
    sver, iver = confluent_kafka.version()
    assert len(sver) > 0
    assert iver > 0

    print('Using librdkafka version %s (0x%x)' % confluent_kafka.libversion())
    sver, iver = confluent_kafka.libversion()
    assert len(sver) > 0
    assert iver > 0

# global variable for error_cb call back function
seen_error_cb = False

def test_error_cb():
    """ Tests error_cb. """

    def error_cb(error_msg):
        global seen_error_cb
        seen_error_cb = True
        assert error_msg.code() in (confluent_kafka.KafkaError._TRANSPORT, confluent_kafka.KafkaError._ALL_BROKERS_DOWN)

    conf = {'bootstrap.servers': 'localhost:65531', # Purposely cause connection refused error
            'group.id':'test',
            'socket.timeout.ms':'100',
            'session.timeout.ms': 1000, # Avoid close() blocking too long
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

    conf = {'group.id':'test',
            'socket.timeout.ms':'100',
            'session.timeout.ms': 1000, # Avoid close() blocking too long
            'statistics.interval.ms': 200,
            'stats_cb': stats_cb
           }

    kc = confluent_kafka.Consumer(**conf)

    kc.subscribe(["test"])
    while not seen_stats_cb:
        kc.poll(timeout=1)
    kc.close()

