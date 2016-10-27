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

def test_error_cb():
    """ Tests error_cb. """

    def error_cb(error_msg):
        print('OK: error_cb() called')
        assert error_msg.code() in (confluent_kafka.KafkaError._TRANSPORT, confluent_kafka.KafkaError._ALL_BROKERS_DOWN)

    conf = {'bootstrap.servers': 'localhost:9093', # Purposely cause connection refused error
            'group.id':'test',
            'socket.timeout.ms':'100',
            'session.timeout.ms': 1000, # Avoid close() blocking too long
            'error_cb': error_cb
           }

    kc = confluent_kafka.Consumer(**conf)

    kc.subscribe(["test"])
    kc.poll(timeout=0.001)
    time.sleep(1)
    kc.unsubscribe()

    kc.close()

def test_stats_cb():
    """ Tests stats_cb. """

    def stats_cb(stats_json_str):
        # print(stats_json_str)
        try:
            stats_json = json.loads(stats_json_str)
            if 'type' in stats_json:
                print("stats_cb: type=%s" % stats_json['type'])
            print('OK: stats_cb() called')
        except Exception as e:
            assert False

    conf = {'group.id':'test',
            'socket.timeout.ms':'100',
            'session.timeout.ms': 1000, # Avoid close() blocking too long
            'statistics.interval.ms': 200,
            'stats_cb': stats_cb
           }

    kc = confluent_kafka.Consumer(**conf)

    kc.subscribe(["test"])
    kc.poll(timeout=0.001)
    time.sleep(1)

    kc.close()

