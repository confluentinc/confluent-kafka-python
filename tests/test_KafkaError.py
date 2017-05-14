#!/usr/bin/env python

from confluent_kafka import Producer, KafkaError, KafkaException
import time

seen_all_brokers_down = False


def error_cb(err):
    print('error_cb', err)
    if err.code() == KafkaError._ALL_BROKERS_DOWN:
        global seen_all_brokers_down
        seen_all_brokers_down = True


def test_error_cb():
    """ Test the error callback. """

    global seen_all_brokers_down

    # Configure an invalid broker and make sure the ALL_BROKERS_DOWN
    # error is seen in the error callback.
    p = Producer({'bootstrap.servers': '127.0.0.1:1', 'socket.timeout.ms': 10,
                  'error_cb': error_cb})

    t_end = time.time() + 5

    while not seen_all_brokers_down and time.time() < t_end:
        p.poll(1)

    assert seen_all_brokers_down


def test_subclassing():
    class MyExc(KafkaException):
        def a_method(self):
            return "yes"
    err = MyExc()
    assert err.a_method() == "yes"
    assert isinstance(err, KafkaException)
