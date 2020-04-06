#!/usr/bin/env python

import pytest
from confluent_kafka import Producer, KafkaError, KafkaException
import time

seen_all_brokers_down = False


def error_cb(err):
    print('error_cb', err)
    assert err.fatal() is False
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


def test_fatal():
    """ Test fatal exceptions """

    # Configure an invalid broker and make sure the ALL_BROKERS_DOWN
    # error is seen in the error callback.
    p = Producer({'error_cb': error_cb})

    with pytest.raises(KafkaException) as exc:
        raise KafkaException(KafkaError(KafkaError.MEMBER_ID_REQUIRED,
                                        fatal=True))
    err = exc.value.args[0]
    assert isinstance(err, KafkaError)
    assert err.fatal()
    assert not err.retriable()
    assert not err.txn_requires_abort()

    p.poll(0)  # Need some p use to avoid flake8 unused warning


def test_retriable():
    """ Test retriable exceptions """

    with pytest.raises(KafkaException) as exc:
        raise KafkaException(KafkaError(KafkaError.MEMBER_ID_REQUIRED,
                                        retriable=True))
    err = exc.value.args[0]
    assert isinstance(err, KafkaError)
    assert not err.fatal()
    assert err.retriable()
    assert not err.txn_requires_abort()


def test_abortable():
    """ Test abortable exceptions """

    with pytest.raises(KafkaException) as exc:
        raise KafkaException(KafkaError(KafkaError.MEMBER_ID_REQUIRED,
                                        txn_requires_abort=True))
    err = exc.value.args[0]
    assert isinstance(err, KafkaError)
    assert not err.fatal()
    assert not err.retriable()
    assert err.txn_requires_abort()


def test_subclassing():
    class MyExc(KafkaException):
        def a_method(self):
            return "yes"
    err = MyExc()
    assert err.a_method() == "yes"
    assert isinstance(err, KafkaException)


def test_kafkaError_custom_msg():
    err = KafkaError(KafkaError._ALL_BROKERS_DOWN, "Mayday!")
    assert err == KafkaError._ALL_BROKERS_DOWN
    assert err.str() == "Mayday!"
    assert not err.fatal()
    assert not err.fatal()
    assert not err.retriable()
    assert not err.txn_requires_abort()


def test_kafkaError_unknonw_error():
    with pytest.raises(KafkaException, match="Err-12345?") as e:
        raise KafkaError(12345)
    assert not e.value.args[0].fatal()
    assert not e.value.args[0].retriable()
    assert not e.value.args[0].txn_requires_abort()


def test_kafkaException_unknown_KafkaError_with_subclass():
    class MyException(KafkaException):
        def __init__(self, error_code):
            super(MyException, self).__init__(KafkaError(error_code))

    with pytest.raises(KafkaException, match="Err-12345?") as e:
        raise MyException(12345)
    assert not e.value.args[0].fatal()
    assert not e.value.args[0].fatal()
    assert not e.value.args[0].retriable()
    assert not e.value.args[0].txn_requires_abort()
