#!/usr/bin/env python

import time

import pytest

from confluent_kafka import (
    KafkaError,
    KafkaException,
    KafkaFatalException,
    KafkaRetriableException,
    KafkaTxnRequiresAbortException,
    Producer,
)
from confluent_kafka.error import ConsumeError, ProduceError

seen_all_brokers_down = False


def error_cb(err):
    print('error_cb', err)
    assert err.fatal() is False
    if err.code() == KafkaError._ALL_BROKERS_DOWN:
        global seen_all_brokers_down
        seen_all_brokers_down = True


def test_error_cb():
    """Test the error callback."""

    # Configure an invalid broker and make sure the ALL_BROKERS_DOWN
    # error is seen in the error callback.
    p = Producer({'bootstrap.servers': '127.0.0.1:1', 'socket.timeout.ms': 10, 'error_cb': error_cb})

    t_end = time.time() + 5

    while not seen_all_brokers_down and time.time() < t_end:
        p.poll(1)

    assert seen_all_brokers_down


def test_fatal():
    """Test fatal exceptions"""

    # Configure an invalid broker and make sure the ALL_BROKERS_DOWN
    # error is seen in the error callback.
    p = Producer({'error_cb': error_cb})

    with pytest.raises(KafkaException) as exc:
        raise KafkaException(KafkaError(KafkaError.MEMBER_ID_REQUIRED, fatal=True))
    err = exc.value.args[0]
    assert isinstance(err, KafkaError)
    assert err.fatal()
    assert not err.retriable()
    assert not err.txn_requires_abort()

    p.poll(0)  # Need some p use to avoid flake8 unused warning


def test_retriable():
    """Test retriable exceptions"""

    with pytest.raises(KafkaException) as exc:
        raise KafkaException(KafkaError(KafkaError.MEMBER_ID_REQUIRED, retriable=True))
    err = exc.value.args[0]
    assert isinstance(err, KafkaError)
    assert not err.fatal()
    assert err.retriable()
    assert not err.txn_requires_abort()


def test_abortable():
    """Test abortable exceptions"""

    with pytest.raises(KafkaException) as exc:
        raise KafkaException(KafkaError(KafkaError.MEMBER_ID_REQUIRED, txn_requires_abort=True))
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


def test_kafkaError_unknown_error():
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


def _expected_exception_type(err):
    if err.fatal():
        return KafkaFatalException
    if err.txn_requires_abort():
        return KafkaTxnRequiresAbortException
    if err.retriable():
        return KafkaRetriableException
    return KafkaException


@pytest.mark.parametrize(
    "flags,expected",
    [
        ({}, KafkaException),
        ({'retriable': True}, KafkaRetriableException),
        ({'fatal': True}, KafkaFatalException),
        ({'txn_requires_abort': True}, KafkaTxnRequiresAbortException),
        # The most severe flag wins: fatal, then abort, then retriable.
        ({'fatal': True, 'retriable': True}, KafkaFatalException),
        ({'fatal': True, 'txn_requires_abort': True}, KafkaFatalException),
        ({'txn_requires_abort': True, 'retriable': True}, KafkaTxnRequiresAbortException),
    ],
)
def test_kafkaException_picks_subclass_from_flags(flags, expected):
    err = KafkaError(KafkaError._TIMED_OUT, **flags)
    exc = KafkaException(err)

    assert type(exc) is expected
    assert isinstance(exc, KafkaException)
    assert exc.args == (err,)
    assert exc.args[0] is err
    assert str(exc) == str(err)

    # Handlers for the specific class and for the base class both catch it.
    with pytest.raises(expected):
        raise KafkaException(err)
    with pytest.raises(KafkaException):
        raise KafkaException(err)


def test_kafkaException_subclass_hierarchy():
    for cls in (KafkaRetriableException, KafkaFatalException, KafkaTxnRequiresAbortException):
        assert issubclass(cls, KafkaException)
        assert cls.__doc__
    assert not issubclass(KafkaRetriableException, KafkaFatalException)
    assert not issubclass(KafkaFatalException, KafkaRetriableException)
    assert not issubclass(KafkaTxnRequiresAbortException, KafkaRetriableException)


def test_kafkaException_without_flagged_error_stays_base_class():
    assert type(KafkaException()) is KafkaException
    assert type(KafkaException("some message")) is KafkaException
    assert type(KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN))) is KafkaException
    # Only the first argument is inspected.
    assert type(KafkaException("msg", KafkaError(KafkaError._TIMED_OUT, retriable=True))) is KafkaException
    assert KafkaException("a", "b").args == ("a", "b")


def test_kafkaException_subclasses_are_not_redirected():
    retriable = KafkaError(KafkaError._TIMED_OUT, retriable=True)

    # Constructing a subclass directly always gives that subclass.
    assert type(KafkaFatalException(retriable)) is KafkaFatalException
    assert type(KafkaRetriableException(KafkaError(KafkaError._TIMED_OUT))) is KafkaRetriableException

    # User and library subclasses keep their own type.
    class MyExc(KafkaException):
        pass

    assert type(MyExc(retriable)) is MyExc
    assert type(ConsumeError(retriable)) is ConsumeError
    assert type(ProduceError(retriable)) is ProduceError


def test_transactional_api_raises_retriable_exception():
    """Errors raised from C carry the subclass matching their flags."""
    p = Producer({"transactional.id": "test", "bootstrap.servers": "127.0.0.1:1"})

    with pytest.raises(KafkaRetriableException) as ex:
        p.init_transactions(0.5)
    err = ex.value.args[0]
    assert err.code() == KafkaError._TIMED_OUT
    assert err.retriable()
    assert type(ex.value) is _expected_exception_type(err)

    # Subsequent calls fail too; whatever librdkafka reports, the class
    # must agree with the KafkaError flags.
    with pytest.raises(KafkaException) as ex:
        p.begin_transaction()
    assert type(ex.value) is _expected_exception_type(ex.value.args[0])
