import concurrent.futures
import sys
import sysconfig
import threading

import pytest

from confluent_kafka import Producer

FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

pytestmark = pytest.mark.skipif(
    not FREE_THREADED_BUILD,
    reason="requires a free-threaded CPython build",
)


def producer_config():
    return {
        "bootstrap.servers": "127.0.0.1:1",
        "message.timeout.ms": 50,
        "socket.timeout.ms": 10,
        "queue.buffering.max.messages": 100_000,
    }


def test_cimpl_keeps_gil_disabled():
    assert not sys._is_gil_enabled()


def test_independent_producers_from_multiple_threads():
    def produce(thread_id):
        delivered = 0

        def on_delivery(_error, _message):
            nonlocal delivered
            delivered += 1

        producer = Producer(producer_config())
        for index in range(200):
            producer.produce(
                "free-threading-independent",
                value=f"{thread_id}:{index}",
                callback=on_delivery,
            )
        assert producer.flush(5.0) == 0
        return delivered

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        delivered = list(executor.map(produce, range(8)))

    assert delivered == [200] * 8
    assert not sys._is_gil_enabled()


def test_shared_producer_from_multiple_threads():
    producer = Producer(producer_config())
    delivered = 0
    delivered_lock = threading.Lock()

    def on_delivery(_error, _message):
        nonlocal delivered
        with delivered_lock:
            delivered += 1

    def produce(thread_id):
        for index in range(500):
            producer.produce(
                "free-threading-shared",
                value=f"{thread_id}:{index}",
                callback=on_delivery,
            )
            if index % 25 == 0:
                producer.poll(0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(produce, range(8)))

    assert producer.flush(10.0) == 0
    assert delivered == 4_000
    assert not sys._is_gil_enabled()
