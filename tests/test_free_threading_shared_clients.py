import concurrent.futures
import os
import sys
import sysconfig
import threading
import time
import uuid

import pytest

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
BOOTSTRAP_SERVERS = os.environ.get("TEST_FREE_THREADED_BROKER")

pytestmark = [
    pytest.mark.skipif(
        not FREE_THREADED_BUILD,
        reason="requires a free-threaded CPython build",
    ),
    pytest.mark.skipif(
        not BOOTSTRAP_SERVERS,
        reason="TEST_FREE_THREADED_BROKER is not set",
    ),
]


def test_shared_clients_against_real_broker():
    topic = f"confluent-kafka-nogil-{uuid.uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(15)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            metadata = list(executor.map(lambda _: admin.list_topics(timeout=5), range(32)))
        assert all(topic in item.topics for item in metadata)

        producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "queue.buffering.max.messages": 100_000,
            }
        )
        delivered = 0
        delivered_lock = threading.Lock()

        def on_delivery(error, _message):
            nonlocal delivered
            assert error is None
            with delivered_lock:
                delivered += 1

        def produce(thread_id):
            for index in range(250):
                producer.produce(
                    topic,
                    key=f"{thread_id}:{index}",
                    value=b"free-threaded",
                    callback=on_delivery,
                )
                if index % 25 == 0:
                    producer.poll(0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(produce, range(8)))

        assert producer.flush(20) == 0
        assert delivered == 2_000

        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "group.id": f"confluent-kafka-nogil-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        seen = set()
        seen_lock = threading.Lock()
        stop = threading.Event()
        deadline = time.monotonic() + 30

        def consume():
            while not stop.is_set() and time.monotonic() < deadline:
                message = consumer.poll(0.2)
                if message is None:
                    continue
                assert message.error() is None
                with seen_lock:
                    seen.add(message.key())
                    if len(seen) == 2_000:
                        stop.set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(lambda _: consume(), range(4)))

        consumer.close()
        assert len(seen) == 2_000
        assert not sys._is_gil_enabled()
    finally:
        admin.delete_topics([topic], operation_timeout=10)[topic].result(15)
