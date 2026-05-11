"""
Smoke test for Message.delivery_count() on the ShareConsumer.

Requires a Kafka broker with KIP-932 enabled (share groups) reachable at
localhost:9092. Produces one message, RELEASEs the first delivery,
re-consumes, and prints the delivery_count each time. Expected output:

    poll #1: value=b'hello' delivery_count=1  -> RELEASE
    poll #2: value=b'hello' delivery_count=2  -> ACCEPT
"""

import time
import uuid

from confluent_kafka import AcknowledgeType, Producer, ShareConsumer

BOOTSTRAP = 'localhost:9092'
TOPIC = f'delivery-count-{uuid.uuid4().hex[:6]}'
GROUP = f'g-{uuid.uuid4().hex[:6]}'


def main():
    p = Producer({'bootstrap.servers': BOOTSTRAP})

    # Create topic by producing a sentinel and waiting for metadata.
    p.produce(TOPIC, value=b'__init__')
    p.flush(timeout=5.0)
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        md = p.list_topics(TOPIC, timeout=2.0)
        if TOPIC in md.topics and md.topics[TOPIC].partitions:
            break
        time.sleep(0.1)

    sc = ShareConsumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': GROUP,
        'share.acknowledgement.mode': 'explicit',
    })
    sc.subscribe([TOPIC])

    # Warm up: drive empty polls until SPSO snaps to 'latest'.
    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        sc.poll(timeout=1.0)

    p.produce(TOPIC, value=b'hello')
    p.flush(timeout=5.0)

    # --- first delivery: RELEASE so the record is redelivered ---
    msg = _poll_one(sc, timeout=10.0)
    print(f'poll #1: value={msg.value()!r} delivery_count={msg.delivery_count()}  -> RELEASE')
    assert msg.delivery_count() == 1, msg.delivery_count()
    sc.acknowledge(msg, AcknowledgeType.RELEASE)
    sc.commit_sync(timeout=5.0)

    # --- second delivery: count must be 2, then ACCEPT ---
    msg2 = _poll_one(sc, timeout=10.0)
    print(f'poll #2: value={msg2.value()!r} delivery_count={msg2.delivery_count()}  -> ACCEPT')
    assert msg2.delivery_count() == 2, msg2.delivery_count()
    sc.acknowledge(msg2, AcknowledgeType.ACCEPT)
    sc.commit_sync(timeout=5.0)

    sc.close()
    print('OK')


def _poll_one(sc, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msgs = sc.poll(timeout=1.0)
        for m in msgs:
            if m.error() is None:
                return m
    raise TimeoutError('no message received')


if __name__ == '__main__':
    main()
