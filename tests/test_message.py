# #!/usr/bin/env python

from confluent_kafka import Message, KafkaError


def test_init_no_params():
    m = Message()
    assert m.topic() is None
    assert m.partition() is None
    assert m.offset() is None
    assert m.key() is None
    assert m.value() is None
    assert m.headers() is None
    assert m.error() is None
    assert m.timestamp() == (0, 0)
    assert m.latency() is None
    assert m.leader_epoch() is None

def test_init_all_params():
    m = Message(topic="test", partition=1, offset=2, key=b"key", value=b"value", headers=[("h1", "v1")], error=KafkaError(0),
                timestamp=(1, 1762499956), latency=0.05, leader_epoch=1762499956)
    assert m.topic() == "test"
    assert m.partition() == 1
    assert m.offset() == 2
    assert m.key() == b"key"
    assert m.value() == b"value"
    assert m.headers() == [("h1", "v1")]
    assert m.error() == KafkaError(0)
    assert m.timestamp() == (1, 1762499956)
    assert m.latency() == 0.05
    assert m.leader_epoch() == 1762499956

def test_init_negative_param_values():
    m = Message(partition=-1, offset=-1, latency=-1.0, leader_epoch=-1762499956)
    assert m.partition() is None
    assert m.offset() is None
    assert m.latency() is None
    assert m.leader_epoch() is None


def test_set_headers():
    m = Message()
    m.set_headers([("h1", "v1")])
    assert m.headers() == [("h1", "v1")]
    m.set_headers([("h2", "v2")])
    assert m.headers() == [("h2", "v2")]

def test_set_key():
    m = Message()
    m.set_key(b"key")
    assert m.key() == b"key"

def test_set_value():
    m = Message()
    m.set_value(b"value")
    assert m.value() == b"value"
