#!/usr/bin/env python
"""Tests for specifying configuration properties as Python lists (issue #711).

List and tuple values for string configuration properties are translated
to comma-separated strings before being passed to librdkafka, so that
e.g. 'bootstrap.servers' can be given as ['host1:9092', 'host2:9092'].

These tests do not require a broker.
"""
import pytest

from confluent_kafka import Consumer, Producer


def test_consumer_bootstrap_servers_as_list():
    c = Consumer(
        {
            'bootstrap.servers': ['localhost:9092', 'localhost:9093'],
            'group.id': 'test-list-config',
            'socket.timeout.ms': '100',
            'session.timeout.ms': 1000,
        }
    )
    c.close()


def test_consumer_bootstrap_servers_as_tuple():
    c = Consumer(
        {
            'bootstrap.servers': ('localhost:9092', 'localhost:9093'),
            'group.id': 'test-tuple-config',
            'socket.timeout.ms': '100',
            'session.timeout.ms': 1000,
        }
    )
    c.close()


def test_producer_bootstrap_servers_as_list():
    p = Producer({'bootstrap.servers': ['localhost:9092', 'localhost:9093'], 'socket.timeout.ms': 10})
    p.flush(0.1)


def test_single_element_list():
    c = Consumer(
        {
            'bootstrap.servers': ['localhost:9092'],
            'group.id': 'test-single-list-config',
            'socket.timeout.ms': '100',
            'session.timeout.ms': 1000,
        }
    )
    c.close()


def test_list_value_is_joined_not_stringified():
    """A one-element numeric list must reach librdkafka as '100', not '[100]'.

    With the old behaviour the Python list was stringified, and librdkafka
    rejected '[100]' as an invalid integer for socket.timeout.ms.
    """
    p = Producer({'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': [100]})
    p.flush(0.1)


def test_list_with_none_item_rejected():
    with pytest.raises(TypeError) as ex:
        Consumer({'bootstrap.servers': ['localhost:9092', None], 'group.id': 'test-none-item'})
    assert 'bootstrap.servers' in str(ex.value)


def test_list_with_nested_list_item_rejected():
    with pytest.raises(TypeError) as ex:
        Producer({'bootstrap.servers': [['localhost:9092']]})
    assert 'bootstrap.servers' in str(ex.value)
