import pickle
import pytest
import sys

from confluent_kafka.cimpl import Message


def empty_message_1():
    return Message()


def empty_message_2():
    return Message(None, None, None, None, None, -2, -2, -2, -2, -2)


def empty_message_3():
    msg = Message()
    msg.set_topic(None)
    msg.set_value(None)
    msg.set_key(None)
    msg.set_headers(None)
    msg.set_error(None)
    return msg


def empty_message_4():
    return Message.__new__(Message)


class Message2(Message):
    def __init__(self, *args):
        super().__init__(*args)
        self.dummy = 1


def empty_message_5():
    msg = Message2()
    assert type(msg) is Message2
    assert msg.dummy == 1
    return msg


@pytest.mark.parametrize(
    "make_message",
    [
        empty_message_1,
        empty_message_2,
        empty_message_3,
        empty_message_4,
        empty_message_5,
    ],
)
def test_message_create_empty(make_message):
    # Checks the creation of an empty Message with no data.

    msg = make_message()

    assert len(msg) == 0
    assert msg.topic() is None
    assert msg.value() is None
    assert msg.key() is None
    assert msg.headers() is None
    assert msg.error() is None
    assert msg.partition() is None
    assert msg.offset() is None
    assert msg.leader_epoch() is None
    assert msg.timestamp() == (0, 0)
    assert msg.latency() is None
    assert str(msg)
    assert repr(msg)

    subtest_pickling(msg, (None,) * 5 + (-1, -1, -1, 0, -1))


def test_message_create_with_dummy():
    # Checks the creation of an Message with any kind of dummy arguments. Useful
    # to create Message objects in unit tests with Mock objects as arguments,
    # for instance.

    dummy = object()
    msg = Message(dummy, dummy, dummy, dummy, dummy)
    assert msg.topic() is dummy
    assert msg.value() is dummy
    assert msg.key() is dummy
    assert msg.headers() is dummy
    assert msg.error() is dummy
    assert str(msg)
    assert repr(msg)


def test_message_create_with_args():
    # Tests all positional arguments.

    headers, error = [], object()
    msg = Message("t", "v", "k", headers, error, 1, 2, 3, 4, 5.67)
    assert len(msg) == 1
    assert msg.topic() == "t"
    assert msg.value() == "v"
    assert msg.key() == "k"
    assert msg.headers() is headers
    assert msg.error() is error
    assert msg.partition() == 1
    assert msg.offset() == 2
    assert msg.leader_epoch() == 3
    assert msg.timestamp() == (0, 4)
    assert msg.latency() == 5.67
    assert str(msg)
    assert repr(msg)


def test_message_create_with_kwds():
    # Tests all keyword arguments.

    headers, error = [], object()
    msg = Message(
        topic="t",
        value="v",
        key="k",
        headers=headers,
        error=error,
        partition=1,
        offset=2,
        leader_epoch=3,
        timestamp=4,
        latency=5.67,
    )
    assert len(msg) == 1
    assert msg.topic() == "t"
    assert msg.value() == "v"
    assert msg.key() == "k"
    assert msg.headers() is headers
    assert msg.error() is error
    assert msg.partition() == 1
    assert msg.offset() == 2
    assert msg.leader_epoch() == 3
    assert msg.timestamp() == (0, 4)
    assert msg.latency() == 5.67


def test_message_set_properties():
    # Tests all set_<name>() methods.

    headers, error = [], object()
    msg = Message()
    assert len(msg) == 0
    msg.set_topic("t")
    assert msg.topic() == "t"
    msg.set_value("v")
    assert msg.value() == "v"
    assert len(msg) == 1
    msg.set_key("k")
    assert msg.key() == "k"
    msg.set_headers(headers)
    assert msg.headers() is headers
    msg.set_error(error)
    assert msg.error() is error


@pytest.mark.parametrize("value", [None, object()])
def test_message_exceptions(value):
    # Tests many situations which should raise TypeError. This is important to
    # ensure the "self" object is type checked to be a Message before trying to
    # do anything with it internally in the C code.

    with pytest.raises(TypeError):
        Message.__new__(value)
    with pytest.raises(TypeError):
        Message.__new__(str)

    with pytest.raises(TypeError):
        Message.__init__(value)

    with pytest.raises(TypeError):
        Message.topic(value)
    with pytest.raises(TypeError):
        Message.value(value)
    with pytest.raises(TypeError):
        Message.key(value)
    with pytest.raises(TypeError):
        Message.headers(value)
    with pytest.raises(TypeError):
        Message.error(value)

    with pytest.raises(TypeError):
        Message.partition(value)
    with pytest.raises(TypeError):
        Message.offset(value)
    with pytest.raises(TypeError):
        Message.leader_epoch(value)
    with pytest.raises(TypeError):
        Message.timestamp(value)
    with pytest.raises(TypeError):
        Message.latency(value)

    with pytest.raises(TypeError):
        Message.set_topic(value, "t")
    with pytest.raises(TypeError):
        Message.set_value(value, "v")
    with pytest.raises(TypeError):
        Message.set_key(value, "k")
    with pytest.raises(TypeError):
        Message.set_headers(value, [])
    with pytest.raises(TypeError):
        Message.set_error(value, object())

    with pytest.raises(TypeError):
        len(Message(value=1))
    with pytest.raises(TypeError):
        len(Message(value=object()))


def subtest_pickling(msg, exp_args):
    assert msg.__reduce__() == (type(msg), exp_args)

    pickled = pickle.dumps(msg)
    restored = pickle.loads(pickled)

    assert restored.__reduce__() == (type(msg), exp_args)
    assert msg is not restored
    assert type(msg) is type(restored)

    assert len(msg) == len(restored)
    assert msg.topic() == restored.topic()
    assert msg.value() == restored.value()
    assert msg.key() == restored.key()
    assert msg.headers() == restored.headers()
    assert msg.error() == restored.error()
    assert msg.partition() == restored.partition()
    assert msg.offset() == restored.offset()
    assert msg.leader_epoch() == restored.leader_epoch()
    assert msg.timestamp() == restored.timestamp()
    assert msg.latency() == restored.latency()


def test_message_pickle():
    args = "t", "v", "k", [], None, 1, 2, 3, 4, 5.67
    msg = Message(*args)
    assert msg.latency() == 5.67

    subtest_pickling(msg, args)


def test_message_compare():
    args0 = "t", "v", "k", [], None, 1, 2, 3, 4, 5.67
    args1 = "t", "v", "z", [], None, 1, 2, 3, 4, 5.67

    msg0 = Message(*args0)
    msg01 = Message(*args0)
    msg1 = Message(*args1)

    assert msg0 == msg0
    assert msg0 == msg01
    assert msg0 != msg1
    assert msg0 != 1
    assert msg0 != None
    assert msg0 != object()

    with pytest.raises(TypeError):
        assert msg0 < msg0
    with pytest.raises(TypeError):
        assert msg0 < None
    with pytest.raises(TypeError):
        assert msg0 < object()
