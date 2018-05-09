#!/usr/bin/env python
import pytest

from confluent_kafka import AdminClient, NewTopic, KafkaError, ConfigResource, libversion
import confluent_kafka
import concurrent.futures


def test_types():
    ConfigResource(confluent_kafka.RESOURCE_BROKER, "2")
    ConfigResource("broker", "2")
    ConfigResource(confluent_kafka.RESOURCE_GROUP, "mygroup")
    ConfigResource(confluent_kafka.RESOURCE_TOPIC, "")
    with pytest.raises(ValueError):
        ConfigResource("doesnt exist", "hi")
    with pytest.raises(ValueError):
        ConfigResource(confluent_kafka.RESOURCE_TOPIC, None)


@pytest.mark.skipif(True or libversion()[1] < 0x000b0500,
                    reason="AdminAPI requires librdkafka >= v0.11.5")
def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    with pytest.raises(TypeError):
        a = AdminClient()

    a = AdminClient({'socket.timeout.ms': 10})

    with pytest.raises(TypeError):
        a.create_topics(None)

    with pytest.raises(TypeError):
        a.create_topics("mytopic")

    with pytest.raises(TypeError):
        a.create_topics(["mytopic"])

    with pytest.raises(TypeError):
        a.create_topics([None, "mytopic"])

    f = a.create_topics([NewTopic("mytopic", 3, 2)])
    with pytest.raises(concurrent.futures.TimeoutError):
        f.result(timeout=1)

    f = a.create_topics([NewTopic("mytopic", 3, 2)])
    e = f.exception(timeout=1)
    assert isinstance(e, KafkaError)
    assert e.code() == KafkaError.TIMED_OUT

    f = a.create_topics([NewTopic("mytopic", 3, 2)],
                        validate_only=True,
                        request_timeout=0.5,
                        operation_timeout=300.1)
    e = f.exception(timeout=1)
    assert isinstance(e, KafkaError)
    assert e.code() == KafkaError.TIMED_OUT

    with pytest.raises(ValueError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        validate_only="maybe")

    with pytest.raises(ValueError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        validate_only=False,
                        request_timeout=-5)

    with pytest.raises(ValueError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        operation_timeout=-4.12345678)

    with pytest.raises(ValueError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        unknown_operation="it is")

    a.poll(0.001)


if __name__ == '__main__':
    test_basic_api()
