#!/usr/bin/env python
import pytest

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource
from confluent_kafka import KafkaException, KafkaError, libversion
import confluent_kafka
import concurrent.futures


def test_types():
    ConfigResource(confluent_kafka.admin.RESOURCE_BROKER, "2")
    ConfigResource("broker", "2")
    ConfigResource(confluent_kafka.admin.RESOURCE_GROUP, "mygroup")
    ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, "")
    with pytest.raises(ValueError):
        ConfigResource("doesnt exist", "hi")
    with pytest.raises(ValueError):
        ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, None)


@pytest.mark.skipif(libversion()[1] < 0x000b0500,
                    reason="AdminAPI requires librdkafka >= v0.11.5")
def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    with pytest.raises(TypeError):
        a = AdminClient()

    a = AdminClient({"socket.timeout.ms": 10})

    a.poll(0.001)

    try:
        a.list_topics(timeout=0.2)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT)


@pytest.mark.skipif(libversion()[1] < 0x000b0500,
                    reason="AdminAPI requires librdkafka >= v0.11.5")
def test_create_topics_api():
    """ create_topics() tests, these wont really do anything since there is no
        broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})
    f = a.create_topics([NewTopic("mytopic", 3, 2)],
                        validate_only=True)
    # ignore the result

    with pytest.raises(Exception):
        a.create_topics(None)

    with pytest.raises(Exception):
        a.create_topics("mytopic")

    with pytest.raises(Exception):
        a.create_topics(["mytopic"])

    with pytest.raises(Exception):
        a.create_topics([None, "mytopic"])

    with pytest.raises(Exception):
        a.create_topics([None, NewTopic("mytopic", 1, 2)])

    fs = a.create_topics([NewTopic("mytopic", 3, 2)])
    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)

    fs = a.create_topics([NewTopic("mytopic", 3,
                                   replica_assignment=[[10, 11], [0, 1, 2], [15, 20]],
                                   config={"some": "config"})])
    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)

    fs = a.create_topics([NewTopic("mytopic", 3, 2),
                          NewTopic("othertopic", 1, 10),
                          NewTopic("third", 500, 1, config={"more": "config",
                                                            "anint": 13,
                                                            "config2": "val"})],
                         validate_only=True,
                         request_timeout=0.5,
                         operation_timeout=300.1)

    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT

    with pytest.raises(TypeError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        validate_only="maybe")

    with pytest.raises(ValueError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        validate_only=False,
                        request_timeout=-5)

    with pytest.raises(ValueError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        operation_timeout=-4.12345678)

    with pytest.raises(TypeError):
        a.create_topics([NewTopic("mytopic", 3, 2)],
                        unknown_operation="it is")

    with pytest.raises(TypeError):
        a.create_topics([NewTopic("mytopic", 3, 2,
                                  config=["fails", "because not a dict"])])


@pytest.mark.skipif(libversion()[1] < 0x000b0500,
                    reason="AdminAPI requires librdkafka >= v0.11.5")
def test_delete_topics_api():
    """ delete_topics() tests, these wont really do anything since there is no
        broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})
    fs = a.delete_topics(["mytopic"])
    # ignore the result

    with pytest.raises(Exception):
        a.delete_topics(None)

    with pytest.raises(Exception):
        a.delete_topics("mytopic")

    with pytest.raises(Exception):
        a.delete_topics([])

    with pytest.raises(ValueError):
        a.delete_topics([None, "mytopic"])

    fs = a.delete_topics(["mytopic", "other"])
    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)

    fs = a.delete_topics(["mytopic", "othertopic", "third"],
                         request_timeout=0.5,
                         operation_timeout=300.1)
    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT

    with pytest.raises(TypeError):
        a.delete_topics(["mytopic"],
                        validate_only="maybe")


@pytest.mark.skipif(libversion()[1] < 0x000b0500,
                    reason="AdminAPI requires librdkafka >= v0.11.5")
def test_create_partitions_api():
    """ create_partitions() tests, these wont really do anything since there
        is no broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})
    fs = a.create_partitions([NewPartitions("mytopic", 50)])
    # ignore the result

    with pytest.raises(TypeError):
        a.create_partitions(None)

    with pytest.raises(Exception):
        a.create_partitions("mytopic")

    with pytest.raises(Exception):
        a.create_partitions([])

    with pytest.raises(Exception):
        a.create_partitions([None, NewPartitions("mytopic", 2)])

    fs = a.create_partitions([NewPartitions("mytopic", 100),
                              NewPartitions("other", 3,
                                            replica_assignment=[[10, 11], [15, 20]])])
    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)

    fs = a.create_partitions([NewPartitions("mytopic", 2),
                              NewPartitions("othertopic", 10),
                              NewPartitions("third", 55,
                                            replica_assignment=[[1, 2, 3, 4, 5, 6, 7], [2]])],
                             validate_only=True,
                             request_timeout=0.5,
                             operation_timeout=300.1)

    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT


@pytest.mark.skipif(libversion()[1] < 0x000b0500,
                    reason="AdminAPI requires librdkafka >= v0.11.5")
def test_describe_configs_api():
    """ describe_configs() tests, these wont really do anything since there
        is no broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})
    fs = a.describe_configs([ConfigResource(confluent_kafka.admin.RESOURCE_BROKER, "3")])
    # ignore the result

    with pytest.raises(Exception):
        a.describe_configs(None)

    with pytest.raises(Exception):
        a.describe_configs("something")

    with pytest.raises(Exception):
        a.describe_configs([])

    with pytest.raises(ValueError):
        a.describe_configs([None, ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, "mytopic")])

    fs = a.describe_configs([ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, "mytopic"),
                             ConfigResource(confluent_kafka.admin.RESOURCE_GROUP, "mygroup")],
                            request_timeout=0.123)
    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)


@pytest.mark.skipif(libversion()[1] < 0x000b0500,
                    reason="AdminAPI requires librdkafka >= v0.11.5")
def test_alter_configs_api():
    """ alter_configs() tests, these wont really do anything since there
        is no broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})
    fs = a.alter_configs([ConfigResource(confluent_kafka.admin.RESOURCE_BROKER, "3",
                                         set_config={"some": "config"})])
    # ignore the result

    with pytest.raises(Exception):
        a.alter_configs(None)

    with pytest.raises(Exception):
        a.alter_configs("something")

    with pytest.raises(ValueError):
        a.alter_configs([])

    fs = a.alter_configs([ConfigResource("topic", "mytopic",
                                         set_config={"set": "this",
                                                     "and": "this"}),
                          ConfigResource(confluent_kafka.admin.RESOURCE_GROUP,
                                         "mygroup")],
                         request_timeout=0.123)

    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)
