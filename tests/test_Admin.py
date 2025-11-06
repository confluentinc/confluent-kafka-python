#!/usr/bin/env python
import pytest

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, \
    ConfigResource, ConfigEntry, AclBinding, AclBindingFilter, ResourceType, \
    ResourcePatternType, AclOperation, AclPermissionType, AlterConfigOpType, \
    ScramCredentialInfo, ScramMechanism, \
    UserScramCredentialAlteration, UserScramCredentialDeletion, \
    UserScramCredentialUpsertion, OffsetSpec, _ElectionType as ElectionType
from confluent_kafka import KafkaException, KafkaError, \
    TopicPartition, ConsumerGroupTopicPartitions, ConsumerGroupState, \
    IsolationLevel, TopicCollection
import concurrent.futures


def test_types():
    ConfigResource(ResourceType.BROKER, "2")
    ConfigResource("broker", "2")
    ConfigResource(ResourceType.GROUP, "mygroup")
    ConfigResource(ResourceType.TOPIC, "")
    with pytest.raises(ValueError):
        ConfigResource("doesnt exist", "hi")
    with pytest.raises(ValueError):
        ConfigResource(ResourceType.TOPIC, None)


def test_acl_binding_type():
    attrs = [ResourceType.TOPIC, "topic", ResourcePatternType.LITERAL,
             "User:u1", "*", AclOperation.WRITE, AclPermissionType.ALLOW]

    attrs_nullable_acl_binding_filter = [1, 3, 4]

    # at first it creates correctly
    AclBinding(*attrs)
    for i, _ in enumerate(attrs):

        # no attribute is nullable
        attrs_copy = list(attrs)
        attrs_copy[i] = None
        with pytest.raises(ValueError):
            AclBinding(*attrs_copy)

        # string attributes of AclBindingFilter are nullable
        if i in attrs_nullable_acl_binding_filter:
            AclBindingFilter(*attrs_copy)
        else:
            with pytest.raises(ValueError):
                AclBindingFilter(*attrs_copy)

    for (attr_num, attr_value) in [
        (0, ResourceType.ANY),
        (2, ResourcePatternType.ANY),
        (2, ResourcePatternType.MATCH),
        (5, AclOperation.ANY),
        (6, AclPermissionType.ANY),
    ]:
        attrs_copy = list(attrs)
        attrs_copy[attr_num] = attr_value
        # forbidden enums in AclBinding
        with pytest.raises(ValueError):
            AclBinding(*attrs_copy)

        # AclBindingFilter can hold all the enum values
        AclBindingFilter(*attrs_copy)

    # UNKNOWN values are not forbidden, for received values
    for (attr_num, attr_value) in [
        (0, ResourceType.UNKNOWN),
        (2, ResourcePatternType.UNKNOWN),
        (2, ResourcePatternType.UNKNOWN),
        (5, AclOperation.UNKNOWN),
        (6, AclPermissionType.UNKNOWN),
    ]:
        attrs_copy = list(attrs)
        attrs_copy[attr_num] = attr_value
        AclBinding(*attrs_copy)


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

    try:
        a.list_groups(timeout=0.2)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT)


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

    try:
        a.create_topics([NewTopic("mytopic")])
    except Exception as err:
        assert False, f"When none of the partitions, \
            replication and assignment is present, the request should not fail, but it does with error {err}"
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


def test_describe_configs_api():
    """ describe_configs() tests, these wont really do anything since there
        is no broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})
    fs = a.describe_configs([ConfigResource(ResourceType.BROKER, "3")])
    # ignore the result

    with pytest.raises(Exception):
        a.describe_configs(None)

    with pytest.raises(Exception):
        a.describe_configs("something")

    with pytest.raises(Exception):
        a.describe_configs([])

    with pytest.raises(ValueError):
        a.describe_configs([None, ConfigResource(ResourceType.TOPIC, "mytopic")])

    fs = a.describe_configs([ConfigResource(ResourceType.TOPIC, "mytopic"),
                             ConfigResource(ResourceType.GROUP, "mygroup")],
                            request_timeout=0.123)
    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)


def test_alter_configs_api():
    """ alter_configs() tests, these wont really do anything since there
        is no broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})
    fs = a.alter_configs([ConfigResource(ResourceType.BROKER, "3",
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
                          ConfigResource(ResourceType.GROUP,
                                         "mygroup")],
                         request_timeout=0.123)

    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)


def verify_incremental_alter_configs_api_call(a,
                                              restype, resname,
                                              incremental_configs,
                                              error,
                                              constructor_param=True):
    if constructor_param:
        resources = [ConfigResource(restype, resname,
                                    incremental_configs=incremental_configs)]
    else:
        resources = [ConfigResource(restype, resname)]
        for config_entry in incremental_configs:
            resources[0].add_incremental_config(config_entry)

    if error:
        with pytest.raises(error):
            fs = a.incremental_alter_configs(resources)
            for f in concurrent.futures.as_completed(iter(fs.values())):
                f.result(timeout=1)
    else:
        fs = a.incremental_alter_configs(resources)
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)


def test_incremental_alter_configs_api():
    a = AdminClient({"socket.timeout.ms": 10})

    with pytest.raises(TypeError):
        a.incremental_alter_configs(None)

    with pytest.raises(TypeError):
        a.incremental_alter_configs("something")

    with pytest.raises(ValueError):
        a.incremental_alter_configs([])

    for use_constructor in [True, False]:
        # incremental_operation not of type AlterConfigOpType
        verify_incremental_alter_configs_api_call(a, ResourceType.BROKER, "1",
                                                  [
                                                      ConfigEntry("advertised.listeners",
                                                                  "host1",
                                                                  incremental_operation="NEW_OPERATION")
                                                  ],
                                                  TypeError,
                                                  use_constructor)
        # None name
        verify_incremental_alter_configs_api_call(a, ResourceType.BROKER, "1",
                                                  [
                                                      ConfigEntry(None,
                                                                  "host1",
                                                                  incremental_operation=AlterConfigOpType.APPEND)
                                                  ],
                                                  TypeError,
                                                  use_constructor)

        # name type
        verify_incremental_alter_configs_api_call(a, ResourceType.BROKER, "1",
                                                  [
                                                      ConfigEntry(5,
                                                                  "host1",
                                                                  incremental_operation=AlterConfigOpType.APPEND)
                                                  ],
                                                  TypeError,
                                                  use_constructor)

        # Empty list
        verify_incremental_alter_configs_api_call(a, ResourceType.BROKER, "1",
                                                  [],
                                                  ValueError,
                                                  use_constructor)

        # String instead of ConfigEntry list, treated as an iterable
        verify_incremental_alter_configs_api_call(a, ResourceType.BROKER, "1",
                                                  "something",
                                                  TypeError,
                                                  use_constructor)

        # Duplicate ConfigEntry found
        verify_incremental_alter_configs_api_call(a, ResourceType.BROKER, "1",
                                                  [
                                                      ConfigEntry(
                                                          name="advertised.listeners",
                                                          value="host1:9092",
                                                          incremental_operation=AlterConfigOpType.APPEND
                                                      ),
                                                      ConfigEntry(
                                                          name="advertised.listeners",
                                                          value=None,
                                                          incremental_operation=AlterConfigOpType.DELETE
                                                      )
                                                  ],
                                                  KafkaException,
                                                  use_constructor)

        # Request timeout
        verify_incremental_alter_configs_api_call(a, ResourceType.BROKER, "1",
                                                  [
                                                      ConfigEntry(
                                                          name="advertised.listeners",
                                                          value="host1:9092",
                                                          incremental_operation=AlterConfigOpType.APPEND
                                                      ),
                                                      ConfigEntry(
                                                          name="background.threads",
                                                          value=None,
                                                          incremental_operation=AlterConfigOpType.DELETE
                                                      )
                                                  ],
                                                  KafkaException,
                                                  use_constructor)

    # Positive test that times out
    resources = [ConfigResource(ResourceType.BROKER, "1"),
                 ConfigResource(ResourceType.TOPIC, "test2")]

    resources[0].add_incremental_config(
        ConfigEntry("advertised.listeners", "host:9092",
                    incremental_operation=AlterConfigOpType.SUBTRACT))
    resources[0].add_incremental_config(
        ConfigEntry("background.threads", None,
                    incremental_operation=AlterConfigOpType.DELETE))
    resources[1].add_incremental_config(
        ConfigEntry("cleanup.policy", "compact",
                    incremental_operation=AlterConfigOpType.APPEND))
    resources[1].add_incremental_config(
        ConfigEntry("retention.ms", "10000",
                    incremental_operation=AlterConfigOpType.SET))

    fs = a.incremental_alter_configs(resources)

    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)


def test_create_acls_api():
    """ create_acls() tests, these wont really do anything since there is no
        broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})

    acl_binding1 = AclBinding(ResourceType.TOPIC, "topic1", ResourcePatternType.LITERAL,
                              "User:u1", "*", AclOperation.WRITE, AclPermissionType.ALLOW)
    acl_binding2 = AclBinding(ResourceType.TOPIC, "topic2", ResourcePatternType.LITERAL,
                              "User:u2", "*", AclOperation.READ, AclPermissionType.DENY)

    f = a.create_acls([acl_binding1],
                      request_timeout=10.0)
    # ignore the result

    with pytest.raises(TypeError):
        a.create_acls(None)

    with pytest.raises(ValueError):
        a.create_acls("topic")

    with pytest.raises(ValueError):
        a.create_acls([])

    with pytest.raises(ValueError):
        a.create_acls(["topic"])

    with pytest.raises(ValueError):
        a.create_acls([None, "topic"])

    with pytest.raises(ValueError):
        a.create_acls([None, acl_binding1])

    with pytest.raises(ValueError):
        a.create_acls([acl_binding1, acl_binding1])

    fs = a.create_acls([acl_binding1, acl_binding2])
    with pytest.raises(KafkaException):
        for f in fs.values():
            f.result(timeout=1)

    fs = a.create_acls([acl_binding1, acl_binding2],
                       request_timeout=0.5)
    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT

    with pytest.raises(ValueError):
        a.create_acls([acl_binding1],
                      request_timeout=-5)

    with pytest.raises(TypeError):
        a.create_acls([acl_binding1],
                      unknown_operation="it is")


def test_delete_acls_api():
    """ delete_acls() tests, these wont really do anything since there is no
        broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})

    acl_binding_filter1 = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    acl_binding_filter2 = AclBindingFilter(ResourceType.ANY, "topic2", ResourcePatternType.MATCH,
                                           None, "*", AclOperation.WRITE, AclPermissionType.ALLOW)

    fs = a.delete_acls([acl_binding_filter1])
    # ignore the result

    with pytest.raises(TypeError):
        a.delete_acls(None)

    with pytest.raises(ValueError):
        a.delete_acls([])

    with pytest.raises(ValueError):
        a.delete_acls([None, acl_binding_filter1])

    with pytest.raises(ValueError):
        a.delete_acls([acl_binding_filter1, acl_binding_filter1])

    fs = a.delete_acls([acl_binding_filter1, acl_binding_filter2])
    with pytest.raises(KafkaException):
        for f in concurrent.futures.as_completed(iter(fs.values())):
            f.result(timeout=1)

    fs = a.delete_acls([acl_binding_filter1, acl_binding_filter2],
                       request_timeout=0.5)
    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT

    with pytest.raises(ValueError):
        a.create_acls([acl_binding_filter1],
                      request_timeout=-5)

    with pytest.raises(TypeError):
        a.delete_acls([acl_binding_filter1],
                      unknown_operation="it is")


def test_describe_acls_api():
    """ describe_acls() tests, these wont really do anything since there is no
        broker configured. """

    a = AdminClient({"socket.timeout.ms": 10})

    acl_binding_filter1 = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    acl_binding1 = AclBinding(ResourceType.TOPIC, "topic1", ResourcePatternType.LITERAL,
                              "User:u1", "*", AclOperation.WRITE, AclPermissionType.ALLOW)

    a.describe_acls(acl_binding_filter1)
    # ignore the result

    with pytest.raises(TypeError):
        a.describe_acls(None)

    with pytest.raises(TypeError):
        a.describe_acls(acl_binding1)

    f = a.describe_acls(acl_binding_filter1)
    with pytest.raises(KafkaException):
        f.result(timeout=1)

    f = a.describe_acls(acl_binding_filter1,
                        request_timeout=0.5)
    e = f.exception(timeout=1)
    assert isinstance(e, KafkaException)
    assert e.args[0].code() == KafkaError._TIMED_OUT

    with pytest.raises(ValueError):
        a.describe_acls(acl_binding_filter1,
                        request_timeout=-5)

    with pytest.raises(TypeError):
        a.describe_acls(acl_binding_filter1,
                        unknown_operation="it is")


def test_list_consumer_groups_api():
    a = AdminClient({"socket.timeout.ms": 10})

    a.list_consumer_groups()

    a.list_consumer_groups(states={ConsumerGroupState.EMPTY, ConsumerGroupState.STABLE})

    with pytest.raises(TypeError):
        a.list_consumer_groups(states="EMPTY")

    with pytest.raises(TypeError):
        a.list_consumer_groups(states=["EMPTY"])

    with pytest.raises(TypeError):
        a.list_consumer_groups(states=[ConsumerGroupState.EMPTY, ConsumerGroupState.STABLE])


def test_describe_consumer_groups_api():
    a = AdminClient({"socket.timeout.ms": 10})

    group_ids = ["test-group-1", "test-group-2"]

    a.describe_consumer_groups(group_ids)

    with pytest.raises(TypeError):
        a.describe_consumer_groups("test-group-1")

    with pytest.raises(ValueError):
        a.describe_consumer_groups([])


def test_describe_topics_api():
    a = AdminClient({"socket.timeout.ms": 10})

    # Wrong option types
    for kwargs in [{"include_authorized_operations": "wrong_type"},
                   {"request_timeout": "wrong_type"}]:
        with pytest.raises(TypeError):
            a.describe_topics(TopicCollection([]), **kwargs)

    # Wrong option values
    for kwargs in [{"request_timeout": -1}]:
        with pytest.raises(ValueError):
            a.describe_topics(TopicCollection([]), **kwargs)

    # Test with different options
    for kwargs in [{},
                   {"include_authorized_operations": True},
                   {"request_timeout": 0.01},
                   {"include_authorized_operations": False,
                    "request_timeout": 0.01}]:

        topic_names = ["test-topic-1", "test-topic-2"]

        # Empty TopicCollection returns empty futures
        fs = a.describe_topics(TopicCollection([]), **kwargs)
        assert len(fs) == 0

        # Normal call
        fs = a.describe_topics(TopicCollection(topic_names), **kwargs)
        for f in concurrent.futures.as_completed(iter(fs.values())):
            e = f.exception(timeout=1)
            assert isinstance(e, KafkaException)
            assert e.args[0].code() == KafkaError._TIMED_OUT

        # Wrong argument type
        for args in [
            [topic_names],
            ["test-topic-1"],
            [TopicCollection([3])],
            [TopicCollection(["correct", 3])],
            [TopicCollection([None])]
        ]:
            with pytest.raises(TypeError):
                a.describe_topics(*args, **kwargs)

        # Wrong argument value
        for args in [
            [TopicCollection([""])],
            [TopicCollection(["correct", ""])]
        ]:
            with pytest.raises(ValueError):
                a.describe_topics(*args, **kwargs)


def test_describe_cluster():
    a = AdminClient({"socket.timeout.ms": 10})

    a.describe_cluster(include_authorized_operations=True)

    with pytest.raises(TypeError):
        a.describe_cluster(unknown_operation="it is")


def test_delete_consumer_groups_api():
    a = AdminClient({"socket.timeout.ms": 10})

    group_ids = ["test-group-1", "test-group-2"]

    a.delete_consumer_groups(group_ids)

    with pytest.raises(TypeError):
        a.delete_consumer_groups("test-group-1")

    with pytest.raises(ValueError):
        a.delete_consumer_groups([])


def test_list_consumer_group_offsets_api():

    a = AdminClient({"socket.timeout.ms": 10})

    only_group_id_request = ConsumerGroupTopicPartitions("test-group1")
    request_with_group_and_topic_partition = ConsumerGroupTopicPartitions(
        "test-group2", [TopicPartition("test-topic1", 1)])
    same_name_request = ConsumerGroupTopicPartitions("test-group2", [TopicPartition("test-topic1", 3)])

    a.list_consumer_group_offsets([only_group_id_request])

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets(None)

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets(1)

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets("")

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([only_group_id_request,
                                       request_with_group_and_topic_partition])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([request_with_group_and_topic_partition,
                                       same_name_request])

    fs = a.list_consumer_group_offsets([only_group_id_request])
    with pytest.raises(KafkaException):
        for f in fs.values():
            f.result(timeout=10)

    fs = a.list_consumer_group_offsets([only_group_id_request],
                                       request_timeout=0.5)
    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([only_group_id_request],
                                      request_timeout=-5)

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions()])

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(1)])

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(None)])

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions([])])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("")])

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", "test-topic")])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [])])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [None])])

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", ["test"])])

    with pytest.raises(TypeError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [TopicPartition(None)])])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [TopicPartition("")])])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(
            "test-group1", [TopicPartition("test-topic", -1)])])

    with pytest.raises(ValueError):
        a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(
            "test-group1", [TopicPartition("test-topic", 1, 1)])])

    a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1")])
    a.list_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group2", [TopicPartition("test-topic1", 1)])])


def test_alter_consumer_group_offsets_api():

    a = AdminClient({"socket.timeout.ms": 10})

    request_with_group_and_topic_partition_offset1 = ConsumerGroupTopicPartitions(
        "test-group1", [TopicPartition("test-topic1", 1, 5)])
    same_name_request = ConsumerGroupTopicPartitions("test-group1", [TopicPartition("test-topic2", 4, 3)])
    request_with_group_and_topic_partition_offset2 = ConsumerGroupTopicPartitions(
        "test-group2", [TopicPartition("test-topic2", 1, 5)])

    a.alter_consumer_group_offsets([request_with_group_and_topic_partition_offset1])

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets(None)

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets(1)

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets("")

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([request_with_group_and_topic_partition_offset1,
                                       request_with_group_and_topic_partition_offset2])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([request_with_group_and_topic_partition_offset1,
                                        same_name_request])

    # TODO: This test is failing intermittently with Fatal Error for MacOS builds.
    # Uncomment and fix this after the release v2.10.0.

    # with pytest.raises(KafkaException):
    #     fs = a.alter_consumer_group_offsets([request_with_group_and_topic_partition_offset1])
    #     for f in fs.values():
    #         f.result(timeout=10)

    # fs = a.alter_consumer_group_offsets([request_with_group_and_topic_partition_offset1],
    #                                     request_timeout=0.5)
    # for f in concurrent.futures.as_completed(iter(fs.values())):
    #     e = f.exception(timeout=1)
    #     assert isinstance(e, KafkaException)
    #     assert e.args[0].code() == KafkaError._TIMED_OUT

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([request_with_group_and_topic_partition_offset1],
                                       request_timeout=-5)

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions()])

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(1)])

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(None)])

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions([])])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("")])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1")])

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", "test-topic")])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [])])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [None])])

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", ["test"])])

    with pytest.raises(TypeError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [TopicPartition(None)])])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [TopicPartition("")])])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("test-group1", [TopicPartition("test-topic")])])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(
            "test-group1", [TopicPartition("test-topic", -1)])])

    with pytest.raises(ValueError):
        a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(
            "test-group1", [TopicPartition("test-topic", 1, -1001)])])

    a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(
        "test-group2", [TopicPartition("test-topic1", 1, 23)])])


def test_describe_user_scram_credentials_api():
    # Describe User Scram API
    a = AdminClient({"socket.timeout.ms": 10})

    f = a.describe_user_scram_credentials()
    assert isinstance(f, concurrent.futures.Future)

    futmap = a.describe_user_scram_credentials(["user"])
    assert isinstance(futmap, dict)

    with pytest.raises(TypeError):
        a.describe_user_scram_credentials(10)
    with pytest.raises(TypeError):
        a.describe_user_scram_credentials([None])
    with pytest.raises(ValueError):
        a.describe_user_scram_credentials([""])
    with pytest.raises(KafkaException) as ex:
        futmap = a.describe_user_scram_credentials(["sam", "sam"])
        futmap["sam"].result(timeout=3)
        assert "Duplicate users" in str(ex.value)

    fs = a.describe_user_scram_credentials(["user1", "user2"])
    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT


def test_alter_user_scram_credentials_api():
    # Alter User Scram API
    a = AdminClient({"socket.timeout.ms": 10})

    scram_credential_info = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 10000)
    upsertion = UserScramCredentialUpsertion("sam", scram_credential_info, b"password", b"salt")
    upsertion_without_salt = UserScramCredentialUpsertion("sam", scram_credential_info, b"password")
    upsertion_with_none_salt = UserScramCredentialUpsertion("sam", scram_credential_info, b"password", None)
    deletion = UserScramCredentialDeletion("sam", ScramMechanism.SCRAM_SHA_512)
    alterations = [upsertion, upsertion_without_salt, upsertion_with_none_salt, deletion]

    fs = a.alter_user_scram_credentials(alterations)
    for f in concurrent.futures.as_completed(iter(fs.values())):
        e = f.exception(timeout=1)
        assert isinstance(e, KafkaException)
        assert e.args[0].code() == KafkaError._TIMED_OUT

    # Request type tests
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials(None)
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials(234)
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials("test")

    # Individual request tests
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([None])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials(["test"])

    # User tests
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialAlteration(None)])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialAlteration(123)])
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([UserScramCredentialAlteration("")])

    # Upsertion request user test
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion(None,
                                                                     scram_credential_info,
                                                                     b"password",
                                                                     b"salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion(123,
                                                                     scram_credential_info,
                                                                     b"password",
                                                                     b"salt")])
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("",
                                                                     scram_credential_info,
                                                                     b"password",
                                                                     b"salt")])

    # Upsertion password user test
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", scram_credential_info, b"", b"salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", scram_credential_info, None, b"salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam",
                                                                     scram_credential_info,
                                                                     "password",
                                                                     b"salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", scram_credential_info, 123, b"salt")])

    # Upsertion salt user test
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", scram_credential_info, b"password", b"")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam",
                                                                     scram_credential_info,
                                                                     b"password",
                                                                     "salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", scram_credential_info, b"password", 123)])

    # Upsertion scram_credential_info tests
    sci_incorrect_mechanism_type = ScramCredentialInfo("string type", 10000)
    sci_incorrect_iteration_type = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, "string type")
    sci_negative_iteration = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, -1)
    sci_zero_iteration = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 0)

    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", None, b"password", b"salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", "string type", b"password", b"salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam",
                                                                     sci_incorrect_mechanism_type,
                                                                     b"password",
                                                                     b"salt")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam",
                                                                     sci_incorrect_iteration_type,
                                                                     b"password",
                                                                     b"salt")])
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam",
                                                                     sci_negative_iteration,
                                                                     b"password",
                                                                     b"salt")])
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([UserScramCredentialUpsertion("sam", sci_zero_iteration, b"password", b"salt")])

    # Deletion user tests
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialDeletion(None, ScramMechanism.SCRAM_SHA_256)])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialDeletion(123, ScramMechanism.SCRAM_SHA_256)])
    with pytest.raises(ValueError):
        a.alter_user_scram_credentials([UserScramCredentialDeletion("", ScramMechanism.SCRAM_SHA_256)])

    # Deletion mechanism tests
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialDeletion("sam", None)])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialDeletion("sam", "string type")])
    with pytest.raises(TypeError):
        a.alter_user_scram_credentials([UserScramCredentialDeletion("sam", 123)])


def test_list_offsets_api():
    a = AdminClient({"socket.timeout.ms": 10})

    # Wrong option types
    for kwargs in [
        {
            "isolation_level": 10
        },
        {
            "request_timeout": "test"
        }
    ]:
        requests = {
            TopicPartition("topic1", 0, 10): OffsetSpec.earliest()
        }
        with pytest.raises(TypeError):
            a.list_offsets(requests, **kwargs)

    # Wrong option values
    for kwargs in [
        {
            "request_timeout": -1
        }
    ]:
        requests = {
            TopicPartition("topic1", 0, 10): OffsetSpec.earliest()
        }
        with pytest.raises(ValueError):
            a.list_offsets(requests, **kwargs)

    for kwargs in [{},
                   {"isolation_level": IsolationLevel.READ_UNCOMMITTED},
                   {"request_timeout": 0.01},
                   {"isolation_level": IsolationLevel.READ_COMMITTED,
                    "request_timeout": 0.01}]:

        # Not a dictionary
        with pytest.raises(TypeError):
            a.list_offsets(None, **kwargs)

        # Empty partitions
        requests = {}
        fs = a.list_offsets(requests, **kwargs)
        assert len(fs) == 0

        # Invalid TopicPartition
        for requests in [
            {
                TopicPartition("", 0, 10): OffsetSpec.earliest()
            },
            {
                TopicPartition("correct", -1, 10): OffsetSpec.earliest()
            }
        ]:
            with pytest.raises(ValueError):
                a.list_offsets(requests, **kwargs)

        # Same partition with different offsets
        requests = {
            TopicPartition("topic1", 0, 10): OffsetSpec.earliest(),
            TopicPartition("topic1", 0, 15): OffsetSpec.earliest(),
        }
        fs = a.list_offsets(requests, **kwargs)
        assert len(fs) == 1
        for f in concurrent.futures.as_completed(iter(fs.values())):
            e = f.exception(timeout=1)
            assert isinstance(e, KafkaException)
            assert e.args[0].code() == KafkaError._TIMED_OUT

        # Two different partitions
        requests = {
            TopicPartition("topic1", 0, 10): OffsetSpec.earliest(),
            TopicPartition("topic1", 1, 15): OffsetSpec.earliest(),
        }
        fs = a.list_offsets(requests, **kwargs)
        assert len(fs) == 2
        for f in concurrent.futures.as_completed(iter(fs.values())):
            e = f.exception(timeout=1)
            assert isinstance(e, KafkaException)
            assert e.args[0].code() == KafkaError._TIMED_OUT

        # Key isn't a TopicPartition
        for requests in [
            {
                "not-topic-partition": OffsetSpec.latest()
            },
            {
                TopicPartition("topic1", 0, 10): OffsetSpec.latest(),
                "not-topic-partition": OffsetSpec.latest()
            },
            {
                None: OffsetSpec.latest()
            }
        ]:
            with pytest.raises(TypeError):
                a.list_offsets(requests, **kwargs)

        # Value isn't a OffsetSpec
        for requests in [
            {
                TopicPartition("topic1", 0, 10): "test"
            },
            {
                TopicPartition("topic1", 0, 10): OffsetSpec.latest(),
                TopicPartition("topic1", 0, 10): "test"
            },
            {
                TopicPartition("topic1", 0, 10): None
            }
        ]:
            with pytest.raises(TypeError):
                a.list_offsets(requests, **kwargs)


def test_delete_records():
    a = AdminClient({"socket.timeout.ms": 10})

    # Request-type tests
    with pytest.raises(TypeError, match="Expected Request to be a list, got 'NoneType'"):
        a.delete_records(None)

    with pytest.raises(TypeError, match="Expected Request to be a list, got 'int'"):
        a.delete_records(1)

    # Request-specific tests
    with pytest.raises(TypeError,
                       match="Element of the request list must be of type 'TopicPartition' got 'str'"):
        a.delete_records(["test-1"])

    with pytest.raises(TypeError):
        a.delete_records([TopicPartition(None)])

    with pytest.raises(ValueError):
        a.delete_records([TopicPartition("")])

    with pytest.raises(ValueError):
        a.delete_records([TopicPartition("test-topic1")])


def test_elect_leaders():
    a = AdminClient({"socket.timeout.ms": 10})

    correct_partitions = TopicPartition("test-topic1", 0)
    incorrect_partitions = TopicPartition("test-topic1", -1)

    correct_election_type = ElectionType.PREFERRED

    # Incorrect Election Type
    with pytest.raises(TypeError):
        a.elect_leaders(None, [correct_partitions])

    with pytest.raises(TypeError):
        a.elect_leaders("1", [correct_partitions])

    # Incorrect Partitions type
    with pytest.raises(TypeError, match="Expected 'partitions' to be a list, got 'str'"):
        a.elect_leaders(correct_election_type, "1")

    # Partition-specific tests
    with pytest.raises(TypeError,
                       match="Element of the 'partitions' list must be of type 'TopicPartition' got 'str'"):
        a.elect_leaders(correct_election_type, ["test-1"])

    with pytest.raises(TypeError,
                       match="Element of the 'partitions' list must be of type 'TopicPartition' got 'NoneType'"):
        a.elect_leaders(correct_election_type, [None])

    with pytest.raises(ValueError):
        a.elect_leaders(correct_election_type, [TopicPartition("")])

    with pytest.raises(ValueError):
        a.elect_leaders(correct_election_type, [incorrect_partitions])

    with pytest.raises(KafkaException):
        a.elect_leaders(correct_election_type, [correct_partitions])\
            .result(timeout=1)


def test_admin_callback_exception_no_system_error():
    """Test AdminClient callbacks exception handling with different exception types"""

    # Test error_cb with different exception types
    def error_cb_kafka_exception(error):
        raise KafkaException(KafkaError._FAIL, "KafkaException from error_cb")

    def error_cb_value_error(error):
        raise ValueError("ValueError from error_cb")

    def error_cb_runtime_error(error):
        raise RuntimeError("RuntimeError from error_cb")

    # Test error_cb with KafkaException
    admin = AdminClient({
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'error_cb': error_cb_kafka_exception
    })

    with pytest.raises(KafkaException) as exc_info:
        admin.poll(timeout=0.2)
    assert "KafkaException from error_cb" in str(exc_info.value)

    # Test error_cb with ValueError
    admin = AdminClient({
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'error_cb': error_cb_value_error
    })

    with pytest.raises(ValueError) as exc_info:
        admin.poll(timeout=0.2)
    assert "ValueError from error_cb" in str(exc_info.value)

    # Test error_cb with RuntimeError
    admin = AdminClient({
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'error_cb': error_cb_runtime_error
    })

    with pytest.raises(RuntimeError) as exc_info:
        admin.poll(timeout=0.2)
    assert "RuntimeError from error_cb" in str(exc_info.value)


def test_admin_multiple_callbacks_different_error_types():
    """Test AdminClient with multiple callbacks configured with different error types
to see which one gets triggered"""

    callbacks_called = []

    def error_cb_that_raises_runtime(error):
        callbacks_called.append('error_cb_runtime')
        raise RuntimeError("RuntimeError from error_cb")

    def stats_cb_that_raises_value(stats_json):
        callbacks_called.append('stats_cb_value')
        raise ValueError("ValueError from stats_cb")

    def throttle_cb_that_raises_kafka(throttle_event):
        callbacks_called.append('throttle_cb_kafka')
        raise KafkaException(KafkaError._FAIL, "KafkaException from throttle_cb")

    admin = AdminClient({
        'bootstrap.servers': 'nonexistent-broker:9092',
        'socket.timeout.ms': 100,
        'statistics.interval.ms': 100,  # Enable stats callback
        'error_cb': error_cb_that_raises_runtime,
        'stats_cb': stats_cb_that_raises_value,
        'throttle_cb': throttle_cb_that_raises_kafka
    })

    # Test that error_cb callback raises an exception (it's triggered by connection failures)
    with pytest.raises(RuntimeError):
        admin.poll(timeout=0.2)

    # Verify that error_cb was called
    assert len(callbacks_called) > 0
    assert 'error_cb_runtime' in callbacks_called


def test_admin_context_manager_basic():
    """Test basic AdminClient context manager usage and return value"""
    config = {
        'socket.timeout.ms': 10
    }

    # Test __enter__ returns self
    admin = AdminClient(config)
    entered = admin.__enter__()
    assert entered is admin
    admin.__exit__(None, None, None)  # Clean up

    # Test basic context manager usage
    with AdminClient(config) as admin:
        assert admin is not None
        admin.poll(0.001)

    # AdminClient should be closed after exiting context
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.poll(0.001)


def test_admin_context_manager_exception_propagation():
    """Test exceptions propagate and admin client is cleaned up"""
    config = {
        'socket.timeout.ms': 10
    }

    # Test exception propagation
    exception_caught = False
    try:
        with AdminClient(config) as admin:
            admin.poll(0.001)
            raise ValueError("Test exception")
    except ValueError as e:
        assert str(e) == "Test exception"
        exception_caught = True

    assert exception_caught, "Exception should have propagated"

    # AdminClient should be closed even after exception
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.poll(0.001)


def test_admin_context_manager_exit_with_exceptions():
    """Test __exit__ properly handles exception arguments"""
    config = {
        'socket.timeout.ms': 10
    }

    admin = AdminClient(config)
    admin.poll(0.001)

    # Simulate exception in with block
    exc_type = ValueError
    exc_value = ValueError("Test error")
    exc_traceback = None

    # __exit__ should cleanup and return None (propagate exception)
    result = admin.__exit__(exc_type, exc_value, exc_traceback)
    assert result is None  # None means propagate exception

    # AdminClient should be closed
    with pytest.raises(RuntimeError):
        admin.poll(0.001)


def test_admin_context_manager_after_exit():
    """Test AdminClient behavior after context manager exit"""
    config = {
        'socket.timeout.ms': 10
    }

    # Normal exit
    with AdminClient(config) as admin:
        admin.poll(0.001)

    # All methods should fail after context exit
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.poll(0.001)

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.create_topics([NewTopic("test", 1, 1)])

    with pytest.raises(RuntimeError, match="Handle has been closed"):
        admin.list_topics()

    # __len__ should return 0 for closed admin client
    assert len(admin) == 0

    # Test already-closed admin client edge case
    # Using __enter__ and __exit__ directly on already-closed admin
    entered = admin.__enter__()
    assert entered is admin

    # Operations should still fail
    with pytest.raises(RuntimeError):
        admin.poll(0.001)

    # __exit__ should handle already-closed gracefully
    result = admin.__exit__(None, None, None)
    assert result is None


def test_admin_context_manager_multiple_instances():
    """Test AdminClient context manager with multiple instances"""
    config = {
        'socket.timeout.ms': 10
    }

    # Test multiple sequential instances
    with AdminClient(config) as admin1:
        admin1.poll(0.001)

    with AdminClient(config) as admin2:
        admin2.poll(0.001)
        # Both should be independent
        assert admin1 is not admin2

    # Both should be closed
    with pytest.raises(RuntimeError):
        admin1.poll(0.001)
    with pytest.raises(RuntimeError):
        admin2.poll(0.001)

    # Test nested context managers
    with AdminClient(config) as admin1:
        with AdminClient(config) as admin2:
            assert admin1 is not admin2
            admin1.poll(0.001)
            admin2.poll(0.001)
        # admin2 should be closed, admin1 still open
        admin1.poll(0.001)

    # Both should be closed now
    with pytest.raises(RuntimeError):
        admin1.poll(0.001)
    with pytest.raises(RuntimeError):
        admin2.poll(0.001)


def test_admin_context_manager_with_admin_apis():
    """Test AdminClient context manager with various Admin APIs"""
    config = {
        'socket.timeout.ms': 10
    }

    acl_binding = AclBinding(
        ResourceType.TOPIC, "topic1", ResourcePatternType.LITERAL,
        "User:u1", "*", AclOperation.WRITE, AclPermissionType.ALLOW
    )

    with AdminClient(config) as admin:
        # Test 1: create_topics API
        fs1 = admin.create_topics([NewTopic("test_topic", 1, 1)])
        assert fs1 is not None
        assert "test_topic" in fs1

        # Test 2: delete_topics API
        fs2 = admin.delete_topics(["test_topic"])
        assert fs2 is not None
        assert "test_topic" in fs2

        # Test 3: describe_configs API
        fs3 = admin.describe_configs([ConfigResource(ResourceType.TOPIC, "test_topic")])
        assert fs3 is not None

        # Test 4: list_topics API
        try:
            metadata = admin.list_topics(timeout=0.2)
            assert metadata is not None
        except KafkaException:
            # Expected when broker is not available
            pass

        # Test 5: list_groups API
        try:
            groups = admin.list_groups(timeout=0.2)
            assert groups is not None
        except KafkaException:
            # Expected when broker is not available
            pass

        # Test 6: create_acls API
        fs4 = admin.create_acls([acl_binding])
        assert fs4 is not None

        # Test 7: describe_consumer_groups API
        fs5 = admin.describe_consumer_groups(["test-group-1"])
        assert fs5 is not None
        assert "test-group-1" in fs5

        # Poll to process callbacks
        admin.poll(0.001)

    # Test 8: All operations should fail after context exit
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.create_topics([NewTopic("another_topic", 1, 1)])
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.delete_topics(["another_topic"])
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_configs([ConfigResource(ResourceType.TOPIC, "test_topic")])
    with pytest.raises(RuntimeError, match="Handle has been closed"):
        admin.list_topics(timeout=0.1)
    with pytest.raises(RuntimeError, match="Handle has been closed"):
        admin.list_groups(timeout=0.1)
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.create_acls([acl_binding])
    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_consumer_groups(["test-group-2"])


def test_uninitialized_admin_client_methods():
    """Test that all AdminClient methods raise RuntimeError when called on uninitialized instance.
    """

    class UninitializedAdmin(AdminClient):
        def __init__(self, config):
            # Don't call super().__init__() - leaves self->rk as NULL
            pass

    admin = UninitializedAdmin({})

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.create_topics([NewTopic("test", 1, 1)])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.delete_topics(["test"])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.create_partitions([NewPartitions("test", 2)])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_configs([ConfigResource(ResourceType.TOPIC, "test")])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.incremental_alter_configs([ConfigResource(ResourceType.TOPIC, "test")])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.alter_configs([ConfigResource(ResourceType.TOPIC, "test")])

    acl_binding = AclBinding(
        ResourceType.TOPIC, "topic1", ResourcePatternType.LITERAL,
        "User:u1", "*", AclOperation.WRITE, AclPermissionType.ALLOW
    )

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.create_acls([acl_binding])

    acl_filter = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY,
                                  None, None, AclOperation.ANY, AclPermissionType.ANY)

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.delete_acls([acl_filter])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_acls(acl_filter)

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.list_consumer_groups()

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_user_scram_credentials(["user"])

    scram_info = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 10000)
    upsertion = UserScramCredentialUpsertion("user", scram_info, b"password")

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.alter_user_scram_credentials([upsertion])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_consumer_groups(["group"])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_topics(TopicCollection(["topic"]))

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.describe_cluster()

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.delete_consumer_groups(["group"])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions("group")])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("group", [TopicPartition("topic", 0, 5)])])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.list_offsets({TopicPartition("topic", 0, 10): OffsetSpec.earliest()})

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.delete_records([TopicPartition("topic", 0, 10)])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.elect_leaders(ElectionType.PREFERRED, [TopicPartition("topic", 0)])

    with pytest.raises(RuntimeError, match="AdminClient has been closed"):
        admin.poll(0.001)

    # Test __len__() - should return 0 for closed admin (safe, no crash)
    assert len(admin) == 0
