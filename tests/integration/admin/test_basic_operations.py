# -*- coding: utf-8 -*-
# Copyright 2022 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import confluent_kafka
import struct
import time
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition, ConsumerGroupState
from confluent_kafka.admin import (NewPartitions, ConfigResource,
                                   AclBinding, AclBindingFilter, ResourceType,
                                   ResourcePatternType, AclOperation, AclPermissionType)
from confluent_kafka.error import ConsumeError

topic_prefix = "test-topic"


# Shared between producer and consumer tests and used to verify
# that consumed headers are what was actually produced.
produce_headers = [('foo1', 'bar'),
                   ('foo1', 'bar2'),
                   ('foo2', b'1'),
                   (u'Jämtland', u'Härjedalen'),  # automatically utf-8 encoded
                   ('nullheader', None),
                   ('empty', ''),
                   ('foobin', struct.pack('hhl', 10, 20, 30))]


def verify_commit_result(err, partitions):
    assert err is not None


def verify_admin_acls(admin_client,
                      topic,
                      group):

    #
    # Add three ACLs
    #
    acl_binding_1 = AclBinding(ResourceType.TOPIC, topic, ResourcePatternType.LITERAL,
                               "User:test-user-1", "*", AclOperation.READ, AclPermissionType.ALLOW)
    acl_binding_2 = AclBinding(ResourceType.TOPIC, topic, ResourcePatternType.PREFIXED,
                               "User:test-user-2", "*", AclOperation.WRITE, AclPermissionType.DENY)
    acl_binding_3 = AclBinding(ResourceType.GROUP, group, ResourcePatternType.PREFIXED,
                               "User:test-user-2", "*", AclOperation.ALL, AclPermissionType.ALLOW)

    fs = admin_client.create_acls([acl_binding_1, acl_binding_2, acl_binding_3])
    for acl_binding, f in fs.items():
        f.result()  # trigger exception if there was an error

    acl_binding_filter1 = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    acl_binding_filter2 = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.PREFIXED,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    acl_binding_filter3 = AclBindingFilter(ResourceType.TOPIC, None, ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    acl_binding_filter4 = AclBindingFilter(ResourceType.GROUP, None, ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)

    expected_acl_bindings = [acl_binding_1, acl_binding_2, acl_binding_3]
    acl_bindings = admin_client.describe_acls(acl_binding_filter1).result()
    assert sorted(acl_bindings) == sorted(expected_acl_bindings), \
        "ACL bindings don't match, actual: {} expected: {}".format(acl_bindings,
                                                                   expected_acl_bindings)

    #
    # Delete the ACLs with PREFIXED
    #
    expected_acl_bindings = [acl_binding_2, acl_binding_3]
    fs = admin_client.delete_acls([acl_binding_filter2])
    deleted_acl_bindings = sorted(fs[acl_binding_filter2].result())
    assert deleted_acl_bindings == expected_acl_bindings, \
        "Deleted ACL bindings don't match, actual {} expected {}".format(deleted_acl_bindings,
                                                                         expected_acl_bindings)

    #
    # Delete the ACLs with TOPIC and GROUP
    #
    expected_acl_bindings = [[acl_binding_1], []]
    delete_acl_binding_filters = [acl_binding_filter3, acl_binding_filter4]
    fs = admin_client.delete_acls(delete_acl_binding_filters)
    for acl_binding, expected in zip(delete_acl_binding_filters, expected_acl_bindings):
        deleted_acl_bindings = sorted(fs[acl_binding].result())
        assert deleted_acl_bindings == expected, \
            "Deleted ACL bindings don't match, actual {} expected {}".format(deleted_acl_bindings,
                                                                             expected)
    #
    # All the ACLs should have been deleted
    #
    expected_acl_bindings = []
    acl_bindings = admin_client.describe_acls(acl_binding_filter1).result()
    assert acl_bindings == expected_acl_bindings, \
        "ACL bindings don't match, actual: {} expected: {}".format(acl_bindings,
                                                                   expected_acl_bindings)


def verify_topic_metadata(client, exp_topics, *args, **kwargs):
    """
    Verify that exp_topics (dict<topicname,partcnt>) is reported in metadata.
    Will retry and wait for some time to let changes propagate.

    Non-controller brokers may return the previous partition count for some
    time before being updated, in this case simply retry.
    """

    for retry in range(0, 3):
        do_retry = 0

        md = client.list_topics(*args, **kwargs)

        for exptopic, exppartcnt in exp_topics.items():
            if exptopic not in md.topics:
                print("Topic {} not yet reported in metadata: retrying".format(exptopic))
                do_retry += 1
                continue

            if len(md.topics[exptopic].partitions) < exppartcnt:
                print("Topic {} partition count not yet updated ({} != expected {}): retrying".format(
                    exptopic, len(md.topics[exptopic].partitions), exppartcnt))
                do_retry += 1
                continue

            assert len(md.topics[exptopic].partitions) == exppartcnt, \
                "Expected {} partitions for topic {}, not {}".format(
                    exppartcnt, exptopic, md.topics[exptopic].partitions)

        if do_retry == 0:
            return  # All topics okay.

        time.sleep(1)


def verify_consumer_group_offsets_operations(client, our_topic, group_id):

    # List Consumer Group Offsets check with just group name
    request = ConsumerGroupTopicPartitions(group_id)
    fs = client.list_consumer_group_offsets([request])
    f = fs[group_id]
    res = f.result()
    assert isinstance(res, ConsumerGroupTopicPartitions)
    assert res.group_id == group_id
    assert len(res.topic_partitions) == 2
    is_any_message_consumed = False
    for topic_partition in res.topic_partitions:
        assert topic_partition.topic == our_topic
        if topic_partition.offset > 0:
            is_any_message_consumed = True
    assert is_any_message_consumed

    # Alter Consumer Group Offsets check
    alter_group_topic_partitions = list(map(lambda topic_partition: TopicPartition(topic_partition.topic,
                                                                                   topic_partition.partition,
                                                                                   0),
                                            res.topic_partitions))
    alter_group_topic_partition_request = ConsumerGroupTopicPartitions(group_id,
                                                                       alter_group_topic_partitions)
    afs = client.alter_consumer_group_offsets([alter_group_topic_partition_request])
    af = afs[group_id]
    ares = af.result()
    assert isinstance(ares, ConsumerGroupTopicPartitions)
    assert ares.group_id == group_id
    assert len(ares.topic_partitions) == 2
    for topic_partition in ares.topic_partitions:
        assert topic_partition.topic == our_topic
        assert topic_partition.offset == 0

    # List Consumer Group Offsets check with group name and partitions
    list_group_topic_partitions = list(map(lambda topic_partition: TopicPartition(topic_partition.topic,
                                                                                  topic_partition.partition),
                                           ares.topic_partitions))
    list_group_topic_partition_request = ConsumerGroupTopicPartitions(group_id,
                                                                      list_group_topic_partitions)
    lfs = client.list_consumer_group_offsets([list_group_topic_partition_request])
    lf = lfs[group_id]
    lres = lf.result()

    assert isinstance(lres, ConsumerGroupTopicPartitions)
    assert lres.group_id == group_id
    assert len(lres.topic_partitions) == 2
    for topic_partition in lres.topic_partitions:
        assert topic_partition.topic == our_topic
        assert topic_partition.offset == 0


def test_basic_operations(kafka_cluster):
    num_partitions = 2
    topic_config = {"compression.type": "gzip"}

    #
    # First iteration: validate our_topic creation.
    # Second iteration: create topic.
    #
    for validate in (True, False):
        our_topic = kafka_cluster.create_topic(topic_prefix,
                                               {
                                                   "num_partitions": num_partitions,
                                                   "config": topic_config,
                                                   "replication_factor": 1,
                                               },
                                               validate_only=validate
                                               )

    admin_client = kafka_cluster.admin()

    #
    # Find the topic in list_topics
    #
    verify_topic_metadata(admin_client, {our_topic: num_partitions})
    verify_topic_metadata(admin_client, {our_topic: num_partitions}, topic=our_topic)
    verify_topic_metadata(admin_client, {our_topic: num_partitions}, our_topic)

    #
    # Increase the partition count
    #
    num_partitions += 3
    fs = admin_client.create_partitions([NewPartitions(our_topic,
                                                       new_total_count=num_partitions)],
                                        operation_timeout=10.0)

    for topic2, f in fs.items():
        f.result()  # trigger exception if there was an error

    #
    # Verify with list_topics.
    #
    verify_topic_metadata(admin_client, {our_topic: num_partitions})

    #
    # Verify with list_groups.
    #

    # Produce some messages
    p = kafka_cluster.producer()
    p.produce(our_topic, 'Hello Python!', headers=produce_headers)
    p.produce(our_topic, key='Just a key and headers', headers=produce_headers)
    p.flush()

    def consume_messages(group_id, num_messages=None):
        # Consume messages
        conf = {'group.id': group_id,
                'session.timeout.ms': 6000,
                'enable.auto.commit': False,
                'on_commit': verify_commit_result,
                'auto.offset.reset': 'earliest',
                'enable.partition.eof': True}
        c = kafka_cluster.consumer(conf)
        c.subscribe([our_topic])
        eof_reached = dict()
        read_messages = 0
        msg = None
        while True:
            try:
                msg = c.poll()
                if msg is None:
                    raise Exception('Got timeout from poll() without a timeout set: %s' % msg)
                # Commit offset
                c.commit(msg, asynchronous=False)
                read_messages += 1
                if num_messages is not None and read_messages == num_messages:
                    print('Read all the required messages: exiting')
                    break
            except ConsumeError as e:
                if msg is not None and e.code == confluent_kafka.KafkaError._PARTITION_EOF:
                    print('Reached end of %s [%d] at offset %d' % (
                          msg.topic(), msg.partition(), msg.offset()))
                    eof_reached[(msg.topic(), msg.partition())] = True
                    if len(eof_reached) == len(c.assignment()):
                        print('EOF reached for all assigned partitions: exiting')
                        break
                else:
                    print('Consumer error: %s: ignoring' % str(e))
                    break
        c.close()

    group1 = 'test-group-1'
    group2 = 'test-group-2'
    acls_topic = our_topic + "-acls"
    acls_group = "test-group-acls"
    consume_messages(group1, 2)
    consume_messages(group2, 2)

    # list_groups without group argument
    groups = set(group.id for group in admin_client.list_groups(timeout=10))
    assert group1 in groups, "Consumer group {} not found".format(group1)
    assert group2 in groups, "Consumer group {} not found".format(group2)
    # list_groups with group argument
    groups = set(group.id for group in admin_client.list_groups(group1))
    assert group1 in groups, "Consumer group {} not found".format(group1)
    groups = set(group.id for group in admin_client.list_groups(group2))
    assert group2 in groups, "Consumer group {} not found".format(group2)

    # List Consumer Groups new API test
    future = admin_client.list_consumer_groups(request_timeout=10)
    result = future.result()
    group_ids = [group.group_id for group in result.valid]
    assert group1 in group_ids, "Consumer group {} not found".format(group1)
    assert group2 in group_ids, "Consumer group {} not found".format(group2)

    future = admin_client.list_consumer_groups(request_timeout=10, states={ConsumerGroupState.STABLE})
    result = future.result()
    assert isinstance(result.valid, list)
    assert not result.valid

    def verify_config(expconfig, configs):
        """
        Verify that the config key,values in expconfig are found
        and matches the ConfigEntry in configs.
        """
        for key, expvalue in expconfig.items():
            entry = configs.get(key, None)
            assert entry is not None, "Config {} not found in returned configs".format(key)

            assert entry.value == str(expvalue), \
                "Config {} with value {} does not match expected value {}".format(key, entry, expvalue)

    #
    # Get current topic config
    #
    resource = ConfigResource(ResourceType.TOPIC, our_topic)
    fs = admin_client.describe_configs([resource])
    configs = fs[resource].result()  # will raise exception on failure

    # Verify config matches our expectations
    verify_config(topic_config, configs)

    #
    # Now change the config.
    #
    topic_config["file.delete.delay.ms"] = 12345
    topic_config["compression.type"] = "snappy"

    for key, value in topic_config.items():
        resource.set_config(key, value)

    fs = admin_client.alter_configs([resource])
    fs[resource].result()  # will raise exception on failure

    #
    # Read the config back again and verify.
    #
    fs = admin_client.describe_configs([resource])
    configs = fs[resource].result()  # will raise exception on failure

    # Verify config matches our expectations
    verify_config(topic_config, configs)

    # Verify Consumer Offset Operations
    verify_consumer_group_offsets_operations(admin_client, our_topic, group1)

    # Delete groups
    fs = admin_client.delete_consumer_groups([group1, group2], request_timeout=10)
    fs[group1].result()  # will raise exception on failure
    fs[group2].result()  # will raise exception on failure

    #
    # Delete the topic
    #
    fs = admin_client.delete_topics([our_topic])
    fs[our_topic].result()  # will raise exception on failure
    print("Topic {} marked for deletion".format(our_topic))

    # Verify ACL operations
    verify_admin_acls(admin_client, acls_topic, acls_group)
