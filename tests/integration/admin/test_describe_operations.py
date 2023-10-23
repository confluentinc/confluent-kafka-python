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

import pytest
from confluent_kafka.admin import (AclBinding, AclBindingFilter, ResourceType,
                                   ResourcePatternType, AclOperation, AclPermissionType)
from confluent_kafka.error import ConsumeError
from confluent_kafka import ConsumerGroupState, TopicCollection

topic_prefix = "test-topic"


def verify_commit_result(err, _):
    assert err is not None


def consume_messages(sasl_cluster, group_id, topic, num_messages=None):
    conf = {'group.id': group_id,
            'session.timeout.ms': 6000,
            'enable.auto.commit': False,
            'on_commit': verify_commit_result,
            'auto.offset.reset': 'earliest'}
    consumer = sasl_cluster.consumer(conf)
    consumer.subscribe([topic])
    read_messages = 0
    msg = None
    while True:
        try:
            msg = consumer.poll()
            if msg is None:
                raise Exception('Got timeout from poll() without a timeout set: %s' % msg)
            # Commit offset
            consumer.commit(msg, asynchronous=False)
            read_messages += 1
            if num_messages is not None and read_messages == num_messages:
                print('Read all the required messages: exiting')
                break
        except ConsumeError as e:
            print('Consumer error: %s: ignoring' % str(e))
            break
        except Exception:
            raise
    consumer.close()


def get_future_key_list(arg_as_list):
    return arg_as_list[0]


def get_future_key_TopicCollection(arg_as_TopicCollection):
    return get_future_key_list(arg_as_TopicCollection.topic_names)


def get_future_key(*arg):
    if len(arg) > 0:
        arg_type = type(arg[0])
        if arg_type is list:
            return get_future_key_list(arg[0])
        elif arg_type is TopicCollection:
            return get_future_key_TopicCollection(arg[0])
    return None


def perform_admin_operation_sync(operation, *arg, **kwargs):
    future_key = get_future_key(*arg)
    fs = operation(*arg, **kwargs)
    fs = fs[future_key] if future_key else fs
    return fs.result()


def create_acls(admin_client, acl_bindings):
    perform_admin_operation_sync(admin_client.create_acls, acl_bindings)


def delete_acls(admin_client, acl_binding_filters):
    perform_admin_operation_sync(admin_client.delete_acls, acl_binding_filters)


def verify_provided_describe_for_authorized_operations(
        admin_client,
        describe_fn,
        operation_to_allow,
        operation_to_check,
        restype,
        resname,
        *arg):
    kwargs = {}
    kwargs['request_timeout'] = 10

    # Check with include_authorized_operations as False
    kwargs['include_authorized_operations'] = False
    desc = perform_admin_operation_sync(describe_fn, *arg, **kwargs)
    assert desc.authorized_operations is None

    # Check with include_authorized_operations as True
    kwargs['include_authorized_operations'] = True
    desc = perform_admin_operation_sync(describe_fn, *arg, **kwargs)
    assert len(desc.authorized_operations) > 0
    assert operation_to_allow in desc.authorized_operations
    assert operation_to_check in desc.authorized_operations

    # Update Authorized Operation by creating new ACLs
    acl_binding = AclBinding(restype, resname, ResourcePatternType.LITERAL,
                             "User:sasl_user", "*", operation_to_allow, AclPermissionType.ALLOW)
    create_acls(admin_client, [acl_binding])

    # Check with updated authorized operations
    desc = perform_admin_operation_sync(describe_fn, *arg, **kwargs)
    assert len(desc.authorized_operations) > 0
    assert operation_to_allow in desc.authorized_operations
    assert operation_to_check not in desc.authorized_operations

    # Delete Updated ACLs
    acl_binding_filter = AclBindingFilter(restype, resname, ResourcePatternType.ANY,
                                          None, None, AclOperation.ANY, AclPermissionType.ANY)
    delete_acls(admin_client, [acl_binding_filter])
    return desc


def verify_describe_topics(admin_client, topic_name):
    desc = verify_provided_describe_for_authorized_operations(admin_client,
                                                              admin_client.describe_topics,
                                                              AclOperation.READ,
                                                              AclOperation.DELETE,
                                                              ResourceType.TOPIC,
                                                              topic_name,
                                                              TopicCollection([topic_name]))
    assert desc.name == topic_name
    assert len(desc.partitions) == 1
    assert not desc.is_internal
    assert desc.partitions[0].id == 0
    assert desc.partitions[0].leader is not None
    assert len(desc.partitions[0].replicas) == 1
    assert len(desc.partitions[0].isr) == 1


def verify_describe_groups(cluster, admin_client, topic):

    # Produce some messages
    p = cluster.producer()
    p.produce(topic, 'Hello Python!')
    p.produce(topic, key='Just a key')
    p.flush()

    # Consume some messages for the group
    group = 'test-group'
    consume_messages(cluster, group, topic, 2)

    # Verify Describe Consumer Groups
    desc = verify_provided_describe_for_authorized_operations(admin_client,
                                                              admin_client.describe_consumer_groups,
                                                              AclOperation.READ,
                                                              AclOperation.DELETE,
                                                              ResourceType.GROUP,
                                                              group,
                                                              [group])
    assert group == desc.group_id
    assert desc.is_simple_consumer_group is False
    assert desc.state == ConsumerGroupState.EMPTY

    # Delete group
    perform_admin_operation_sync(admin_client.delete_consumer_groups, [group], request_timeout=10)


def verify_describe_cluster(admin_client):
    desc = verify_provided_describe_for_authorized_operations(admin_client,
                                                              admin_client.describe_cluster,
                                                              AclOperation.ALTER,
                                                              AclOperation.ALTER_CONFIGS,
                                                              ResourceType.BROKER,
                                                              "kafka-cluster")
    assert isinstance(desc.cluster_id, str)
    assert len(desc.nodes) > 0
    assert desc.controller is not None


@pytest.mark.parametrize('sasl_cluster', [{'broker_cnt': 1}], indirect=True)
def test_describe_operations(sasl_cluster):

    admin_client = sasl_cluster.admin()

    # Verify Authorized Operations in Describe Cluster
    verify_describe_cluster(admin_client)

    # Create Topic
    topic_config = {"compression.type": "gzip"}
    our_topic = sasl_cluster.create_topic(topic_prefix,
                                          {
                                              "num_partitions": 1,
                                              "config": topic_config,
                                              "replication_factor": 1,
                                          },
                                          validate_only=False
                                          )

    # Verify Authorized Operations in Describe Topics
    verify_describe_topics(admin_client, our_topic)

    # Verify Authorized Operations in Describe Groups
    verify_describe_groups(sasl_cluster, admin_client, our_topic)

    # Delete Topic
    perform_admin_operation_sync(admin_client.delete_topics, [our_topic], operation_timeout=0, request_timeout=10)
