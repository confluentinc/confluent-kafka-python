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
import pytest
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition, ConsumerGroupState, KafkaException
from confluent_kafka.admin import (NewPartitions, ConfigResource,
                                   AclBinding, AclBindingFilter, ResourceType,
                                   ResourcePatternType, AclOperation, AclPermissionType,
                                   UserScramCredentialsDescription, UserScramCredentialUpsertion,
                                   UserScramCredentialDeletion, ScramCredentialInfo,
                                   ScramMechanism)
from confluent_kafka.error import ConsumeError, KafkaException, KafkaError

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

def consume_messages(sasl_cluster, group_id, topic, num_messages=None):
    conf = {'group.id': group_id,
            'session.timeout.ms': 6000,
            'enable.auto.commit': False,
            'on_commit': verify_commit_result,
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True}
    consumer = sasl_cluster.consumer(conf)
    consumer.subscribe([topic])
    eof_reached = dict()
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
            if msg is not None and e.code == confluent_kafka.KafkaError._PARTITION_EOF:
                print('Reached end of %s [%d] at offset %d' % (
                        msg.topic(), msg.partition(), msg.offset()))
                eof_reached[(msg.topic(), msg.partition())] = True
                if len(eof_reached) == len(consumer.assignment()):
                    print('EOF reached for all assigned partitions: exiting')
                    break
            else:
                print('Consumer error: %s: ignoring' % str(e))
                break
    consumer.close()

def create_acls(admin_client, acl_bindings):
    fs = admin_client.create_acls(acl_bindings)
    for acl_binding, f in fs.items():
        f.result()  # trigger exception if there was an error

def delete_acls(admin_client, acl_binding_filters):
    fs = admin_client.delete_acls(acl_binding_filters)
    for acl_binding_filters, f in fs.items():
        f.result()  # trigger exception if there was an error

def verify_describe_topics(admin_client, topic):
    futureMap = admin_client.describe_topics(
        [topic], request_timeout=10, include_topic_authorized_operations=False)
    for topic, future in futureMap.items():
        try:
            t = future.result()
            assert t.topic == topic  # SUCCESS
            assert t.authorized_operations == []
        except Exception:
            raise

    futureMap = admin_client.describe_topics([topic], request_timeout=10, include_topic_authorized_operations=True)
    for topic, future in futureMap.items():
        try:
            t = future.result()
            assert t.topic == topic
            assert len(t.authorized_operations) > 0
            assert AclOperation.CREATE in t.authorized_operations
            assert AclOperation.DELETE in t.authorized_operations
        except KafkaException as e:
            assert False, "DescribeTopics failed"
        except Exception:
            raise

    topic_acl_binding = AclBinding(ResourceType.TOPIC, topic, ResourcePatternType.LITERAL,
                               "User:sasl_user", "*", AclOperation.READ, AclPermissionType.ALLOW)
    create_acls(admin_client, [topic_acl_binding])
    time.sleep(2)
    futureMap = admin_client.describe_topics([topic], request_timeout=10, include_topic_authorized_operations=True)
    for topic, future in futureMap.items():
        try:
            t = future.result()
            assert t.topic == topic
            assert len(t.authorized_operations) > 0
            assert AclOperation.READ in t.authorized_operations
            assert AclOperation.DESCRIBE in t.authorized_operations
            assert AclOperation.CREATE not in t.authorized_operations
            assert AclOperation.DELETE not in t.authorized_operations
        except KafkaException as e:
            assert False, "DescribeTopics failed"
        except Exception:
            raise
    
    topic_acl_binding_filter = AclBindingFilter(ResourceType.TOPIC, None, ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    delete_acls(admin_client, [topic_acl_binding_filter])
    time.sleep(2)


def verify_describe_group(sasl_cluster, admin_client, our_topic):

    # Produce some messages
    p = sasl_cluster.producer()
    p.produce(our_topic, 'Hello Python!', headers=produce_headers)
    p.produce(our_topic, key='Just a key and headers', headers=produce_headers)
    p.flush()

    # Consume some messages for the group
    group = 'test-group'
    consume_messages(sasl_cluster, group, our_topic, 2)

    # Describe Consumer Groups API test
    futureMap = admin_client.describe_consumer_groups(
        [group], request_timeout=10, include_authorized_operations=False)
    for _, future in futureMap.items():
        g = future.result()
        assert g.authorized_operations == []
 
    futureMap = admin_client.describe_consumer_groups(
        [group], request_timeout=10, include_authorized_operations=True)
    for _, future in futureMap.items():
        g = future.result()
        assert len(g.authorized_operations) > 0
        assert AclOperation.DELETE in g.authorized_operations

    group_acl_binding = AclBinding(ResourceType.GROUP, group, ResourcePatternType.LITERAL,
                               "User:sasl_user", "*", AclOperation.READ, AclPermissionType.ALLOW)
    create_acls(admin_client, [group_acl_binding])
    time.sleep(2)

    futureMap = admin_client.describe_consumer_groups(
        [group], request_timeout=10, include_authorized_operations=True)
    for _, future in futureMap.items():
        try:
            g = future.result()
            assert len(g.authorized_operations) > 0
            assert AclOperation.READ in g.authorized_operations
            assert AclOperation.DESCRIBE in g.authorized_operations
            assert AclOperation.DELETE not in g.authorized_operations
        except Exception:
            raise
    
    group_acl_binding_filter = AclBindingFilter(ResourceType.GROUP, None, ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    delete_acls(admin_client, [group_acl_binding_filter])
    time.sleep(2)

    # Delete groups
    fs = admin_client.delete_consumer_groups([group], request_timeout=10)
    fs[group].result()  # will raise exception on failure

def verify_describe_cluster(admin_client):

    # Describe Cluster API test
    fs = admin_client.describe_cluster(request_timeout=10)
    try:
        clus_desc = fs.result()
        assert len(clus_desc.nodes) > 0
        assert clus_desc.authorized_operations == []
    except KafkaException as e:
        assert False, "DescribeCluster Failed"
    except Exception:
        raise

    fs = admin_client.describe_cluster(request_timeout=10, include_cluster_authorized_operations=True)
    try:
        clus_desc = fs.result()
        assert len(clus_desc.authorized_operations) > 0
        assert AclOperation.ALTER_CONFIGS in clus_desc.authorized_operations
    except KafkaException as e:
        assert False, "DescribeCluster Failed"
    except Exception:
        raise

    cluster_acl_binding = AclBinding(ResourceType.BROKER, "kafka-cluster", ResourcePatternType.LITERAL,
                               "User:*", "*", AclOperation.ALTER, AclPermissionType.ALLOW)
    create_acls(admin_client, [cluster_acl_binding])
    time.sleep(2)

    fs = admin_client.describe_cluster(request_timeout=10, include_cluster_authorized_operations=True)
    try:
        clus_desc = fs.result()
        assert len(clus_desc.authorized_operations) > 0
        assert AclOperation.ALTER_CONFIGS not in clus_desc.authorized_operations
    except KafkaException as e:
        assert False, "DescribeCluster Failed"
    except Exception:
        raise

    cluster_acl_binding_filter = AclBindingFilter(ResourceType.BROKER, "kafka-cluster", ResourcePatternType.ANY,
                                           None, None, AclOperation.ANY, AclPermissionType.ANY)
    delete_acls(admin_client, [cluster_acl_binding_filter])
    time.sleep(2)

def test_authorized_operations(sasl_cluster):
    num_partitions = 1
    topic_config = {"compression.type": "gzip"}

    our_topic = sasl_cluster.create_topic(topic_prefix,
                                        {
                                            "num_partitions": num_partitions,
                                            "config": topic_config,
                                            "replication_factor": 1,
                                        },
                                        validate_only=False
                                        )

    admin_client = sasl_cluster.admin()

    # Describe Topic with Authorized Operations
    verify_describe_topics(admin_client, our_topic)
    verify_describe_group(sasl_cluster, admin_client, our_topic)
    verify_describe_cluster(admin_client)

