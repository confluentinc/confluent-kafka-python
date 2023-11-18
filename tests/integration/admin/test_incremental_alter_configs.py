# -*- coding: utf-8 -*-
# Copyright 2023 Confluent Inc.
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

from confluent_kafka.admin import ConfigResource, \
    ConfigEntry, ResourceType, \
    AlterConfigOpType


def assert_expected_config_entries(fs, num_fs, expected):
    """
    Verify that the list of non-default entries corresponds
    to the expected one for each config resource.
    """
    assert len(fs.items()) == num_fs
    for res, f in fs.items():
        configs = f.result()
        entries = sorted([str(entry) for entry in configs.values()
                          if not entry.is_default])
        assert entries == expected[res]


def assert_operation_succeeded(fs, num_fs):
    """
    Verify that the operation succeeded
    for each resource.
    """
    assert len(fs.items()) == num_fs
    for _, f in fs.items():
        assert f.result() is None  # empty, but raises exception on failure


def test_incremental_alter_configs(kafka_cluster):
    """
    Incrementally change the configuration entries of two topics
    and verify that the configuration description corresponds.
    """

    topic_prefix = "test-topic"
    topic_prefix2 = "test-topic2"
    num_partitions = 2
    topic_config = {"compression.type": "gzip"}

    our_topic = kafka_cluster.create_topic(topic_prefix,
                                           {
                                               "num_partitions": num_partitions,
                                               "config": topic_config,
                                               "replication_factor": 1,
                                           })
    our_topic2 = kafka_cluster.create_topic(topic_prefix2,
                                            {
                                                "num_partitions": num_partitions,
                                                "config": topic_config,
                                                "replication_factor": 1,
                                            })

    admin_client = kafka_cluster.admin()

    res1 = ConfigResource(
        ResourceType.TOPIC,
        our_topic,
        incremental_configs=[
            ConfigEntry("cleanup.policy", "compact",
                        incremental_operation=AlterConfigOpType.APPEND),
            ConfigEntry("retention.ms", "10000",
                        incremental_operation=AlterConfigOpType.SET)
        ]
    )
    res2 = ConfigResource(
        ResourceType.TOPIC,
        our_topic2,
        incremental_configs=[
            ConfigEntry("cleanup.policy", "delete",
                        incremental_operation=AlterConfigOpType.SUBTRACT),
            ConfigEntry("retention.ms", "5000",
                        incremental_operation=AlterConfigOpType.SET)
        ]
    )
    expected = {
        res1: ['cleanup.policy="delete,compact"',
               'compression.type="gzip"',
               'retention.ms="10000"'],
        res2: ['cleanup.policy=""',
               'compression.type="gzip"',
               'retention.ms="5000"']
    }

    #
    # Incrementally alter some configuration values
    #
    fs = admin_client.incremental_alter_configs([res1, res2])

    assert_operation_succeeded(fs, 2)

    #
    # Get current topic config
    #
    fs = admin_client.describe_configs([res1, res2])

    # Assert expected config entries.
    assert_expected_config_entries(fs, 2, expected)

    #
    # Delete an entry and change a second one.
    #
    res2 = ConfigResource(
        ResourceType.TOPIC,
        our_topic2,
        incremental_configs=[
            ConfigEntry("compression.type", None,
                        incremental_operation=AlterConfigOpType.DELETE),
            ConfigEntry("retention.ms", "10000",
                        incremental_operation=AlterConfigOpType.SET)
        ]
    )
    expected[res2] = ['cleanup.policy=""',
                      'retention.ms="10000"']

    #
    # Incrementally alter some configuration values
    #
    fs = admin_client.incremental_alter_configs([res2])

    assert_operation_succeeded(fs, 1)

    #
    # Get current topic config
    #
    fs = admin_client.describe_configs([res2])

    # Assert expected config entries.
    assert_expected_config_entries(fs, 1, expected)
