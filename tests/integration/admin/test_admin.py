#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2019 Confluent Inc.
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
#


import time
from uuid import uuid4

from confluent_kafka.cimpl import RESOURCE_TOPIC

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource


def test_admin(kafka_cluster_fixture, topic_fixture):
    """ Verify Admin API """

    a = AdminClient(kafka_cluster_fixture.client_conf())
    our_topic = topic_fixture + '_admin_' + str(uuid4())
    num_partitions = 2

    topic_config = {"compression.type": "gzip"}

    #
    # First iteration: validate our_topic creation.
    # Second iteration: create topic.
    #
    for validate in (True, False):
        fs = a.create_topics([NewTopic(our_topic,
                                       num_partitions=num_partitions,
                                       config=topic_config,
                                       replication_factor=1)],
                             validate_only=validate,
                             operation_timeout=10.0)

        for topic2, f in fs.items():
            f.result()  # trigger exception if there was an error

    #
    # Find the topic in list_topics
    #
    verify_topic_metadata(a, {our_topic: num_partitions})

    #
    # Increase the partition count
    #
    num_partitions += 3
    fs = a.create_partitions([NewPartitions(our_topic,
                                            new_total_count=num_partitions)],
                             operation_timeout=10.0)

    for topic2, f in fs.items():
        f.result()  # trigger exception if there was an error

    #
    # Verify with list_topics.
    #
    verify_topic_metadata(a, {our_topic: num_partitions})

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
    resource = ConfigResource(RESOURCE_TOPIC, our_topic)
    fs = a.describe_configs([resource])
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

    fs = a.alter_configs([resource])
    fs[resource].result()  # will raise exception on failure

    #
    # Read the config back again and verify.
    #
    fs = a.describe_configs([resource])
    configs = fs[resource].result()  # will raise exception on failure

    # Verify config matches our expectations
    verify_config(topic_config, configs)

    #
    # Delete the topic
    #
    fs = a.delete_topics([our_topic])
    fs[our_topic].result()  # will raise exception on failure
    print("Topic {} marked for deletion".format(our_topic))


def verify_topic_metadata(client, exp_topics):
    """
    Verify that exp_topics (dict<topicname,partcnt>) is reported in metadata.
    Will retry and wait for some time to let changes propagate.

    Non-controller brokers may return the previous partition count for some
    time before being updated, in this case simply retry.
    """

    for retry in range(0, 3):
        do_retry = 0

        md = client.list_topics()

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

    raise Exception("Timed out waiting for topics {} in metadata".format(exp_topics))
