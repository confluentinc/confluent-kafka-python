#!/usr/bin/env python
#
# Copyright 2019 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
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

import array
import re
import sys
import json

from confluent_kafka.cimpl import KafkaException, KafkaError, Producer

from confluent_kafka.admin import AdminClient, NewTopic


def partition_set(topics):
    """
    Return an array of TopicPartition set, so we can send messages to them one by one.
    For example, for topics
    {
      "foo" : { "numPartitions": 3 }
      "bar" : { "numPartitions": 2 }
    }
    return
    [("foo", 0), ("foo", 1), ("foo", 2), ("bar", 0), ("bar", 1)]
    """
    topic_partitions = []
    for t_name in topics:
        t = topics[t_name]
        nr_partitions = t.get("numPartitions", 1)
        for i in range(0, nr_partitions):
            topic_partitions.append((t_name, i))
    return topic_partitions


def expand_topics(topics):
    """
    Return extended topics.
    For example, for topics { "foo[1-3]" : {} }, return { "foo1" : {}, "foo2" : {}, "foo3" : {}}
    """
    topic_expand_matter = re.compile(r'(.*?)\[([0-9]+)-([0-9]+)\](.*)')
    expanded = {}
    for topicName in topics:
        match = topic_expand_matter.match(topicName)
        if match:
            pre_name = match.group(1)
            start_index = int(match.group(2))
            end_index = int(match.group(3))
            last_name = match.group(4)
            if end_index >= start_index:
                for t in range(start_index, end_index + 1):
                    newTopicName = "%s%d%s" % (pre_name, t, last_name)
                    expanded[newTopicName] = topics[topicName]
            else:
                raise Exception('Invalid range in the topic name %s' % (topicName))
        else:
            expanded[topicName] = topics[topicName]
    return expanded


def merge_topics(active_topics, inactive_topics):
    all_topics = active_topics.copy()
    all_topics.update(inactive_topics)
    return all_topics


def create_kafka_conf(bootstrap_servers, *args):
    k_conf = {"bootstrap.servers": bootstrap_servers}
    for conf in args:
        k_conf.update(conf)
    return k_conf


def create_admin_conf(bootstrap_servers, common_client_config, admin_client_config):
    # Refer Java Trogdor tools/src/main/java/org/apache/kafka/trogdor/common/WorkerUtils.java#L305
    admin_request_timeout_ms = 25000
    create_kafka_conf(bootstrap_servers, common_client_config, admin_client_config)
    admin_conf = create_kafka_conf(bootstrap_servers, common_client_config, admin_client_config)
    admin_conf["socket.timeout.ms"] = admin_request_timeout_ms
    return admin_conf


def create_producer_conn(bootstrap_servers, common_client_config, producer_config):
    producer_conf = create_kafka_conf(bootstrap_servers, common_client_config, producer_config)
    return Producer(producer_conf)


def create_admin_client(bootstrap_servers, common_client_config, admin_client_config):
    admin_conf = create_admin_conf(bootstrap_servers, common_client_config, admin_client_config)
    admin_client = AdminClient(admin_conf)
    return admin_client


def create_topics(admin_client, topics):
    for topic_name in topics:
        topic = topics[topic_name]
        create_topic(admin_client, topic_name, topic)


def create_topic(admin_client, topic_name, topic):
    num_partitions = topic["numPartitions"]
    replication_factor = topic["replicationFactor"]
    fs = admin_client.create_topics([NewTopic(topic_name, num_partitions, replication_factor)])
    fs = fs[topic_name]
    try:
        return fs.result()
    except KafkaException as ex:
        if ex.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            trogdor_log("Topic %s already exists" % (topic_name))
        else:
            raise ex


def get_payload_generator(spec, default_generator=None):
    if "type" not in spec:
        return default_generator
    if spec["type"] == "constant":
        if "size" not in spec:
            return default_generator
        if "value" in spec:
            return ConstGenerator(spec["size"], spec["value"])
        else:
            return ConstGenerator(spec["size"], 0x0)
    elif spec["type"] == "sequential":
        if "size" not in spec or "startOffset" not in spec:
            return default_generator
        return SeqGenerator(spec["size"], spec["startOffset"])
    else:
        return default_generator


# msg is a JSON string {"status":status, "error":error, "log":log}
def output_trogdor_message(msg):
    print(msg)
    sys.stdout.flush()


def update_trogdor_status(status):
    msg = json.dumps({"status": status})
    output_trogdor_message(msg)


def update_trogdor_error(error):
    msg = json.dumps({"error": error})
    output_trogdor_message(msg)


def trogdor_log(log):
    msg = json.dumps({"log": log})
    output_trogdor_message(msg)


class SeqGenerator:
    def __init__(self, size, start_offset):
        self.size = size
        self.start_offset = start_offset

    def generate(self, position):
        return (self.start_offset + position).to_bytes(self.size, byteorder='little')


class ConstGenerator:
    def __init__(self, size, val):
        self.size = size
        if type(val) is int:
            self.const_bytes = val.to_bytes(self.size, byteorder='little')
        elif type(val) is list:
            list_p = []
            for i in range(0, size):
                val = val[i] if len(val) > i else 0
                list_p[i] = val % 0x100
            self.const_bytes = array.array('B', list_p)
        else:
            raise Exception("Unrecognized type of ConstGenerator value: " + type(val))

    def generate(self, position):
        return self.const_bytes


class PayloadGenerator:
    def __init__(self, pyload_generator):
        self.generator = pyload_generator
        self.position = 0
        self.last = None

    def nextVal(self):
        load = self.generator.generate(self.position)
        self.position += 1
        self.last = load
        return self.last

    def fetch_last(self):
        return self.last
