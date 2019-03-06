# Copyright 2016 Confluent Inc.
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

import re
import sys
import json

from confluent_kafka.cimpl import KafkaException, KafkaError, Producer

from confluent_kafka.admin import AdminClient, NewTopic


def partition_set(topics):
    """ Return a set of TopicPartition """
    topic_partitions = []
    for t_name in topics:
        t = topics[t_name]
        nr_partitions = t.get("numPartitions", 1)
        for i in range(0, nr_partitions):
            topic_partitions.append((t_name, i))
    return topic_partitions


def expand_topics(topics):
    topic_expand_matter = re.compile(r'(.*?)\[([0-9]*)\-([0-9]*)\](.*)')
    expanded = {}
    for topicName in topics:
        match = topic_expand_matter.match(topicName)
        if (match):
            pre = match.group(1)
            start = int(match.group(2))
            end = int(match.group(3))
            last = match.group(4)
            if end >= start:
                for t in range(start, end + 1):
                    newTopicName = "%s%d%s" % (pre, t, last)
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
    admin_conf = create_kafka_conf(bootstrap_servers, common_client_config, admin_client_config);
    admin_conf["socket.timeout.ms"] = admin_request_timeout_ms
    return admin_conf

def create_producer_conn(bootstrap_servers, common_client_config, producer_config):
    producer_conf = create_kafka_conf(bootstrap_servers, common_client_config, producer_config)
    return Producer(**producer_conf)

def record_on_delivery(err, msg):
    if err is not None:
        trogdor_log("ProduceSpecRunner: delivery failed: {} [{}]: {}".format(msg.topic(), msg.partition(), err))
    else:
        trogdor_log("ProduceSpecRunner: delivery successed: {}".format(str(msg)))
def create_admin_conn(bootstrap_servers, common_client_config, admin_client_config):
    admin_conf = create_admin_conf(bootstrap_servers, common_client_config, admin_client_config)
    admin_conn = AdminClient(admin_conf)
    return admin_conn


def create_topics(admin_conn, topics):
    for topic_name in topics:
        topic = topics[topic_name]
        create_topic(admin_conn, topic_name, topic)


def create_topic(admin_conn, topic_name, topic):
    num_partitions = topic["numPartitions"]
    replication_factor = topic["replicationFactor"]
    fs = admin_conn.create_topics([NewTopic(topic_name, num_partitions, replication_factor)])
    fs = fs[topic_name]
    try:
        res = fs.result()
    except KafkaException as ex:
        if ex.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            trogdor_log("Topic %s already exists" % (topic_name))
        else:
            raise ex


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
        self.const_bytes = val.to_bytes(self.size, byteorder='little')

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