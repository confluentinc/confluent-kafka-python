#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
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

#
# Example Admin clients.
#

from confluent_kafka import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigEntry, KafkaException
import sys


def example_create_topics(a, topics):
    """ Create topics """

    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    # Call create_topics to asynchronously create topics, a dict
    # of <topic,future> is returned.
    fs = a.create_topics(new_topics)

    # Wait for operation to finish.
    # Timeouts are preferably controlled by passing request_timeout=15.0
    # to the create_topics() call.
    # All futures will finish at the same time.
    for topic, f in fs.iteritems():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def example_delete_topics(a, topics):
    """ delete topics """

    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while
    # topics are deleted in the background. But here we give it some time (30s)
    # to propagate in the cluster before returning.
    #
    # Returns a dict of <topic,future>.
    fs = a.delete_topics(topics, operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.iteritems():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))


def example_create_partitions(a, topics):
    """ create partitions """

    new_parts = [NewPartitions(topic, int(new_total_count)) for
                 topic, new_total_count in zip(topics[0::2], topics[1::2])]

    # Try switching validate_only to True to only validate the operation
    # on the broker but not actually perform it.
    fs = a.create_partitions(new_parts, validate_only=False)

    # Wait for operation to finish.
    for topic, f in fs.iteritems():
        try:
            f.result()  # The result itself is None
            print("Additional partitions created for topic {}".format(topic))
        except Exception as e:
            print("Failed to add partitions to topic {}: {}".format(topic, e))


def print_config(config, depth):
    print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
          ((' ' * depth) + config.name, config.value,
           ConfigEntry.config_source_to_str(config.source),
           config.is_read_only, config.is_default,
           config.is_sensitive, config.is_synonym,
           ["%s:%s" % (x.name, ConfigEntry.config_source_to_str(x.source))
            for x in iter(config.synonyms.values())]))


def example_describe_configs(a, args):
    """ describe configs """

    resources = [ConfigResource(restype, resname) for
                 restype, resname in zip(args[0::2], args[1::2])]

    fs = a.describe_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.iteritems():
        try:
            configs = f.result()
            for config in iter(configs.values()):
                print_config(config, 1)

        except KafkaException as e:
            print("Failed to describe {}: {}".format(res, e))
        except Exception as e:
            raise


def example_alter_configs(a, args):
    """ alter configs """

    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        resource = ConfigResource(restype, resname)
        resources.append(resource)
        for k, v in [conf.split('=') for conf in configs.split(',')]:
            resource.set_config(k, v)

    fs = a.alter_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.iteritems():
        try:
            f.result()  # empty, but raises exception on failure
            print("{} configuration successfully altered".format(res))
        except Exception:
            raise


def example_list(a, args):
    """ list topics and cluster metadata """

    if len(args) == 0:
        what = "all"
    else:
        what = args[0]

    md = a.list_topics(timeout=10)

    print("Cluster {} metadata (response from broker {}):".format(md.cluster_id, md.orig_broker_name))

    if what in ("all", "brokers"):
        print(" {} brokers:".format(len(md.brokers)))
        for b in iter(md.brokers.values()):
            if b.id == md.controller_id:
                print("  {}  (controller)".format(b))
            else:
                print("  {}".format(b))

    if what not in ("all", "topics"):
        return

    print(" {} topics:".format(len(md.topics)))
    for t in iter(md.topics.values()):
        if t.error is not None:
            errstr = ": {}".format(t.error)
        else:
            errstr = ""

        print("  \"{}\" with {} partition(s){}".format(t, len(t.partitions), errstr))

        for p in iter(t.partitions.values()):
            if p.error is not None:
                errstr = ": {}".format(p.error)
            else:
                errstr = ""

            print("    partition {} leader: {}, replicas: {}, isrs: {}".format(
                p.id, p.leader, p.replicas, p.isrs, errstr))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <operation> <args..>\n\n' % sys.argv[0])
        sys.stderr.write('operations:\n')
        sys.stderr.write(' create_topics <topic1> <topic2> ..\n')
        sys.stderr.write(' delete_topics <topic1> <topic2> ..\n')
        sys.stderr.write(' create_partitions <topic1> <new_total_count1> <topic2> <new_total_count2> ..\n')
        sys.stderr.write(' describe_configs <resource_type1> <resource_name1> <resource2> <resource_name2> ..\n')
        sys.stderr.write(' alter_configs <resource_type1> <resource_name1> ' +
                         '<config=val,config2=val2> <resource_type2> <resource_name2> <config..> ..\n')
        sys.stderr.write(' list [<all|topics|brokers>]\n')
        sys.exit(1)

    broker = sys.argv[1]
    operation = sys.argv[2]
    args = sys.argv[3:]

    # Create Admin client
    a = AdminClient({'bootstrap.servers': broker})

    opsmap = {'create_topics': example_create_topics,
              'delete_topics': example_delete_topics,
              'create_partitions': example_create_partitions,
              'describe_configs': example_describe_configs,
              'alter_configs': example_alter_configs,
              'list': example_list}

    if operation not in opsmap:
        sys.stderr.write('Unknown operation: %s\n' % operation)
        sys.exit(1)

    opsmap[operation](a, args)
