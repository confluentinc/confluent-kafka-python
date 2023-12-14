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


# Example use of AdminClient operations.

from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             TopicPartition, ConsumerGroupState, TopicCollection,
                             IsolationLevel)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource,
                                   ConfigEntry, ConfigSource, AclBinding,
                                   AclBindingFilter, ResourceType, ResourcePatternType,
                                   AclOperation, AclPermissionType, AlterConfigOpType,
                                   ScramMechanism, ScramCredentialInfo,
                                   UserScramCredentialUpsertion, UserScramCredentialDeletion,
                                   OffsetSpec)
import sys
import threading
import logging

logging.basicConfig()


def parse_nullable_string(s):
    if s == "None":
        return None
    else:
        return s


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
    for topic, f in fs.items():
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
    for topic, f in fs.items():
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
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Additional partitions created for topic {}".format(topic))
        except Exception as e:
            print("Failed to add partitions to topic {}: {}".format(topic, e))


def print_config(config, depth):
    print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
          ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
           config.is_read_only, config.is_default,
           config.is_sensitive, config.is_synonym,
           ["%s:%s" % (x.name, ConfigSource(x.source))
            for x in iter(config.synonyms.values())]))


def example_describe_configs(a, args):
    """ describe configs """

    resources = [ConfigResource(restype, resname) for
                 restype, resname in zip(args[0::2], args[1::2])]

    fs = a.describe_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            configs = f.result()
            for config in iter(configs.values()):
                print_config(config, 1)

        except KafkaException as e:
            print("Failed to describe {}: {}".format(res, e))
        except Exception:
            raise


def example_create_acls(a, args):
    """ create acls """

    acl_bindings = [
        AclBinding(
            ResourceType[restype],
            parse_nullable_string(resname),
            ResourcePatternType[resource_pattern_type],
            parse_nullable_string(principal),
            parse_nullable_string(host),
            AclOperation[operation],
            AclPermissionType[permission_type]
        )
        for restype, resname, resource_pattern_type,
        principal, host, operation, permission_type
        in zip(
            args[0::7],
            args[1::7],
            args[2::7],
            args[3::7],
            args[4::7],
            args[5::7],
            args[6::7],
        )
    ]

    try:
        fs = a.create_acls(acl_bindings, request_timeout=10)
    except ValueError as e:
        print(f"create_acls() failed: {e}")
        return

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            result = f.result()
            if result is None:
                print("Created {}".format(res))

        except KafkaException as e:
            print("Failed to create ACL {}: {}".format(res, e))
        except Exception:
            raise


def example_describe_acls(a, args):
    """ describe acls """

    acl_binding_filters = [
        AclBindingFilter(
            ResourceType[restype],
            parse_nullable_string(resname),
            ResourcePatternType[resource_pattern_type],
            parse_nullable_string(principal),
            parse_nullable_string(host),
            AclOperation[operation],
            AclPermissionType[permission_type]
        )
        for restype, resname, resource_pattern_type,
        principal, host, operation, permission_type
        in zip(
            args[0::7],
            args[1::7],
            args[2::7],
            args[3::7],
            args[4::7],
            args[5::7],
            args[6::7],
        )
    ]

    fs = [
        a.describe_acls(acl_binding_filter, request_timeout=10)
        for acl_binding_filter in acl_binding_filters
    ]
    # Wait for operations to finish.
    for acl_binding_filter, f in zip(acl_binding_filters, fs):
        try:
            print("Acls matching filter: {}".format(acl_binding_filter))
            acl_bindings = f.result()
            for acl_binding in acl_bindings:
                print(acl_binding)

        except KafkaException as e:
            print("Failed to describe {}: {}".format(acl_binding_filter, e))
        except Exception:
            raise


def example_delete_acls(a, args):
    """ delete acls """

    acl_binding_filters = [
        AclBindingFilter(
            ResourceType[restype],
            parse_nullable_string(resname),
            ResourcePatternType[resource_pattern_type],
            parse_nullable_string(principal),
            parse_nullable_string(host),
            AclOperation[operation],
            AclPermissionType[permission_type]
        )
        for restype, resname, resource_pattern_type,
        principal, host, operation, permission_type
        in zip(
            args[0::7],
            args[1::7],
            args[2::7],
            args[3::7],
            args[4::7],
            args[5::7],
            args[6::7],
        )
    ]

    try:
        fs = a.delete_acls(acl_binding_filters, request_timeout=10)
    except ValueError as e:
        print(f"delete_acls() failed: {e}")
        return

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            acl_bindings = f.result()
            print("Deleted acls matching filter: {}".format(res))
            for acl_binding in acl_bindings:
                print(" ", acl_binding)

        except KafkaException as e:
            print("Failed to delete {}: {}".format(res, e))
        except Exception:
            raise


def example_incremental_alter_configs(a, args):
    """ Incrementally alter configs, keeping non-specified
    configuration properties with their previous values.

    Input Format : ResourceType1 ResourceName1 Key=Operation:Value;Key2=Operation2:Value2;Key3=DELETE
    ResourceType2 ResourceName2 ...

    Example: TOPIC T1 compression.type=SET:lz4;cleanup.policy=ADD:compact;
    retention.ms=DELETE TOPIC T2 compression.type=SET:gzip ...
    """
    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        incremental_configs = []
        for name, operation_and_value in [conf.split('=') for conf in configs.split(';')]:
            if operation_and_value == "DELETE":
                operation, value = operation_and_value, None
            else:
                operation, value = operation_and_value.split(':')
            operation = AlterConfigOpType[operation]
            incremental_configs.append(ConfigEntry(name, value,
                                       incremental_operation=operation))
        resources.append(ConfigResource(restype, resname,
                                        incremental_configs=incremental_configs))

    fs = a.incremental_alter_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            f.result()  # empty, but raises exception on failure
            print("{} configuration successfully altered".format(res))
        except Exception:
            raise


def example_alter_configs(a, args):
    """ Alter configs atomically, replacing non-specified
    configuration properties with their default values.
    """

    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        resource = ConfigResource(restype, resname)
        resources.append(resource)
        for k, v in [conf.split('=') for conf in configs.split(',')]:
            resource.set_config(k, v)

    fs = a.alter_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            f.result()  # empty, but raises exception on failure
            print("{} configuration successfully altered".format(res))
        except Exception:
            raise


def example_delta_alter_configs(a, args):
    """
    The AlterConfigs Kafka API requires all configuration to be passed,
    any left out configuration properties will revert to their default settings.

    This example shows how to just modify the supplied configuration entries
    by first reading the configuration from the broker, updating the supplied
    configuration with the broker configuration (without overwriting), and
    then writing it all back.

    The async nature of futures is also show-cased, which makes this example
    a bit more complex than it needs to be in the synchronous case.
    """

    # Convert supplied config to resources.
    # We can reuse the same resources both for describe_configs and
    # alter_configs.
    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        resource = ConfigResource(restype, resname)
        resources.append(resource)
        for k, v in [conf.split('=') for conf in configs.split(',')]:
            resource.set_config(k, v)

    # Set up a locked counter and an Event (for signaling) to track when the
    # second level of futures are done. This is a bit of contrived example
    # due to no other asynchronous mechanism being used, so we'll need
    # to wait on something to signal completion.

    class WaitZero(object):
        def __init__(self, waitcnt):
            self.cnt = waitcnt
            self.lock = threading.Lock()
            self.event = threading.Event()

        def decr(self):
            """ Decrement cnt by 1"""
            with self.lock:
                assert self.cnt > 0
                self.cnt -= 1
            self.event.set()

        def wait(self):
            """ Wait until cnt reaches 0 """
            self.lock.acquire()
            while self.cnt > 0:
                self.lock.release()
                self.event.wait()
                self.event.clear()
                self.lock.acquire()
            self.lock.release()

        def __len__(self):
            with self.lock:
                return self.cnt

    wait_zero = WaitZero(len(resources))

    # Read existing configuration from cluster
    fs = a.describe_configs(resources)

    def delta_alter_configs_done(fut, resource):
        e = fut.exception()
        if e is not None:
            print("Config update for {} failed: {}".format(resource, e))
        else:
            print("Config for {} updated".format(resource))
        wait_zero.decr()

    def delta_alter_configs(resource, remote_config):
        print("Updating {} supplied config entries {} with {} config entries read from cluster".format(
            len(resource), resource, len(remote_config)))
        # Only set configuration that is not default
        for k, entry in [(k, v) for k, v in remote_config.items() if not v.is_default]:
            resource.set_config(k, entry.value, overwrite=False)

        fs = a.alter_configs([resource])
        fs[resource].add_done_callback(lambda fut: delta_alter_configs_done(fut, resource))

    # For each resource's future set up a completion callback
    # that in turn calls alter_configs() on that single resource.
    # This is ineffective since the resources can usually go in
    # one single alter_configs() call, but we're also show-casing
    # the futures here.
    for res, f in fs.items():
        f.add_done_callback(lambda fut, resource=res: delta_alter_configs(resource, fut.result()))

    # Wait for done callbacks to be triggered and operations to complete.
    print("Waiting for {} resource updates to finish".format(len(wait_zero)))
    wait_zero.wait()


def example_list(a, args):
    """ list topics, groups and cluster metadata """

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

    if what in ("all", "topics"):
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

                print("partition {} leader: {}, replicas: {},"
                      " isrs: {} errstr: {}".format(p.id, p.leader, p.replicas,
                                                    p.isrs, errstr))

    if what in ("all", "groups"):
        groups = a.list_groups(timeout=10)
        print(" {} consumer groups".format(len(groups)))
        for g in groups:
            if g.error is not None:
                errstr = ": {}".format(g.error)
            else:
                errstr = ""

            print(" \"{}\" with {} member(s), protocol: {}, protocol_type: {}{}".format(
                g, len(g.members), g.protocol, g.protocol_type, errstr))

            for m in g.members:
                print("id {} client_id: {} client_host: {}".format(m.id, m.client_id, m.client_host))


def example_list_consumer_groups(a, args):
    """
    List Consumer Groups
    """
    states = {ConsumerGroupState[state] for state in args}
    future = a.list_consumer_groups(request_timeout=10, states=states)
    try:
        list_consumer_groups_result = future.result()
        print("{} consumer groups".format(len(list_consumer_groups_result.valid)))
        for valid in list_consumer_groups_result.valid:
            print("    id: {} is_simple: {} state: {}".format(
                valid.group_id, valid.is_simple_consumer_group, valid.state))
        print("{} errors".format(len(list_consumer_groups_result.errors)))
        for error in list_consumer_groups_result.errors:
            print("    error: {}".format(error))
    except Exception:
        raise


def example_describe_consumer_groups(a, args):
    """
    Describe Consumer Groups
    """
    include_auth_ops = bool(int(args[0]))
    args = args[1:]
    futureMap = a.describe_consumer_groups(args, include_authorized_operations=include_auth_ops, request_timeout=10)

    for group_id, future in futureMap.items():
        try:
            g = future.result()
            print("Group Id: {}".format(g.group_id))
            print("  Is Simple          : {}".format(g.is_simple_consumer_group))
            print("  State              : {}".format(g.state))
            print("  Partition Assignor : {}".format(g.partition_assignor))
            print(
                f"  Coordinator        : {g.coordinator}")
            print("  Members: ")
            for member in g.members:
                print("    Id                : {}".format(member.member_id))
                print("    Host              : {}".format(member.host))
                print("    Client Id         : {}".format(member.client_id))
                print("    Group Instance Id : {}".format(member.group_instance_id))
                if member.assignment:
                    print("    Assignments       :")
                    for toppar in member.assignment.topic_partitions:
                        print("      {} [{}]".format(toppar.topic, toppar.partition))
            if (include_auth_ops):
                print("  Authorized operations: ")
                op_string = ""
                for acl_op in g.authorized_operations:
                    op_string += acl_op.name + "  "
                print("    {}".format(op_string))
        except KafkaException as e:
            print("Error while describing group id '{}': {}".format(group_id, e))
        except Exception:
            raise


def example_describe_topics(a, args):
    """
    Describe Topics
    """
    include_auth_ops = bool(int(args[0]))
    args = args[1:]
    topics = TopicCollection(topic_names=args)
    futureMap = a.describe_topics(topics, request_timeout=10, include_authorized_operations=include_auth_ops)

    for topic_name, future in futureMap.items():
        try:
            t = future.result()
            print("Topic name             : {}".format(t.name))
            print("Topic id               : {}".format(t.topic_id))
            if (t.is_internal):
                print("Topic is Internal")

            if (include_auth_ops):
                print("Authorized operations  : ")
                op_string = ""
                for acl_op in t.authorized_operations:
                    op_string += acl_op.name + "  "
                print("    {}".format(op_string))

            print("Partition Information")
            for partition in t.partitions:
                print("    Id                : {}".format(partition.id))
                leader = partition.leader
                print(f"    Leader            : {leader}")
                print("    Replicas          : {}".format(len(partition.replicas)))
                for replica in partition.replicas:
                    print(f"         Replica            : {replica}")
                print("    In-Sync Replicas  : {}".format(len(partition.isr)))
                for isr in partition.isr:
                    print(f"         In-Sync Replica    : {isr}")
                print("")
            print("")

        except KafkaException as e:
            print("Error while describing topic '{}': {}".format(topic_name, e))
        except Exception:
            raise


def example_describe_cluster(a, args):
    """
    Describe Cluster
    """
    include_auth_ops = bool(int(args[0]))
    args = args[1:]
    future = a.describe_cluster(request_timeout=10, include_authorized_operations=include_auth_ops)
    try:
        c = future.result()
        print("Cluster_id           : {}".format(c.cluster_id))

        if (c.controller):
            print(f"Controller: {c.controller}")
        else:
            print("No Controller Information Available")

        print("Nodes                :")
        for node in c.nodes:
            print(f"  Node: {node}")

        if (include_auth_ops):
            print("Authorized operations: ")
            op_string = ""
            for acl_op in c.authorized_operations:
                op_string += acl_op.name + "  "
            print("    {}".format(op_string))
    except KafkaException as e:
        print("Error while describing cluster: {}".format(e))
    except Exception:
        raise


def example_delete_consumer_groups(a, args):
    """
    Delete Consumer Groups
    """
    groups = a.delete_consumer_groups(args, request_timeout=10)
    for group_id, future in groups.items():
        try:
            future.result()  # The result itself is None
            print("Deleted group with id '" + group_id + "' successfully")
        except KafkaException as e:
            print("Error deleting group id '{}': {}".format(group_id, e))
        except Exception:
            raise


def example_list_consumer_group_offsets(a, args):
    """
    List consumer group offsets
    """

    topic_partitions = []
    for topic, partition in zip(args[1::2], args[2::2]):
        topic_partitions.append(TopicPartition(topic, int(partition)))
    if len(topic_partitions) == 0:
        topic_partitions = None
    groups = [ConsumerGroupTopicPartitions(args[0], topic_partitions)]

    futureMap = a.list_consumer_group_offsets(groups)

    for group_id, future in futureMap.items():
        try:
            response_offset_info = future.result()
            print("Group: " + response_offset_info.group_id)
            for topic_partition in response_offset_info.topic_partitions:
                if topic_partition.error:
                    print("    Error: " + topic_partition.error.str() + " occurred with " +
                          topic_partition.topic + " [" + str(topic_partition.partition) + "]")
                else:
                    print("    " + topic_partition.topic +
                          " [" + str(topic_partition.partition) + "]: " + str(topic_partition.offset))

        except KafkaException as e:
            print("Failed to list {}: {}".format(group_id, e))
        except Exception:
            raise


def example_alter_consumer_group_offsets(a, args):
    """
    Alter consumer group offsets
    """

    topic_partitions = []
    for topic, partition, offset in zip(args[1::3], args[2::3], args[3::3]):
        topic_partitions.append(TopicPartition(topic, int(partition), int(offset)))
    if len(topic_partitions) == 0:
        topic_partitions = None
    groups = [ConsumerGroupTopicPartitions(args[0], topic_partitions)]

    futureMap = a.alter_consumer_group_offsets(groups)

    for group_id, future in futureMap.items():
        try:
            response_offset_info = future.result()
            print("Group: " + response_offset_info.group_id)
            for topic_partition in response_offset_info.topic_partitions:
                if topic_partition.error:
                    print("    Error: " + topic_partition.error.str() + " occurred with " +
                          topic_partition.topic + " [" + str(topic_partition.partition) + "]")
                else:
                    print("    " + topic_partition.topic +
                          " [" + str(topic_partition.partition) + "]: " + str(topic_partition.offset))

        except KafkaException as e:
            print("Failed to alter {}: {}".format(group_id, e))
        except Exception:
            raise


def example_describe_user_scram_credentials(a, args):
    """
    Describe User Scram Credentials
    """
    if len(args) == 0:
        """
        Case: Describes all user scram credentials
        Input: no argument passed or None
        Gets a future which result will give a
        dict[str, UserScramCredentialsDescription]
        or will throw a KafkaException
        """
        f = a.describe_user_scram_credentials()
        try:
            results = f.result()
            for username, response in results.items():
                print("Username : {}".format(username))
                for scram_credential_info in response.scram_credential_infos:
                    print(f"    Mechanism: {scram_credential_info.mechanism} " +
                          f"Iterations: {scram_credential_info.iterations}")
        except KafkaException as e:
            print("Failed to describe all user scram credentials : {}".format(e))
        except Exception:
            raise
    else:
        """
        Case: Describe specified user scram credentials
        Input: users is a list
        Gets a dict[str, future] where the result() of
        each future will give a UserScramCredentialsDescription
        or a KafkaException
        """
        futmap = a.describe_user_scram_credentials(args)
        for username, fut in futmap.items():
            print("Username: {}".format(username))
            try:
                response = fut.result()
                for scram_credential_info in response.scram_credential_infos:
                    print(f"    Mechanism: {scram_credential_info.mechanism} " +
                          f"Iterations: {scram_credential_info.iterations}")
            except KafkaException as e:
                print("    Error: {}".format(e))
            except Exception:
                raise


def example_alter_user_scram_credentials(a, args):
    """
    AlterUserScramCredentials
    """
    alterations_args = []
    alterations = []
    i = 0
    op_cnt = 0

    while i < len(args):
        op = args[i]
        if op == "UPSERT":
            if i + 5 >= len(args):
                raise ValueError(
                    f"Invalid number of arguments for alteration {op_cnt}, expected 5, got {len(args) - i - 1}")
            user = args[i + 1]
            mechanism = ScramMechanism[args[i + 2]]
            iterations = int(args[i + 3])
            password = bytes(args[i + 4], 'utf8')
            # if salt is an empty string,
            # set it to None to generate it randomly.
            salt = args[i + 5]
            if not salt:
                salt = None
            else:
                salt = bytes(salt, 'utf8')
            alterations_args.append([op, user, mechanism, iterations,
                                     iterations, password, salt])
            i += 6
        elif op == "DELETE":
            if i + 2 >= len(args):
                raise ValueError(
                    f"Invalid number of arguments for alteration {op_cnt}, expected 2, got {len(args) - i - 1}")
            user = args[i + 1]
            mechanism = ScramMechanism[args[i + 2]]
            alterations_args.append([op, user, mechanism])
            i += 3
        else:
            raise ValueError(f"Invalid alteration {op}, must be UPSERT or DELETE")
        op_cnt += 1

    for alteration_arg in alterations_args:
        op = alteration_arg[0]
        if op == "UPSERT":
            [_, user, mechanism, iterations,
             iterations, password, salt] = alteration_arg
            scram_credential_info = ScramCredentialInfo(mechanism, iterations)
            upsertion = UserScramCredentialUpsertion(user, scram_credential_info,
                                                     password, salt)
            alterations.append(upsertion)
        elif op == "DELETE":
            [_, user, mechanism] = alteration_arg
            deletion = UserScramCredentialDeletion(user, mechanism)
            alterations.append(deletion)

    futmap = a.alter_user_scram_credentials(alterations)
    for username, fut in futmap.items():
        try:
            fut.result()
            print("{}: Success".format(username))
        except KafkaException as e:
            print("{}: Error: {}".format(username, e))


def example_list_offsets(a, args):
    topic_partition_offsets = {}
    if len(args) == 0:
        raise ValueError(
            "Invalid number of arguments for list offsets, expected at least 1, got 0")
    i = 1
    partition_i = 1
    isolation_level = IsolationLevel[args[0]]
    while i < len(args):
        if i + 3 > len(args):
            raise ValueError(
                f"Invalid number of arguments for list offsets, partition {partition_i}, expected 3," +
                f" got {len(args) - i}")
        topic = args[i]
        partition = int(args[i+1])
        topic_partition = TopicPartition(topic, partition)

        if "EARLIEST" == args[i+2]:
            offset_spec = OffsetSpec.earliest()

        elif "LATEST" == args[i+2]:
            offset_spec = OffsetSpec.latest()

        elif "MAX_TIMESTAMP" == args[i+2]:
            offset_spec = OffsetSpec.max_timestamp()

        elif "TIMESTAMP" == args[i+2]:
            if i + 4 > len(args):
                raise ValueError(
                    f"Invalid number of arguments for list offsets, partition {partition_i}, expected 4" +
                    f", got {len(args) - i}")
            offset_spec = OffsetSpec.for_timestamp(int(args[i+3]))
            i += 1
        else:
            raise ValueError("Invalid OffsetSpec, must be EARLIEST, LATEST, MAX_TIMESTAMP or TIMESTAMP")
        topic_partition_offsets[topic_partition] = offset_spec
        i = i + 3
        partition_i += 1

    futmap = a.list_offsets(topic_partition_offsets, isolation_level=isolation_level, request_timeout=30)
    for partition, fut in futmap.items():
        try:
            result = fut.result()
            print("Topicname : {} Partition_Index : {} Offset : {} Timestamp : {}"
                  .format(partition.topic, partition.partition, result.offset,
                          result.timestamp))
        except KafkaException as e:
            print("Topicname : {} Partition_Index : {} Error : {}"
                  .format(partition.topic, partition.partition, e))


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
        sys.stderr.write(' incremental_alter_configs <resource_type1> <resource_name1> ' +
                         '<config1=op1:val1;config2=op2:val2;config3=DELETE> ' +
                         '<resource_type2> <resource_name2> <config1=op1:..> ..\n')
        sys.stderr.write(' delta_alter_configs <resource_type1> <resource_name1> ' +
                         '<config=val,config2=val2> <resource_type2> <resource_name2> <config..> ..\n')
        sys.stderr.write(' create_acls <resource_type1> <resource_name1> <resource_patter_type1> ' +
                         '<principal1> <host1> <operation1> <permission_type1> ..\n')
        sys.stderr.write(' describe_acls <resource_type1 <resource_name1> <resource_patter_type1> ' +
                         '<principal1> <host1> <operation1> <permission_type1> ..\n')
        sys.stderr.write(' delete_acls <resource_type1> <resource_name1> <resource_patter_type1> ' +
                         '<principal1> <host1> <operation1> <permission_type1> ..\n')
        sys.stderr.write(' list [<all|topics|brokers|groups>]\n')
        sys.stderr.write(' list_consumer_groups [<state1> <state2> ..]\n')
        sys.stderr.write(' describe_consumer_groups <include_authorized_operations> <group1> <group2> ..\n')
        sys.stderr.write(' describe_topics <include_authorized_operations> <topic1> <topic2> ..\n')
        sys.stderr.write(' describe_cluster <include_authorized_operations>\n')
        sys.stderr.write(' delete_consumer_groups <group1> <group2> ..\n')
        sys.stderr.write(' list_consumer_group_offsets <group> [<topic1> <partition1> <topic2> <partition2> ..]\n')
        sys.stderr.write(
            ' alter_consumer_group_offsets <group> <topic1> <partition1> <offset1> ' +
            '<topic2> <partition2> <offset2> ..\n')
        sys.stderr.write(' describe_user_scram_credentials [<user1> <user2> ..]\n')
        sys.stderr.write(' alter_user_scram_credentials UPSERT <user1> <mechanism1> ' +
                         '<iterations1> <password1> <salt1> ' +
                         '[UPSERT <user2> <mechanism2> <iterations2> ' +
                         ' <password2> <salt2> DELETE <user3> <mechanism3> ..]\n')
        sys.stderr.write(' list_offsets <isolation_level> <topic1> <partition1> <offset_spec1> ' +
                         '[<topic2> <partition2> <offset_spec2> ..]\n')

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
              'incremental_alter_configs': example_incremental_alter_configs,
              'delta_alter_configs': example_delta_alter_configs,
              'create_acls': example_create_acls,
              'describe_acls': example_describe_acls,
              'delete_acls': example_delete_acls,
              'list': example_list,
              'list_consumer_groups': example_list_consumer_groups,
              'describe_consumer_groups': example_describe_consumer_groups,
              'describe_topics': example_describe_topics,
              'describe_cluster': example_describe_cluster,
              'delete_consumer_groups': example_delete_consumer_groups,
              'list_consumer_group_offsets': example_list_consumer_group_offsets,
              'alter_consumer_group_offsets': example_alter_consumer_group_offsets,
              'describe_user_scram_credentials': example_describe_user_scram_credentials,
              'alter_user_scram_credentials': example_alter_user_scram_credentials,
              'list_offsets': example_list_offsets}

    if operation not in opsmap:
        sys.stderr.write('Unknown operation: %s\n' % operation)
        sys.exit(1)

    opsmap[operation](a, args)
