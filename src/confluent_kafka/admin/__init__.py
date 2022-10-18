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

"""
Kafka admin client: create, view, alter, and delete topics and resources.
"""
import concurrent.futures

# Unused imports are keeped to be accessible using this public module
from ._config import (ConfigSource,  # noqa: F401
                      ConfigEntry,
                      ConfigResource)
from ._resource import (ResourceType,  # noqa: F401
                        ResourcePatternType)
from ._acl import (AclOperation,  # noqa: F401
                   AclPermissionType,
                   AclBinding,
                   AclBindingFilter)
from ._offset import (ConsumerGroupTopicPartitions,  # noqa: F401
                      ListConsumerGroupOffsetsRequest,
                      ListConsumerGroupOffsetsResponse,
                      AlterConsumerGroupOffsetsRequest,
                      AlterConsumerGroupOffsetsResponse)
from ._metadata import (BrokerMetadata,  # noqa: F401
                        ClusterMetadata,
                        GroupMember,
                        GroupMetadata,
                        PartitionMetadata,
                        TopicMetadata)
from ._group import (DeleteConsumerGroupsResponse) #noqa: F401
from ..cimpl import (KafkaException,  # noqa: F401
                     KafkaError,
                     _AdminClientImpl,
                     NewTopic,
                     NewPartitions,
                     TopicPartition,
                     CONFIG_SOURCE_UNKNOWN_CONFIG,
                     CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG,
                     CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG,
                     CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG,
                     CONFIG_SOURCE_STATIC_BROKER_CONFIG,
                     CONFIG_SOURCE_DEFAULT_CONFIG,
                     RESOURCE_UNKNOWN,
                     RESOURCE_ANY,
                     RESOURCE_TOPIC,
                     RESOURCE_GROUP,
                     RESOURCE_BROKER)

try:
    string_type = basestring
except NameError:
    string_type = str

class AdminClient (_AdminClientImpl):
    """
    AdminClient provides admin operations for Kafka brokers, topics, groups,
    and other resource types supported by the broker.

    The Admin API methods are asynchronous and return a dict of
    concurrent.futures.Future objects keyed by the entity.
    The entity is a topic name for create_topics(), delete_topics(), create_partitions(),
    and a ConfigResource for alter_configs() and describe_configs().

    All the futures for a single API call will currently finish/fail at
    the same time (backed by the same protocol request), but this might
    change in future versions of the client.

    See examples/adminapi.py for example usage.

    For more information see the `Java Admin API documentation
    <https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/clients/admin/package-frame.html>`_.

    Requires broker version v0.11.0.0 or later.
    """

    def __init__(self, conf):
        """
        Create a new AdminClient using the provided configuration dictionary.

        The AdminClient is a standard Kafka protocol client, supporting
        the standard librdkafka configuration properties as specified at
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

        At least 'bootstrap.servers' should be configured.
        """
        super(AdminClient, self).__init__(conf)

    @staticmethod
    def _make_topics_result(f, futmap):
        """
        Map per-topic results to per-topic futures in futmap.
        The result value of each (successful) future is None.
        """
        try:
            result = f.result()
            for topic, error in result.items():
                fut = futmap.get(topic, None)
                if fut is None:
                    raise RuntimeError("Topic {} not found in future-map: {}".format(topic, futmap))

                if error is not None:
                    # Topic-level exception
                    fut.set_exception(KafkaException(error))
                else:
                    # Topic-level success
                    fut.set_result(None)
        except Exception as e:
            # Request-level exception, raise the same for all topics
            for topic, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_resource_result(f, futmap):
        """
        Map per-resource results to per-resource futures in futmap.
        The result value of each (successful) future is a ConfigResource.
        """
        try:
            result = f.result()
            for resource, configs in result.items():
                fut = futmap.get(resource, None)
                if fut is None:
                    raise RuntimeError("Resource {} not found in future-map: {}".format(resource, futmap))
                if resource.error is not None:
                    # Resource-level exception
                    fut.set_exception(KafkaException(resource.error))
                else:
                    # Resource-level success
                    # configs will be a dict for describe_configs()
                    # and None for alter_configs()
                    fut.set_result(configs)
        except Exception as e:
            # Request-level exception, raise the same for all resources
            for resource, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_consumer_group_offsets_result(f, futmap):
        # Improve this doc
        """
        Map per-group results to per-group futures in futmap.
        The result value of each (successful) future is None.
        """
        try:

            results = f.result()
            futmap_values = list(futmap.values())
            len_results = len(results)
            len_futures = len(futmap_values)
            if len_results != len_futures:
                raise RuntimeError(
                    "Results length {} is different from future-map length {}".format(len_results, len_futures))
            for i, result in enumerate(results):
                fut = futmap_values[i]
                if isinstance(result, KafkaError):
                    fut.set_exception(KafkaException(result))
                else:
                    fut.set_result(result)
        except Exception as e:
            # Request-level exception, raise the same for all groups
            for topic, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_acls_result(f, futmap):
        """
        Map create ACL binding results to corresponding futures in futmap.
        For create_acls the result value of each (successful) future is None.
        For delete_acls the result value of each (successful) future is the list of deleted AclBindings.
        """
        try:
            results = f.result()
            futmap_values = list(futmap.values())
            len_results = len(results)
            len_futures = len(futmap_values)
            if len_results != len_futures:
                raise RuntimeError(
                    "Results length {} is different from future-map length {}".format(len_results, len_futures))
            for i, result in enumerate(results):
                fut = futmap_values[i]
                if isinstance(result, KafkaError):
                    fut.set_exception(KafkaException(result))
                else:
                    fut.set_result(result)
        except Exception as e:
            # Request-level exception, raise the same for all the AclBindings or AclBindingFilters
            for resource, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _create_future():
        f = concurrent.futures.Future()
        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")
        return f

    @staticmethod
    def _make_futures(futmap_keys, class_check, make_result_fn):
        """
        Create futures and a futuremap for the keys in futmap_keys,
        and create a request-level future to be bassed to the C API.
        """
        futmap = {}
        for key in futmap_keys:
            if class_check is not None and not isinstance(key, class_check):
                raise ValueError("Expected list of {}".format(repr(class_check)))
            futmap[key] = AdminClient._create_future()

        # Create an internal future for the entire request,
        # this future will trigger _make_..._result() and set result/exception
        # per topic,future in futmap.
        f = AdminClient._create_future()
        f.add_done_callback(lambda f: make_result_fn(f, futmap))

        return f, futmap

    @staticmethod
    def _has_duplicates(items):
        return len(set(items)) != len(items)

    def create_topics(self, new_topics, **kwargs):
        """
        Create one or more new topics.

        :param list(NewTopic) new_topics: A list of specifictions (NewTopic) for
                  the topics that should be created.
        :param float operation_timeout: The operation timeout in seconds,
                  controlling how long the CreateTopics request will block
                  on the broker waiting for the topic creation to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param bool validate_only: If true, the request is only validated
                  without creating the topic. Default: False

        :returns: A dict of futures for each topic, keyed by the topic name.
                  The future result() method returns None.

        :rtype: dict(<topic_name, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures([x.topic for x in new_topics],
                                              None,
                                              AdminClient._make_topics_result)

        super(AdminClient, self).create_topics(new_topics, f, **kwargs)

        return futmap

    def delete_topics(self, topics, **kwargs):
        """
        Delete one or more topics.

        :param list(str) topics: A list of topics to mark for deletion.
        :param float operation_timeout: The operation timeout in seconds,
                  controlling how long the DeleteTopics request will block
                  on the broker waiting for the topic deletion to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: A dict of futures for each topic, keyed by the topic name.
                  The future result() method returns None.

        :rtype: dict(<topic_name, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures(topics, None,
                                              AdminClient._make_topics_result)

        super(AdminClient, self).delete_topics(topics, f, **kwargs)

        return futmap

    def list_topics(self, *args, **kwargs):

        return super(AdminClient, self).list_topics(*args, **kwargs)

    def list_groups(self, *args, **kwargs):

        return super(AdminClient, self).list_groups(*args, **kwargs)

    def create_partitions(self, new_partitions, **kwargs):
        """
        Create additional partitions for the given topics.

        :param list(NewPartitions) new_partitions: New partitions to be created.
        :param float operation_timeout: The operation timeout in seconds,
                  controlling how long the CreatePartitions request will block
                  on the broker waiting for the partition creation to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param bool validate_only: If true, the request is only validated
                  without creating the partitions. Default: False

        :returns: A dict of futures for each topic, keyed by the topic name.
                  The future result() method returns None.

        :rtype: dict(<topic_name, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures([x.topic for x in new_partitions],
                                              None,
                                              AdminClient._make_topics_result)

        super(AdminClient, self).create_partitions(new_partitions, f, **kwargs)

        return futmap

    def describe_configs(self, resources, **kwargs):
        """
        Get the configuration of the specified resources.

        :warning: Multiple resources and resource types may be requested,
                  but at most one resource of type RESOURCE_BROKER is allowed
                  per call since these resource requests must be sent to the
                  broker specified in the resource.

        :param list(ConfigResource) resources: Resources to get the configuration for.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: A dict of futures for each resource, keyed by the ConfigResource.
                  The type of the value returned by the future result() method is
                  dict(<configname, ConfigEntry>).

        :rtype: dict(<ConfigResource, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures(resources, ConfigResource,
                                              AdminClient._make_resource_result)

        super(AdminClient, self).describe_configs(resources, f, **kwargs)

        return futmap

    def alter_configs(self, resources, **kwargs):
        """
        Update configuration properties for the specified resources.
        Updates are not transactional so they may succeed for a subset
        of the provided resources while the others fail.
        The configuration for a particular resource is updated atomically,
        replacing the specified values while reverting unspecified configuration
        entries to their default values.

        :warning: alter_configs() will replace all existing configuration for
                  the provided resources with the new configuration given,
                  reverting all other configuration for the resource back
                  to their default values.

        :warning: Multiple resources and resource types may be specified,
                  but at most one resource of type RESOURCE_BROKER is allowed
                  per call since these resource requests must be sent to the
                  broker specified in the resource.

        :param list(ConfigResource) resources: Resources to update configuration of.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`.
        :param bool validate_only: If true, the request is validated only,
                  without altering the configuration. Default: False

        :returns: A dict of futures for each resource, keyed by the ConfigResource.
                  The future result() method returns None.

        :rtype: dict(<ConfigResource, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures(resources, ConfigResource,
                                              AdminClient._make_resource_result)

        super(AdminClient, self).alter_configs(resources, f, **kwargs)

        return futmap

    def create_acls(self, acls, **kwargs):
        """
        Create one or more ACL bindings.

        :param list(AclBinding) acls: A list of unique ACL binding specifications (:class:`.AclBinding`)
                         to create.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: A dict of futures for each ACL binding, keyed by the :class:`AclBinding` object.
                  The future result() method returns None on success.

        :rtype: dict[AclBinding, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """
        if AdminClient._has_duplicates(acls):
            raise ValueError("duplicate ACL bindings not allowed")

        f, futmap = AdminClient._make_futures(acls, AclBinding,
                                              AdminClient._make_acls_result)

        super(AdminClient, self).create_acls(acls, f, **kwargs)

        return futmap

    def describe_acls(self, acl_binding_filter, **kwargs):
        """
        Match ACL bindings by filter.

        :param AclBindingFilter acl_binding_filter: a filter with attributes that
                  must match.
                  String attributes match exact values or any string if set to None.
                  Enums attributes match exact values or any value if equal to `ANY`.
                  If :class:`ResourcePatternType` is set to :attr:`ResourcePatternType.MATCH`
                  returns ACL bindings with:
                  :attr:`ResourcePatternType.LITERAL` pattern type with resource name equal
                  to the given resource name;
                  :attr:`ResourcePatternType.LITERAL` pattern type with wildcard resource name
                  that matches the given resource name;
                  :attr:`ResourcePatternType.PREFIXED` pattern type with resource name
                  that is a prefix of the given resource name
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: A future returning a list(:class:`AclBinding`) as result

        :rtype: future

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f = AdminClient._create_future()

        super(AdminClient, self).describe_acls(acl_binding_filter, f, **kwargs)

        return f

    def delete_acls(self, acl_binding_filters, **kwargs):
        """
        Delete ACL bindings matching one or more ACL binding filters.

        :param list(AclBindingFilter) acl_binding_filters: a list of unique ACL binding filters
                  to match ACLs to delete.
                  String attributes match exact values or any string if set to None.
                  Enums attributes match exact values or any value if equal to `ANY`.
                  If :class:`ResourcePatternType` is set to :attr:`ResourcePatternType.MATCH`
                  deletes ACL bindings with:
                  :attr:`ResourcePatternType.LITERAL` pattern type with resource name
                  equal to the given resource name;
                  :attr:`ResourcePatternType.LITERAL` pattern type with wildcard resource name
                  that matches the given resource name;
                  :attr:`ResourcePatternType.PREFIXED` pattern type with resource name
                  that is a prefix of the given resource name
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: A dict of futures for each ACL binding filter, keyed by the :class:`AclBindingFilter` object.
                  The future result() method returns a list of :class:`AclBinding`.

        :rtype: dict[AclBindingFilter, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """
        if AdminClient._has_duplicates(acl_binding_filters):
            raise ValueError("duplicate ACL binding filters not allowed")

        f, futmap = AdminClient._make_futures(acl_binding_filters, AclBindingFilter,
                                              AdminClient._make_acls_result)

        super(AdminClient, self).delete_acls(acl_binding_filters, f, **kwargs)

        return futmap

    def list_consumer_group_offsets(self, list_consumer_group_offsets_request, **kwargs):
        """
        List offset information for the consumer group and (optional) topic partition provided in the request.

        :note: Currently, the API supports only a single group.

        :param list(ListConsumerGroupOffsetsRequest) list_consumer_group_offsets_request: List of
                    :class:`ListConsumerGroupOffsetsRequest` which consist of group name and topic
                    partition information for which offset detail is expected. If only group name is
                    provided, then offset information of all the topic and partition associated with
                    that group is returned.
        :param bool require_stable: If True, fetches stable offsets. Default - False
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: A dict of futures for each group, keyed by the :class:`ListConsumerGroupOffsetsRequest` object.
                  The future result() method returns a list of :class:`ListConsumerGroupOffsetsResponse`.

        :rtype: dict[ListConsumerGroupOffsetsRequest, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """
        if not isinstance(list_consumer_group_offsets_request, list):
            raise TypeError("Expected input to be list of ListConsumerGroupOffsetsRequest")

        if len(list_consumer_group_offsets_request) == 0:
            raise ValueError("Expected atleast one ListConsumerGroupOffsetsRequest request")

        if len(list_consumer_group_offsets_request) > 1:
            raise ValueError("Currently we support only 1 ListConsumerGroupOffsetsRequest request")

        f, futmap = AdminClient._make_futures(list_consumer_group_offsets_request, ListConsumerGroupOffsetsRequest,
                                              AdminClient._make_consumer_group_offsets_result)

        super(AdminClient, self).list_consumer_group_offsets(list_consumer_group_offsets_request, f, **kwargs)

        return futmap

    def alter_consumer_group_offsets(self, alter_consumer_group_offsets_request, **kwargs):
        """
        Alter offset for the consumer group and topic partition provided in the request.

        :note: Currently, the API supports only a single group.

        :param list(AlterConsumerGroupOffsetsRequest) alter_consumer_group_offsets_request: List of
                    :class:`AlterConsumerGroupOffsetsRequest` which consist of group name and topic
                    partition; and corresponding offset to be updated.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: A dict of futures for each group, keyed by the :class:`AlterConsumerGroupOffsetsRequest` object.
                  The future result() method returns a list of :class:`AlterConsumerGroupOffsetsResponse`.

        :rtype: dict[AlterConsumerGroupOffsetsRequest, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """
        if not isinstance(alter_consumer_group_offsets_request, list):
            raise TypeError("Expected input to be list of AlterConsumerGroupOffsetsRequest")

        if len(alter_consumer_group_offsets_request) == 0:
            raise ValueError("Expected atleast one AlterConsumerGroupOffsetsRequest request")

        if len(alter_consumer_group_offsets_request) > 1:
            raise ValueError("Currently we support only 1 AlterConsumerGroupOffsetsRequest request")

        f, futmap = AdminClient._make_futures(alter_consumer_group_offsets_request, AlterConsumerGroupOffsetsRequest,
                                              AdminClient._make_consumer_group_offsets_result)

        super(AdminClient, self).alter_consumer_group_offsets(alter_consumer_group_offsets_request, f, **kwargs)

        return futmap

    def delete_consumer_groups(self, group_ids, **kwargs):
        """
        TODO: Add docs
        """
        if not isinstance(group_ids, list):
            raise TypeError("Expected input to be list of group ids")

        if len(group_ids) == 0:
            raise ValueError("Expected atleast one group id to be deleted in the group ids list")

        if AdminClient._has_duplicates(group_ids):
            raise ValueError("duplicate group ids not allowed")

        f, futmap = AdminClient._make_futures(group_ids, string_type, AdminClient._make_consumer_group_offsets_result)

        super(AdminClient, self).delete_consumer_groups(group_ids, f, **kwargs)

        return futmap
