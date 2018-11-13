"""
Kafka Admin client: create, view, alter, delete topics and resources.
"""
from ..cimpl import (KafkaException, # noqa
                     _AdminClientImpl,
                     NewTopic,
                     NewPartitions,
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

import concurrent.futures
import functools

from enum import Enum


class ConfigSource(Enum):
    """
    Config sources returned in ConfigEntry by `describe_configs()`.
    """
    UNKNOWN_CONFIG = CONFIG_SOURCE_UNKNOWN_CONFIG  #:
    DYNAMIC_TOPIC_CONFIG = CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG  #:
    DYNAMIC_BROKER_CONFIG = CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG  #:
    DYNAMIC_DEFAULT_BROKER_CONFIG = CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG  #:
    STATIC_BROKER_CONFIG = CONFIG_SOURCE_STATIC_BROKER_CONFIG  #:
    DEFAULT_CONFIG = CONFIG_SOURCE_DEFAULT_CONFIG  #:


class ConfigEntry(object):
    """
    ConfigEntry is returned by describe_configs() for each configuration
    entry for the specified resource.

    This class is typically not user instantiated.

    :ivar str name: Configuration property name.
    :ivar str value: Configuration value (or None if not set or is_sensitive==True).
    :ivar ConfigSource source: Configuration source.
    :ivar bool is_read_only: Indicates if configuration property is read-only.
    :ivar bool is_default: Indicates if configuration property is using its default value.
    :ivar bool is_sensitive: Indicates if configuration property value
                              contains sensitive information
                              (such as security settings),
                              in which case .value is None.
    :ivar bool is_synonym: Indicates if configuration property is a
                            synonym for the parent configuration entry.
    :ivar list synonyms: A ConfigEntry list of synonyms and alternate sources for this configuration property.

    """

    def __init__(self, name, value,
                 source=ConfigSource.UNKNOWN_CONFIG,
                 is_read_only=False,
                 is_default=False,
                 is_sensitive=False,
                 is_synonym=False,
                 synonyms=[]):
        """
        This class is typically not user instantiated.
        """
        super(ConfigEntry, self).__init__()
        self.name = name
        self.value = value
        self.source = source
        self.is_read_only = bool(is_read_only)
        self.is_default = bool(is_default)
        self.is_sensitive = bool(is_sensitive)
        self.is_synonym = bool(is_synonym)
        self.synonyms = synonyms

    def __repr__(self):
        return "ConfigEntry(%s=\"%s\")" % (self.name, self.value)

    def __str__(self):
        return "%s=\"%s\"" % (self.name, self.value)


@functools.total_ordering
class ConfigResource(object):
    """
    Class representing resources that have configs.

    Instantiate with a resource type and a resource name.
    """

    class Type(Enum):
        """
        ConfigResource.Type depicts the type of a Kafka resource.
        """
        UNKNOWN = RESOURCE_UNKNOWN  #: Resource type is not known or not set.
        ANY = RESOURCE_ANY  #: Match any resource, used for lookups.
        TOPIC = RESOURCE_TOPIC  #: Topic resource. Resource name is topic name
        GROUP = RESOURCE_GROUP  #: Group resource. Resource name is group.id
        BROKER = RESOURCE_BROKER  #: Broker resource. Resource name is broker id

    def __init__(self, restype, name,
                 set_config=None, described_configs=None, error=None):
        """
        :param ConfigResource.Type restype: Resource type.
        :param str name: Resource name, depending on restype.
                          For RESOURCE_BROKER the resource name is the broker id.
        :param dict set_config: Configuration to set/overwrite. Dict of str, str.
        :param dict described_configs: For internal use only.
        :param KafkaError error: For internal use only.
        """
        super(ConfigResource, self).__init__()

        if name is None:
            raise ValueError("Expected resource name to be a string")

        if type(restype) == str:
            # Allow resource type to be specified as case-insensitive string, for convenience.
            try:
                restype = ConfigResource.Type[restype.upper()]
            except KeyError:
                raise ValueError("Unknown resource type \"%s\": should be a ConfigResource.Type" % restype)

        elif type(restype) == int:
            # The C-code passes restype as an int, convert to Type.
            restype = ConfigResource.Type(restype)

        self.restype = restype
        self.restype_int = int(self.restype.value)  # for the C code
        self.name = name

        if set_config is not None:
            self.set_config_dict = set_config.copy()
        else:
            self.set_config_dict = dict()

        self.configs = described_configs
        self.error = error

    def __repr__(self):
        if self.error is not None:
            return "ConfigResource(%s,%s,%r)" % (self.restype, self.name, self.error)
        else:
            return "ConfigResource(%s,%s)" % (self.restype, self.name)

    def __hash__(self):
        return hash((self.restype, self.name))

    def __lt__(self, other):
        if self.restype < other.restype:
            return True
        return self.name.__lt__(other.name)

    def __eq__(self, other):
        return self.restype == other.restype and self.name == other.name

    def __len__(self):
        """
        :rtype: int
        :returns: number of configuration entries/operations
        """
        return len(self.set_config_dict)

    def set_config(self, name, value, overwrite=True):
        """
        Set/Overwrite configuration entry

        Any configuration properties that are not included will be reverted to their default values.
        As a workaround use describe_configs() to retrieve the current configuration and
        overwrite the settings you want to change.

        :param str name: Configuration property name
        :param str value: Configuration value
        :param bool overwrite: If True overwrite entry if already exists (default).
                               If False do nothing if entry already exists.
        """
        if not overwrite and name in self.set_config_dict:
            return
        self.set_config_dict[name] = value


class AdminClient (_AdminClientImpl):
    """
    The Kafka AdminClient provides admin operations for Kafka brokers,
    topics, groups, and other resource types supported by the broker.

    The Admin API methods are asynchronous and returns a dict of
    concurrent.futures.Future objects keyed by the entity.
    The entity is a topic name for create_topics(), delete_topics(), create_partitions(),
    and a ConfigResource for alter_configs(), describe_configs().

    All the futures for a single API call will currently finish/fail at
    the same time (backed by the same protocol request), but this might
    change in future versions of the client.

    See examples/adminapi.py for example usage.

    For more information see the Java Admin API documentation:
    https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/clients/admin/package-frame.html

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
    def _make_futures(futmap_keys, class_check, make_result_fn):
        """
        Create futures and a futuremap for the keys in futmap_keys,
        and create a request-level future to be bassed to the C API.
        """
        futmap = {}
        for key in futmap_keys:
            if class_check is not None and not isinstance(key, class_check):
                raise ValueError("Expected list of {}".format(type(class_check)))
            futmap[key] = concurrent.futures.Future()
            if not futmap[key].set_running_or_notify_cancel():
                raise RuntimeError("Future was cancelled prematurely")

        # Create an internal future for the entire request,
        # this future will trigger _make_..._result() and set result/exception
        # per topic,future in futmap.
        f = concurrent.futures.Future()
        f.add_done_callback(lambda f: make_result_fn(f, futmap))

        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")

        return f, futmap

    def create_topics(self, new_topics, **kwargs):
        """
        Create new topics in cluster.

        The future result() value is None.

        :param list(NewTopic) new_topics: New topics to be created.
        :param float operation_timeout: Set broker's operation timeout in seconds,
                  controlling how long the CreateTopics request will block
                  on the broker waiting for the topic creation to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param float request_timeout: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param bool validate_only: Tell broker to only validate the request,
                  without creating the topic. Default: False

        :returns: a dict of futures for each topic, keyed by the topic name.
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
        Delete topics.

        The future result() value is None.

        :param list(str) topics: Topics to mark for deletion.
        :param float operation_timeout: Set broker's operation timeout in seconds,
                  controlling how long the DeleteTopics request will block
                  on the broker waiting for the topic deletion to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param float request_timeout: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: a dict of futures for each topic, keyed by the topic name.
        :rtype: dict(<topic_name, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures(topics, None,
                                              AdminClient._make_topics_result)

        super(AdminClient, self).delete_topics(topics, f, **kwargs)

        return futmap

    def create_partitions(self, new_partitions, **kwargs):
        """
        Create additional partitions for the given topics.

        The future result() value is None.

        :param list(NewPartitions) new_partitions: New partitions to be created.
        :param float operation_timeout: Set broker's operation timeout in seconds,
                  controlling how long the CreatePartitions request will block
                  on the broker waiting for the partition creation to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param float request_timeout: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param bool validate_only: Tell broker to only validate the request,
                  without creating the partitions. Default: False

        :returns: a dict of futures for each topic, keyed by the topic name.
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
        Get configuration for the specified resources.

        The future result() value is a dict(<configname, ConfigEntry>).

        :warning: Multiple resources and resource types may be requested,
                  but at most one resource of type RESOURCE_BROKER is allowed
                  per call since these resource requests must be sent to the
                  broker specified in the resource.

        :param list(ConfigResource) resources: Resources to get configuration for.
        :param float request_timeout: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param bool validate_only: Tell broker to only validate the request,
                  without creating the partitions. Default: False

        :returns: a dict of futures for each resource, keyed by the ConfigResource.
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
        Update configuration values for the specified resources.
        Updates are not transactional so they may succeed for a subset
        of the provided resources while the others fail.
        The configuration for a particular resource is updated atomically,
        replacing the specified values while reverting unspecified configuration
        entries to their default values.

        The future result() value is None.

        :warning: alter_configs() will replace all existing configuration for
                  the provided resources with the new configuration given,
                  reverting all other configuration for the resource back
                  to their default values.

        :warning: Multiple resources and resource types may be specified,
                  but at most one resource of type RESOURCE_BROKER is allowed
                  per call since these resource requests must be sent to the
                  broker specified in the resource.

        :param list(ConfigResource) resources: Resources to update configuration for.
        :param float request_timeout: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`.
        :param bool validate_only: Tell broker to only validate the request,
                  without altering the configuration. Default: False

        :returns: a dict of futures for each resource, keyed by the ConfigResource.
        :rtype: dict(<ConfigResource, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures(resources, ConfigResource,
                                              AdminClient._make_resource_result)

        super(AdminClient, self).alter_configs(resources, f, **kwargs)

        return futmap


class ClusterMetadata (object):
    """
    ClusterMetadata as returned by list_topics() contains information
    about the Kafka cluster, brokers, and topics.

    This class is typically not user instantiated.

    :ivar str cluster_id: Cluster id string, if supported by broker, else None.
    :ivar id controller_id: Current controller broker id, or -1.
    :ivar dict brokers: Map of brokers indexed by the int broker id. Value is BrokerMetadata object.
    :ivar dict topics: Map of topics indexed by the topic name. Value is TopicMetadata object.
    :ivar int orig_broker_id: The broker this metadata originated from.
    :ivar str orig_broker_name: Broker name/address this metadata originated from.
    """
    def __init__(self):
        self.cluster_id = None
        self.controller_id = -1
        self.brokers = {}
        self.topics = {}
        self.orig_broker_id = -1
        self.orig_broker_name = None

    def __repr__(self):
        return "ClusterMetadata({})".format(self.cluster_id)

    def __str__(self):
        return str(self.cluster_id)


class BrokerMetadata (object):
    """
    BrokerMetadata contains information about a Kafka broker.

    This class is typically not user instantiated.

    :ivar int id: Broker id.
    :ivar str host: Broker hostname.
    :ivar int port: Broker port.
    """
    def __init__(self):
        self.id = -1
        self.host = None
        self.port = -1

    def __repr__(self):
        return "BrokerMetadata({}, {}:{})".format(self.id, self.host, self.port)

    def __str__(self):
        return "{}:{}/{}".format(self.host, self.port, self.id)


class TopicMetadata (object):
    """
    TopicMetadata contains information about a Kafka topic.

    This class is typically not user instantiated.

    :ivar str topic: Topic name.
    :ivar dict partitions: Map of partitions indexed by partition id. Value is PartitionMetadata object.
    :ivar KafkaError error: Topic error, or None. Value is a KafkaError object.
    """
    def __init__(self):
        self.topic = None
        self.partitions = {}
        self.error = None

    def __repr__(self):
        if self.error is not None:
            return "TopicMetadata({}, {} partitions, {})".format(self.topic, len(self.partitions), self.error)
        else:
            return "TopicMetadata({}, {} partitions)".format(self.topic, len(self.partitions))

    def __str__(self):
        return self.topic


class PartitionMetadata (object):
    """
    PartitionsMetadata contains information about a Kafka partition.

    This class is typically not user instantiated.

    :ivar int id: Partition id.
    :ivar int leader: Current leader broker for this partition, or -1.
    :ivar list(int) replicas: List of replica broker ids for this partition.
    :ivar list(int) isrs: List of in-sync-replica broker ids for this partition.
    :ivar KafkaError error: Partition error, or None. Value is a KafkaError object.

    :warning: Depending on cluster state the broker ids referenced in
              leader, replicas and isrs may temporarily not be reported
              in ClusterMetadata.brokers. Always check the availability
              of a broker id in the brokers dict.
    """
    def __init__(self):
        self.id = -1
        self.leader = -1
        self.replicas = []
        self.isrs = []
        self.error = None

    def __repr__(self):
        if self.error is not None:
            return "PartitionMetadata({}, {})".format(self.id, self.error)
        else:
            return "PartitionMetadata({})".format(self.id)

    def __str__(self):
        return "{}".format(self.id)
