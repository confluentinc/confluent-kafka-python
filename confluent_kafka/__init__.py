__all__ = ['cimpl', 'avro', 'kafkatest']
from .cimpl import (Consumer,  # noqa
                    KafkaError,
                    KafkaException,
                    Message,
                    Producer,
                    TopicPartition,
                    AdminClientImpl,
                    NewTopic,
                    NewPartitions,
                    libversion,
                    version,
                    TIMESTAMP_NOT_AVAILABLE,
                    TIMESTAMP_CREATE_TIME,
                    TIMESTAMP_LOG_APPEND_TIME,
                    OFFSET_BEGINNING,
                    OFFSET_END,
                    OFFSET_STORED,
                    OFFSET_INVALID,
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

__version__ = version()[0]


class ConfigEntry(object):
    """
    ConfigEntry is returned by describe_configs() for each configuration
    entry for the specified resource.

    This class is typically not user instantiated.

    :ivar name str: Configuration property name.
    :ivar value str: Configuration value (or None if not set or is_sensitive==True).
    :ivar source int: Configuration source (see CONFIG_SOURCE_ ..constants and config_source_to_str()).
    :ivar is_read_only bool: Indicates if configuration property is read-only.
    :ivar is_default bool: Indicates if configuration property is using its default value.
    :ivar is_sensitive bool: Indicates if configuration property value
                              contains sensitive information
                              (such as security settings),
                              in which case .value is None.
    :ivar is_synonym bool: Indicates if configuration property is a
                            synonym for the parent configuration entry.
    :ivar synonyms list: A ConfigEntry list of synonyms and alternate sources for this configuration property.


    Source types (.source):
        :py:const:`CONFIG_SOURCE_UNKNOWN_CONFIG`,
        :py:const:`CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG`,
        :py:const:`CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG`,
        :py:const:`CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG`,
        :py:const:`CONFIG_SOURCE_STATIC_BROKER_CONFIG`,
        :py:const:`CONFIG_SOURCE_DEFAULT_CONFIG`

    """

    _source_name_by_type = {
        CONFIG_SOURCE_UNKNOWN_CONFIG: "UNKNOWN_CONFIG",
        CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG: "DYNAMIC_TOPIC_CONFIG",
        CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG: "DYNAMIC_BROKER_CONFIG",
        CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG: "DYNAMIC_DEFAULT_BROKER_CONFIG",
        CONFIG_SOURCE_STATIC_BROKER_CONFIG: "STATIC_BROKER_CONFIG",
        CONFIG_SOURCE_DEFAULT_CONFIG: "DEFAULT_CONFIG"
    }

    def __init__(self, name, value,
                 source=CONFIG_SOURCE_UNKNOWN_CONFIG,
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

    @classmethod
    def config_source_to_str(cls, source):
        """Return string representation of a config source."""
        return ConfigEntry._source_name_by_type.get(source, "%d?" % source)


@functools.total_ordering
class ConfigResource(object):
    """
    Class representing resources that have configs.


    :py:const:`RESOURCE_ANY` (used for lookups),
    :py:const:`RESOURCE_UNKNOWN` (used for reporting),
    :py:const:`RESOURCE_TOPIC`,
    :py:const:`RESOURCE_GROUP`,
    :py:const:`RESOURCE_BROKER`

    Resource names for types:
      * RESOURCE_TOPIC - topic name
      * RESOURCE_BROKER - broker id (as str)
      * RESOURCE_GROUP - group name
    """
    _res_name_by_type = {RESOURCE_UNKNOWN: "unknown",
                         RESOURCE_ANY: "any",
                         RESOURCE_TOPIC: "topic",
                         RESOURCE_GROUP: "group",
                         RESOURCE_BROKER: "broker"}
    _res_type_by_name = {v: k for k, v in _res_name_by_type.items()}

    def __init__(self, restype, name,
                 set_config=None, add_config=None, del_config=None,
                 described_configs=None, error=None):
        """
        :param restype int: Resource type, see the RESOURCE_ constants below.
        :param name str: Resource name, depending on restype.
                          For RESOURCE_BROKER the resource name is the broker id.
        :param set_config dict: Configuration to set/overwrite. Dict of str, str.
        :param add_config dict: Configuration to add/append. Dict of str, str. Requires broker version >=2.0.
        :param del_config list: Configuration to delete/revert to default. List of str. Requires broker version >=2.0.
        :param described_configs dict: For internal use only.
        :param error KafkaError: For internal use only.

        When alter_configs(incremental=False) only set_config is permitted,
        and any configuration parameter not specified will be reverted to
        its default value.

        With alter_configs(incremental=True) (requires broker version >=2.0),
        only the configuration parameters specified through set, add or del
        will be modified.
        """
        super(ConfigResource, self).__init__()

        if name is None:
            raise ValueError("Expected resource name to be a string")

        if type(restype) == str:
            restype_int = self._res_type_by_name.get(restype.lower(), None)
            if restype_int is None:
                raise ValueError("Unknown resource type \"%s\": should be a RESOURCE_.. constant or one of %s" %
                                 (restype, ",".join(self._res_type_by_name.keys())))
            restype = restype_int

        self.restype = restype
        self.name = name

        if set_config is not None:
            self.set_config_dict = set_config.copy()
        else:
            self.set_config_dict = dict()

        if add_config is not None:
            self.add_config_dict = add_config.copy()
        else:
            self.add_config_dict = dict()

        if del_config is not None:
            self.del_config_dict = dict((k, None) for k in del_config)
        else:
            self.del_config_dict = dict()

        self.configs = described_configs
        self.error = error

    def __repr__(self):
        if self.error is not None:
            return "ConfigResource(%s,%s,%r)" % \
                (self._res_name_by_type.get(self.restype, "%d" % self.restype), self.name, self.error)
        else:
            return "ConfigResource(%s,%s)" % \
                (self._res_name_by_type.get(self.restype, "%d" % self.restype), self.name)

    def __hash__(self):
        return hash((self.restype, self.name))

    def __lt__(self, other):
        if self.restype < other.restype:
            return True
        return self.name.__lt__(other.name)

    def __eq__(self, other):
        return self.restype == other.restype and self.name == other.name

    def set_config(self, name, value):
        """
        Set/Overwrite configuration entry

        Unless alter_configs(.., incremental=True) any configuration properties
        that are not included will be reverted to their default values.
        As a workaround use describe_configs() to retrieve the current
        configuration.

        :param name str: Configuration property name
        :param value str: Configuration value
        """
        self.set_config_dict[name] = value

    def add_config(self, name, value):
        """
        Append value to configuration entry.

        Requires broker version >=2.0.0 and alter_configs(.., incremental=True).

        :param name str: Configuration property name
        :param value str: Configuration value
        """
        self.add_config_dict[name] = value

    def del_config(self, name):
        """
        Delete configuration entry, reverting it to the default value.

        Requires broker version >=2.0.0 and alter_configs(.., incremental=True).

        :param name str: Configuration property name
        """
        self.del_config_dict[name] = None


class AdminClient (AdminClientImpl):
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
            for topic, error in result.iteritems():
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
            for topic, fut in futmap.iteritems():
                fut.set_exception(e)

    @staticmethod
    def _make_resource_result(f, futmap):
        """
        Map per-resource results to per-resource futures in futmap.
        The result value of each (successful) future is a ConfigResource.
        """
        try:
            result = f.result()
            for resource, configs in result.iteritems():
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
            for resource, fut in futmap.iteritems():
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
                raise TypeError("Expected list of {}".format(type(class_check)))
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

        :param new_topics list(NewTopic): New topics to be created.
        :param operation_timeout float: Set broker's operation timeout in seconds,
                  controlling how long the CreateTopics request will block
                  on the broker waiting for the topic creation to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param request_timeout float: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param validate_only bool: Tell broker to only validate the request,
                  without creating the topic. Default: False

        :returns: a dict of futures for each topic, keyed by the topic name.
        :rtype dict(<topic_name, future>):

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

        :param topics list(str): Topics to mark for deletion.
        :param operation_timeout float: Set broker's operation timeout in seconds,
                  controlling how long the DeleteTopics request will block
                  on the broker waiting for the topic deletion to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param request_timeout float: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`

        :returns: a dict of futures for each topic, keyed by the topic name.
        :rtype dict(<topic_name, future>):

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

        :param new_partitions list(NewPartitions): New partitions to be created.
        :param operation_timeout float: Set broker's operation timeout in seconds,
                  controlling how long the CreatePartitions request will block
                  on the broker waiting for the partition creation to propagate
                  in the cluster. A value of 0 returns immediately. Default: 0
        :param request_timeout float: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param validate_only bool: Tell broker to only validate the request,
                  without creating the partitions. Default: False

        :returns: a dict of futures for each topic, keyed by the topic name.
        :rtype dict(<topic_name, future>):

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

        :param resources list(ConfigResource): Resources to get configuration for.
        :param request_timeout float: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param validate_only bool: Tell broker to only validate the request,
                  without creating the partitions. Default: False

        :returns: a dict of futures for each resource, keyed by the ConfigResource.
        :rtype dict(<ConfigResource, future>):

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
                  Use incremental=True to change the behaviour so that only the
                  passed configuration is modified.
                  Requires broker version >=2.0.

        :warning: Multiple resources and resource types may be specified,
                  but at most one resource of type RESOURCE_BROKER is allowed
                  per call since these resource requests must be sent to the
                  broker specified in the resource.

        :param resources list(ConfigResource): Resources to update configuration for.
        :param request_timeout float: Set the overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param validate_only bool: Tell broker to only validate the request,
                  without altering the configuration. Default: False
        :param incremental bool: If true, only update the specified configuration
                  entries, not reverting unspecified configuration.
                  This requires broker version >=2.0. Default: False

        :returns: a dict of futures for each resource, keyed by the ConfigResource.
        :rtype dict(<ConfigResource, future>):

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

    :ivar cluster_id: Cluster id string, if supported by broker, else None.
    :ivar controller_id: Current controller broker id, or -1.
    :ivar brokers: Map of brokers indexed by the int broker id. Value is BrokerMetadata object.
    :ivar topics: Map of topics indexed by the topic name. Value is TopicMetadata object.
    :ivar orig_broker_id: The broker this metadata originated from.
    :ivar orig_broker_name: Broker name/address this metadata originated from.

    This class is typically not user instantiated.
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

    :ivar id: Broker id.
    :ivar host: Broker hostname.
    :ivar port: Broker port.

    This class is typically not user instantiated.
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

    :ivar topic: Topic name.
    :ivar partitions: Map of partitions indexed by partition id. Value is PartitionMetadata object.
    :ivar error: Topic error, or None. Value is a KafkaError object.

    This class is typically not user instantiated.
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

    :ivar id: Partition id.
    :ivar leader: Current leader broker for this partition, or -1.
    :ivar replicas: List of replica broker ids for this partition.
    :ivar isrs: List of in-sync-replica broker ids for this partition.
    :ivar error: Partition error, or None. Value is a KafkaError object.

    :warning: Depending on cluster state the broker ids referenced in
              leader, replicas and isrs may temporarily not be reported
              in ClusterMetadata.brokers. Always check the availability
              of a broker id in the brokers dict.

    This class is typically not user instantiated.
    """
    def __init__(self):
        self.partition = -1
        self.leader = -1
        self.replicas = []
        self.isrs = []
        self.error = None

    def __repr__(self):
        if self.error is not None:
            return "PartitionMetadata({}, {})".format(self.partition, self.error)
        else:
            return "PartitionMetadata({})".format(self.partition)

    def __str__(self):
        return "{}".format(self.partition)
