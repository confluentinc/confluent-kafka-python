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

__version__ = version()[0]


class ConfigEntry(object):
    """
        :py:const:`CONFIG_SOURCE_UNKNOWN_CONFIG`,
        :py:const:`CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG`,
        :py:const:`CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG`,
        :py:const:`CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER`,
        :py:const:`CONFIG_SOURCE_STATIC_BROKER_CONFIG`,
        :py:const:`CONFIG_SOURCE_DEFAULT_CONFIG`

    """

    source_name_by_type = {
        CONFIG_SOURCE_UNKNOWN_CONFIG: 'UNKNOWN_CONFIG',
        CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG: 'DYNAMIC_TOPIC_CONFIG',
        CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG: 'DYNAMIC_BROKER_CONFIG',
        CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG: 'DYNAMIC_DEFAULT_BROKER_CONFIG',
        CONFIG_SOURCE_STATIC_BROKER_CONFIG: 'STATIC_BROKER_CONFIG',
        CONFIG_SOURCE_DEFAULT_CONFIG: 'DEFAULT_CONFIG'
    }

    def __init__(self, name, value,
                 source=CONFIG_SOURCE_UNKNOWN_CONFIG,
                 is_read_only=False,
                 is_default=False,
                 is_sensitive=False,
                 is_synonym=False,
                 synonyms=[]):
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
        return ConfigEntry.source_name_by_type.get(source, '%d?' % source)


class ConfigResource(object):
    res_name_by_type = {RESOURCE_UNKNOWN: 'unknown',
                        RESOURCE_ANY: 'any',
                        RESOURCE_TOPIC: 'topic',
                        RESOURCE_GROUP: 'group',
                        RESOURCE_BROKER: 'broker'}
    res_type_by_name = {v: k for k, v in res_name_by_type.items()}

    def __init__(self, restype, name, configs=None, error=None):
        """
        :param: restype int: Resource type, see the RESOURCE_ constants below.
        :param: name str: Resource name, depending on restype.
                          For RESOURCE_BROKER the resource name is the broker id.

        :const:`RESOURCE_ANY` (used for lookups),
        :const:`RESOURCE_TOPIC`,
        :const:`RESOURCE_GROUP`,
        :const:`RESOURCE_BROKER`
        """
        super(ConfigResource, self).__init__()

        if type(name) != str:
            raise ValueError("Resource name must be a string")

        if type(restype) == str:
            if restype.lower() not in self.res_type_by_name:
                raise ValueError("Unknown resource type \"%s\": should be a RESOURCE_.. constant or one of %s" %
                                 (restype, ",".join(self.res_type_by_name.keys())))
            restype = self.res_type_by_name[restype]
        self.restype = restype
        self.name = name
        self.set_config_dict = dict()
        self.add_config_dict = dict()
        self.del_config_dict = dict()
        self.configs = configs
        self.error = error

    def __repr__(self):
        if self.error is not None:
            return "ConfigResource(%s,%s,%r)" % \
                (self.res_name_by_type.get(self.restype, '%d' % self.restype), self.name, self.error)
        else:
            return "ConfigResource(%s,%s)" % \
                (self.res_name_by_type.get(self.restype, '%d' % self.restype), self.name)

    def __hash__(self):
        return hash((self.restype, self.name))

    def __cmp__(self, other):
        r = self.restype - other.restype
        if r != 0:
            return r
        return self.name.__cmp__(other.name)

    def set_config(self, name, value):
        """ Set/Overwrite configuration entry """
        self.set_config_dict[name] = value

    def add_config(self, name, value):
        """
        Append value to configuration entry.

        Requires broker version >=2.0.0.
        """
        self.add_config_dict[name] = value

    def del_config(self, name):
        """
        Delete configuration entry, reverting it to the default value.

        Requires broker version >=2.0.0.
        """
        self.del_config_dict[name] = True


class AdminClient (AdminClientImpl):
    def __init__(self, conf):
        super(AdminClient, self).__init__(conf)

    def create_topics(self, new_topics, **kwargs):
        """ FIXME create topics """

        f = concurrent.futures.Future()
        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")
        return super(AdminClient, self).create_topics(new_topics, f, *kwargs)

    def delete_topics(self, topics, **kwargs):
        """ FIXME delete topics """

        f = concurrent.futures.Future()
        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")
        return super(AdminClient, self).delete_topics(topics, f, **kwargs)

    def create_partitions(self, topics, **kwargs):
        """ FIXME create partitions """

        f = concurrent.futures.Future()
        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")
        return super(AdminClient, self).create_partitions(topics, f, **kwargs)

    def describe_configs(self, resources, **kwargs):
        """ FIXME describe configs """

        f = concurrent.futures.Future()
        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")
        return super(AdminClient, self).describe_configs(resources, f, **kwargs)

    def alter_configs(self, resources, **kwargs):
        """ FIXME alter configs """

        f = concurrent.futures.Future()
        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")
        return super(AdminClient, self).alter_configs(resources, f, **kwargs)
