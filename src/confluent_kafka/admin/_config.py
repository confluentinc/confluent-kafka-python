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

from enum import Enum
import functools
from typing import Dict, List, Optional, Union
from .. import cimpl as _cimpl
from ._resource import ResourceType


class AlterConfigOpType(Enum):
    """
    Set of incremental operations that can be used with
    incremental alter configs.
    """

    #: Set the value of the configuration entry.
    SET = _cimpl.ALTER_CONFIG_OP_TYPE_SET

    #: Revert the configuration entry
    #: to the default value (possibly null).
    DELETE = _cimpl.ALTER_CONFIG_OP_TYPE_DELETE

    #: (For list-type configuration entries only.)
    #:  Add the specified values
    #:  to the current list of values
    #:  of the configuration entry.
    APPEND = _cimpl.ALTER_CONFIG_OP_TYPE_APPEND

    #: (For list-type configuration entries only.)
    #:  Removes the specified values
    #:  from the current list of values
    #:  of the configuration entry.
    SUBTRACT = _cimpl.ALTER_CONFIG_OP_TYPE_SUBTRACT


class ConfigSource(Enum):
    """
    Enumerates the different sources of configuration properties.
    Used by ConfigEntry to specify the
    source of configuration properties returned by `describe_configs()`.
    """
    UNKNOWN_CONFIG = _cimpl.CONFIG_SOURCE_UNKNOWN_CONFIG  #: Unknown
    DYNAMIC_TOPIC_CONFIG = _cimpl.CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG  #: Dynamic Topic
    DYNAMIC_BROKER_CONFIG = _cimpl.CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG  #: Dynamic Broker
    DYNAMIC_DEFAULT_BROKER_CONFIG = _cimpl.CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG  #: Dynamic Default Broker
    STATIC_BROKER_CONFIG = _cimpl.CONFIG_SOURCE_STATIC_BROKER_CONFIG  #: Static Broker
    DEFAULT_CONFIG = _cimpl.CONFIG_SOURCE_DEFAULT_CONFIG  #: Default


class ConfigEntry(object):
    """
    Represents a configuration property. Returned by describe_configs() for each configuration
    entry of the specified resource.

    This class is typically not user instantiated.
    """

    def __init__(self, name: str, value: str,
                 source: ConfigSource=ConfigSource.UNKNOWN_CONFIG,
                 is_read_only: bool=False,
                 is_default: bool=False,
                 is_sensitive: bool=False,
                 is_synonym: bool=False,
                 synonyms: List[str]=[],
                 incremental_operation: Optional[AlterConfigOpType]=None):
        """
        This class is typically not user instantiated.
        """
        super(ConfigEntry, self).__init__()

        self.name = name
        """Configuration property name."""
        self.value = value
        """Configuration value (or None if not set or is_sensitive==True.
           Ignored when altering configurations incrementally
           if incremental_operation is DELETE)."""
        self.source = source
        """Configuration source."""
        self.is_read_only = bool(is_read_only)
        """Indicates whether the configuration property is read-only."""
        self.is_default = bool(is_default)
        """Indicates whether the configuration property is using its default value."""
        self.is_sensitive = bool(is_sensitive)
        """
        Indicates whether the configuration property value contains
        sensitive information (such as security settings), in which
        case .value is None."""
        self.is_synonym = bool(is_synonym)
        """Indicates whether the configuration property is a synonym for the parent configuration entry."""
        self.synonyms = synonyms
        """A list of synonyms (ConfigEntry) and alternate sources for this configuration property."""
        self.incremental_operation = incremental_operation
        """The incremental operation (AlterConfigOpType) to use in incremental_alter_configs."""

    def __repr__(self) -> str:
        return "ConfigEntry(%s=\"%s\")" % (self.name, self.value)

    def __str__(self) -> str:
        return "%s=\"%s\"" % (self.name, self.value)


@functools.total_ordering
class ConfigResource(object):
    """
    Represents a resource that has configuration, and (optionally)
    a collection of configuration properties for that resource. Used by
    describe_configs() and alter_configs().

    Parameters
    ----------
    restype : `ConfigResource.Type`
       The resource type.
    name : `str`
       The resource name, which depends on the resource type. For RESOURCE_BROKER, the resource name is the broker id.
    set_config : `dict`
        The configuration to set/overwrite. Dictionary of str, str.
    """

    Type = ResourceType

    def __init__(self, restype: Union[str, int, ResourceType], name: str,
                 set_config: Optional[Dict[str, str]]=None, described_configs: Optional[object]=None, error: Optional[object]=None, incremental_configs: Optional[List[ConfigEntry]]=None):
        """
        :param ConfigResource.Type restype: Resource type.
        :param str name: The resource name, which depends on restype.
                         For RESOURCE_BROKER, the resource name is the broker id.
        :param dict set_config: The configuration to set/overwrite. Dictionary of str, str.
        :param list(ConfigEntry) incremental_configs: The configuration entries to alter incrementally.
        :param dict described_configs: For internal use only.
        :param KafkaError error: For internal use only.
        """
        super(ConfigResource, self).__init__()

        if name is None:
            raise ValueError("Expected resource name to be a string")

        if isinstance(restype, str):
            # Allow resource type to be specified as case-insensitive string, for convenience.
            try:
                self.restype = ConfigResource.Type[restype.upper()]
            except KeyError:
                raise ValueError("Unknown resource type \"%s\": should be a ConfigResource.Type" % restype)

        elif isinstance(restype, int):
            # The C-code passes restype as an int, convert to Type.
            self.restype = ConfigResource.Type(restype)

        else:
            self.restype = restype

        self.restype_int = int(self.restype.value)  # for the C code
        self.name = name

        if set_config is not None:
            self.set_config_dict = set_config.copy()
        else:
            self.set_config_dict = dict()

        self.incremental_configs = incremental_configs or []

        self.configs = described_configs
        self.error = error

    def __repr__(self) -> str:
        if self.error is not None:
            return "ConfigResource(%s,%s,%r)" % (self.restype, self.name, self.error)
        else:
            return "ConfigResource(%s,%s)" % (self.restype, self.name)

    def __hash__(self) -> int:
        return hash((self.restype, self.name))

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, ConfigResource)
        if self.restype < other.restype:
            return True
        return self.name.__lt__(other.name)

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, ConfigResource)
        return self.restype == other.restype and self.name == other.name

    def __len__(self) -> int:
        """
        :rtype: int
        :returns: number of configuration entries/operations
        """
        return len(self.set_config_dict)

    def set_config(self, name: str, value: str, overwrite: bool=True) -> None:
        """
        Set/overwrite a configuration value.

        When calling alter_configs, any configuration properties that are not included
        in the request will be reverted to their default values. As a workaround, use
        describe_configs() to retrieve the current configuration and overwrite the
        settings you want to change.

        :param str name: Configuration property name
        :param str value: Configuration value
        :param bool overwrite: If True, overwrite entry if it already exists (default).
                               If False, do nothing if entry already exists.
        """
        if not overwrite and name in self.set_config_dict:
            return
        self.set_config_dict[name] = value

    def add_incremental_config(self, config_entry: ConfigEntry) -> None:
        """
        Add a ConfigEntry for incremental alter configs, using the
        configured incremental_operation.

        :param ConfigEntry config_entry: config entry to incrementally alter.
        """
        self.incremental_configs.append(config_entry)
