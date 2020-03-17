#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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

"""
Serializers
"""
from copy import deepcopy

"""
Serialization
"""
KEY_SERIALIZER = 'key.serializer'
VALUE_SERIALIZER = 'value.serializer'
KEY_DESERIALIZER = 'key.deserializer'
VALUE_DESERIALIZER = 'value.deserializer'

"""
Callbacks
"""
ERROR_CB = 'error_cb'
LOG_CB = 'log_cb'
STATS_CB = 'thottle_cb'


class Config(object):
    """
    Application configuration context.

    This class manages common application configuration procedures.
        - Makes a copy the original source configuration.
        - Validates configuration property values
        - Raises a value error for any unknown property names

    Config instances may be self-contained or nested within another.
    When nesting configurations it's recommended you use the ``prefix``
    arg to avoid name clashes on common property names.

    All source dicts are copied prior to processing and all unknown
    configuration properties will result in a ``ValueError``.

    Validators may also be provided to ensure the provided values are
    suitable for the intended Application's needs.

    Attributes:
        properties (dict): Declare the properties associated with this
            config object and their defaults
        include ([class Config], optional): Set of configuration definitions to
            be handled within this Config. Use Config.defer() to provide args
            to ``__init__`` which will be invoked by the top-level Config object.

    Keyword Args:
        - data (dict): source configuration dict
        - prefix (str, optional): optional config property prefix.

    Raises:
        ValueError if there are any unrecognized properties or the provided
            properties violate one of the validation tests.

    """
    properties = {}
    include = None

    def __init__(self, data=None, prefix=''):
        if len(prefix) > 1 and not prefix.endswith('.'):
            prefix += '.'

        self.prefix = prefix
        self.data = {}

        if data is None:
            return

        # shallow copy to keep referenced types in tact
        _data = data.copy()

        # Pass source dict copy to each ConfigDef
        if self.include is not None:
            for conf_def in self.include:
                conf = conf_def()
                conf.update(_data)
                conf.validate()
                # Attach conf reference to this instance
                self.__dict__[conf.__class__.__name__] = conf

        # Process Config properties
        self.update(_data)
        self.validate()

        # Raise ValueError if any properties remain
        if len(_data) > 0:
            raise ValueError("Unrecognized propert{} {}".format(
                "y" if len(_data) == 1 else "ies",
                [k for k in _data.keys()]))

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        if key in self.data:
            raise TypeError("{} Property {} already set".format(self, key))
        self.data[key] = value

    @classmethod
    def defer(cls, prefix=''):
        """
        Defers initialization of a Config instance to allow configuration
        definition chaining.

        Args:
            prefix (str, optional): Optional property prefix.
                Use this to avoid property name clashes when nesting definitions.

        Returns
            callable: Config instance's ``__init__`` function with prefix set.

        """
        def init():
            return cls(prefix=prefix)
        return init

    def update(self, data):
        """
        Updates only known values from source dict. If the key can't be found
        the default value defined in Config.properties will be used.

        This method intentionally mutates the source dict. If called directly
        be sure to copy the original first.

        Args:
            data (dict): Source dictionary

        Raises:
            TypeError for duplicate configuration properties.

        """
        for prop, value in self.properties.items():
            # generates lots of garbage; simplicity was favored over cleverness
            self[prop] = data.pop(self.prefix + prop, value)

    def validate(self):
        """
        Validate configuration values are correct.

        Raises:
             ValueError if Validation tests fail.

        """
        pass

    @property
    def Config(self):
        return deepcopy(self.data)


class ClientConfig(Config):
    """
    Configures SerializingProducer/SerializingConsumer instances.

    .. Note: All configs not previously handled by other ``Config`` are passed
    through to the Producer super class. If nesting this Config definition
    with in another you must ensure it's the last element in the ``include``
    list.


    """
    properties = {
        'key.serializer': None,
        'value.serializer': None,
        'error_cb': None,
        'log_cb': None,
        'stats_cb': None
    }

    def __init__(self, data=None, prefix=''):
        # Extract serde definitions
        self._key_serializer = None
        self._value_serializer = None
        self._key_deserializer = None
        self._value_deserializer = None

        super(ClientConfig, self).__init__(data=data, prefix=prefix)

    def update(self, data):
        """
        Captures all remaining properties deferring validation to librdkafka.

        This method intentionally mutates the source dict. If called directly
        be sure to copy the original first.

        Args:
            data (dict): Source dictionary
        """
        # intercept known non-librdkafka configs
        self._key_serializer = data.pop(KEY_SERIALIZER, None)
        self._value_serializer = data.pop(VALUE_SERIALIZER, None)
        self._key_deserializer = data.pop(KEY_DESERIALIZER, None)
        self._value_deserializer = data.pop(VALUE_DESERIALIZER, None)

        for key in data.keys():
            self[key] = data.pop(key)

    @property
    def KeySerializer(self):
        return self._key_serializer

    @property
    def ValueSerializer(self):
        return self._value_serializer

    @property
    def KeyDeserializer(self):
        return self._key_deserializer

    @property
    def ValueDeserializer(self):
        return self._value_deserializer
