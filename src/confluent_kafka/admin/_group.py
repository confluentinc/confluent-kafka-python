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
from re import X
from .. import cimpl as _cimpl
from ._offset import ConsumerGroupTopicPartitions

try:
    string_type = basestring
except NameError:
    string_type = str


class ConsumerGroupListing:
    def __init__(self, group_id, is_simple_consumer_group, state=None, error=None):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        self.error = error
        self._check_group_id()
        self._check_is_simple_consumer_group()
        self._check_error()
        if state is not None:
            self.state = self._convert_to_enum(state, ConsumerGroupState)

    def _convert_to_enum(self, val, enum_clazz):
        if type(val) == str:
            # Allow it to be specified as case-insensitive string, for convenience.
            try:
                val = enum_clazz[val.upper()]
            except KeyError:
                raise ValueError("Unknown value \"%s\": should be a %s" % (val, enum_clazz.__name__))

        elif type(val) == int:
            # The C-code passes restype as an int, convert to enum.
            val = enum_clazz(val)

        elif type(val) != enum_clazz:
            raise TypeError("Unknown value \"%s\": should be a %s" % (val, enum_clazz.__name__))

        return val

    def _check_group_id(self):
        if self.group_id is not None:
            if not isinstance(self.group_id, string_type):
                raise TypeError("'group_id' must be a string")
            if not self.group_id:
                raise ValueError("'group_id' cannot be empty")

    def _check_is_simple_consumer_group(self):
        if self.is_simple_consumer_group is not None:
            if not isinstance(self.is_simple_consumer_group, bool):
                raise TypeError("'is_simple_consumer_group' must be a bool")

    def _check_error(self):
        if self.error is not None:
            if not isinstance(self.error, _cimpl.KafkaError):
                raise TypeError("'error' must be of type KafkaError")


class ListConsumerGroupsResponse:
    def __init__(self, valid=None, errors=None):
        self.valid = valid
        self.errors = errors
        self._check_valid()
        self._check_errors()

    def _check_valid(self):
        if self.valid is not None:
            if not isinstance(self.valid, list):
                raise TypeError("'valid' should be None or a list")
            for v in self.valid:
                if not isinstance(v, ConsumerGroupListing):
                    raise TypeError("Element of 'valid' must be of type ConsumerGroupListing")

    def _check_errors(self):
        if self.errors is not None:
            if not isinstance(self.errors, list):
                raise TypeError("'errors' should be None or a list")
            for error in self.errors:
                if not isinstance(error, _cimpl.KafkaError):
                    raise TypeError("Element of 'errors' must be of type KafkaError")


class ConsumerGroupState(Enum):
    """
    Enumerates the different types of Consumer Group State.

    TODO: Add proper descriptions for the Enums
    """
    UNKOWN = _cimpl.CONSUMER_GROUP_STATE_UNKNOWN  #: State is not known or not set.
    PREPARING_REBALANCING = _cimpl.CONSUMER_GROUP_STATE_PREPARING_REBALANCE  #: Preparing rebalance for the consumer group.
    COMPLETING_REBALANCING = _cimpl.CONSUMER_GROUP_STATE_COMPLETING_REBALANCE  #: Consumer Group is completing rebalancing.
    STABLE = _cimpl.CONSUMER_GROUP_STATE_STABLE  #: Consumer Group is stable.
    DEAD = _cimpl.CONSUMER_GROUP_STATE_DEAD  #: Consumer Group is Dead.
    EMPTY = _cimpl.CONSUMER_GROUP_STATE_EMPTY  #: Consumer Group is Empty.

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value


class DeleteConsumerGroupsResponse:
    def __init__(self, group_name):
        self.group_name = group_name
