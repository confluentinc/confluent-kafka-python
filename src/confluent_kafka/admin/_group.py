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
from .. import cimpl as _cimpl
from ..util import ValidationUtil
from ..util import ConversionUtil


class ConsumerGroupListing:
    def __init__(self, group_id, is_simple_consumer_group, state=None, error=None):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        self.error = error
        self._check_group_id()
        self._check_is_simple_consumer_group()
        self._check_error()
        if state is not None:
            self.state = ConversionUtil.convert_to_enum(state, ConsumerGroupState)

    def _check_group_id(self):
        if self.group_id is not None:
            ValidationUtil.check_is_string(self, "group_id")
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


# TODO: Change name to Result instead of Response to match with Java
class ListConsumerGroupsResult:
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


class MemberAssignment:
    def __init__(self, topic_partitions=[]):
        self.topic_partitions = topic_partitions
        if self.topic_partitions is None:
            self.topic_partitions = []
        self._check_topic_partitions()

    def _check_topic_partitions(self):
        if not isinstance(self.topic_partitions, list):
            raise TypeError("'topic_partitions' should be a list")
        for topic_partition in self.topic_partitions:
            if topic_partition is None:
                raise ValueError("Element of 'topic_partitions' cannot be None")
            if not isinstance(topic_partition, _cimpl.TopicPartition):
                raise TypeError("Element of 'topic_partitions' must be of type TopicPartition")


class MemberDescription:
    def __init__(self, member_id, client_id, host, assignment, group_instance_id=None):
        self.member_id = member_id
        self.client_id = client_id
        self.host = host
        self.assignment = assignment
        self.group_instance_id = group_instance_id

        ValidationUtil.check_multiple_not_none(self, ["member_id", "client_id", "host", "assignment"])

        string_args = ["member_id", "client_id", "host"]
        if group_instance_id is not None:
            string_args.append("group_instance_id")
        ValidationUtil.check_multiple_is_string(self, string_args)

    def _check_assignment(self):
        if not isinstance(self.assignment, MemberAssignment):
            raise TypeError("'assignment' should be a MemberAssignment")


class ConsumerGroupDescription:
    def __init__(self, group_id, is_simple_consumer_group, members, partition_assignor, state, coordinator, error=None):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        self.members = members
        self.partition_assignor = partition_assignor
        self.state = ConversionUtil.convert_to_enum(state, ConsumerGroupState)
        self.coordinator = coordinator

    # TODO: Add validations?


# TODO: Check return type for DeleteConsumerGroups
class DeleteConsumerGroupsResult:
    def __init__(self, group_id):
        self.group_id = group_id
