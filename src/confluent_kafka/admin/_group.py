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
from ..util import ConversionUtil


class ConsumerGroupListing:
    def __init__(self, group_id, is_simple_consumer_group, state=None):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        if state is not None:
            self.state = ConversionUtil.convert_to_enum(state, ConsumerGroupState)


class ListConsumerGroupsResult:
    def __init__(self, valid=None, errors=None):
        self.valid = valid
        self.errors = errors


class ConsumerGroupState(Enum):
    """
    Enumerates the different types of Consumer Group State.
    """
    #: State is not known or not set.
    UNKOWN = _cimpl.CONSUMER_GROUP_STATE_UNKNOWN
    #: Preparing rebalance for the consumer group.
    PREPARING_REBALANCING = _cimpl.CONSUMER_GROUP_STATE_PREPARING_REBALANCE
    #: Consumer Group is completing rebalancing.
    COMPLETING_REBALANCING = _cimpl.CONSUMER_GROUP_STATE_COMPLETING_REBALANCE
    #: Consumer Group is stable.
    STABLE = _cimpl.CONSUMER_GROUP_STATE_STABLE
    #: Consumer Group is Dead.
    DEAD = _cimpl.CONSUMER_GROUP_STATE_DEAD
    #: Consumer Group is Empty.
    EMPTY = _cimpl.CONSUMER_GROUP_STATE_EMPTY

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value


class MemberAssignment:
    def __init__(self, topic_partitions=[]):
        self.topic_partitions = topic_partitions
        if self.topic_partitions is None:
            self.topic_partitions = []


class MemberDescription:
    def __init__(self, member_id, client_id, host, assignment, group_instance_id=None):
        self.member_id = member_id
        self.client_id = client_id
        self.host = host
        self.assignment = assignment
        self.group_instance_id = group_instance_id


class ConsumerGroupDescription:
    def __init__(self, group_id, is_simple_consumer_group, members, partition_assignor, state,
                 coordinator):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        self.members = members
        self.partition_assignor = partition_assignor
        if state is not None:
            self.state = ConversionUtil.convert_to_enum(state, ConsumerGroupState)
        self.coordinator = coordinator


class DeleteConsumerGroupsResult:
    def __init__(self, group_id):
        self.group_id = group_id
