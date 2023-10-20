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


from typing import List, Optional

from confluent_kafka.cimpl import KafkaException, TopicPartition
from .._util import ConversionUtil
from .._model import ConsumerGroupState, Node


class ConsumerGroupListing:
    """
    Represents consumer group listing information for a group used in list consumer group operation.
    Used by :class:`ListConsumerGroupsResult`.

    Parameters
    ----------
    group_id : str
        The consumer group id.
    is_simple_consumer_group : bool
        Whether a consumer group is simple or not.
    state : ConsumerGroupState
        Current state of the consumer group.
    """
    def __init__(self, group_id: str, is_simple_consumer_group: bool, state: Optional[ConsumerGroupState]=None):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        if state is not None:
            self.state = ConversionUtil.convert_to_enum(state, ConsumerGroupState)


class ListConsumerGroupsResult:
    """
    Represents result of List Consumer Group operation.
    Used by :meth:`AdminClient.list_consumer_groups`.

    Parameters
    ----------
    valid : list(ConsumerGroupListing)
        List of successful consumer group listing responses.
    errors : list(KafkaException)
        List of errors encountered during the operation, if any.
    """
    def __init__(self, valid: Optional[List[ConsumerGroupListing]]=None, errors: Optional[List[KafkaException]]=None):
        self.valid = valid
        self.errors = errors


class MemberAssignment:
    """
    Represents member assignment information.
    Used by :class:`MemberDescription`.

    Parameters
    ----------
    topic_partitions : list(TopicPartition)
        The topic partitions assigned to a group member.
    """
    def __init__(self, topic_partitions: Optional[List[TopicPartition]]=None):
        self.topic_partitions = topic_partitions
        if self.topic_partitions is None:
            self.topic_partitions = []


class MemberDescription:
    """
    Represents member information.
    Used by :class:`ConsumerGroupDescription`.

    Parameters
    ----------
    member_id : str
        The consumer id of the group member.
    client_id : str
        The client id of the group member.
    host: str
        The host where the group member is running.
    assignment: MemberAssignment
        The assignment of the group member
    group_instance_id : str
        The instance id of the group member.
    """
    def __init__(self, member_id: str, client_id: str, host: str, assignment: MemberAssignment, group_instance_id: Optional[str]=None):
        self.member_id = member_id
        self.client_id = client_id
        self.host = host
        self.assignment = assignment
        self.group_instance_id = group_instance_id


class ConsumerGroupDescription:
    """
    Represents consumer group description information for a group used in describe consumer group operation.
    Used by :meth:`AdminClient.describe_consumer_groups`.

    Parameters
    ----------
    group_id : str
        The consumer group id.
    is_simple_consumer_group : bool
        Whether a consumer group is simple or not.
    members: list(MemberDescription)
        Description of the memebers of the consumer group.
    partition_assignor: str
        Partition assignor.
    state : ConsumerGroupState
        Current state of the consumer group.
    coordinator: Node
        Consumer group coordinator.
    """
    def __init__(self, group_id:str , is_simple_consumer_group: bool, members: List[MemberDescription], partition_assignor: str, state: ConsumerGroupState,
                 coordinator: Node):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        self.members = members
        self.partition_assignor = partition_assignor
        if state is not None:
            self.state = ConversionUtil.convert_to_enum(state, ConsumerGroupState)
        self.coordinator = coordinator
