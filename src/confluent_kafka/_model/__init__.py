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
from .. import cimpl


class Node:
    """
    Represents node information.
    Used by :class:`ConsumerGroupDescription`

    Parameters
    ----------
    id: int
        The node id of this node.
    id_string:
        String representation of the node id.
    host:
        The host name for this node.
    port: int
        The port for this node.
    rack: str
        The rack for this node.
    """

    def __init__(self, id, host, port, rack=None):
        self.id = id
        self.id_string = str(id)
        self.host = host
        self.port = port
        self.rack = rack

    def __str__(self):
        return f"({self.id}) {self.host}:{self.port} {f'(Rack - {self.rack})' if self.rack else ''}"


class Uuid:
    """
    Represents UUID
    
    Parameters
    ----------
    most_significant_bits: int
        Most significant 64 bits of the 128 bits UUID
    least_significant_bits: int
        Least significant 64 bits of the 128 bits UUID
    """
    def __init__(self, most_significant_bits, least_significant_bits, base64str=None):
        self.__most_significant_bits = most_significant_bits
        self.__least_significant_bits = least_significant_bits
        self.__base64str = base64str

    def get_most_significant_bits(self):
        return self.__most_significant_bits
    
    def get_least_significant_bits(self):
        return self.__least_significant_bits
    
    def _to_tuple(self):
        return (self.__most_significant_bits, self.least_significant_bits)

    def __str__(self):
        return self.__base64str
        
    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self._to_tuple() == other._to_tuple()


class ConsumerGroupTopicPartitions:
    """
    Represents consumer group and its topic partition information.
    Used by :meth:`AdminClient.list_consumer_group_offsets` and
    :meth:`AdminClient.alter_consumer_group_offsets`.

    Parameters
    ----------
    group_id: str
        Id of the consumer group.
    topic_partitions: list(TopicPartition)
        List of topic partitions information.
    """

    def __init__(self, group_id, topic_partitions=None):
        self.group_id = group_id
        self.topic_partitions = topic_partitions


class ConsumerGroupState(Enum):
    """
    Enumerates the different types of Consumer Group State.

    Note that the state :py:attr:`UNKOWN` (typo one) is deprecated and will be removed in
    future major release. Use :py:attr:`UNKNOWN` instead.
    """
    #: State is not known or not set
    UNKNOWN = cimpl.CONSUMER_GROUP_STATE_UNKNOWN
    #: .. deprecated:: 2.3.0
    #:
    #:    Use :py:attr:`UNKNOWN` instead.
    UNKOWN = UNKNOWN
    #: Preparing rebalance for the consumer group.
    PREPARING_REBALANCING = cimpl.CONSUMER_GROUP_STATE_PREPARING_REBALANCE
    #: Consumer Group is completing rebalancing.
    COMPLETING_REBALANCING = cimpl.CONSUMER_GROUP_STATE_COMPLETING_REBALANCE
    #: Consumer Group is stable.
    STABLE = cimpl.CONSUMER_GROUP_STATE_STABLE
    #: Consumer Group is dead.
    DEAD = cimpl.CONSUMER_GROUP_STATE_DEAD
    #: Consumer Group is empty.
    EMPTY = cimpl.CONSUMER_GROUP_STATE_EMPTY

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value


class TopicCollection:
    """
    Represents collection of topics in the form of different identifiers
    for the topic.

    Parameters
    ----------
    topic_names: list(str)
        List of topic names.
    """

    def __init__(self, topic_names):
        self.topic_names = topic_names


class TopicPartitionInfo:
    """
    Represents partition information.
    Used by :class:`TopicDescription`.

    Parameters
    ----------
    id : int
        Id of the partition.
    leader : Node
        Leader broker for the partition.
    replicas: list(Node)
        Replica brokers for the partition.
    isr: list(Node)
        In-Sync-Replica brokers for the partition.
    """

    def __init__(self, id, leader, replicas, isr):
        self.id = id
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
