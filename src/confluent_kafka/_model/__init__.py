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

    Values
    ------
    UNKOWN                  : State is not known or not set.
    PREPARING_REBALANCING   : Preparing rebalance for the consumer group.
    COMPLETING_REBALANCING  : Consumer Group is completing rebalancing.
    STABLE                  : Consumer Group is stable.
    DEAD                    : Consumer Group is Dead.
    EMPTY                   : Consumer Group is Empty.
    """
    UNKOWN = cimpl.CONSUMER_GROUP_STATE_UNKNOWN
    PREPARING_REBALANCING = cimpl.CONSUMER_GROUP_STATE_PREPARING_REBALANCE
    COMPLETING_REBALANCING = cimpl.CONSUMER_GROUP_STATE_COMPLETING_REBALANCE
    STABLE = cimpl.CONSUMER_GROUP_STATE_STABLE
    DEAD = cimpl.CONSUMER_GROUP_STATE_DEAD
    EMPTY = cimpl.CONSUMER_GROUP_STATE_EMPTY

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value
