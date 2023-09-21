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


from .._util import ConversionUtil
from ._acl import AclOperation


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


class TopicDescription:
    """
    Represents topic description information for a topic used in describe topic operation.
    Used by :meth:`AdminClient.describe_topics`.

    Parameters
    ----------
    name : str
        The topic name.
    partitions : list(TopicPartitionInfo)
        Partition information.
    authorized_operations: list(AclOperation)
        AclOperations allowed for the topic.
    """

    def __init__(self, name, is_internal, partitions, authorized_operations):
        self.name = name
        self.is_internal = is_internal
        self.partitions = partitions
        self.authorized_operations = []
        if authorized_operations:
            for op in authorized_operations:
                self.authorized_operations.append(ConversionUtil.convert_to_enum(op, AclOperation))
