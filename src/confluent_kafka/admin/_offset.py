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

from tkinter import OFF
from confluent_kafka import TopicPartition, OFFSET_INVALID
from abc import ABC, abstractmethod

try:
    string_type = basestring
except NameError:
    string_type = str


# Add type checking here
# Add __repr__ function
# Make properties readonly once it is set
class ConsumerGroupTopicPartitions(ABC):
    def __init__(self, group_name: str = None, topic_partition_list:list = None):
        self.group_name = group_name
        self.topic_partition_list = topic_partition_list
        self._check_valid_group_name()
        self._check_topic_partition_list()

    def __hash__(self) -> int:
        return hash(self.group_name)

    @abstractmethod
    def _check_topic_partition_list(self):
        pass

    @abstractmethod
    def _check_valid_group_name(self):
        pass


# Relook at __eq__ and __hash__ logic when the ListConsumerGroupOffsets 
# API of librdkafka accepts multiple group names
class ListConsumerGroupOffsetsRequest(ConsumerGroupTopicPartitions):
    """
    Request object for list consumer group offset API.

    Parameters
    ----------
    group_name : str
        Group name for which offset information is expected. **Mandatory**
    topic_partition_list : list
        List of :class:`TopicPartition` for which offset information is expected. . **Optional**
        * Can be null
        * Cannot be empty
    """
    def _check_valid_group_name(self):
        if self.group_name is None:
            raise TypeError("'group_name' cannot be None")
        if not isinstance(self.group_name, string_type):
            raise TypeError("'group_name' must be a string")
        if not self.group_name:
            raise ValueError("'group_name' cannot be empty")

    def _check_topic_partition_list(self):
        if self.topic_partition_list is not None:
            if not isinstance(self.topic_partition_list, list):
                raise TypeError("'topic_partition_list' must be a list or None")
            if len(self.topic_partition_list) == 0:
                raise ValueError("'topic_partition_list' cannot be empty")
            for topic_partition in self.topic_partition_list:
                self._check_topic_partition(topic_partition)

    def _check_topic_partition(self, topic_partition):
        if topic_partition is None:
            raise ValueError("Element of 'topic_partition_list' cannot be None")
        if not isinstance(topic_partition, TopicPartition):
            raise TypeError("Element of 'topic_partition_list' must be of type TopicPartition")
        if topic_partition.topic is None:
            raise TypeError("Element of 'topic_partition_list' must not have 'topic' attibute as None")
        if not topic_partition.topic:
            raise ValueError("Element of 'topic_partition_list' must not have 'topic' attibute as Empty")
        if topic_partition.partition < 0:
            raise ValueError("Element of 'topic_partition_list' must not have negative 'partition' value")
        if topic_partition.offset != OFFSET_INVALID:
            print(topic_partition.offset)
            raise ValueError("Element of 'topic_partition_list' must not have 'offset' value")


class ListConsumerGroupOffsetsResponse(ConsumerGroupTopicPartitions):
    """
    Response object for list consumer group offset API.

    Parameters
    ----------
    group_name : str
        Group name for which offset information is fetched.
    topic_partition_list : list
        List of :class:`TopicPartition` containing offset information.
    """
    def _check_valid_group_name(self):
        pass

    def _check_topic_partition_list(self):
        pass


# Relook at __eq__ and __hash__ logic when the AlterConsumerGroupOffsets 
# API of librdkafka accepts multiple group information
class AlterConsumerGroupOffsetsRequest(ConsumerGroupTopicPartitions):
    """
    Request object for alter consumer group offset API.

    Parameters
    ----------
    group_name : str
        Group name for which offset information is expected. **Mandatory**
    topic_partition_list : list
        List of :class:`TopicPartition` for which offset information is expected. . **Mandatory**
        * Cannot be empty or null
    """
    def _check_valid_group_name(self):
        if self.group_name is None:
            raise TypeError("'group_name' cannot be None")
        if not isinstance(self.group_name, string_type):
            raise TypeError("'group_name' must be a string")
        if not self.group_name:
            raise ValueError("'group_name' cannot be empty")

    def _check_topic_partition_list(self):
        if self.topic_partition_list is None:
            raise ValueError("'topic_partition_list' cannot be null")
        if self.topic_partition_list is not None:
            if not isinstance(self.topic_partition_list, list):
                raise TypeError("'topic_partition_list' must be a list")
            if len(self.topic_partition_list) == 0:
                raise ValueError("'topic_partition_list' cannot be empty")
            for topic_partition in self.topic_partition_list:
                self._check_topic_partition(topic_partition)

    def _check_topic_partition(self, topic_partition):
        if topic_partition is None:
            raise ValueError("Element of 'topic_partition_list' cannot be None")
        if not isinstance(topic_partition, TopicPartition):
            raise TypeError("Element of 'topic_partition_list' must be of type TopicPartition")
        if topic_partition.topic is None:
            raise TypeError("Element of 'topic_partition_list' must not have 'topic' attibute as None")
        if not topic_partition.topic:
            raise ValueError("Element of 'topic_partition_list' must not have 'topic' attibute as Empty")
        if topic_partition.partition < 0:
            raise ValueError("Element of 'topic_partition_list' must not have negative value for 'partition' field")
        if topic_partition.offset < 0:
            raise ValueError("Element of 'topic_partition_list' must not have negative value for 'offset' field")



class AlterConsumerGroupOffsetsResponse(ConsumerGroupTopicPartitions):
    """
    Response object for alter consumer group offset API.

    Parameters
    ----------
    group_name : str
        Group name for which offset information is altered.
    topic_partition_list : list
        List of :class:`TopicPartition` showing offset information after completion of the operation.
    """
    def _check_valid_group_name(self):
        pass

    def _check_topic_partition_list(self):
        pass