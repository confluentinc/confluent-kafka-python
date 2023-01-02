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

from ..cimpl import TopicPartition, OFFSET_INVALID, KafkaError
from ..admin import ConsumerGroupTopicPartitions

try:
    string_type = basestring
except NameError:
    string_type = str


class ValidationUtil:
    @staticmethod
    def check_multiple_not_none(obj, vars_to_check):
        for param in vars_to_check:
            ValidationUtil.check_not_none(obj, param)

    @staticmethod
    def check_not_none(obj, param):
        if getattr(obj, param) is None:
            raise ValueError("Expected %s to be not None" % (param,))

    @staticmethod
    def check_multiple_is_string(obj, vars_to_check):
        for param in vars_to_check:
            ValidationUtil.check_is_string(obj, param)

    @staticmethod
    def check_is_string(obj, param):
        param_value = getattr(obj, param)
        if param_value is not None and not isinstance(param_value, string_type):
            raise TypeError("Expected %s to be a string" % (param,))

    @staticmethod
    def check_kafka_errors(errors):
        if not isinstance(errors, list):
            raise TypeError("errors should be None or a list")
        for error in errors:
            if not isinstance(error, KafkaError):
                raise TypeError("Expected list of KafkaError")

    @staticmethod
    def check_kafka_error(error):
        if not isinstance(error, KafkaError):
            raise TypeError("Expected error to be a KafkaError")

    @staticmethod
    def check_list_consumer_group_offsets_request(request):
        if request is None:
            raise TypeError("request cannot be None")
        if not isinstance(request, list):
            raise TypeError("request must be a list")
        if len(request) != 1:
            raise ValueError("Currently we support listing only 1 consumer groups offset information")
        for req in request:
            if not isinstance(req, ConsumerGroupTopicPartitions):
                raise TypeError("Expected list of 'ConsumerGroupTopicPartitions'")

            if req.group_id is None:
                raise TypeError("'group_id' cannot be None")
            if not isinstance(req.group_id, string_type):
                raise TypeError("'group_id' must be a string")
            if not req.group_id:
                raise ValueError("'group_id' cannot be empty")

            if req.topic_partition_list is not None:
                if not isinstance(req.topic_partition_list, list):
                    raise TypeError("'topic_partition_list' must be a list or None")
                if len(req.topic_partition_list) == 0:
                    raise ValueError("'topic_partition_list' cannot be empty")
                for topic_partition in req.topic_partition_list:
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

    @staticmethod
    def check_alter_consumer_group_offsets_request(request):
        if request is None:
            raise TypeError("request cannot be None")
        if not isinstance(request, list):
            raise TypeError("request must be a list")
        if len(request) != 1:
            raise ValueError("Currently we support alter consumer groups offset request for 1 group only")
        for req in request:
            if not isinstance(req, ConsumerGroupTopicPartitions):
                raise TypeError("Expected list of 'ConsumerGroupTopicPartitions'")
            if req.group_id is None:
                raise TypeError("'group_id' cannot be None")
            if not isinstance(req.group_id, string_type):
                raise TypeError("'group_id' must be a string")
            if not req.group_id:
                raise ValueError("'group_id' cannot be empty")
            if req.topic_partition_list is None:
                raise ValueError("'topic_partition_list' cannot be null")
            if not isinstance(req.topic_partition_list, list):
                raise TypeError("'topic_partition_list' must be a list")
            if len(req.topic_partition_list) == 0:
                raise ValueError("'topic_partition_list' cannot be empty")
            for topic_partition in req.topic_partition_list:
                if topic_partition is None:
                    raise ValueError("Element of 'topic_partition_list' cannot be None")
                if not isinstance(topic_partition, TopicPartition):
                    raise TypeError("Element of 'topic_partition_list' must be of type TopicPartition")
                if topic_partition.topic is None:
                    raise TypeError("Element of 'topic_partition_list' must not have 'topic' attibute as None")
                if not topic_partition.topic:
                    raise ValueError("Element of 'topic_partition_list' must not have 'topic' attibute as Empty")
                if topic_partition.partition < 0:
                    raise ValueError(
                        "Element of 'topic_partition_list' must not have negative value for 'partition' field")
                if topic_partition.offset < 0:
                    raise ValueError(
                        "Element of 'topic_partition_list' must not have negative value for 'offset' field")
