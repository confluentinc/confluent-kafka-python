# Copyright 2023 Confluent Inc.
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
from abc import ABC, abstractmethod
from .. import cimpl


class OffsetSpec(ABC):
    """
    OffsetSpec
    Used to specify the OffsetSpec corresponding to a TopicPartition for ListOffsets.
    """
    _values = {}

    @property
    @abstractmethod
    def _value(self):
        pass

    @classmethod
    def _fill_values(cls):
        cls._max_timestamp = MaxTimestampSpec()
        cls._earliest = EarliestSpec()
        cls._latest = LatestSpec()
        cls._values.update({
            cimpl.OFFSET_SPEC_MAX_TIMESTAMP: cls._max_timestamp,
            cimpl.OFFSET_SPEC_EARLIEST: cls._earliest,
            cimpl.OFFSET_SPEC_LATEST: cls._latest,
        })

    @classmethod
    def earliest(cls):
        return cls._earliest

    @classmethod
    def latest(cls):
        return cls._latest

    @classmethod
    def max_timestamp(cls):
        return cls._max_timestamp

    @classmethod
    def for_timestamp(cls, timestamp):
        return TimestampSpec(timestamp)

    def __call__(cls, index):
        if index < 0:
            return cls._values[index]
        else:
            return cls.for_timestamp(index)

    def __lt__(self, other):
        if not isinstance(other, OffsetSpec):
            return NotImplemented
        return self._value < other._value


class TimestampSpec(OffsetSpec):
    """
    TimestampSpec : OffsetSpec
    Used to specify the Timestamp of TopicPartition for ListOffsets.
    Parameters
    ----------
    timestamp: int
        timestamp of the OffsetSpec
    """

    @property
    def _value(self):
        return self.timestamp

    def __init__(self, timestamp):
        self.timestamp = timestamp

class MaxTimestampSpec(OffsetSpec):
    """
    MaxTimestampSpec : OffsetSpec
    Used to specify the Offset corresponding to the Timestamp
    of TopicPartition (as Timestamp can be set on client side) for ListOffsets.
    """

    @property
    def _value(self):
        return cimpl.OFFSET_SPEC_MAX_TIMESTAMP



class LatestSpec(OffsetSpec):
    """
    LatestSpec : OffsetSpec
    Used to specify the Latest Offset corresponding to the TopicPartition for ListOffsets.
    """

    @property
    def _value(self):
        return cimpl.OFFSET_SPEC_LATEST


class EarliestSpec(OffsetSpec):
    """
    EarliestSpec : OffsetSpec
    Used to specify the Earliest Offset corresponding to the TopicPartition for ListOffsets.
    """

    @property
    def _value(self):
        return cimpl.OFFSET_SPEC_EARLIEST


OffsetSpec.TimestampSpec = TimestampSpec
OffsetSpec.MaxTimestampSpec = MaxTimestampSpec
OffsetSpec.LatestSpec = LatestSpec
OffsetSpec.EarliestSpec = EarliestSpec
OffsetSpec._fill_values()

class ListOffsetsResultInfo:
    """
    ListOffsetsResultInfo
    Used to specify the result of ListOffsets of a TopicPartiton.
    Holds Offset, Timestamp and LeaderEpoch for a TopicPartition.
    Parameters
    ----------
    offset: int
        offset corresponding to the TopicPartiton with the OffsetSpec specified returned by ListOffsets call.
    timestamp: int
        timestamp corresponding to the offset.
    leader_epoch: int
        leader_epoch corresponding to the TopicPartiton.
    """
    def __init__(self, offset, timestamp, leader_epoch):
        self.offset = offset
        self.timestamp = timestamp
        self.leader_epoch = leader_epoch
        if self.leader_epoch < 0:
            self.leader_epoch = None
