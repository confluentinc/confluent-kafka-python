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
from .. import cimpl

class OffsetSpec:
    """
    OffsetSpec 
    Used to specify the OffsetSpec corresponding to TopicPartition for ListOffsets.
    """
    def __init__(self):
        pass

class TimestampOffsetSpec(OffsetSpec):
    """
    TimestampOffsetSpec : OffsetSpec 
    Used to specify the Timestamp of TopicPartition for ListOffsets.
    Parameters
    ----------
    timestamp: int
        timestamp of the OffsetSpec
    """
    def __init__(self,timestamp:int):
        self.timestamp = timestamp

class MaxTimestampOffsetSpec(OffsetSpec):
    """
    MaxTimestampOffsetSpec : OffsetSpec 
    Used to specify the Offset corresponding to the Timestamp of TopicPartition (as Timestamp can be set on client side) for ListOffsets.
    """
    def __init__(self):
        pass

class LatestOffsetSpec(OffsetSpec):
    """
    LatestOffsetSpec : OffsetSpec 
    Used to specify the Latest Offset corresponding to the TopicPartition for ListOffsets.
    """
    def __init__(self):
        pass

class EarliestOffsetSpec(OffsetSpec):
    """
    EarliestOffsetSpec : OffsetSpec 
    Used to specify the Earliest Offset corresponding to the TopicPartition for ListOffsets.
    """
    def __init__(self):
        pass

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
    leaderEpoch: int
        leaderEpoch corresponding to the TopicPartiton.
    """
    def __init__(self,offset:int,timestamp:int,leaderEpoch:int):
        self.offset = offset
        self.timestamp = timestamp
        self.leaderEpoch = leaderEpoch

class IsolationLevel(Enum):
    """
    IsolationLevel
    Enum used for Admin Options to make ListOffsets call.
    """
    READ_COMMITTED = cimpl.READ_COMMITTED
    READ_UNCOMMITTED = cimpl.READ_UNCOMMITTED
    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value

class OffsetSpecEnumValue(Enum):
    """
    OffsetSpecEnumValue
    Enum used for internal mapping of OffsetSpec to make ListOffsets call.
    """
    MAX_TIMESTAMP_OFFSET_SPEC = cimpl.MAX_TIMESTAMP_OFFSET_SPEC
    EARLIEST_OFFSET_SPEC = cimpl.EARLIEST_OFFSET_SPEC
    LATEST_OFFSET_SPEC = cimpl.LATEST_OFFSET_SPEC
    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value