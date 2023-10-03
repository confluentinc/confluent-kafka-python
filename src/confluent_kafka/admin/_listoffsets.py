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

class OffsetSpec:
    """
    OffsetSpec
    """
    def __init__(self):
        pass

class TimestampOffsetSpec(OffsetSpec):
    def __init__(self,timestamp:int):
        self.timestamp = timestamp

class MaxTimestampOffsetSpec(OffsetSpec):
    def __init__(self):
        pass

class LatestOffsetSpec(OffsetSpec):
    def __init__(self):
        pass

class EarliestOffsetSpec(OffsetSpec):
    def __init__(self):
        pass

class ListOffsetsResultInfo:
    def __init__(self,offset:int,timestamp:int,leaderEpoch:int):
        self.offset = offset
        self.timestamp = timestamp
        self.leaderEpoch = leaderEpoch

class IsolationLevel(Enum):
    READ_COMMITTED = cimpl.READ_COMMITTED
    READ_UNCOMMITTED = cimpl.READ_UNCOMMITTED
    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value

class OffsetSpecEnumValue(Enum):
    MAX_TIMESTAMP_OFFSET_SPEC = cimpl.MAX_TIMESTAMP_OFFSET_SPEC
    EARLIEST_OFFSET_SPEC = cimpl.EARLIEST_OFFSET_SPEC
    LATEST_OFFSET_SPEC = cimpl.LATEST_OFFSET_SPEC
    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value