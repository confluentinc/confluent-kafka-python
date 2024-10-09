# Copyright 2024 Confluent Inc.
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


class ElectionType(Enum):
    """
    Enumerates the different types of leader elections.
    """
    PREFERRED = _cimpl.ELECTION_TYPE_PREFERRED  #: Preferred election
    UNCLEAN = _cimpl.ELECTION_TYPE_UNCLEAN  #: Unclean election

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value
