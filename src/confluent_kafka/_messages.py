#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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
#


class Messages:
    """Batch of messages returned by ShareConsumer.poll().

    Read-only sequence (iterate, len, index, slice) plus count()/is_empty()/
    records(). Not a list subclass on purpose -- a polled batch shouldn't be
    mutated, and a private backing list stays swappable later.
    """

    def __init__(self, messages=()):
        self._records = list(messages)

    @classmethod
    def _from_list(cls, records):
        # C poll already built the list -- take it as-is, no second copy.
        obj = cls.__new__(cls)
        obj._records = records
        return obj

    def records(self):
        # Hand out a copy so callers can't mutate the batch.
        return list(self._records)

    def count(self):
        return len(self._records)

    def is_empty(self):
        return not self._records

    def __len__(self):
        return len(self._records)

    def __iter__(self):
        return iter(self._records)

    def __getitem__(self, index):
        # Slices stay Messages -- a bare list would quietly lose the accessors.
        if isinstance(index, slice):
            return self._from_list(self._records[index])
        return self._records[index]

    def __repr__(self):
        return f"Messages({self._records!r})"
