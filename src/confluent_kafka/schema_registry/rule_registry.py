#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
#

from typing import Optional, List

from confluent_kafka.schema_registry.serde import RuleExecutor, RuleAction


class RuleRegistry(object):
    __slots__ = ['_rule_executors', '_rule_actions']

    def __init__(self):
        self._rule_executors = {}
        self._rule_actions = {}

    def register_executor(self, rule_executor: RuleExecutor):
        self._rule_executors[rule_executor.type()] = rule_executor

    def get_executor(self, name: str) -> Optional[RuleExecutor]:
        return self._rule_executors.get(name)

    def get_executors(self) -> List[RuleExecutor]:
        return list(self._rule_executors.values())

    def register_action(self, rule_action: RuleAction):
        self._rule_actions[rule_action.type()] = rule_action

    def get_action(self, name: str) -> Optional[RuleAction]:
        return self._rule_actions.get(name)

    def get_actions(self) -> List[RuleAction]:
        return list(self._rule_actions.values())

    def clear(self):
        self._rule_executors.clear()
        self._rule_actions.clear()

    @staticmethod
    def get_global_instance():
        return _global_instance

    @staticmethod
    def register_rule_executor(rule_executor: RuleExecutor):
        _global_instance.register_executor(rule_executor)

    @staticmethod
    def register_rule_action(rule_action: RuleAction):
        _global_instance.register_action(rule_action)


_global_instance = RuleRegistry()
