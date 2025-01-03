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

import os
from confluent_kafka import Consumer

_GROUP_PROTOCOL_ENV = 'TEST_CONSUMER_GROUP_PROTOCOL'
_TRIVUP_CLUSTER_TYPE_ENV = 'TEST_TRIVUP_CLUSTER_TYPE'


def _update_conf_group_protocol(conf=None):
    if conf is not None and 'group.id' in conf and TestUtils.use_group_protocol_consumer():
        if 'group.protocol' not in conf:
            conf['group.protocol'] = 'consumer'


def _trivup_cluster_type_kraft():
    return _TRIVUP_CLUSTER_TYPE_ENV in os.environ and os.environ[_TRIVUP_CLUSTER_TYPE_ENV] == 'kraft'


class TestUtils:
    @staticmethod
    def use_kraft():
        return TestUtils.use_group_protocol_consumer() or _trivup_cluster_type_kraft()

    @staticmethod
    def use_group_protocol_consumer():
        return _GROUP_PROTOCOL_ENV in os.environ and os.environ[_GROUP_PROTOCOL_ENV] == 'consumer'

    @staticmethod
    def update_conf_group_protocol(conf=None):
        if conf is not None and 'group.id' in conf and TestUtils.use_group_protocol_consumer():
            conf['group.protocol'] = 'consumer'


class TestConsumer(Consumer):
    def __init__(self, conf=None, **kwargs):
        TestUtils.update_conf_group_protocol(conf)
        super(TestConsumer, self).__init__(conf, **kwargs)
