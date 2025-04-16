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

from confluent_kafka import DeserializingConsumer
from confluent_kafka.avro import AvroConsumer
from tests.common import TestUtils


class TestDeserializingConsumer(DeserializingConsumer):
    def __init__(self, conf=None, **kwargs):
        TestUtils.update_conf_group_protocol(conf)
        TestUtils.remove_forbidden_conf_group_protocol_consumer(conf)
        super(TestDeserializingConsumer, self).__init__(conf, **kwargs)


class TestAvroConsumer(AvroConsumer):
    def __init__(self, conf=None, **kwargs):
        TestUtils.update_conf_group_protocol(conf)
        TestUtils.remove_forbidden_conf_group_protocol_consumer(conf)
        super(TestAvroConsumer, self).__init__(conf, **kwargs)
