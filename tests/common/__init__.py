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
from confluent_kafka import Consumer, DeserializingConsumer
from confluent_kafka.avro import AvroConsumer

_GROUP_PROTOCOL_ENV = 'TEST_CONSUMER_GROUP_PROTOCOL'
_TRIVUP_CLUSTER_TYPE_ENV = 'TEST_TRIVUP_CLUSTER_TYPE'


def _trivup_cluster_type_kraft():
    return _TRIVUP_CLUSTER_TYPE_ENV in os.environ and os.environ[_TRIVUP_CLUSTER_TYPE_ENV] == 'kratf'


def use_group_protocol_consumer():
    return _GROUP_PROTOCOL_ENV in os.environ and os.environ[_GROUP_PROTOCOL_ENV] == 'consumer'


def use_kraft():
    return use_group_protocol_consumer() or _trivup_cluster_type_kraft()


def _get_consumer_generic(consumer_clazz, conf=None, **kwargs):
    if use_group_protocol_consumer():
        if conf is None:
            conf = {}
        conf['group.protocol'] = 'consumer'
    return consumer_clazz(conf, **kwargs)


def get_consumer(conf=None, **kwargs):
    return _get_consumer_generic(Consumer, conf, **kwargs)


def get_avro_consumer(conf=None, **kwargs):
    return _get_consumer_generic(AvroConsumer, conf, **kwargs)


def get_deserializing_consumer(conf=None, **kwargs):
    return _get_consumer_generic(DeserializingConsumer, conf, **kwargs)
