#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
# limit
#

from confluent_kafka import KafkaError
from confluent_kafka.error import ConsumeError, \
    ProduceError


def test_new_consume_error_constant():
    ce = ConsumeError(KafkaError(KafkaError._PARTITION_EOF))

    assert ce.code == KafkaError._PARTITION_EOF
    assert ce.name == u'_PARTITION_EOF'


def test_new_consume_error_caused_by():
    ce = ConsumeError(KafkaError(KafkaError.INVALID_CONFIG),
                      exception=ValueError())

    assert ce.code == KafkaError.INVALID_CONFIG
    assert ce.name == u'INVALID_CONFIG'
    assert isinstance(ce.exception, ValueError)


def test_new_consume_error_custom_message():
    ce = ConsumeError(KafkaError(KafkaError._KEY_SERIALIZATION,
                                 "Unable to serialize key"))

    assert ce.code == KafkaError._KEY_SERIALIZATION
    assert ce.name == u'_KEY_SERIALIZATION'
    assert ce.args[0].str() == "Unable to serialize key"


def test_new_produce_error_constant():
    pe = ProduceError(KafkaError(KafkaError._PARTITION_EOF))

    assert pe.code == KafkaError._PARTITION_EOF
    assert pe.name == u'_PARTITION_EOF'


def test_new_produce_error_caused_by():
    pe = ProduceError(KafkaError(KafkaError.INVALID_CONFIG),
                      exception=ValueError())

    assert pe.code == KafkaError.INVALID_CONFIG
    assert pe.name == u'INVALID_CONFIG'
    assert isinstance(pe.exception, ValueError)


def test_new_produce_error_custom_message():
    pe = ProduceError(KafkaError(KafkaError._KEY_SERIALIZATION,
                                 "Unable to serialize key"))

    assert pe.code == KafkaError._KEY_SERIALIZATION
    assert pe.name == u'_KEY_SERIALIZATION'
    assert pe.args[0].str() == "Unable to serialize key"
