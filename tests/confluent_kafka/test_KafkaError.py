#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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

import time

import pytest

from confluent_kafka import Producer, KafkaError, KafkaException

seen_all_brokers_down = False


def error_cb(err):
    print('error_cb', err)
    assert err.fatal() is False
    if err.code() == KafkaError._ALL_BROKERS_DOWN:
        global seen_all_brokers_down
        seen_all_brokers_down = True


def test_error_cb():
    """ Test the error callback. """

    global seen_all_brokers_down

    # Configure an invalid broker and make sure the ALL_BROKERS_DOWN
    # error is seen in the error callback.
    p = Producer({'bootstrap.servers': '127.0.0.1:1', 'socket.timeout.ms': 10,
                  'error_cb': error_cb})

    t_end = time.time() + 5

    while not seen_all_brokers_down and time.time() < t_end:
        p.poll(1)

    assert seen_all_brokers_down


def test_fatal():
    """ Test fatal exceptions """

    # Configure an invalid broker and make sure the ALL_BROKERS_DOWN
    # error is seen in the error callback.
    p = Producer({'error_cb': error_cb})

    with pytest.raises(KafkaException) as exc:
        KafkaError._test_raise_fatal()
    err = exc.value.args[0]
    assert isinstance(err, KafkaError)
    assert err.fatal() is True

    p.poll(0)  # Need some p use to avoid flake8 unused warning


def test_subclassing():
    class MyExc(KafkaException):
        def a_method(self):
            return "yes"
    err = MyExc()
    assert err.a_method() == "yes"
    assert isinstance(err, KafkaException)
