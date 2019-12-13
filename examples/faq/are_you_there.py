#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
# limitations under the License.
#

from confluent_kafka import Producer, KafkaError


class ConfluentException(Exception):
    """
        Converts KafkaError into an exception for handling by an application.

        :param KafkaError kafka_error: The KafkaError to convert.
        :param str msg: Optional Error message override.
    """

    def __init__(self, kafka_error, msg=None):
        self.code = kafka_error.code()

        if msg is None:
            msg = kafka_error.str()

        self.message = msg


class DeliveryReporter(object):
    __slots__ = ["success"]

    def __init__(self):
        self.success = False

    def __call__(self, err, msg):
        """
        Handles per message delivery reports indicating success or failed delivery attempts.

        :param err: KafkaError indicating message delivery failed.
        :param msg: The message delivered to the broker.
        """
        if err:
            self.error(err)

        self.success = True
        print("Message {} delivered!".format(msg.offset()))

    def error(self, err):
        """
        Handler for global/generic and message delivery errors.

        Raises ConfluentException for `KafkaError.__ALL_BROKERS_DOWN` if seen before a successful delivery.
        Often times this is the result of a misconfiguration.
        """
        if not self.success and err.code() is KafkaError._ALL_BROKERS_DOWN:
            raise ConfluentException(err)

        print('Error: %s' % err)


if __name__ == "__main__":
    """
    This example is designed to demonstrate how a user might report broker connection failures.

    https://github.com/confluentinc/confluent-kafka-python/issues/705
    """

    dr = DeliveryReporter()

    p = Producer({'bootstrap.servers': 'incorrect.address',
                  'socket.timeout.ms': 10,
                  'retries': 0,
                  'error_cb': dr.error})

    try:
        for data in some_data_source:
            p.poll(0)
            p.produce('mytopic', data.encode('utf-8'), callback=dr)
    except ConfluentException:
        print("Failed to successfully send any messages prior to socket.timeout.ms")
        # Custom logic to handle retry, reporting, etc
