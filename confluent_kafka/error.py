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
# limitations under the License.
#
from confluent_kafka.cimpl import KafkaException, KafkaError

from confluent_kafka.serialization import SerializationError


class ConsumeError(KafkaException):
    """
    Wraps all errors encountered during the consumption of a message.

    Note:
        In the event of a serialization error the original message contents
        may be retrieved from the ``message`` attribute.

    Args:
        error_code (KafkaError): Error code indicating the type of error.

        exception(Exception, optional): The original exception

        message (Message, optional): The Kafka Message returned from the broker.

    """
    def __init__(self, error_code, exception=None, message=None):
        if exception is not None:
            kafka_error = KafkaError(error_code, repr(exception))
            self.exception = exception
        else:
            kafka_error = KafkaError(error_code)
            self.exception = None

        super(ConsumeError, self).__init__(kafka_error)
        self.message = message

    @property
    def code(self):
        return self.code()

    @property
    def name(self):
        return self.name()


class KeyDeserializationError(ConsumeError, SerializationError):
    """
    Wraps all errors encountered during the deserialization of a Kafka
    Message's key.

    Args:
        exception(Exception, optional): The original exception

        message (Message, optional): The Kafka Message returned from the broker.

    """
    def __init__(self, exception=None, message=None):
        super(KeyDeserializationError, self).__init__(
            KafkaError._KEY_DESERIALIZATION, exception=exception, message=message)


class ValueDeserializationError(ConsumeError, SerializationError):
    """
    Wraps all errors encountered during the deserialization of a Kafka
    Message's value.

    Args:
        exception(Exception, optional): The original exception

        message (Message, optional): The Kafka Message returned from the broker.

    """
    def __init__(self, exception=None, message=None):
        super(ValueDeserializationError, self).__init__(
            KafkaError._VALUE_DESERIALIZATION, exception=exception, message=message)


class ProduceError(KafkaException):
    """
    Wraps all errors encountered when Producing messages.

    Args:
        error_code (KafkaError): Error code indicating the type of error.

        exception(Exception, optional): The original exception.

    """
    def __init__(self, error_code, exception=None):
        if exception is not None:
            kafka_error = KafkaError(error_code, repr(exception))
            self.exception = exception
        else:
            kafka_error = KafkaError(error_code)
            self.exception = None

        super(ProduceError, self).__init__(kafka_error)

    @property
    def code(self):
        return self.code()

    @property
    def name(self):
        return self.name()


class KeySerializationError(ProduceError, SerializationError):
    """
    Wraps all errors encountered during the serialization of a Message key.

    Args:
        exception (Exception): The exception that occurred during serialization.
    """
    def __init__(self, exception=None):
        super(KeySerializationError, self).__init__(
            KafkaError._KEY_SERIALIZATION, exception=exception)


class ValueSerializationError(ProduceError, SerializationError):
    """
    Wraps all errors encountered during the serialization of a Message value.

    Args:
        exception (Exception): The exception that occurred during serialization.
    """
    def __init__(self, exception=None):
        super(ValueSerializationError, self).__init__(
            KafkaError._VALUE_SERIALIZATION, exception=exception)
