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


class ConsumeError(Exception):
    __slots__ = ['message', 'error_message', 'error_code']

    """
    Wraps all errors encountered during the consumption of a message.

    Note:
        In the event of a serialization error the original message contents
        may be retrieved from the ``message`` attribute.

    Args:
        error (KafkaError): The error that occurred

    Keyword Args:
        message (Message, optional): The message returned from the broker.
        error_message (str): String description of the error.

    """
    def __init__(self, error, reason=None, message=None):
        self.error = error
        if reason is None:
            reason = error.str()

        self.error_message = reason
        self.message = message

    def __repr__(self):
        return '{klass}(error={error})'.format(
            klass=self.__class__.__name__,
            error=self.message
        )

    def __str__(self):
        return self.message
