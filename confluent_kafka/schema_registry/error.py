#!/usr/bin/env python
#
# Copyright 2017 Confluent Inc.
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

from fastavro.schema import SchemaParseException, UnknownType

__all__ = ['SchemaRegistryError', 'SchemaParseException', 'UnknownType']


class SchemaRegistryError(BaseException):
    """
    Error thrown by Schema Registry clients

    Args:
        error_message (str) = description of th error

    Keyword Args:
        code (int, optional) = Schema Registry error code

    Note:
        In the event the Schema Registry hits an unexpected error ``code`` will
        be the HTTP code returnend from the server.

    See Also:
        https://docs.confluent.io/current/schema-registry/develop/api.html#errors

    """
    def __init__(self, error_message, code=None):
        self.error_message = error_message
        self.code = code
        print("{} {}".format(error_message, code))

    def __repr__(self):
        return self.error_message

    def __str__(self):
        return self.error_message
