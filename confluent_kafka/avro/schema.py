#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
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

from avro.schema import PrimitiveSchema

# Python 2 considers int an instance of str
try:
    string_type = basestring  # noqa
except NameError:
    string_type = str


class GenericAvroRecord(dict):
    """
    Pairs an AvroRecord with it's schema

    :param schema schema: A parsed Avro schema.
    :param dict record: Wraps existing dict in GenericAvroRecord.
    :raises ValueError: If schema is None.
    :returns: Avro record with its schema.
    :rtype: GenericAvroRecord
    """
    __slots__ = ['schema']

    def __init__(self, schema, record=None):
        if schema is None:
            raise ValueError("schema must not be None")
        self.schema = schema
        if record is not None:
            self.update(record)

    def put(self, key, value):
        self[key] = value


def get_schema(datum):
    if isinstance(datum, GenericAvroRecord):
        return datum.schema
    elif (datum is None):
        return PrimitiveSchema("null")
    elif isinstance(datum, basestring):
        return PrimitiveSchema('string')
    elif isinstance(datum, bool):
        return PrimitiveSchema('boolean')
    elif isinstance(datum, int):
        return PrimitiveSchema('int')
    elif isinstance(datum, float):
        return PrimitiveSchema('float')
    else:
        raise ValueError("Unsupported Avro type {}.".format(type(datum)))
