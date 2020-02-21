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

import json

from fastavro._schema_common import PRIMITIVES
from fastavro.schema import parse_schema, schema_name


def loads(schema):
    """
    Create Schema object from a string

    :param str schema: Schema string to be parsed
    :returns: Confluent Schema object
    :rtype: Schema
    """
    if schema.lstrip()[0] != "{":
        schema = '{{"type": {} }}'.format(schema)

    return AvroSchema(json.loads(schema))


def load(avsc):
    """
    Create Schema Object from a file path

    :param avsc: Path to schema file to parse
    :returns: Confluent Schema object
    :rtype: Schema
    """
    with open(avsc) as fd:
        schema = json.load(fd)

    return AvroSchema(schema)


class AvroSchema(object):
    __slots__ = ["_hash", "schema", "name", "id", "subjects"]
    schema_type = "AVRO"

    def __init__(self, schema):
        """
        Schema enhances Avro Schema definitions for use with the Confluent Schema Registry.

        :param str schema: Parsed Schema to be used when serializing objects
        """
        self.id = -1
        self.subjects = set()
        self.schema = parse_schema(schema)

        if self.schema['type'] in PRIMITIVES:
            self.name = self.schema["type"]
        else:
            self.name = schema_name(self.schema, None)[1]

        # schema should be treated as an immutable object and thus there is no need to recalculate the hash.
        self._hash = hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)

    def __str__(self):
        return json.dumps(
            {key: value for key, value in self.schema.items()
                if key != "__fastavro_parsed"})

    def __hash__(self):
        return self._hash
