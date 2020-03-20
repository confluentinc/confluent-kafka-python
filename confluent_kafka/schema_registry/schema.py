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

PRIMITIVES = {'string', 'bytes', 'boolean', 'null',
              'int', 'double', 'float', 'long'}


class Schema(object):
    """
    An unregistered Schema.

    Args:
        schema (str): String representation of the schema.
        references [Schema]: Schema's referenced by this one.
        schema_type (str): The schema type: AVRO, PROTOBUF or JSON.

    """
    __slots__ = ['schema', 'references', 'schema_type']

    def __init__(self, schema, schema_type='AVRO', references=[]):
        super(Schema, self).__init__()

        self.schema = schema
        self.schema_type = schema_type
        self.references = references

    def __eq__(self, other):
        return str(self) == str(other)

    def __str__(self):
        return json.dumps(self.to_json(), sort_keys=True)

    # TODO: consider metaclass and/or decorators
    # JSON duck type handling
    def to_json(self):
        """
        Converts Schema to JSON serializable type (dict).
        Use with the standard JSONEncoder with the JSONDuckEncoder.


        Returns:
            dict: JSON serializable Schema

        """
        schema_dict = {'schema': self.schema}
        # new fields require schema registry 6.0+
        if len(self.references) == 0 and self.schema_type == 'AVRO':
            return schema_dict

        schema_dict['references'] = self.references
        schema_dict['schemaType'] = self.schema_type

        return schema_dict

    # TODO: consider metaclass and/or decorators
    # JSON duck type handling
    @staticmethod
    def from_json(json_dict):
        """
        Converts JSON object(dict) to Schema instance.

        Args:
            json_dict (dict): The dictionary returned from `json.loads`

        Returns:
            Schema: an instance of Schema

        Raises:
            KeyError: If JSON object is not representative of Schema
        """
        return Schema(schema=json_dict['schema'],
                      schema_type=json_dict.get('schemaType', 'AVRO'),
                      references=json_dict.get('references', []))


class RegisteredSchema(object):
    """
    Schema registration information.

    Args:
        schema_id = Schema registration id
        schema_type = Type of schema
        subject = Subject this schema is registered under
        version = Version of this subject this schema is registered to
    """

    def __init__(self, schema_id, schema_type, schema, subject, version):
        self.schema_id = schema_id
        self.schema_type = schema_type
        self.schema = schema
        self.subject = subject
        self.version = version

    def __str__(self):
        return json.dumps(self.to_json(), sort_keys=True)

    # TODO: consider metaclass and/or decorators
    # JSON duck type handling
    def to_json(self):
        """
        Converts RegisteredSchema to JSON serializable type (dict).

        Use with the standard JSONEncoder with the JSONDuckEncoder.

        Returns:
            dict: JSON serializable RegisteredSchema
        """
        return {'schema_id': self.schema_id,
                'schema_type': self.schema_type,
                'schema': self.schema,
                'subject': self.subject,
                'version': self.version}

    # TODO: consider metaclass and/or decorators
    # JSON duck type handling
    @staticmethod
    def from_json(json_dict):
        """
        Converts JSON object(dict) to RegisteredSchema instance.

        Args:
            json_dict (dict): The dictionary returned from `json.loads`

        Returns:
            RegisteredSchema: an instance of RegisteredSchema

        Raises:
            KeyError: If JSON object is not representative of SchemaReference
        """
        return RegisteredSchema(schema_id=json_dict['id'],
                                schema_type=json_dict.get('schemaType', 'AVRO'),
                                schema=json_dict['schema'],
                                subject=json_dict['subject'],
                                version=json_dict['version'])


class SchemaReference(object):
    """
    Reference to a Schema registered with the Schema Registry.

    Args:
        name (str): Schema name
        subject (str): Subject this Schema is registered with
        version (int): This Schema's version

    """
    def __init__(self, name, subject, version):
        super(SchemaReference, self).__init__()
        self.name = name
        self.subject = subject
        self.version = version

    def __str__(self):
        return json.dumps(self.to_json(), sort_keys=True)

    # TODO: consider metaclass and/or decorators
    # JSON duck type handling
    def to_json(self):
        """
        Converts SchemaReference to JSON serializable type (dict).

        Use with the standard JSONEncoder with the JSONDuckEncoder.

        Returns:
            dict: JSON serializable SchemaReference
        """
        return {'name': self.name,
                'subject': self.subject,
                'version': self.version}

    # TODO: consider metaclass and/or decorators
    # JSON duck type handling
    @staticmethod
    def from_json(json_dict):
        """
        Converts JSON object(dict) to SchemaReference instance.

        Returns:
            SchemaReference: an instance of SchemaReference

        Raises:
            KeyError: If JSON object is not representative of SchemaReference
        """
        return SchemaReference(
            name=json_dict['name'],
            subject=json_dict['subject'],
            version=json_dict['version']
        )
