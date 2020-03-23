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


class Schema(object):
    """
    An unregistered Schema.

    Args:
        schema_str (str): String representation of the schema.
        references ([Schema]): Schemas referenced by this schema.
        schema_type (str): The schema type: AVRO, PROTOBUF or JSON.

    """
    __slots__ = ['schema_str', 'references', 'schema_type']

    def __init__(self, schema_str, schema_type, references=[]):
        super(Schema, self).__init__()

        self.schema_str = schema_str
        self.schema_type = schema_type
        self.references = references

    def __eq__(self, other):
        return str(self) == str(other)


class RegisteredSchema(object):
    """
    Schema registration information.

    As of Confluent Platform 5.5 Schema's may now hold references to other
    registered schemas.

    The class represents a reference to another schema which is used by this
    schema. As long as there is a references to a schema it may not be deleted.

    Args:
        schema_id (int): Registered Schema id
        schema_type (str): Type of schema
        schema (Schema): Registered Schema
        subject (str): Subject this schema is registered under
        version (int): Version of this subject this schema is registered to

    """
    def __init__(self, schema_id, schema_type, schema, subject, version):
        self.schema_id = schema_id
        self.schema_type = schema_type
        self.schema = schema
        self.subject = subject
        self.version = version


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
