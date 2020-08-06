#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
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


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#

from confluent_kafka.avro import ClientError


class MockSchemaRegistryClient(object):
    """
    A client that acts as a schema registry locally.

    Compatibiity related methods are not implemented at this time.
    """

    def __init__(self, max_schemas_per_subject=1000):
        self.max_schemas_per_subject = max_schemas_per_subject
        # subj => { schema => id }
        self.subject_to_schema_ids = {}
        # id => avro_schema
        self.id_to_schema = {}
        # subj => { schema => version }
        self.subject_to_schema_versions = {}

        self.subject_to_latest_schema = {}

        # counters
        self.next_id = 1
        self.schema_to_id = {}

        self.auto_register_schemas = True

    def _get_next_id(self, schema):
        if schema in self.schema_to_id:
            return self.schema_to_id[schema]
        result = self.next_id
        self.next_id += 1
        self.schema_to_id[schema] = result
        return result

    def _get_next_version(self, subject):
        if subject not in self.subject_to_schema_versions:
            self.subject_to_schema_versions[subject] = {}
        return len(self.subject_to_schema_versions[subject])

    def _get_all_versions(self, subject):
        versions = self.subject_to_schema_versions.get(subject, {})
        return sorted(versions)

    def _add_to_cache(self, cache, subject, schema, value):
        if subject not in cache:
            cache[subject] = {}
        sub_cache = cache[subject]
        sub_cache[schema] = value

    def _cache_schema(self, schema, schema_id, subject, version):
        # don't overwrite anything
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            self.id_to_schema[schema_id] = schema

        self._add_to_cache(self.subject_to_schema_ids,
                           subject, schema, schema_id)

        self._add_to_cache(self.subject_to_schema_versions,
                           subject, schema, version)

        if subject in self.subject_to_latest_schema:
            si, s, v = self.subject_to_latest_schema[subject]
            if v > version:
                return
        self.subject_to_latest_schema[subject] = (schema_id, schema, version)

    def register(self, subject, avro_schema):
        """
        Register a schema with the registry under the given subject
        and receive a schema id.

        avro_schema must be a parsed schema from the python avro library

        Multiple instances of the same schema will result in inconsistencies.
        """
        schemas_to_id = self.subject_to_schema_ids.get(subject, {})
        schema_id = schemas_to_id.get(avro_schema, -1)
        if schema_id != -1:
            return schema_id

        # add it
        version = self._get_next_version(subject)
        schema_id = self._get_next_id(avro_schema)

        # cache it
        self._cache_schema(avro_schema, schema_id, subject, version)
        return schema_id

    def check_registration(self, subject, avro_schema):
        """
        Check if a schema has already been registered under the specified subject.
        If so, returns the schema id. Otherwise, raises a ClientError.

        avro_schema must be a parsed schema from the python avro library

        Multiple instances of the same schema will result in inconsistencies.
        """
        schemas_to_id = self.subject_to_schema_ids.get(subject, {})
        schema_id = schemas_to_id.get(avro_schema, -1)
        if schema_id != -1:
            return schema_id

        version = self._get_next_version(subject) - 1

        # cache it
        self._cache_schema(avro_schema, schema_id, subject, version)
        return schema_id

    def get_by_id(self, schema_id):
        """Retrieve a parsed avro schema by id or None if not found"""
        return self.id_to_schema.get(schema_id, None)

    def get_by_version(self, subject, version):
        """
        Return the 3-tuple of:
        (the schema id, the parsed avro schema, the schema version)
        for a particular subject and version.

        If the subject or version are not found, (None,None,None) is returned.
        """
        schema_versions = self.subject_to_schema_versions.get(subject) or []
        # get schema+version by version
        schema_versions = [x for x in schema_versions.items() if x[1] == version]
        schema = None
        schema_version = None
        if len(schema_versions) > 0:
            schema = schema_versions[0][0]
            schema_version = schema_versions[0][1]

        # get schema_id by schema
        schema_id = self.subject_to_schema_ids.get(subject).get(schema)

        return (schema_id, schema, schema_version)

    def get_latest_schema(self, subject):
        """
        Return the latest 3-tuple of:
        (the schema id, the parsed avro schema, the schema version)
        for a particular subject.

        If the subject is not found, (None,None,None) is returned.
        """
        return self.subject_to_latest_schema.get(subject, (None, None, None))

    def get_version(self, subject, avro_schema):
        """
        Get the version of a schema for a given subject.

        Returns -1 if not found.
        """
        schemas_to_version = self.subject_to_schema_versions.get(subject, {})
        return schemas_to_version.get(avro_schema, -1)

    def get_id_for_schema(self, subject, avro_schema):
        """
        Get the ID of a parsed schema
        """
        schemas_to_id = self.subject_to_schema_ids.get(subject, {})
        return schemas_to_id.get(avro_schema, -1)

    def test_compatibility(self, subject, avro_schema, version='latest'):
        raise ClientError("not implemented")

    def update_compatibility(self, level, subject=None):
        raise ClientError("not implemented")

    def get_compatibility(self, subject=None):
        raise ClientError("not implemented")
