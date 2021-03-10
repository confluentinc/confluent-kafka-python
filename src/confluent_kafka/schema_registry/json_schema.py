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

from io import BytesIO

import json
import struct

from jsonschema import validate, ValidationError

from confluent_kafka.schema_registry import (_MAGIC_BYTE,
                                             Schema,
                                             topic_subject_name_strategy)
from confluent_kafka.serialization import (SerializationError,
                                           Deserializer,
                                           Serializer)


class _ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.

    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class JSONSerializer(Serializer):
    """
    JsonSerializer serializes objects in the Confluent Schema Registry binary
    format for JSON.

    JsonSerializer configuration properties:

    +---------------------------+----------+--------------------------------------------------+
    | Property Name             | Type     | Description                                      |
    +===========================+==========+==================================================+
    |                           |          | Registers schemas automatically if not           |
    | ``auto.register.schemas`` | bool     | previously associated with a particular subject. |
    |                           |          | Defaults to True.                                |
    +---------------------------+----------+--------------------------------------------------+
    |                           |          | Callable(SerializationContext, str) -> str       |
    |                           |          |                                                  |
    | ``subject.name.strategy`` | callable | Instructs the JsonSerializer on how to construct |
    |                           |          | Schema Registry subject names.                   |
    |                           |          | Defaults to topic_subject_name_strategy.         |
    +---------------------------+----------+--------------------------------------------------+

    Schemas are registered to namespaces known as Subjects which define how a
    schema may evolve over time. By default the subject name is formed by
    concatenating the topic name with the message field separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    ``subject.name.strategy``.

    Supported subject name strategies:

    +--------------------------------------+------------------------------+
    | Subject Name Strategy                | Output Format                |
    +======================================+==============================+
    | topic_subject_name_strategy(default) | {topic name}-{message field} |
    +--------------------------------------+------------------------------+
    | topic_record_subject_name_strategy   | {topic name}-{record name}   |
    +--------------------------------------+------------------------------+
    | record_subject_name_strategy         | {record name}                |
    +--------------------------------------+------------------------------+

    See `Subject name strategy <https://docs.confluent.io/current/schema-registry/serializer-formatter.html#subject-name-strategy>`_ for additional details.

    Note:
        The ``title`` annotation, referred to as a record name
        elsewhere in this document, is not strictly required by the JSON Schema
        specification. It is however required by this Serializer. This
        annotation(record name) is used to register the Schema with the Schema
        Registry. See documentation below for additional details on Subjects
        and schema registration.

    Args:
        schema_str (str): `JSON Schema definition. <https://json-schema.org/understanding-json-schema/reference/generic.html>`_

        schema_registry_client (SchemaRegistryClient): Schema Registry
            client instance.

        to_dict (callable, optional): Callable(object, SerializationContext) -> dict.
            Converts object to a dict.

        conf (dict): JsonSerializer configuration.

    """  # noqa: E501
    __slots__ = ['_hash', '_auto_register', '_known_subjects', '_parsed_schema',
                 '_registry', '_schema', '_schema_id', '_schema_name',
                 '_subject_name_func', '_to_dict']

    # default configuration
    _default_conf = {'auto.register.schemas': True,
                     'subject.name.strategy': topic_subject_name_strategy}

    def __init__(self, schema_str, schema_registry_client, to_dict=None,
                 conf=None):
        self._registry = schema_registry_client
        self._schema_id = None
        # Avoid calling registry if schema is known to be registered
        self._known_subjects = set()

        if to_dict is not None and not callable(to_dict):
            raise ValueError("to_dict must be callable with the signature"
                             " to_dict(object, SerializationContext)->dict")

        self._to_dict = to_dict

        # handle configuration
        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = conf_copy.pop('auto.register.schemas')
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        schema_dict = json.loads(schema_str)
        schema_name = schema_dict.get('title', None)
        if schema_name is None:
            raise ValueError("Missing required JSON schema annotation title")

        self._schema_name = schema_name
        self._parsed_schema = schema_dict
        self._schema = Schema(schema_str, schema_type="JSON")

    def __call__(self, obj, ctx):
        """
        Serializes an object to the Confluent Schema Registry's JSON binary
        format.

        Args:
            obj (object): object instance to serialize.

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if any error occurs serializing obj

        Returns:
            bytes: Confluent Schema Registry formatted JSON bytes

        """
        if obj is None:
            return None

        subject = self._subject_name_func(ctx, self._schema_name)

        # Check to ensure this schema has been registered under subject_name.
        if self._auto_register and subject not in self._known_subjects:
            # The schema name will always be the same. We can't however register
            # a schema without a subject so we set the schema_id here to handle
            # the initial registration.
            self._schema_id = self._registry.register_schema(subject,
                                                             self._schema)
            self._known_subjects.add(subject)
        elif not self._auto_register and subject not in self._known_subjects:
            registered_schema = self._registry.lookup_schema(subject,
                                                             self._schema)
            self._schema_id = registered_schema.schema_id
            self._known_subjects.add(subject)

        if self._to_dict is not None:
            value = self._to_dict(obj, ctx)
        else:
            value = obj

        try:
            validate(instance=value, schema=self._parsed_schema)
        except ValidationError as ve:
            raise SerializationError(ve.message)

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(struct.pack('>bI', _MAGIC_BYTE, self._schema_id))
            # JSON dump always writes a str never bytes
            # https://docs.python.org/3/library/json.html
            fo.write(json.dumps(value).encode('utf8'))

            return fo.getvalue()


class JSONDeserializer(Deserializer):
    """
    JsonDeserializer decodes bytes written in the Schema Registry
    JSON format to an object.

    Args:
        schema_str (str): `JSON schema definition <https://json-schema.org/understanding-json-schema/reference/generic.html>`_ use for validating records.

        from_dict (callable, optional): Callable(dict, SerializationContext) -> object.
            Converts dict to an instance of some object.

    """  # noqa: E501
    __slots__ = ['_parsed_schema', '_from_dict']

    def __init__(self, schema_str, from_dict=None):
        self._parsed_schema = json.loads(schema_str)

        if from_dict is not None and not callable(from_dict):
            raise ValueError("from_dict must be callable with the signature"
                             " from_dict(dict, SerializationContext) -> object")

        self._from_dict = from_dict

    def __call__(self, value, ctx):
        """
        Deserializes Schema Registry formatted JSON to JSON object literal(dict).

        Args:
            value (bytes): Confluent Schema Registry formatted JSON bytes

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Returns:
            dict: Deserialized JSON

        Raises:
            SerializerError: If ``value`` cannot be validated by the schema
                configured with this JsonDeserializer instance.

        """
        if value is None:
            return None

        if len(value) <= 5:
            raise SerializationError("Message too small. This message was not"
                                     " produced with a Confluent"
                                     " Schema Registry serializer")

        with _ContextStringIO(value) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unknown magic byte. This message was"
                                         " not produced with a Confluent"
                                         " Schema Registry serializer")

            # JSON documents are self-describing; no need to query schema
            obj_dict = json.loads(payload.read())

            try:
                validate(instance=obj_dict, schema=self._parsed_schema)
            except ValidationError as ve:
                raise SerializationError(ve.message)

            if self._from_dict is not None:
                return self._from_dict(obj_dict, ctx)

            return obj_dict
