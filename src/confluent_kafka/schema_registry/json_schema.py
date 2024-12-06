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

import decimal
from io import BytesIO

import json
import struct
from typing import Union, Optional, List, Set, Tuple, Callable

import httpx
import referencing
from jsonschema import validate, ValidationError
from referencing import Registry, Resource
from referencing._core import Resolver

from confluent_kafka.schema_registry import (_MAGIC_BYTE,
                                             Schema,
                                             topic_subject_name_strategy,
                                             RuleKind,
                                             RuleMode, SchemaRegistryClient)
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.serde import BaseSerializer, \
    BaseDeserializer, RuleContext, FieldTransform, FieldType, \
    RuleConditionError, ParsedSchemaCache
from confluent_kafka.serialization import (SerializationError,
                                           SerializationContext)


JsonMessage = Union[
    None,  # 'null' Avro type
    str,  # 'string' and 'enum'
    float,  # 'float' and 'double'
    int,  # 'int' and 'long'
    decimal.Decimal,  # 'fixed'
    bool,  # 'boolean'
    list,  # 'array'
    dict,  # 'map' and 'record'
]

JsonSchema = Union[bool, dict]

DEFAULT_SPEC = referencing.jsonschema.DRAFT7


class _ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _retrieve_via_httpx(uri: str):
    response = httpx.get(uri)
    return Resource.from_contents(
        response.json(), default_specification=DEFAULT_SPEC)


def _resolve_named_schema(
    schema: Schema, schema_registry_client: SchemaRegistryClient,
    ref_registry: Registry = None
) -> Registry:
    """
    Resolves named schemas referenced by the provided schema recursively.
    :param schema: Schema to resolve named schemas for.
    :param schema_registry_client: SchemaRegistryClient to use for retrieval.
    :param ref_registry: Registry of named schemas resolved recursively.
    :return: Registry
    """
    if ref_registry is None:
        # Retrieve external schemas for backward compatibility
        ref_registry = Registry(retrieve=_retrieve_via_httpx)
    if schema.references is not None:
        for ref in schema.references:
            referenced_schema = schema_registry_client.get_version(ref.subject, ref.version, True)
            ref_registry = _resolve_named_schema(referenced_schema.schema, schema_registry_client, ref_registry)
            referenced_schema_dict = json.loads(referenced_schema.schema.schema_str)
            resource = Resource.from_contents(
                referenced_schema_dict, default_specification=DEFAULT_SPEC)
            ref_registry = ref_registry.with_resource(ref.name, resource)
    return ref_registry


class JSONSerializer(BaseSerializer):
    """
    Serializer that outputs JSON encoded data with Confluent Schema Registry framing.

    Configuration properties:

    +-----------------------------+----------+----------------------------------------------------+
    | Property Name               | Type     | Description                                        |
    +=============================+==========+====================================================+
    |                             |          | If True, automatically register the configured     |
    | ``auto.register.schemas``   | bool     | schema with Confluent Schema Registry if it has    |
    |                             |          | not previously been associated with the relevant   |
    |                             |          | subject (determined via subject.name.strategy).    |
    |                             |          |                                                    |
    |                             |          | Defaults to True.                                  |
    |                             |          |                                                    |
    |                             |          | Raises SchemaRegistryError if the schema was not   |
    |                             |          | registered against the subject, or could not be    |
    |                             |          | successfully registered.                           |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Whether to normalize schemas, which will           |
    | ``normalize.schemas``       | bool     | transform schemas to have a consistent format,     |
    |                             |          | including ordering properties and references.      |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Whether to use the latest subject version for      |
    | ``use.latest.version``      | bool     | serialization.                                     |
    |                             |          |                                                    |
    |                             |          | WARNING: There is no check that the latest         |
    |                             |          | schema is backwards compatible with the object     |
    |                             |          | being serialized.                                  |
    |                             |          |                                                    |
    |                             |          | Defaults to False.                                 |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Whether to use the latest subject version with     |
    | ``use.latest.with.metadata``| bool     | the given metadata.                                |
    |                             |          |                                                    |
    |                             |          | WARNING: There is no check that the latest         |
    |                             |          | schema is backwards compatible with the object     |
    |                             |          | being serialized.                                  |
    |                             |          |                                                    |
    |                             |          | Defaults to None.                                  |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Callable(SerializationContext, str) -> str         |
    |                             |          |                                                    |
    | ``subject.name.strategy``   | callable | Defines how Schema Registry subject names are      |
    |                             |          | constructed. Standard naming strategies are        |
    |                             |          | defined in the confluent_kafka.schema_registry     |
    |                             |          | namespace.                                         |
    |                             |          |                                                    |
    |                             |          | Defaults to topic_subject_name_strategy.           |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Whether to validate the payload against the        |
    | ``validate``                | bool     | the given schema.                                  |
    |                             |          |                                                    |
    +-----------------------------+----------+----------------------------------------------------+

    Schemas are registered against subject names in Confluent Schema Registry that
    define a scope in which the schemas can be evolved. By default, the subject name
    is formed by concatenating the topic name with the message field (key or value)
    separated by a hyphen.

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

    Notes:
        The ``title`` annotation, referred to elsewhere as a record name
        is not strictly required by the JSON Schema specification. It is
        however required by this serializer in order to register the schema
        with Confluent Schema Registry.

        Prior to serialization, all objects must first be converted to
        a dict instance. This may be handled manually prior to calling
        :py:func:`Producer.produce()` or by registering a `to_dict`
        callable with JSONSerializer.

    Args:
        schema_str (str, Schema):
            `JSON Schema definition. <https://json-schema.org/understanding-json-schema/reference/generic.html>`_
            Accepts schema as either a string or a :py:class:`Schema` instance.
            Note that string definitions cannot reference other schemas. For
            referencing other schemas, use a :py:class:`Schema` instance.

        schema_registry_client (SchemaRegistryClient): Schema Registry
            client instance.

        to_dict (callable, optional): Callable(object, SerializationContext) -> dict.
            Converts object to a dict.

        conf (dict): JsonSerializer configuration.
    """  # noqa: E501
    __slots__ = ['_known_subjects', '_parsed_schema', '_ref_registry',
                 '_schema', '_schema_id', '_schema_name', '_to_dict',
                 '_parsed_schemas', '_validate']

    _default_conf = {'auto.register.schemas': True,
                     'normalize.schemas': False,
                     'use.latest.version': False,
                     'use.latest.with.metadata': None,
                     'subject.name.strategy': topic_subject_name_strategy,
                     'validate': True}

    def __init__(
        self,
        schema_str: Union[str, Schema, None],
        schema_registry_client: SchemaRegistryClient,
        to_dict: Callable[[object, SerializationContext], dict] = None,
        conf: dict = None,
        rule_conf: dict = None,
        rule_registry: RuleRegistry = None
    ):
        super().__init__()
        if isinstance(schema_str, str):
            self._schema = Schema(schema_str, schema_type="JSON")
        elif isinstance(schema_str, Schema):
            self._schema = schema_str
        else:
            self._schema = None

        self._registry = schema_registry_client
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
        self._schema_id = None
        self._known_subjects = set()
        self._parsed_schemas = ParsedSchemaCache()

        if to_dict is not None and not callable(to_dict):
            raise ValueError("to_dict must be callable with the signature "
                             "to_dict(object, SerializationContext)->dict")

        self._to_dict = to_dict

        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = conf_copy.pop('auto.register.schemas')
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._normalize_schemas = conf_copy.pop('normalize.schemas')
        if not isinstance(self._normalize_schemas, bool):
            raise ValueError("normalize.schemas must be a boolean value")

        self._use_latest_version = conf_copy.pop('use.latest.version')
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")
        if self._use_latest_version and self._auto_register:
            raise ValueError("cannot enable both use.latest.version and auto.register.schemas")

        self._use_latest_with_metadata = conf_copy.pop('use.latest.with.metadata')
        if (self._use_latest_with_metadata is not None and
                not isinstance(self._use_latest_with_metadata, dict)):
            raise ValueError("use.latest.with.metadata must be a dict value")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        self._validate = conf_copy.pop('validate')
        if not isinstance(self._normalize_schemas, bool):
            raise ValueError("validate must be a boolean value")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        schema_dict, ref_registry = self._get_parsed_schema(self._schema)
        if schema_dict:
            schema_name = schema_dict.get('title', None)
        else:
            schema_name = None

        self._schema_name = schema_name
        self._parsed_schema = schema_dict
        self._ref_registry = ref_registry

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {},
                           rule_conf if rule_conf else {})

    def __call__(self, obj: object, ctx: SerializationContext = None) -> Optional[bytes]:
        """
        Serializes an object to JSON, prepending it with Confluent Schema Registry
        framing.

        Args:
            obj (object): The object instance to serialize.

            ctx (SerializationContext): Metadata relevant to the serialization
                operation.

        Raises:
            SerializerError if any error occurs serializing obj.

        Returns:
            bytes: None if obj is None, else a byte array containing the JSON
            serialized data with Confluent Schema Registry framing.
        """

        if obj is None:
            return None

        subject = self._subject_name_func(ctx, self._schema_name)
        latest_schema = self._get_reader_schema(subject)
        if latest_schema is not None:
            self._schema_id = latest_schema.schema_id
        elif subject not in self._known_subjects:
            # Check to ensure this schema has been registered under subject_name.
            if self._auto_register:
                # The schema name will always be the same. We can't however register
                # a schema without a subject so we set the schema_id here to handle
                # the initial registration.
                self._schema_id = self._registry.register_schema(subject,
                                                                 self._schema,
                                                                 self._normalize_schemas)
            else:
                registered_schema = self._registry.lookup_schema(subject,
                                                                 self._schema,
                                                                 self._normalize_schemas)
                self._schema_id = registered_schema.schema_id

            self._known_subjects.add(subject)

        if self._to_dict is not None:
            value = self._to_dict(obj, ctx)
        else:
            value = obj

        if latest_schema is not None:
            parsed_schema, ref_registry = self._get_parsed_schema(latest_schema.schema)
            root_resource = Resource.from_contents(
                parsed_schema, default_specification=DEFAULT_SPEC)
            ref_resolver = ref_registry.resolver_with_root(root_resource)
            field_transformer = lambda rule_ctx, field_transform, msg: (  # noqa: E731
                transform(rule_ctx, parsed_schema, ref_resolver, "$", msg, field_transform))
            value = self._execute_rules(ctx, subject, RuleMode.WRITE, None,
                                        latest_schema.schema, value, None,
                                        field_transformer)
        else:
            parsed_schema, ref_registry = self._parsed_schema, self._ref_registry

        if self._validate:
            try:
                if ref_registry:
                    validate(instance=value, schema=parsed_schema,
                             registry=ref_registry)
                else:
                    validate(instance=value, schema=parsed_schema)
            except ValidationError as ve:
                raise SerializationError(ve.message)

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(struct.pack('>bI', _MAGIC_BYTE, self._schema_id))
            # JSON dump always writes a str never bytes
            # https://docs.python.org/3/library/json.html
            fo.write(json.dumps(value).encode('utf8'))

            return fo.getvalue()

    def _get_parsed_schema(self, schema: Schema) -> Tuple[Optional[JsonSchema], Optional[Registry]]:
        if schema is None:
            return None, None

        result = self._parsed_schemas.get_parsed_schema(schema)
        if result is not None:
            return result

        ref_registry = _resolve_named_schema(schema, self._registry)
        parsed_schema = json.loads(schema.schema_str)

        self._parsed_schemas.set(schema, (parsed_schema, ref_registry))
        return parsed_schema, ref_registry


class JSONDeserializer(BaseDeserializer):
    """
    Deserializer for JSON encoded data with Confluent Schema Registry
    framing.

    Configuration properties:

    +-----------------------------+----------+----------------------------------------------------+
    | Property Name               | Type     | Description                                        |
    +=============================+==========+====================================================+
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Whether to use the latest subject version for      |
    | ``use.latest.version``      | bool     | deserialization.                                   |
    |                             |          |                                                    |
    |                             |          | Defaults to False.                                 |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Whether to use the latest subject version with     |
    | ``use.latest.with.metadata``| bool     | the given metadata.                                |
    |                             |          |                                                    |
    |                             |          | Defaults to None.                                  |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Callable(SerializationContext, str) -> str         |
    |                             |          |                                                    |
    | ``subject.name.strategy``   | callable | Defines how Schema Registry subject names are      |
    |                             |          | constructed. Standard naming strategies are        |
    |                             |          | defined in the confluent_kafka.schema_registry     |
    |                             |          | namespace.                                         |
    |                             |          |                                                    |
    |                             |          | Defaults to topic_subject_name_strategy.           |
    +-----------------------------+----------+----------------------------------------------------+
    |                             |          | Whether to validate the payload against the        |
    | ``validate``                | bool     | the given schema.                                  |
    |                             |          |                                                    |
    +-----------------------------+----------+----------------------------------------------------+

    Args:
        schema_str (str, Schema, optional):
            `JSON schema definition <https://json-schema.org/understanding-json-schema/reference/generic.html>`_
            Accepts schema as either a string or a :py:class:`Schema` instance.
            Note that string definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.  If not provided, schemas will be
            retrieved from schema_registry_client based on the schema ID in the
            wire header of each message.

        from_dict (callable, optional): Callable(dict, SerializationContext) -> object.
            Converts a dict to a Python object instance.

        schema_registry_client (SchemaRegistryClient, optional): Schema Registry client instance. Needed if ``schema_str`` is a schema referencing other schemas or is not provided.
    """  # noqa: E501

    __slots__ = ['_reader_schema', '_ref_registry', '_from_dict', '_schema',
                 '_parsed_schemas', '_validate']

    _default_conf = {'use.latest.version': False,
                     'use.latest.with.metadata': None,
                     'subject.name.strategy': topic_subject_name_strategy,
                     'validate': True}

    def __init__(
        self,
        schema_str: Union[str, Schema, None],
        from_dict: Callable[[dict, SerializationContext], object] = None,
        schema_registry_client: SchemaRegistryClient = None,
        conf: dict = None,
        rule_conf: dict = None,
        rule_registry: RuleRegistry = None
    ):
        super().__init__()
        if isinstance(schema_str, str):
            schema = Schema(schema_str, schema_type="JSON")
        elif isinstance(schema_str, Schema):
            schema = schema_str
            if bool(schema.references) and schema_registry_client is None:
                raise ValueError(
                    """schema_registry_client must be provided if "schema_str" is a Schema instance with references""")
        elif schema_str is None:
            if schema_registry_client is None:
                raise ValueError(
                    """schema_registry_client must be provided if "schema_str" is not provided"""
                )
            schema = schema_str
        else:
            raise TypeError('You must pass either str or Schema')

        self._schema = schema
        self._registry = schema_registry_client
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
        self._parsed_schemas = ParsedSchemaCache()

        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._use_latest_version = conf_copy.pop('use.latest.version')
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")

        self._use_latest_with_metadata = conf_copy.pop('use.latest.with.metadata')
        if (self._use_latest_with_metadata is not None and
                not isinstance(self._use_latest_with_metadata, dict)):
            raise ValueError("use.latest.with.metadata must be a dict value")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        self._validate = conf_copy.pop('validate')
        if not isinstance(self._validate, bool):
            raise ValueError("validate must be a boolean value")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        if schema:
            self._reader_schema, self._ref_registry = self._get_parsed_schema(self._schema)
        else:
            self._reader_schema, self._ref_registry = None, None

        if from_dict is not None and not callable(from_dict):
            raise ValueError("from_dict must be callable with the signature"
                             " from_dict(dict, SerializationContext) -> object")

        self._from_dict = from_dict

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {},
                           rule_conf if rule_conf else {})

    def __call__(self, data: bytes, ctx: SerializationContext = None) -> Union[dict, object, None]:
        """
        Deserialize a JSON encoded record with Confluent Schema Registry framing to
        a dict, or object instance according to from_dict if from_dict is specified.

        Args:
            data (bytes): A JSON serialized record with Confluent Schema Registry framing.

            ctx (SerializationContext): Metadata relevant to the serialization operation.

        Returns:
            A dict, or object instance according to from_dict if from_dict is specified.

        Raises:
            SerializerError: If there was an error reading the Confluent framing data, or
               if ``data`` was not successfully validated with the configured schema.
        """

        if data is None:
            return None

        if len(data) <= 5:
            raise SerializationError("Expecting data framing of length 6 bytes or "
                                     "more but total data size is {} bytes. This "
                                     "message was not produced with a Confluent "
                                     "Schema Registry serializer".format(len(data)))

        subject = self._subject_name_func(ctx, None)
        latest_schema = None
        if subject is not None and self._registry is not None:
            latest_schema = self._get_reader_schema(subject)

        with _ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unexpected magic byte {}. This message "
                                         "was not produced with a Confluent "
                                         "Schema Registry serializer".format(magic))

            # JSON documents are self-describing; no need to query schema
            obj_dict = json.loads(payload.read())

            if self._registry is not None:
                writer_schema_raw = self._registry.get_schema(schema_id)
                writer_schema, writer_ref_registry = self._get_parsed_schema(writer_schema_raw)
            else:
                writer_schema_raw = None
                writer_schema, writer_ref_registry = None, None

            if subject is None:
                subject = self._subject_name_func(ctx, writer_schema.get("title"))
                if subject is not None and self._registry is not None:
                    latest_schema = self._get_reader_schema(subject)

            if latest_schema is not None:
                migrations = self._get_migrations(subject, writer_schema_raw, latest_schema, None)
                reader_schema_raw = latest_schema.schema
                reader_schema, reader_ref_registry = self._get_parsed_schema(latest_schema.schema)
            elif self._schema is not None:
                migrations = None
                reader_schema_raw = self._schema
                reader_schema, reader_ref_registry = self._reader_schema, self._ref_registry
            else:
                migrations = None
                reader_schema_raw = writer_schema_raw
                reader_schema, reader_ref_registry = writer_schema, writer_ref_registry

            if migrations:
                obj_dict = self._execute_migrations(ctx, subject, migrations, obj_dict)

            reader_root_resource = Resource.from_contents(
                reader_schema, default_specification=DEFAULT_SPEC)
            reader_ref_resolver = reader_ref_registry.resolver_with_root(reader_root_resource)
            field_transformer = lambda rule_ctx, field_transform, message: (  # noqa: E731
                transform(rule_ctx, reader_schema, reader_ref_resolver, "$", message, field_transform))
            obj_dict = self._execute_rules(ctx, subject, RuleMode.READ, None,
                                           reader_schema_raw, obj_dict, None,
                                           field_transformer)

            if self._validate:
                try:
                    if reader_ref_registry:
                        validate(instance=obj_dict, schema=reader_schema,
                                 registry=reader_ref_registry)
                    else:
                        validate(instance=obj_dict, schema=reader_schema)
                except ValidationError as ve:
                    raise SerializationError(ve.message)

            if self._from_dict is not None:
                return self._from_dict(obj_dict, ctx)

            return obj_dict

    def _get_parsed_schema(self, schema: Schema) -> Tuple[Optional[JsonSchema], Optional[Registry]]:
        if schema is None:
            return None, None

        result = self._parsed_schemas.get_parsed_schema(schema)
        if result is not None:
            return result

        ref_registry = _resolve_named_schema(schema, self._registry)
        parsed_schema = json.loads(schema.schema_str)

        self._parsed_schemas.set(schema, (parsed_schema, ref_registry))
        return parsed_schema, ref_registry


def transform(
    ctx: RuleContext, schema: JsonSchema, ref_resolver: Resolver,
    path: str, message: JsonMessage, field_transform: FieldTransform
) -> Optional[JsonMessage]:
    if message is None or schema is None or isinstance(schema, bool):
        return message
    field_ctx = ctx.current_field()
    if field_ctx is not None:
        field_ctx.field_type = get_type(schema)
    all_of = schema.get("allOf")
    if all_of is not None:
        subschema = _validate_subschemas(all_of, message)
        if subschema is not None:
            return transform(ctx, subschema, ref_resolver, path, message, field_transform)
    any_of = schema.get("anyOf")
    if any_of is not None:
        subschema = _validate_subschemas(any_of, message)
        if subschema is not None:
            return transform(ctx, subschema, ref_resolver, path, message, field_transform)
    one_of = schema.get("oneOf")
    if one_of is not None:
        subschema = _validate_subschemas(one_of, message)
        if subschema is not None:
            return transform(ctx, subschema, ref_resolver, path, message, field_transform)
    items = schema.get("items")
    if items is not None:
        if isinstance(message, list):
            return [transform(ctx, items, ref_resolver, path, item, field_transform) for item in message]
    ref = schema.get("$ref")
    if ref is not None:
        ref_schema = ref_resolver.lookup(ref)
        return transform(ctx, ref_schema.contents, ref_resolver, path, message, field_transform)
    schema_type = get_type(schema)
    if schema_type == FieldType.RECORD:
        props = schema.get("properties")
        if props is not None:
            for prop_name, prop_schema in props.items():
                _transform_field(ctx, path, prop_name, message,
                                 prop_schema, ref_resolver, field_transform)
        return message
    if schema_type in (FieldType.ENUM, FieldType.STRING, FieldType.INT, FieldType.DOUBLE, FieldType.BOOLEAN):
        if field_ctx is not None:
            rule_tags = ctx.rule.tags
            if not rule_tags or not _disjoint(set(rule_tags), field_ctx.tags):
                return field_transform(ctx, field_ctx, message)
    return message


def _transform_field(
    ctx: RuleContext, path: str, prop_name: str, message: JsonMessage,
    prop_schema: JsonSchema, ref_resolver: Resolver, field_transform: FieldTransform
):
    full_name = path + "." + prop_name
    try:
        ctx.enter_field(
            message,
            full_name,
            prop_name,
            get_type(prop_schema),
            get_inline_tags(prop_schema)
        )
        value = message[prop_name]
        new_value = transform(ctx, prop_schema, ref_resolver, full_name, value, field_transform)
        if ctx.rule.kind == RuleKind.CONDITION:
            if new_value is False:
                raise RuleConditionError(ctx.rule)
        else:
            message[prop_name] = new_value
    finally:
        ctx.exit_field()


def _validate_subschemas(
    subschemas: List[JsonSchema],
    message: str
) -> Optional[JsonSchema]:
    for subschema in subschemas:
        try:
            validate(instance=message, schema=subschema)
            return subschema
        except ValidationError:
            pass
    return None


def get_type(schema: JsonSchema) -> FieldType:
    if isinstance(schema, list):
        return FieldType.COMBINED
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
    else:
        # string schemas; this could be either a named schema or a primitive type
        schema_type = schema

    if schema.get("const") is not None or schema.get("enum") is not None:
        return FieldType.ENUM
    if schema_type == "object":
        props = schema.get("properties")
        if not props:
            return FieldType.MAP
        return FieldType.RECORD
    if schema_type == "array":
        return FieldType.ARRAY
    if schema_type == "string":
        return FieldType.STRING
    if schema_type == "integer":
        return FieldType.INT
    if schema_type == "number":
        return FieldType.DOUBLE
    if schema_type == "boolean":
        return FieldType.BOOLEAN
    if schema_type == "null":
        return FieldType.NULL
    return FieldType.NULL


def _disjoint(tags1: Set[str], tags2: Set[str]) -> bool:
    for tag in tags1:
        if tag in tags2:
            return False
    return True


def get_inline_tags(schema: JsonSchema) -> Set[str]:
    tags = schema.get("confluent:tags")
    if tags is None:
        return set()
    else:
        return set(tags)
