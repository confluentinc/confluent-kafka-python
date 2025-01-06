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
import re
from collections import defaultdict
from io import BytesIO
from json import loads
from struct import pack, unpack
from typing import Dict, Union, Optional, Set, Callable

from fastavro import (parse_schema,
                      schemaless_reader,
                      schemaless_writer,
                      validate)

from . import (_MAGIC_BYTE,
               Schema,
               topic_subject_name_strategy,
               RuleMode,
               RuleKind, SchemaRegistryClient)
from confluent_kafka.serialization import (SerializationError,
                                           SerializationContext)
from .rule_registry import RuleRegistry
from .serde import BaseSerializer, BaseDeserializer, RuleContext, FieldType, \
    FieldTransform, RuleConditionError, ParsedSchemaCache


AvroMessage = Union[
    None,  # 'null' Avro type
    str,  # 'string' and 'enum'
    float,  # 'float' and 'double'
    int,  # 'int' and 'long'
    decimal.Decimal,  # 'fixed'
    bool,  # 'boolean'
    bytes,  # 'bytes'
    list,  # 'array'
    dict,  # 'map' and 'record'
]
AvroSchema = Union[str, list, dict]


class _ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _schema_loads(schema_str: str) -> Schema:
    """
    Instantiate a Schema instance from a declaration string.

    Args:
        schema_str (str): Avro Schema declaration.

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    Returns:
        Schema: A Schema instance.
    """

    schema_str = schema_str.strip()

    # canonical form primitive declarations are not supported
    if schema_str[0] != "{" and schema_str[0] != "[":
        schema_str = '{"type":' + schema_str + '}'

    return Schema(schema_str, schema_type='AVRO')


def _resolve_named_schema(
    schema: Schema, schema_registry_client: SchemaRegistryClient
) -> Dict[str, AvroSchema]:
    """
    Resolves named schemas referenced by the provided schema recursively.
    :param schema: Schema to resolve named schemas for.
    :param schema_registry_client: SchemaRegistryClient to use for retrieval.
    :return: named_schemas dict.
    """
    named_schemas = {}
    if schema.references is not None:
        for ref in schema.references:
            referenced_schema = schema_registry_client.get_version(ref.subject, ref.version, True)
            ref_named_schemas = _resolve_named_schema(referenced_schema.schema, schema_registry_client)
            parsed_schema = parse_schema(loads(referenced_schema.schema.schema_str), named_schemas=ref_named_schemas)
            named_schemas.update(ref_named_schemas)
            named_schemas[ref.name] = parsed_schema
    return named_schemas


class AvroSerializer(BaseSerializer):
    """
    Serializer that outputs Avro binary encoded data with Confluent Schema Registry framing.

    Configuration properties:

    +-----------------------------+----------+--------------------------------------------------+
    | Property Name               | Type     | Description                                      |
    +=============================+==========+==================================================+
    |                             |          | If True, automatically register the configured   |
    | ``auto.register.schemas``   | bool     | schema with Confluent Schema Registry if it has  |
    |                             |          | not previously been associated with the relevant |
    |                             |          | subject (determined via subject.name.strategy).  |
    |                             |          |                                                  |
    |                             |          | Defaults to True.                                |
    +-----------------------------+----------+--------------------------------------------------+
    |                             |          | Whether to normalize schemas, which will         |
    | ``normalize.schemas``       | bool     | transform schemas to have a consistent format,   |
    |                             |          | including ordering properties and references.    |
    +-----------------------------+----------+--------------------------------------------------+
    |                             |          | Whether to use the latest subject version for    |
    | ``use.latest.version``      | bool     | serialization.                                   |
    |                             |          |                                                  |
    |                             |          | WARNING: There is no check that the latest       |
    |                             |          | schema is backwards compatible with the object   |
    |                             |          | being serialized.                                |
    |                             |          |                                                  |
    |                             |          | Defaults to False.                               |
    +-----------------------------+----------+--------------------------------------------------+
    |                             |          | Whether to use the latest subject version with   |
    | ``use.latest.with.metadata``| bool     | the given metadata.                              |
    |                             |          |                                                  |
    |                             |          | WARNING: There is no check that the latest       |
    |                             |          | schema is backwards compatible with the object   |
    |                             |          | being serialized.                                |
    |                             |          |                                                  |
    |                             |          | Defaults to None.                                |
    +-----------------------------+----------+--------------------------------------------------+
    |                             |          | Callable(SerializationContext, str) -> str       |
    |                             |          |                                                  |
    | ``subject.name.strategy``   | callable | Defines how Schema Registry subject names are    |
    |                             |          | constructed. Standard naming strategies are      |
    |                             |          | defined in the confluent_kafka.schema_registry   |
    |                             |          | namespace.                                       |
    |                             |          |                                                  |
    |                             |          | Defaults to topic_subject_name_strategy.         |
    +-----------------------------+----------+--------------------------------------------------+

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

    Note:
        Prior to serialization, all values must first be converted to
        a dict instance. This may handled manually prior to calling
        :py:func:`Producer.produce()` or by registering a `to_dict`
        callable with AvroSerializer.

        See ``avro_producer.py`` in the examples directory for example usage.

    Note:
       Tuple notation can be used to determine which branch of an ambiguous union to take.

       See `fastavro notation <https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take>`_

    Args:
        schema_registry_client (SchemaRegistryClient): Schema Registry client instance.

        schema_str (str or Schema):
            Avro `Schema Declaration. <https://avro.apache.org/docs/current/spec.html#schemas>`_
            Accepts either a string or a :py:class:`Schema` instance. Note that string
            definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.

        to_dict (callable, optional): Callable(object, SerializationContext) -> dict. Converts object to a dict.

        conf (dict): AvroSerializer configuration.
    """  # noqa: E501
    __slots__ = ['_known_subjects', '_parsed_schema', '_schema',
                 '_schema_id', '_schema_name', '_to_dict', '_parsed_schemas']

    _default_conf = {'auto.register.schemas': True,
                     'normalize.schemas': False,
                     'use.latest.version': False,
                     'use.latest.with.metadata': None,
                     'subject.name.strategy': topic_subject_name_strategy}

    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        schema_str: Union[str, Schema, None] = None,
        to_dict: Callable[[object, SerializationContext], dict] = None,
        conf: dict = None,
        rule_conf: dict = None,
        rule_registry: RuleRegistry = None
    ):
        super().__init__()
        if isinstance(schema_str, str):
            schema = _schema_loads(schema_str)
        elif isinstance(schema_str, Schema):
            schema = schema_str
        else:
            schema = None

        self._registry = schema_registry_client
        self._schema_id = None
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
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

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        if schema:
            parsed_schema = self._get_parsed_schema(schema)

            if isinstance(parsed_schema, list):
                # if parsed_schema is a list, we have an Avro union and there
                # is no valid schema name. This is fine because the only use of
                # schema_name is for supplying the subject name to the registry
                # and union types should use topic_subject_name_strategy, which
                # just discards the schema name anyway
                schema_name = None
            else:
                # The Avro spec states primitives have a name equal to their type
                # i.e. {"type": "string"} has a name of string.
                # This function does not comply.
                # https://github.com/fastavro/fastavro/issues/415
                schema_dict = loads(schema.schema_str)
                schema_name = parsed_schema.get("name", schema_dict.get("type"))
        else:
            schema_name = None
            parsed_schema = None

        self._schema = schema
        self._schema_name = schema_name
        self._parsed_schema = parsed_schema

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {},
                           rule_conf if rule_conf else {})

    def __call__(self, obj: object, ctx: SerializationContext = None) -> Optional[bytes]:
        """
        Serializes an object to Avro binary format, prepending it with Confluent
        Schema Registry framing.

        Args:
            obj (object): The object instance to serialize.

            ctx (SerializationContext): Metadata pertaining to the serialization operation.

        Raises:
            SerializerError: If any error occurs serializing obj.
            SchemaRegistryError: If there was an error registering the schema with
                                 Schema Registry, or auto.register.schemas is
                                 false and the schema was not registered.

        Returns:
            bytes: Confluent Schema Registry encoded Avro bytes
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
                self._schema_id = self._registry.register_schema(
                    subject, self._schema, self._normalize_schemas)
            else:
                registered_schema = self._registry.lookup_schema(
                    subject, self._schema, self._normalize_schemas)
                self._schema_id = registered_schema.schema_id

            self._known_subjects.add(subject)

        if self._to_dict is not None:
            value = self._to_dict(obj, ctx)
        else:
            value = obj

        if latest_schema is not None:
            parsed_schema = self._get_parsed_schema(latest_schema.schema)
            field_transformer = lambda rule_ctx, field_transform, msg: (  # noqa: E731
                transform(rule_ctx, parsed_schema, msg, field_transform))
            value = self._execute_rules(ctx, subject, RuleMode.WRITE, None,
                                        latest_schema.schema, value, get_inline_tags(parsed_schema),
                                        field_transformer)
        else:
            parsed_schema = self._parsed_schema

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(pack('>bI', _MAGIC_BYTE, self._schema_id))
            # write the record to the rest of the buffer
            schemaless_writer(fo, parsed_schema, value)

            return fo.getvalue()

    def _get_parsed_schema(self, schema: Schema) -> AvroSchema:
        parsed_schema = self._parsed_schemas.get_parsed_schema(schema)
        if parsed_schema is not None:
            return parsed_schema

        named_schemas = _resolve_named_schema(schema, self._registry)
        prepared_schema = _schema_loads(schema.schema_str)
        parsed_schema = parse_schema(
            loads(prepared_schema.schema_str), named_schemas=named_schemas, expand=True)

        self._parsed_schemas.set(schema, parsed_schema)
        return parsed_schema


class AvroDeserializer(BaseDeserializer):
    """
    Deserializer for Avro binary encoded data with Confluent Schema Registry
    framing.

    +-----------------------------+----------+--------------------------------------------------+
    | Property Name               | Type     | Description                                      |
    +-----------------------------+----------+--------------------------------------------------+
    |                             |          | Whether to use the latest subject version for    |
    | ``use.latest.version``      | bool     | deserialization.                                 |
    |                             |          |                                                  |
    |                             |          | Defaults to False.                               |
    +-----------------------------+----------+--------------------------------------------------+
    |                             |          | Whether to use the latest subject version with   |
    | ``use.latest.with.metadata``| bool     | the given metadata.                              |
    |                             |          |                                                  |
    |                             |          | Defaults to None.                                |
    +-----------------------------+----------+--------------------------------------------------+
    |                             |          | Callable(SerializationContext, str) -> str       |
    |                             |          |                                                  |
    | ``subject.name.strategy``   | callable | Defines how Schema Registry subject names are    |
    |                             |          | constructed. Standard naming strategies are      |
    |                             |          | defined in the confluent_kafka.schema_registry   |
    |                             |          | namespace.                                       |
    |                             |          |                                                  |
    |                             |          | Defaults to topic_subject_name_strategy.         |
    +-----------------------------+----------+--------------------------------------------------+

    Note:
        By default, Avro complex types are returned as dicts. This behavior can
        be overridden by registering a callable ``from_dict`` with the deserializer to
        convert the dicts to the desired type.

        See ``avro_consumer.py`` in the examples directory in the examples
        directory for example usage.

    Args:
        schema_registry_client (SchemaRegistryClient): Confluent Schema Registry
            client instance.

        schema_str (str, Schema, optional): Avro reader schema declaration Accepts
            either a string or a :py:class:`Schema` instance. If not provided, the
            writer schema will be used as the reader schema. Note that string
            definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.

        from_dict (callable, optional): Callable(dict, SerializationContext) -> object.
            Converts a dict to an instance of some object.

        return_record_name (bool): If True, when reading a union of records, the result will
                                   be a tuple where the first value is the name of the record and the second value is
                                   the record itself.  Defaults to False.

    See Also:
        `Apache Avro Schema Declaration <https://avro.apache.org/docs/current/spec.html#schemas>`_

        `Apache Avro Schema Resolution <https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution>`_
    """

    __slots__ = ['_reader_schema', '_from_dict', '_return_record_name',
                 '_schema', '_parsed_schemas']

    _default_conf = {'use.latest.version': False,
                     'use.latest.with.metadata': None,
                     'subject.name.strategy': topic_subject_name_strategy}

    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        schema_str: Union[str, Schema, None] = None,
        from_dict: Callable[[dict, SerializationContext], object] = None,
        return_record_name: bool = False,
        conf: dict = None,
        rule_conf: dict = None,
        rule_registry: RuleRegistry = None
    ):
        super().__init__()
        schema = None
        if schema_str is not None:
            if isinstance(schema_str, str):
                schema = _schema_loads(schema_str)
            elif isinstance(schema_str, Schema):
                schema = schema_str
            else:
                raise TypeError('You must pass either schema string or schema object')

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

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        if schema:
            self._reader_schema = self._get_parsed_schema(self._schema)
        else:
            self._reader_schema = None

        if from_dict is not None and not callable(from_dict):
            raise ValueError("from_dict must be callable with the signature "
                             "from_dict(SerializationContext, dict) -> object")
        self._from_dict = from_dict

        self._return_record_name = return_record_name
        if not isinstance(self._return_record_name, bool):
            raise ValueError("return_record_name must be a boolean value")

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {},
                           rule_conf if rule_conf else {})

    def __call__(self, data: bytes, ctx: SerializationContext = None) -> Union[dict, object, None]:
        """
        Deserialize Avro binary encoded data with Confluent Schema Registry framing to
        a dict, or object instance according to from_dict, if specified.

        Arguments:
            data (bytes): bytes

            ctx (SerializationContext): Metadata relevant to the serialization
                operation.

        Raises:
            SerializerError: if an error occurs parsing data.

        Returns:
            object: If data is None, then None. Else, a dict, or object instance according
                    to from_dict, if specified.
        """  # noqa: E501

        if data is None:
            return None

        if len(data) <= 5:
            raise SerializationError("Expecting data framing of length 6 bytes or "
                                     "more but total data size is {} bytes. This "
                                     "message was not produced with a Confluent "
                                     "Schema Registry serializer".format(len(data)))

        subject = self._subject_name_func(ctx, None)
        latest_schema = None
        if subject is not None:
            latest_schema = self._get_reader_schema(subject)

        with _ContextStringIO(data) as payload:
            magic, schema_id = unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unexpected magic byte {}. This message "
                                         "was not produced with a Confluent "
                                         "Schema Registry serializer".format(magic))

            writer_schema_raw = self._registry.get_schema(schema_id)
            writer_schema = self._get_parsed_schema(writer_schema_raw)

            if subject is None:
                subject = self._subject_name_func(ctx, writer_schema.get("name"))
                if subject is not None:
                    latest_schema = self._get_reader_schema(subject)

            if latest_schema is not None:
                migrations = self._get_migrations(subject, writer_schema_raw, latest_schema, None)
                reader_schema_raw = latest_schema.schema
                reader_schema = self._get_parsed_schema(latest_schema.schema)
            elif self._schema is not None:
                migrations = None
                reader_schema_raw = self._schema
                reader_schema = self._reader_schema
            else:
                migrations = None
                reader_schema_raw = writer_schema_raw
                reader_schema = writer_schema

            if migrations:
                obj_dict = schemaless_reader(payload,
                                             writer_schema,
                                             None,
                                             self._return_record_name)
                obj_dict = self._execute_migrations(ctx, subject, migrations, obj_dict)
            else:
                obj_dict = schemaless_reader(payload,
                                             writer_schema,
                                             reader_schema,
                                             self._return_record_name)

            field_transformer = lambda rule_ctx, field_transform, message: (  # noqa: E731
                transform(rule_ctx, reader_schema, message, field_transform))
            obj_dict = self._execute_rules(ctx, subject, RuleMode.READ, None,
                                           reader_schema_raw, obj_dict, get_inline_tags(reader_schema),
                                           field_transformer)

            if self._from_dict is not None:
                return self._from_dict(obj_dict, ctx)

            return obj_dict

    def _get_parsed_schema(self, schema: Schema) -> AvroSchema:
        parsed_schema = self._parsed_schemas.get_parsed_schema(schema)
        if parsed_schema is not None:
            return parsed_schema

        named_schemas = _resolve_named_schema(schema, self._registry)
        prepared_schema = _schema_loads(schema.schema_str)
        parsed_schema = parse_schema(
            loads(prepared_schema.schema_str), named_schemas=named_schemas, expand=True)

        self._parsed_schemas.set(schema, parsed_schema)
        return parsed_schema


def transform(
    ctx: RuleContext, schema: AvroSchema, message: AvroMessage,
    field_transform: FieldTransform
) -> AvroMessage:
    if message is None or schema is None:
        return message
    field_ctx = ctx.current_field()
    if field_ctx is not None:
        field_ctx.field_type = get_type(schema)
    if isinstance(schema, list):
        subschema = _resolve_union(schema, message)
        if subschema is None:
            return message
        return transform(ctx, subschema, message, field_transform)
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type == 'array':
            return [transform(ctx, schema["items"], item, field_transform)
                    for item in message]
        elif schema_type == 'map':
            return {key: transform(ctx, schema["values"], value, field_transform)
                    for key, value in message.items()}
        elif schema_type == 'record':
            fields = schema["fields"]
            for field in fields:
                _transform_field(ctx, schema, field, message, field_transform)
            return message

    if field_ctx is not None:
        rule_tags = ctx.rule.tags
        if not rule_tags or not _disjoint(set(rule_tags), field_ctx.tags):
            return field_transform(ctx, field_ctx, message)
    return message


def _transform_field(
    ctx: RuleContext, schema: AvroSchema, field: dict,
    message: AvroMessage, field_transform: FieldTransform
):
    field_type = field["type"]
    name = field["name"]
    full_name = schema["name"] + "." + name
    try:
        ctx.enter_field(
            message,
            full_name,
            name,
            get_type(field_type),
            None
        )
        value = message[name]
        new_value = transform(ctx, field_type, value, field_transform)
        if ctx.rule.kind == RuleKind.CONDITION:
            if new_value is False:
                raise RuleConditionError(ctx.rule)
        else:
            message[name] = new_value
    finally:
        ctx.exit_field()


def get_type(schema: AvroSchema) -> FieldType:
    if isinstance(schema, list):
        return FieldType.COMBINED
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
    else:
        # string schemas; this could be either a named schema or a primitive type
        schema_type = schema

    if schema_type == 'record':
        return FieldType.RECORD
    elif schema_type == 'enum':
        return FieldType.ENUM
    elif schema_type == 'array':
        return FieldType.ARRAY
    elif schema_type == 'map':
        return FieldType.MAP
    elif schema_type == 'union':
        return FieldType.COMBINED
    elif schema_type == 'fixed':
        return FieldType.FIXED
    elif schema_type == 'string':
        return FieldType.STRING
    elif schema_type == 'bytes':
        return FieldType.BYTES
    elif schema_type == 'int':
        return FieldType.INT
    elif schema_type == 'long':
        return FieldType.LONG
    elif schema_type == 'float':
        return FieldType.FLOAT
    elif schema_type == 'double':
        return FieldType.DOUBLE
    elif schema_type == 'boolean':
        return FieldType.BOOLEAN
    elif schema_type == 'null':
        return FieldType.NULL
    else:
        return FieldType.NULL


def _disjoint(tags1: Set[str], tags2: Set[str]) -> bool:
    for tag in tags1:
        if tag in tags2:
            return False
    return True


def _resolve_union(schema: AvroSchema, message: AvroMessage) -> Optional[AvroSchema]:
    for subschema in schema:
        try:
            validate(message, subschema)
        except:  # noqa: E722
            continue
        return subschema
    return None


def get_inline_tags(schema: AvroSchema) -> Dict[str, Set[str]]:
    inline_tags = defaultdict(set)
    _get_inline_tags_recursively('', '', schema, inline_tags)
    return inline_tags


def _get_inline_tags_recursively(
    ns: str, name: str, schema: Optional[AvroSchema],
    tags: Dict[str, Set[str]]
):
    if schema is None:
        return
    if isinstance(schema, list):
        for subschema in schema:
            _get_inline_tags_recursively(ns, name, subschema, tags)
    elif not isinstance(schema, dict):
        # string schemas; this could be either a named schema or a primitive type
        return
    else:
        schema_type = schema.get("type")
        if schema_type == 'array':
            _get_inline_tags_recursively(ns, name, schema.get("items"), tags)
        elif schema_type == 'map':
            _get_inline_tags_recursively(ns, name, schema.get("values"), tags)
        elif schema_type == 'record':
            record_ns = schema.get("namespace")
            record_name = schema.get("name")
            if record_ns is None:
                record_ns = _implied_namespace(name)
            if record_ns is None:
                record_ns = ns
            if record_ns != '' and not record_name.startswith(record_ns):
                record_name = f"{record_ns}.{record_name}"
            fields = schema["fields"]
            for field in fields:
                field_tags = field.get("confluent:tags")
                field_name = field.get("name")
                field_type = field.get("type")
                if field_tags is not None and field_name is not None:
                    tags[record_name + '.' + field_name].update(field_tags)
                if field_type is not None:
                    _get_inline_tags_recursively(record_ns, record_name, field_type, tags)


def _implied_namespace(name: str) -> Optional[str]:
    match = re.match(r"^(.*)\.[^.]+$", name)
    return match.group(1) if match else None
