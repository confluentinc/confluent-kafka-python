import decimal
import re
from collections import defaultdict
from copy import deepcopy
from io import BytesIO
from json import loads
from typing import Dict, Union, Optional, Set

from fastavro import repository, validate
from fastavro.schema import load_schema

from .schema_registry_client import Schema, RuleKind
from confluent_kafka.schema_registry.serde import RuleContext, FieldType, \
    FieldTransform, RuleConditionError

__all__ = [
    'AvroMessage',
    'AvroSchema',
    '_schema_loads',
    'LocalSchemaRepository',
    'parse_schema_with_repo',
    'transform',
    '_transform_field',
    'get_type',
    '_disjoint',
    '_resolve_union',
    'get_inline_tags',
    '_get_inline_tags_recursively',
    '_implied_namespace',
]

AVRO_TYPE = "AVRO"

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


class LocalSchemaRepository(repository.AbstractSchemaRepository):
    def __init__(self, schemas):
        self.schemas = schemas

    def load(self, subject):
        return self.schemas.get(subject)


def parse_schema_with_repo(schema_str: str, named_schemas: Dict[str, AvroSchema]) -> AvroSchema:
    copy = deepcopy(named_schemas)
    copy["$root"] = loads(schema_str)
    repo = LocalSchemaRepository(copy)
    return load_schema("$root", repo=repo)


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
