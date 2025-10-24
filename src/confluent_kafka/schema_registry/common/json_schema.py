
import decimal
from io import BytesIO

import logging
from typing import Union, Optional, List, Set

import httpx
import referencing
from jsonschema import validate, ValidationError
from referencing import Registry, Resource
from referencing._core import Resolver

from confluent_kafka.schema_registry import RuleKind
from confluent_kafka.schema_registry.serde import RuleContext, FieldTransform, FieldType, \
    RuleConditionError

__all__ = [
    'JsonMessage',
    'JsonSchema',
    'DEFAULT_SPEC',
    '_retrieve_via_httpx',
    'transform',
    '_transform_field',
    '_validate_subschemas',
    'get_type',
    '_disjoint',
    'get_inline_tags',
]

JSON_TYPE = "JSON"

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

DEFAULT_SPEC = referencing.jsonschema.DRAFT7  # type: ignore[attr-defined]

log = logging.getLogger(__name__)

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


def transform(
    ctx: RuleContext, schema: JsonSchema, ref_registry: Registry, ref_resolver: Resolver,
    path: str, message: JsonMessage, field_transform: FieldTransform
) -> Optional[JsonMessage]:
    # Only proceed to transform the message if schema is of dict type
    if message is None or schema is None or isinstance(schema, bool):
        return message

    field_ctx = ctx.current_field()
    if field_ctx is not None:
        field_ctx.field_type = get_type(schema)
    original_type = schema.get("type")
    if isinstance(original_type, list) and len(original_type) > 0:
        subschema = _validate_subtypes(schema, message, ref_registry)
        try:
            if subschema is not None:
                return transform(ctx, subschema, ref_registry, ref_resolver, path, message, field_transform)
        finally:
            schema["type"] = original_type  # restore original type
    all_of = schema.get("allOf")
    if all_of is not None:
        subschema = _validate_subschemas(all_of, message, ref_registry, ref_resolver)
        if subschema is not None:
            return transform(ctx, subschema, ref_registry, ref_resolver, path, message, field_transform)
    any_of = schema.get("anyOf")
    if any_of is not None:
        subschema = _validate_subschemas(any_of, message, ref_registry, ref_resolver)
        if subschema is not None:
            return transform(ctx, subschema, ref_registry, ref_resolver, path, message, field_transform)
    one_of = schema.get("oneOf")
    if one_of is not None:
        subschema = _validate_subschemas(one_of, message, ref_registry, ref_resolver)
        if subschema is not None:
            return transform(ctx, subschema, ref_registry, ref_resolver, path, message, field_transform)
    items = schema.get("items")
    if items is not None:
        if isinstance(message, list):
            return [transform(ctx, items, ref_registry, ref_resolver, path, item, field_transform) for item in message]
    ref = schema.get("$ref")
    if ref is not None:
        ref_schema = ref_resolver.lookup(ref)
        return transform(ctx, ref_schema.contents, ref_registry, ref_resolver, path, message, field_transform)

    schema_type = get_type(schema)
    if schema_type == FieldType.RECORD:
        props = schema.get("properties")
        if not isinstance(message, dict):
            log.warning("Incompatible message type for record schema")
            return message
        if props is not None:
            for prop_name, prop_schema in props.items():
                if isinstance(prop_schema, dict):
                    _transform_field(ctx, path, prop_name, message,
                                    prop_schema, ref_registry, ref_resolver, field_transform)
        return message
    if schema_type in (FieldType.ENUM, FieldType.STRING, FieldType.INT, FieldType.DOUBLE, FieldType.BOOLEAN):
        if field_ctx is not None:
            rule_tags = ctx.rule.tags
            if not rule_tags or not _disjoint(set(rule_tags), field_ctx.tags):
                return field_transform(ctx, field_ctx, message)
    return message


def _transform_field(
    ctx: RuleContext, path: str, prop_name: str, message: dict,
    prop_schema: dict, ref_registry: Registry, ref_resolver: Resolver, field_transform: FieldTransform
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
        value = message.get(prop_name)
        if value is not None:
            new_value = transform(ctx, prop_schema, ref_registry, ref_resolver, full_name, value, field_transform)
            if ctx.rule.kind == RuleKind.CONDITION:
                if new_value is False:
                    raise RuleConditionError(ctx.rule)
            else:
                message[prop_name] = new_value
    finally:
        ctx.exit_field()


def _validate_subtypes(
    schema: dict, message: JsonMessage, registry: Registry
) -> Optional[JsonSchema]:
    """
    Validate the message against the subtypes.
    Args:
        schema: The schema to validate the message against.
        message: The message to validate.
        registry: The registry to use for the validation.
    Returns:
        The validated schema if the message is valid against the subtypes, otherwise None.
    """
    schema_type = schema.get("type")
    if not isinstance(schema_type, list) or len(schema_type) == 0:
        return None
    for typ in schema_type:
        schema["type"] = typ
        try:
            validate(instance=message, schema=schema, registry=registry)
            return schema
        except ValidationError:
            pass
    return None


def _validate_subschemas(
    subschemas: List[JsonSchema],
    message: JsonMessage,
    registry: Registry,
    resolver: Resolver,
)-> Optional[JsonSchema]:
    """
    Validate the message against the subschemas.
    Args:
        subschemas: The list of subschemas to validate the message against.
        message: The message to validate.
        registry: The registry to use for the validation.
        resolver: The resolver to use for the validation.
    Returns:
        The validated schema if the message is valid against the subschemas, otherwise None.
    """
    for subschema in subschemas:
        if isinstance(subschema, dict):
            try:
                ref = subschema.get("$ref")
                if ref is not None:
                    subschema = resolver.lookup(ref).contents
                validate(instance=message, schema=subschema, registry=registry, resolver=resolver)
                return subschema
            except ValidationError:
                pass
    return None


def get_type(schema: JsonSchema) -> FieldType:
    if isinstance(schema, bool):
        return FieldType.COMBINED

    schema_type = schema.get("type")
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

    props = schema.get("properties")
    if props is not None:
        return FieldType.RECORD

    return FieldType.NULL


def _disjoint(tags1: Set[str], tags2: Set[str]) -> bool:
    for tag in tags1:
        if tag in tags2:
            return False
    return True


def get_inline_tags(schema: dict) -> Set[str]:
    tags = schema.get("confluent:tags")
    if tags is None:
        return set()
    else:
        return set(tags)
