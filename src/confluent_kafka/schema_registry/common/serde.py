#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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

import abc
import contextvars
import io
import logging
import struct
import uuid
from enum import Enum
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

from confluent_kafka.schema_registry import (
    _MAGIC_BYTE_V0,
    _MAGIC_BYTE_V1,
    RegisteredSchema,
    record_subject_name_strategy,
    topic_record_subject_name_strategy,
    topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, Schema
from confluent_kafka.schema_registry.wildcard_matcher import wildcard_match
from confluent_kafka.serialization import SerializationContext, SerializationError

__all__ = [
    'STRATEGY_TYPE_MAP',
    'SubjectNameStrategyType',
    'FieldType',
    'FieldContext',
    'RuleContext',
    'FieldTransform',
    'FieldTransformer',
    'RuleBase',
    'RuleExecutor',
    'FieldRuleExecutor',
    'RuleAction',
    'ErrorAction',
    'NoneAction',
    'DLQ_HEADER_PREFIX',
    'DLQ_RULE_NAME_HEADER',
    'DLQ_RULE_MODE_HEADER',
    'DLQ_RULE_SUBJECT_HEADER',
    'DLQ_RULE_TOPIC_HEADER',
    'DLQ_RULE_EXCEPTION_HEADER',
    'set_original_key',
    'get_original_key',
    'clear_original_key',
    'RuleError',
    'RuleConditionError',
    'ValidationRulesExecution',
    'ValidationRule',
    'ValidationRuleError',
    'ValidationRuleExecutor',
    'VALIDATION_RULES_PROP',
    'parse_validation_rules',
    'default_validation_rule_executor',
    'evaluate_validation_rule',
    'Migration',
    'ParsedSchemaCache',
    'SchemaId',
]

log = logging.getLogger(__name__)


DLQ_HEADER_PREFIX = "__rule."
DLQ_RULE_NAME_HEADER = DLQ_HEADER_PREFIX + "name"
DLQ_RULE_MODE_HEADER = DLQ_HEADER_PREFIX + "mode"
DLQ_RULE_SUBJECT_HEADER = DLQ_HEADER_PREFIX + "subject"
DLQ_RULE_TOPIC_HEADER = DLQ_HEADER_PREFIX + "topic"
DLQ_RULE_EXCEPTION_HEADER = DLQ_HEADER_PREFIX + "exception"


# Holds the key for the value serde (e.g. the DLQ action), set by the key serde
# earlier in the same thread/asyncio task. Captured only when an SR key serde runs
# before the value serde; otherwise the value-side DLQ key is None. The value serde
# clears it when done so a stale key isn't leaked to the next message.
_ORIGINAL_KEY: contextvars.ContextVar[Any] = contextvars.ContextVar('sr_original_key', default=None)


def set_original_key(key: Any):
    _ORIGINAL_KEY.set(key)


def get_original_key() -> Any:
    return _ORIGINAL_KEY.get()


def clear_original_key():
    _ORIGINAL_KEY.set(None)


class SubjectNameStrategyType(str, Enum):
    NONE = "NONE"
    TOPIC = "TOPIC"
    RECORD = "RECORD"
    TOPIC_RECORD = "TOPIC_RECORD"
    ASSOCIATED = "ASSOCIATED"


# Mapping from SubjectNameStrategyType to strategy functions.
# NONE and ASSOCIATED are handled specially and not included here.
STRATEGY_TYPE_MAP = {
    SubjectNameStrategyType.TOPIC: topic_subject_name_strategy,
    SubjectNameStrategyType.RECORD: record_subject_name_strategy,
    SubjectNameStrategyType.TOPIC_RECORD: topic_record_subject_name_strategy,
}


class FieldType(str, Enum):
    RECORD = "RECORD"
    ENUM = "ENUM"
    ARRAY = "ARRAY"
    MAP = "MAP"
    COMBINED = "COMBINED"
    FIXED = "FIXED"
    STRING = "STRING"
    BYTES = "BYTES"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    NULL = "NULL"


class FieldContext(object):
    __slots__ = ['containing_message', 'full_name', 'name', 'field_type', 'tags']

    def __init__(self, containing_message: Any, full_name: str, name: str, field_type: FieldType, tags: Set[str]):
        self.containing_message = containing_message
        self.full_name = full_name
        self.name = name
        self.field_type = field_type
        self.tags = tags

    def is_primitive(self) -> bool:
        return self.field_type in (
            FieldType.INT,
            FieldType.LONG,
            FieldType.FLOAT,
            FieldType.DOUBLE,
            FieldType.BOOLEAN,
            FieldType.NULL,
            FieldType.STRING,
            FieldType.BYTES,
        )

    def type_name(self) -> str:
        return self.field_type.name


class RuleContext(object):
    __slots__ = [
        'enabled_env',
        'ser_ctx',
        'source',
        'target',
        'subject',
        'rule_mode',
        'rule',
        'index',
        'rules',
        'inline_tags',
        'field_transformer',
        'original_key',
        'original_value',
        '_field_contexts',
    ]

    def __init__(
        self,
        enabled_env: Optional[str],
        ser_ctx: SerializationContext,
        source: Optional[Schema],
        target: Optional[Schema],
        subject: str,
        rule_mode: RuleMode,
        rule: Rule,
        index: int,
        rules: List[Rule],
        inline_tags: Optional[Dict[str, Set[str]]],
        field_transformer,
        original_key: Any = None,
        original_value: Any = None,
    ):
        self.enabled_env = enabled_env
        self.ser_ctx = ser_ctx
        self.source = source
        self.target = target
        self.subject = subject
        self.rule_mode = rule_mode
        self.rule = rule
        self.index = index
        self.rules = rules
        self.inline_tags = inline_tags
        self.field_transformer = field_transformer
        self.original_key = original_key
        self.original_value = original_value
        self._field_contexts: List[FieldContext] = []

    def get_parameter(self, name: str) -> Optional[str]:
        params = self.rule.params
        if params is not None:
            value = params.params.get(name)
            if value is not None:
                return value
        if self.target is not None and self.target.metadata is not None and self.target.metadata.properties is not None:
            value = self.target.metadata.properties.properties.get(name)
            if value is not None:
                return value
        return None

    def _get_inline_tags(self, name: str) -> Set[str]:
        if self.inline_tags is None:
            return set()
        return self.inline_tags.get(name, set())

    def current_field(self) -> Optional[FieldContext]:
        if not self._field_contexts:
            return None
        return self._field_contexts[-1]

    def enter_field(
        self, containing_message: Any, full_name: str, name: str, field_type: FieldType, tags: Optional[Set[str]]
    ) -> FieldContext:
        all_tags = set(tags if tags is not None else self._get_inline_tags(full_name))
        all_tags.update(self.get_tags(full_name))
        field_context = FieldContext(containing_message, full_name, name, field_type, all_tags)
        self._field_contexts.append(field_context)
        return field_context

    def get_tags(self, full_name: str) -> Set[str]:
        result = set()
        if self.target is not None and self.target.metadata is not None and self.target.metadata.tags is not None:
            tags = self.target.metadata.tags.tags
            for k, v in tags.items():
                if wildcard_match(full_name, k):
                    result.update(v)
        return result

    def exit_field(self):
        if self._field_contexts:
            self._field_contexts.pop()


FieldTransform = Callable[[RuleContext, FieldContext, Any], Any]


FieldTransformer = Callable[[RuleContext, FieldTransform, Any], Any]


class RuleBase(metaclass=abc.ABCMeta):
    def configure(self, client_conf: dict, rule_conf: dict):
        pass

    @abc.abstractmethod
    def type(self) -> str:
        raise NotImplementedError()

    def close(self):
        pass


class RuleExecutor(RuleBase):
    @abc.abstractmethod
    def transform(self, ctx: RuleContext, message: Any) -> Any:
        raise NotImplementedError()


class FieldRuleExecutor(RuleExecutor):
    @abc.abstractmethod
    def new_transform(self, ctx: RuleContext) -> FieldTransform:
        raise NotImplementedError()

    def transform(self, ctx: RuleContext, message: Any) -> Any:
        # TODO preserve source
        if ctx.rule_mode in (RuleMode.WRITE, RuleMode.UPGRADE):
            for i in range(ctx.index):
                other_rule = ctx.rules[i]
                if FieldRuleExecutor.are_transforms_with_same_tag(ctx.rule, other_rule):
                    # ignore this transform if an earlier one has the same tag
                    return message
        elif ctx.rule_mode == RuleMode.READ or ctx.rule_mode == RuleMode.DOWNGRADE:
            for i in range(ctx.index + 1, len(ctx.rules)):
                other_rule = ctx.rules[i]
                if FieldRuleExecutor.are_transforms_with_same_tag(ctx.rule, other_rule):
                    # ignore this transform if a later one has the same tag
                    return message
        return ctx.field_transformer(ctx, self.new_transform(ctx), message)

    @staticmethod
    def are_transforms_with_same_tag(rule1: Rule, rule2: Rule) -> bool:
        return (
            bool(rule1.tags)
            and rule1.kind == RuleKind.TRANSFORM
            and rule1.kind == rule2.kind
            and rule1.mode == rule2.mode
            and rule1.type == rule2.type
            and rule1.tags == rule2.tags
        )


class RuleAction(RuleBase):
    @abc.abstractmethod
    def run(self, ctx: RuleContext, message: Any, ex: Optional[Exception]):
        raise NotImplementedError()


class ErrorAction(RuleAction):
    def type(self) -> str:
        return 'ERROR'

    def run(self, ctx: RuleContext, message: Any, ex: Optional[Exception]):
        if ex is None:
            raise SerializationError()
        else:
            raise SerializationError() from ex


class NoneAction(RuleAction):
    def type(self) -> str:
        return 'NONE'

    def run(self, ctx: RuleContext, message: Any, ex: Optional[Exception]):
        pass


class RuleError(Exception):
    pass


class RuleConditionError(RuleError):
    def __init__(self, rule: Rule):
        super().__init__(RuleConditionError.error_message(rule))

    @staticmethod
    def error_message(rule: Rule) -> str:
        if rule.doc:
            return rule.doc
        elif rule.expr:
            return f"Rule expr failed: {rule.expr}"
        else:
            return f"Rule failed: {rule.name}"


class ValidationRulesExecution(str, Enum):
    """
    When inline validation rules run, relative to domain rule transformations.
    """

    DISABLED = 'DISABLED'
    BEFORE_DOMAIN_RULES = 'BEFORE_DOMAIN_RULES'
    AFTER_DOMAIN_RULES = 'AFTER_DOMAIN_RULES'


class ValidationRule(object):
    """
    An inline validation rule (a CHECK constraint) declared on a schema, either
    on a record/message/object or on one of its fields.
    """

    __slots__ = ['name', 'doc', 'expr', 'sql']

    def __init__(
        self,
        name: Optional[str] = None,
        doc: Optional[str] = None,
        expr: Optional[str] = None,
        sql: Optional[str] = None,
    ):
        self.name = name
        self.doc = doc
        self.expr = expr
        self.sql = sql

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ValidationRule):
            return NotImplemented
        return (
            self.name == other.name
            and self.doc == other.doc
            and self.expr == other.expr
            and self.sql == other.sql
        )

    def __hash__(self) -> int:
        return hash((self.name, self.doc, self.expr, self.sql))

    def __repr__(self) -> str:
        return f"ValidationRule{{name={self.name}, doc={self.doc}, expr={self.expr}, sql={self.sql}}}"


class ValidationRuleError(object):
    """
    A single inline validation rule failure, located at ``field_path`` within the
    message that was validated.
    """

    __slots__ = ['rule', 'field_path', 'message', 'cause']

    def __init__(
        self,
        rule: ValidationRule,
        field_path: Optional[str] = None,
        message: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        self.rule = rule
        self.field_path = field_path
        #: Optional dynamic error message returned by the rule itself — set when the
        #: rule expression returned a non-empty string explaining the failure (e.g.
        #: ``x > 0 ? '' : 'x must be positive'``). None when the failure was a plain
        #: False or a CEL evaluation error.
        self.message = message
        self.cause = cause

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ValidationRuleError):
            return NotImplemented
        return (
            self.rule == other.rule and self.field_path == other.field_path and self.message == other.message
        )

    def __hash__(self) -> int:
        return hash((self.rule, self.field_path, self.message))

    def __str__(self) -> str:
        path = self.field_path if self.field_path else '<root>'
        name = self.rule.name if self.rule is not None and self.rule.name else 'unnamed'
        # Prefer the dynamic message returned by the rule itself; fall back to the
        # rule's authored doc / SQL / CEL expression in that order.
        if self.message:
            detail: Any = self.message
        elif self.rule is not None and self.rule.doc:
            detail = self.rule.doc
        elif self.rule is not None and self.rule.sql:
            detail = self.rule.sql
        else:
            detail = self.rule.expr if self.rule is not None else None
        result = f"{path}: {name}: {detail}"
        if self.cause is not None:
            result += f" (caused by: {self.cause})"
        return result

    def __repr__(self) -> str:
        return f"ValidationRuleError({self})"


class ValidationRuleExecutor(metaclass=abc.ABCMeta):
    """
    Evaluates a single inline validation rule against a value.

    Implementations return either a bool (False meaning the rule failed) or a str
    (non-empty meaning the rule failed, with that string as the failure message).
    """

    @abc.abstractmethod
    def execute(self, rule: ValidationRule, schema: Any, message: Any) -> Any:
        raise NotImplementedError()


#: Schema property (Avro) / keyword (JSON Schema) holding inline validation rules.
VALIDATION_RULES_PROP = 'confluent:rules'


def parse_validation_rules(prop_value: Any) -> List[ValidationRule]:
    """
    Parse the ``confluent:rules`` property value — a list of dicts with name/doc/expr/sql
    keys. Missing or malformed entries are ignored, yielding an empty list.
    """
    if not isinstance(prop_value, list):
        return []
    rules = []
    for entry in prop_value:
        if isinstance(entry, dict):
            rules.append(
                ValidationRule(
                    entry.get('name'),
                    entry.get('doc'),
                    entry.get('expr'),
                    entry.get('sql'),
                )
            )
    return rules


def default_validation_rule_executor() -> ValidationRuleExecutor:
    """
    Instantiate the default validation rule executor, the CEL-backed one. Imported
    lazily because the CEL dependencies are an optional extra.
    """
    try:
        from confluent_kafka.schema_registry.rules.cel.cel_validator import CelValidator
    except ImportError as e:
        raise ValueError(
            "The default validation rule executor requires the CEL dependencies; install "
            "them with `pip install confluent-kafka[rules]`, or set "
            "validation.rules.executor to a ValidationRuleExecutor instance."
        ) from e
    return CelValidator()


def evaluate_validation_rule(
    executor: ValidationRuleExecutor,
    rule: ValidationRule,
    schema: Any,
    value: Any,
    path: str,
    out: List[ValidationRuleError],
):
    """
    Evaluate one inline validation rule, appending a ValidationRuleError to ``out``
    when it fails. A rule that raises RuleError is itself recorded as a violation so
    the walk can continue; a rule resolving to something other than a bool or str is
    a programming error and propagates.
    """
    try:
        result = executor.execute(rule, schema, value)
    except RuleError as e:
        out.append(ValidationRuleError(rule, path, None, e))
        return
    if isinstance(result, bool):
        if result is False:
            out.append(ValidationRuleError(rule, path, None, None))
    elif isinstance(result, str):
        if result:
            out.append(ValidationRuleError(rule, path, result, None))
    else:
        raise SerializationError(
            f"Validation rule '{rule.name}' resolved to an unexpected type: {type(result).__name__}"
        )


class Migration(object):
    __slots__ = ['rule_mode', 'source', 'target']

    def __init__(self, rule_mode: RuleMode, source: Optional[RegisteredSchema], target: Optional[RegisteredSchema]):
        self.rule_mode = rule_mode
        self.source = source
        self.target = target


T = TypeVar("T")


class ParsedSchemaCache(object):
    """
    Thread-safe cache for parsed schemas
    """

    def __init__(self):
        self.lock = Lock()
        self.parsed_schemas = {}

    def set(self, schema: Schema, parsed_schema: T):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            schema (Schema): The schema

            parsed_schema (Any): The parsed schema
        """

        with self.lock:
            self.parsed_schemas[schema] = parsed_schema

    def get_parsed_schema(self, schema: Schema) -> Optional[T]:
        """
        Get the parsed schema associated with the schema

        Args:
            schema (Schema): The schema

        Returns:
            The parsed schema if known; else None
        """

        with self.lock:
            return self.parsed_schemas.get(schema, None)

    def clear(self):
        """
        Clear the cache.
        """

        with self.lock:
            self.parsed_schemas.clear()


class SchemaId(object):
    __slots__ = ['schema_type', 'id', 'guid', 'message_indexes']

    def __init__(
        self,
        schema_type: str,
        schema_id: Optional[int] = None,
        guid: Optional[str] = None,
        message_indexes: Optional[List[int]] = None,
    ):
        self.schema_type = schema_type
        self.id = schema_id
        self.guid = uuid.UUID(guid) if guid is not None else None
        self.message_indexes = message_indexes

    def from_bytes(self, payload: io.BytesIO) -> io.BytesIO:
        magic = struct.unpack('>b', payload.read(1))[0]
        if magic == _MAGIC_BYTE_V0:
            self.id = struct.unpack('>I', payload.read(4))[0]
        elif magic == _MAGIC_BYTE_V1:
            self.guid = uuid.UUID(bytes=payload.read(16))
        else:
            raise SerializationError("Invalid magic byte")
        if self.schema_type == "PROTOBUF":
            self.message_indexes = self._read_index_array(payload, zigzag=True)
        return payload

    def id_to_bytes(self) -> bytes:
        if self.id is None:
            raise SerializationError("Schema ID is not set")
        buf = io.BytesIO()
        buf.write(struct.pack('>bI', _MAGIC_BYTE_V0, self.id))
        if self.message_indexes is not None:
            self._encode_varints(buf, self.message_indexes, zigzag=True)
        return buf.getvalue()

    def guid_to_bytes(self) -> bytes:
        if self.guid is None:
            raise SerializationError("Schema GUID is not set")
        buf = io.BytesIO()
        buf.write(struct.pack('>b', _MAGIC_BYTE_V1))
        buf.write(self.guid.bytes)
        if self.message_indexes is not None:
            self._encode_varints(buf, self.message_indexes, zigzag=True)
        return buf.getvalue()

    @staticmethod
    def _decode_varint(buf: io.BytesIO, zigzag: bool = True) -> int:
        """
        Decodes a single varint from a buffer.

        Args:
            buf (BytesIO): buffer to read from
            zigzag (bool): decode as zigzag or uvarint

        Returns:
            int: decoded varint

        Raises:
            EOFError: if buffer is empty
        """

        value = 0
        shift = 0
        try:
            while True:
                i = SchemaId._read_byte(buf)

                value |= (i & 0x7F) << shift
                shift += 7
                if not (i & 0x80):
                    break

            if zigzag:
                value = (value >> 1) ^ -(value & 1)

            return value

        except EOFError:
            raise EOFError("Unexpected EOF while reading index")

    @staticmethod
    def _read_byte(buf: io.BytesIO) -> int:
        """
        Read one byte from buf as an int.

        Args:
            buf (BytesIO): The buffer to read from.

        .. _ord:
            https://docs.python.org/2/library/functions.html#ord
        """

        i = buf.read(1)
        if i == b'':
            raise EOFError("Unexpected EOF encountered")
        return ord(i)

    @staticmethod
    def _read_index_array(buf: io.BytesIO, zigzag: bool = True) -> List[int]:
        """
        Read an index array from buf that specifies the message
        descriptor of interest in the file descriptor.

        Args:
            buf (BytesIO): The buffer to read from.

        Returns:
            list of int: The index array.
        """

        size = SchemaId._decode_varint(buf, zigzag=zigzag)
        if size < 0 or size > 100000:
            raise SerializationError("Invalid msgidx array length")

        if size == 0:
            return [0]

        msg_index = []
        for _ in range(size):
            msg_index.append(SchemaId._decode_varint(buf, zigzag=zigzag))

        return msg_index

    @staticmethod
    def _write_varint(buf: io.BytesIO, val: int, zigzag: bool = True):
        """
        Writes val to buf, either using zigzag or uvarint encoding.

        Args:
            buf (BytesIO): buffer to write to.
            val (int): integer to be encoded.
            zigzag (bool): whether to encode in zigzag or uvarint encoding
        """

        if zigzag:
            val = (val << 1) ^ (val >> 63)

        while (val & ~0x7F) != 0:
            buf.write(SchemaId._bytes((val & 0x7F) | 0x80))
            val >>= 7
        buf.write(SchemaId._bytes(val))

    @staticmethod
    def _encode_varints(buf: io.BytesIO, ints: List[int], zigzag: bool = True):
        """
        Encodes each int as a uvarint onto buf

        Args:
            buf (BytesIO): buffer to write to.
            ints ([int]): ints to be encoded.
            zigzag (bool): whether to encode in zigzag or uvarint encoding
        """

        assert len(ints) > 0
        # The root element at the 0 position does not need a length prefix.
        if ints == [0]:
            buf.write(SchemaId._bytes(0x00))
            return

        SchemaId._write_varint(buf, len(ints), zigzag=zigzag)

        for value in ints:
            SchemaId._write_varint(buf, value, zigzag=zigzag)

    @staticmethod
    def _bytes(v: int) -> bytes:
        """
        Convert int to bytes

        Args:
            v (int): The int to convert to bytes.
        """
        return bytes((v,))
