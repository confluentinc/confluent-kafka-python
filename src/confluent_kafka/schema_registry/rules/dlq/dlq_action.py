# Copyright 2026 Confluent Inc.
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

import copy
import json
import logging
import struct
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from confluent_kafka import Producer
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind
from confluent_kafka.schema_registry.serde import (
    DLQ_HEADER_PREFIX,
    DLQ_RULE_EXCEPTION_HEADER,
    DLQ_RULE_MODE_HEADER,
    DLQ_RULE_NAME_HEADER,
    DLQ_RULE_SUBJECT_HEADER,
    DLQ_RULE_TOPIC_HEADER,
    FieldContext,
    FieldRuleExecutor,
    FieldTransform,
    FieldType,
    RuleAction,
    RuleContext,
)
from confluent_kafka.serialization import SerializationError

__all__ = [
    'FieldRedactionExecutor',
    'DlqAction',
]

log = logging.getLogger(__name__)

# Bounds of a signed 64-bit integer; ints outside this range cannot be packed
# with struct '>q' and are sent via the JSON path instead.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


class FieldRedactionExecutor(FieldRuleExecutor):
    """Replaces tagged STRING/BYTES fields with ``<REDACTED>``; used by
    :class:`DlqAction` to mask encrypted fields before writing to the DLQ."""

    TYPE = "REDACT"

    REDACTED_STRING = "<REDACTED>"
    REDACTED_BYTES = REDACTED_STRING.encode('utf-8')

    def type(self) -> str:
        return self.TYPE

    def new_transform(self, ctx: RuleContext) -> FieldTransform:
        def _redact(ctx: RuleContext, field_ctx: FieldContext, field_value: Any) -> Any:
            if field_value is None:
                return None
            if field_ctx.field_type == FieldType.STRING:
                return FieldRedactionExecutor.REDACTED_STRING
            elif field_ctx.field_type == FieldType.BYTES:
                return FieldRedactionExecutor.REDACTED_BYTES
            return field_value

        return _redact

    @classmethod
    def register(cls) -> 'FieldRedactionExecutor':
        executor = cls()
        RuleRegistry.register_rule_executor(executor)
        return executor


class DlqAction(RuleAction):
    """
    Rule action that tees the record to a dead-letter-queue topic when a rule
    fails, then raises ``SerializationError`` (the original call still fails).

    On WRITE, structured values are JSON-encoded after redacting fields tagged
    by ``dlq.redact.rule.types`` (default ``ENCRYPT,ENCRYPT_PAYLOAD``); on READ
    the original wire bytes are sent verbatim. DLQ records carry ``__rule.*``
    headers, so the failed rule is skipped when they are re-consumed.

    Config keys (constructor ``conf``; ``dlq.*``/``producer`` may also come from
    the serde ``rule_conf``):

    - ``dlq.topic``: DLQ topic (a per-rule ``dlq.topic`` parameter overrides it)
    - ``dlq.auto.flush``: flush after every send
    - ``dlq.redact.rule.types``: rule types whose tagged fields are redacted
    - ``producer``: pre-built producer; any other key is passed to librdkafka

    With the global registry the DLQ is best-effort (a per-serde close does not
    close the shared action); use ``dlq.auto.flush`` or a dedicated ``RuleRegistry``
    for durability. ``ENCRYPT_PAYLOAD`` carries no field tags, so a WRITE failure
    tees plaintext -- restrict DLQ access accordingly. Redaction runs on a deep
    copy, so the caller's object is not mutated.
    """

    TYPE = "DLQ"

    DLQ_TOPIC = "dlq.topic"
    DLQ_AUTO_FLUSH = "dlq.auto.flush"
    DLQ_REDACT_RULE_TYPES = "dlq.redact.rule.types"
    DLQ_REDACT_RULE_TYPES_DEFAULT = "ENCRYPT,ENCRYPT_PAYLOAD"
    PRODUCER = "producer"  # for testing

    HEADER_PREFIX = DLQ_HEADER_PREFIX
    RULE_NAME = DLQ_RULE_NAME_HEADER
    RULE_MODE = DLQ_RULE_MODE_HEADER
    RULE_SUBJECT = DLQ_RULE_SUBJECT_HEADER
    RULE_TOPIC = DLQ_RULE_TOPIC_HEADER
    RULE_EXCEPTION = DLQ_RULE_EXCEPTION_HEADER

    def __init__(self, conf: Optional[dict] = None):
        self._conf: Dict[str, Any] = dict(conf) if conf else {}
        self._lock = threading.Lock()
        self._topic: Optional[str] = None
        self._auto_flush = False
        self._redact_rule_types: List[str] = []
        self._producer: Optional[Producer] = None
        self._parse_conf()

    def type(self) -> str:
        return self.TYPE

    def configure(self, client_conf: dict, rule_conf: dict):
        # Shared across serdes: hold the lock while mutating _conf so a concurrent
        # _get_producer() can't observe a half-written config. client_conf carries
        # no producer properties, so it is unused here.
        with self._lock:
            if rule_conf:
                for key, value in rule_conf.items():
                    if key == self.PRODUCER or key.startswith('dlq.'):
                        self._conf[key] = value
            self._parse_conf()

    def _parse_conf(self):
        # Callers must hold self._lock (except __init__, which runs pre-share).
        self._topic = self._conf.get(self.DLQ_TOPIC)
        auto_flush = self._conf.get(self.DLQ_AUTO_FLUSH)
        if auto_flush is not None:
            self._auto_flush = str(auto_flush).lower() == 'true'
        redact_rule_types = self._conf.get(self.DLQ_REDACT_RULE_TYPES)
        if redact_rule_types is None:
            redact_rule_types = self.DLQ_REDACT_RULE_TYPES_DEFAULT
        self._redact_rule_types = [s.strip() for s in str(redact_rule_types).split(',')]
        producer = self._conf.get(self.PRODUCER)
        if producer is not None:
            self._producer = producer

    @staticmethod
    def _base_producer_conf() -> Dict[str, Any]:
        return {
            'enable.idempotence': False,
            'acks': 'all',
            'max.in.flight.requests.per.connection': 1,
            # 0 = infinite: retry until delivered
            'message.timeout.ms': 0,
        }

    def _get_producer(self) -> Producer:
        if self._producer is None:
            with self._lock:
                if self._producer is None:
                    producer_conf = self._base_producer_conf()
                    for key, value in self._conf.items():
                        if key == self.PRODUCER or key.startswith('dlq.'):
                            continue
                        producer_conf[key] = value
                    self._producer = Producer(producer_conf)
        return self._producer

    def run(self, ctx: RuleContext, message: Any, ex: Optional[Exception]):
        topic = self._topic
        if not topic:
            topic = ctx.get_parameter(self.DLQ_TOPIC)
        if not topic:
            raise SerializationError("Could not send to DLQ as no topic is configured")
        try:
            key_bytes = self._convert_to_bytes(ctx, ctx.original_key)
            value_bytes = self._convert_to_bytes(ctx, ctx.original_value)
            headers = self._populate_headers(ctx, ex)
            producer = self._get_producer()
            producer.produce(
                topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers,
                on_delivery=self._on_delivery(topic),
            )
            producer.poll(0)
            if self._auto_flush:
                producer.flush()
        except Exception as e:
            log.error("Could not produce message to DLQ topic %s: %s", topic, e)
        msg = f"Rule failed: {ctx.rule.name}"
        if ex is not None:
            raise SerializationError(msg) from ex
        raise SerializationError(msg)

    @staticmethod
    def _on_delivery(topic: str) -> Callable[[Any, Any], None]:
        def callback(err: Any, _msg: Any):
            if err is not None:
                log.error("Could not produce message to DLQ topic %s: %s", topic, err)
            else:
                log.info("Sent message to DLQ topic %s", topic)

        return callback

    def _convert_to_bytes(self, ctx: RuleContext, message: Any) -> Optional[bytes]:
        if message is None:
            return None
        elif isinstance(message, bytes):
            return message
        elif isinstance(message, bytearray):
            return bytes(message)
        elif isinstance(message, memoryview):
            return message.tobytes()
        elif isinstance(message, str):
            return message.encode('utf-8')
        elif isinstance(message, uuid.UUID):
            return str(message).encode('utf-8')
        # bool and out-of-int64-range ints fall through to the JSON path
        # (packing the latter with '>q' would raise struct.error).
        elif isinstance(message, int) and not isinstance(message, bool) and _INT64_MIN <= message <= _INT64_MAX:
            return struct.pack('>q', message)
        elif isinstance(message, float):
            return struct.pack('>d', message)
        else:
            return self._convert_to_json_bytes(ctx, message)

    def _convert_to_json_bytes(self, ctx: RuleContext, message: Any) -> bytes:
        message = self._redact_fields(ctx, message)
        if hasattr(message, 'DESCRIPTOR'):
            # protobuf is an optional dependency, so only import it when the
            # message is a protobuf Message
            from google.protobuf import json_format

            return json_format.MessageToJson(message).encode('utf-8')
        return json.dumps(message, default=self._json_default).encode('utf-8')

    @staticmethod
    def _json_default(obj: Any) -> str:
        if isinstance(obj, (bytes, bytearray)):
            # render bytes as an ISO-8859-1 (latin-1) string
            return bytes(obj).decode('latin-1')
        return str(obj)

    def _redact_fields(self, ctx: RuleContext, message: Any) -> Any:
        redact_rules = self._get_rules_to_redact(ctx)
        if not redact_rules:
            # No rules require redaction
            return message
        try:
            tags = self._get_tags_to_redact(redact_rules)
            new_rule = Rule(
                'redact',
                None,
                RuleKind.TRANSFORM,
                ctx.rule_mode,
                # DLQ action's own type, matching Java; unused here (the executor
                # is invoked directly, never looked up by type).
                self.TYPE,
                sorted(tags),
                None,
                None,
                None,
                None,
                False,
            )
            new_ctx = RuleContext(
                ctx.enabled_env,
                ctx.ser_ctx,
                ctx.source,
                ctx.target,
                ctx.subject,
                ctx.rule_mode,
                new_rule,
                0,
                [new_rule],
                ctx.inline_tags,
                ctx.field_transformer,
                ctx.original_key,
                ctx.original_value,
            )
            # Redact a copy so the caller's object is not mutated (Java redacts
            # in place; the DLQ bytes are identical either way).
            message = copy.deepcopy(message)
            executor = FieldRedactionExecutor()
            try:
                return executor.transform(new_ctx, message)
            finally:
                executor.close()
        except Exception as e:
            log.error("Could not redact fields: %s", e)
            return message

    def _get_rules_to_redact(self, ctx: RuleContext) -> List[Rule]:
        return [rule for rule in (ctx.rules or []) if rule.type in self._redact_rule_types]

    @staticmethod
    def _get_tags_to_redact(redact_rules: List[Rule]) -> Set[str]:
        tags: Set[str] = set()
        for rule in redact_rules:
            if rule.tags:
                tags.update(rule.tags)
        return tags

    def _populate_headers(self, ctx: RuleContext, ex: Optional[Exception]) -> List[Tuple[str, Union[str, bytes, None]]]:
        headers: List[Tuple[str, Union[str, bytes, None]]] = []
        incoming = ctx.ser_ctx.headers if ctx.ser_ctx is not None else None
        if incoming:
            if isinstance(incoming, dict):
                headers.extend((k, self._to_header_bytes(v)) for k, v in incoming.items())
            else:
                headers.extend((k, self._to_header_bytes(v)) for k, v in incoming)
        headers.append((self.RULE_NAME, self._to_header_bytes(ctx.rule.name)))
        headers.append((self.RULE_MODE, self._to_header_bytes(ctx.rule_mode.value if ctx.rule_mode else None)))
        headers.append((self.RULE_SUBJECT, self._to_header_bytes(ctx.subject)))
        headers.append((self.RULE_TOPIC, self._to_header_bytes(ctx.ser_ctx.topic if ctx.ser_ctx else None)))
        if ex is not None:
            headers.append((self.RULE_EXCEPTION, self._to_header_bytes(str(ex))))
        return headers

    @staticmethod
    def _to_header_bytes(value: Any) -> Optional[bytes]:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        return str(value).encode('utf-8')

    def close(self):
        # Detach under the lock, then flush outside it: flush() can block
        # indefinitely (message.timeout.ms=0) and must not stall other callers.
        with self._lock:
            producer = self._producer
            self._producer = None
        if producer is not None:
            producer.flush()

    @classmethod
    def register(cls, conf: Optional[dict] = None) -> 'DlqAction':
        action = cls(conf)
        RuleRegistry.register_rule_action(action)
        return action
