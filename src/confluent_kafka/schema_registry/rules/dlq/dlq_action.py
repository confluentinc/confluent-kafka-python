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
    """
    A field-level rule executor that replaces tagged STRING fields with
    ``"<REDACTED>"`` and tagged BYTES fields with ``b"<REDACTED>"``; all other
    field types pass through unchanged. Used by :class:`DlqAction` to redact
    fields tagged by encryption rules before writing a record to the DLQ.
    """

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
    A rule action that sends the record being processed to a dead-letter-queue
    topic when a rule fails, then raises a :class:`SerializationError`. The DLQ
    is a tee, not a swallow: the original serialize/deserialize call still fails.

    The DLQ record is built from the original key/value as they were at entry to
    the current rule phase. On serialize (WRITE), structured values are converted
    to JSON after redacting every field tagged by rules whose type is in
    ``dlq.redact.rule.types`` (default ``ENCRYPT,ENCRYPT_PAYLOAD``), so plaintext
    for encrypted fields does not leak to the DLQ. On deserialize (READ), the
    original wire bytes are sent verbatim (for encrypted payloads this is
    ciphertext, and the DLQ record can be re-consumed directly). Records consumed
    from a DLQ topic carry a ``__rule.name`` header, which causes the previously
    failed rule to be skipped on deserialization.

    Configuration keys (constructor ``conf`` dict; ``dlq.*`` and ``producer``
    may also be supplied via the serializer/deserializer ``rule_conf``):

    +--------------------------+----------------------------------------------------+
    | Property name            | Description                                        |
    +==========================+====================================================+
    | ``dlq.topic``            | DLQ topic (overridable per rule via a ``dlq.topic``|
    |                          | rule parameter)                                    |
    +--------------------------+----------------------------------------------------+
    | ``dlq.auto.flush``       | Flush the producer after each DLQ send             |
    +--------------------------+----------------------------------------------------+
    | ``dlq.redact.rule.types``| Comma-separated rule types whose tagged fields are |
    |                          | redacted; defaults to ``ENCRYPT,ENCRYPT_PAYLOAD``  |
    +--------------------------+----------------------------------------------------+
    | ``producer``             | A pre-built producer to use instead of creating one|
    +--------------------------+----------------------------------------------------+
    | any other key            | Passed to the internal librdkafka producer, e.g.   |
    |                          | ``bootstrap.servers``, ``security.protocol``, ...  |
    +--------------------------+----------------------------------------------------+

    Differences from the Java client, forced by the platform:

    - Java serializers receive the producer's config map, so the Java DlqAction
      inherits ``bootstrap.servers`` etc. automatically. Python serdes only see
      the Schema Registry client config, so producer connectivity must be given
      explicitly in ``conf`` (librdkafka also rejects unknown properties, so the
      Schema Registry config cannot be merged in).
    - Java's ``max.block.ms``/``delivery.timeout.ms`` map to a non-blocking
      ``produce()`` (a full queue raises and is logged) and
      ``message.timeout.ms=0`` (infinite).
    - Python ``int``/``float`` are encoded as 8-byte big-endian (Java picks the
      width from the boxed type).
    - ``dlq.auto.flush`` blocks, which stalls the event loop under asyncio.

    Behaviors intentionally matching Java: redaction failures are fail-open (the
    unredacted value is sent, with an error logged); redaction mutates the failed
    message in place; ``ENCRYPT_PAYLOAD`` failures on serialize send the plaintext
    serialized bytes verbatim (payload-level rules carry no field tags to redact).

    Shared instance / register-once semantics (differs from Java): a ``DlqAction``
    is registered once into a :class:`RuleRegistry` and shared by every serde bound
    to that registry (typically the process-wide global registry). Unlike the Java
    client, which owns a per-serde action instance, this instance holds a single
    ``_conf``/producer. Consequently:

    - ``configure()`` is called by every serde's constructor and merges the
      serde's ``rule_conf`` ``dlq.*``/``producer`` keys into the shared ``_conf``
      (last writer wins). Two serdes sharing one action with different
      ``rule_conf`` (e.g. different ``dlq.topic``) will clobber each other; give a
      serde its own :class:`RuleRegistry` (with its own ``DlqAction``) if it needs
      an isolated DLQ config. A per-rule ``dlq.topic`` parameter (see the table)
      is resolved per invocation and is unaffected.
    - Because the action (and its producer) is shared and process-lifetime, a
      per-serde :meth:`close`/``aclose`` does NOT close it when it lives in the
      global registry; see :meth:`AsyncBaseSerde.aclose`.

    Original-key capture limitation: the value-side DLQ record's ``key`` is taken
    from the key stashed by the key serde via ``set_original_key`` (a contextvar,
    mirroring Java's ThreadLocal). This only works when a Schema-Registry key
    serializer/deserializer runs before the value serde in the same thread/task. If
    the key uses a non-SR serializer, no key serializer is configured, or the key
    serde failed before stashing, the value-side DLQ record's key will be ``None``.
    A value serde clears any stashed key on entry, so a stale key is not leaked
    across messages by an SR value serde; see ``get_original_key`` in
    ``common/serde.py``.
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
        # client_conf is the Schema Registry client config, which contains no
        # Kafka producer properties and cannot be merged into the producer conf
        # (librdkafka rejects unknown properties), so it is not used here.
        #
        # This instance is shared across every serde bound to the same registry, so
        # each serde's constructor calls configure() concurrently in the worst case.
        # Hold the lock while mutating _conf (and deriving fields from it) so a
        # concurrent _get_producer() -- which iterates _conf under the same lock --
        # cannot observe a half-written config or raise "dict changed size during
        # iteration". _parse_conf() must not take the lock (it is called here with
        # the lock already held).
        with self._lock:
            if rule_conf:
                for key, value in rule_conf.items():
                    if key == self.PRODUCER or key.startswith('dlq.'):
                        self._conf[key] = value
            self._parse_conf()

    def _parse_conf(self):
        # Not thread-safe on its own; callers must hold self._lock (or be __init__,
        # which runs before the instance is shared).
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
            # librdkafka equivalent of Java's delivery.timeout.ms=MAX; 0 is infinite
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
        # bool is excluded so that it falls through to the JSON path, as in Java.
        # Ints outside the signed int64 range also fall through to the JSON path
        # rather than raising struct.error, which would abort the whole DLQ send.
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
            # matches the ISO-8859-1 rendering of bytes in the Java client
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
        # Detach the producer under the lock (so it can't race _get_producer /
        # configure), then flush outside the lock: flush() can block for a long
        # time (message.timeout.ms=0 is infinite) and must not stall other callers.
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
