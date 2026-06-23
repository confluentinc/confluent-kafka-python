# Share Consumer Error Reference

How librdkafka share-consumer (KIP-932) errors reach a Python application, and where that differs from the Java `KafkaShareConsumer`.

Verified against source on 2026-06-17:
- **confluent-kafka-python** `dev_kip-932_queues-for-kafka` — Python binding + C extension (`src/confluent_kafka/src/`, `src/confluent_kafka/deserializing_share_consumer.py`)
- **librdkafka** `dev_kip-932_error_translation` — error codes and the translation funnel (`src/rdkafka_share_acknowledgement.c`)
- **Apache Kafka** trunk — `KafkaShareConsumer`, `AcknowledgementCommitCallback`, `Errors.java`

## The model

Python collapses every librdkafka error to a single `KafkaError`, wrapped in a `KafkaException`. The app reads it back via `exc.args[0]` and branches on `.code()` / `.name()`. There is no typed exception hierarchy like Java's `RetriableException` / `InvalidRecordStateException` / `TopicAuthorizationException`.

`.fatal()` / `.retriable()` / `.txn_requires_abort()` are only populated ("rich") where librdkafka returns a `rd_kafka_error_t*` — that's `poll`, `commit_sync` (top-level), `commit_async`, and `close`. Everywhere else the source is a bare `rd_kafka_resp_err_t` and the flags are always `False` ("flag-less"). This tracks librdkafka's own mixed API rather than being a binding limitation. Outside the rich subset, branch on `.code()`, never on `.retriable()`.

### Translation funnel — affects Parts 2 and 3 only

When the linked librdkafka includes the KIP-932 funnel (`rd_kafka_share_translate_app_err()`), the per-partition channels rewrite `_TIMED_OUT*` → `REQUEST_TIMED_OUT` and `_DESTROY*` → `_TRANSPORT` before delivery. Top-level raised exceptions (Part 1) are never funneled. Linked against an older librdkafka without the funnel, the map and callback carry the raw `_TIMED_OUT` / `_DESTROY` codes instead.

### The main Java divergence

Java retries *retriable* acknowledgement errors internally to a deadline, so the share-session codes (122 / 123 / 133) and transient timeouts never reach the app directly — they surface as `TimeoutException`. librdkafka does no ack retry and surfaces the raw code immediately. Most rows below trace back to this one difference.

---

## Part 1 — API error responses (errors raised to the app)

| Exception thrown back to the app by Python | Underlying librdkafka error/exception | How/if it differs from the Java interface |
|---|---|---|
| `poll()` → **rich** `KafkaException`; codes `_STATE` (not subscribed, or explicit-ack mode with unacked records), `_MAX_POLL_EXCEEDED`, transport/auth/fatal | `rd_kafka_consume_batch…` returns `rd_kafka_error_t*` (rich, flags preserved) | Java `poll(Duration)` throws **typed**: `IllegalStateException`, `AuthenticationException`, `AuthorizationException`, `InvalidTopicException`, `WakeupException`, `InterruptException`, `KafkaException`. Python has **no wakeup/interrupt** concept and collapses all of these to one `KafkaError` (branch on `.code()` + flags). |
| `subscribe()` / `unsubscribe()` / `subscription()` → **flag-less** `KafkaException` (`_STATE`, `_INVALID_ARG`); bad arg shape → `TypeError`/`ValueError` | bare `rd_kafka_resp_err_t` | Java `subscribe` throws `IllegalArgumentException` (null/empty topics) + `KafkaException`. Same intent; Java carries no flags either but distinguishes by type. |
| `acknowledge()` / `acknowledge_offset()` → **flag-less** `KafkaException`; `_STATE` (implicit-mode misuse, record not in-flight, GAP, offset not found), `_INVALID_ARG` (null msg, partition/offset < 0, type ∉ [1..3]) | bare `rd_kafka_resp_err_t` | Java collapses all the state checks to one `IllegalStateException` and **does not validate args** (null record → implicit NPE; bad partition/type silently accepted). librdkafka/Python is stricter — explicit `_INVALID_ARG`. |
| `commit_sync(timeout)` → **rich** `KafkaException` **only** on top-level failure (`_STATE` / `_CONFLICT`). Per-partition errors and deadline are **not** raised — see Part 2 | top-level `rd_kafka_error_t*` (rich) | Java `commitSync()` returns `Map<TopicIdPartition, Optional<KafkaException>>` and throws `WakeupException` / `InterruptException` / `KafkaException` / `IllegalArgumentException` at top level. Both put per-partition outcomes and timeout in the return, not the throw. Java adds wakeup/interrupt; Python has neither. |
| `commit_async()` → **rich** `KafkaException` only on enqueue failure; results delivered to the callback — see Part 3 | top-level `rd_kafka_error_t*` (rich) | Java `commitAsync()` is `void` + `KafkaException`; results via callback. Equivalent. |
| `close()` → **rich** `KafkaException` | top-level `rd_kafka_error_t*` (rich) | Java `close()` / `close(Duration)` adds `WakeupException` / `InterruptException` / `IllegalArgumentException`; Python has none of these. |
| Cross-cutting: closed handle → `RuntimeError("Share consumer closed")`; bad arg shape → `TypeError`/`ValueError`; `on_commit` config key → `ValueError('on_commit is not supported')`; Ctrl-C during `poll` → `KeyboardInterrupt` | binding handle-guard / arg validation (not a librdkafka error) | Java uses `IllegalStateException` for a closed/invalid-state consumer; these Python built-in exception types have no Java analog. |

---

## Part 2 — Commit return map (`commit_sync` per-partition outcomes)

`commit_sync(timeout)` returns `Dict[TopicPartition, Optional[KafkaError]]`. These values are **delivered in the dict, not raised**. Each value is `None` (committed) or a **flag-less** `KafkaError`. The Java analog is `Map<TopicIdPartition, Optional<KafkaException>>`.

| `KafkaError` delivered in the dict value (Python) | Underlying librdkafka per-partition error | How/if it differs from the Java interface |
|---|---|---|
| `None` | `NO_ERROR` | `Optional.empty()`. Same. (Empty dict when no acks were pending; Java returns `Map.of()`.) |
| `KafkaError.INVALID_RECORD_STATE` (121) | `INVALID_RECORD_STATE` (broker, or locally synthesized when a sent partition is missing from the reply) | Java `Optional[InvalidRecordStateException]` (non-retriable). Same meaning; Java type vs Python code + `.name()`. |
| `KafkaError.NOT_LEADER_FOR_PARTITION` (6) | `NOT_LEADER_OR_FOLLOWER` (C `#define` alias of code 6) / local synth on stale or terminating leader | Java `NotLeaderOrFollowerException` (retriable → **Java retries first**). The Python constant is `NOT_LEADER_FOR_PARTITION`, not `NOT_LEADER_OR_FOLLOWER` (the latter never becomes a Python attribute). |
| `KafkaError.REQUEST_TIMED_OUT` (7) | post-funnel from `_TIMED_OUT` / `_TIMED_OUT_QUEUE`; also written directly by the `commit_sync` deadline timer | Java `Optional[TimeoutException]` **after** its retry deadline expires. librdkafka surfaces on the **first** RPC timeout (no retry). Both express timeout per-partition, not top-level. |
| `KafkaError._TRANSPORT` | post-funnel from `_DESTROY` / `_DESTROY_BROKER` | Java is **path-dependent**: `UnknownServerException` on the dedicated `ShareAcknowledge` RPC vs `DisconnectException` on the `ShareFetch`-piggyback RPC. librdkafka is **uniform** (`_TRANSPORT`). |
| `KafkaError.SHARE_SESSION_NOT_FOUND` (122), `INVALID_SHARE_SESSION_EPOCH` (123), `SHARE_SESSION_LIMIT_REACHED` (133) | surfaced **verbatim** (no ack retry) | **Hidden in Java** — all three are `RetriableException`; Java retries internally and the app only ever sees a `TimeoutException` on deadline. Biggest divergence. (The session is reset for recovery on both sides; only the in-flight ack visibility differs.) |
| `KafkaError._BAD_MSG` / `KafkaError._UNDERFLOW` | stamped raw per-partition on a corrupt or short wire frame | Java raises `SchemaException` on a background thread; affected acks then time out → per-partition `TimeoutException`. (The librdkafka source comment claims `INVALID_RECORD_STATE` but assigns the raw code — a known latent doc bug.) |
| `KafkaError.UNKNOWN_TOPIC_OR_PART` (3), `FENCED_LEADER_EPOCH`, `UNKNOWN_TOPIC_ID` (100), `INVALID_REQUEST`, `KAFKA_STORAGE_ERROR`, any other broker code | no-whitelist **pass-through verbatim** | Java maps each to its typed exception; retriable ones are retried to deadline first. |
| `KafkaError.FENCED_STATE_EPOCH` (124) — **never appears** | librdkafka never emits code 124 on the share path (dead code there) | Java has `FencedStateEpochException` (non-retriable) and can surface it. Python never sees it. |

---

## Part 3 — Acknowledgement-commit callback errors

Registered with `set_acknowledgement_commit_callback(cb)` — a runtime setter; there is no config key. Signature: `cb(offsets, exception)`, where `offsets` is `{TopicPartition: set[int]}` and `exception` is `None` or a **flag-less** `KafkaException`. It fires **once per partition**, and values are **delivered to the callback, not raised**. The catalog matches Part 2, except the `commit_sync` deadline-timer `REQUEST_TIMED_OUT` is map-only and never reaches the callback.

| `exception` arg the Python callback receives | Underlying librdkafka callback error (post-funnel) | How/if it differs from the Java interface |
|---|---|---|
| `None` | `NO_ERROR` | Java `null`. The **signature matches Java**: `AcknowledgementCommitCallback.onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception)` — same `(offsets, exception)` order. (The regular Python `Consumer.on_commit(err, partitions)` is the reverse order; the share consumer deliberately follows Java.) |
| `KafkaException(INVALID_RECORD_STATE)` (121) | `INVALID_RECORD_STATE` | Java `InvalidRecordStateException`. Same. |
| `KafkaException(NOT_LEADER_FOR_PARTITION)` (6) | `NOT_LEADER_OR_FOLLOWER` / local synth | Java `NotLeaderOrFollowerException` — but Java **retries it first**, so it rarely reaches the callback; librdkafka delivers it immediately. |
| `KafkaException(REQUEST_TIMED_OUT)` (7) | post-funnel from `_TIMED_OUT*` | Java `TimeoutException` after retries exhaust. librdkafka: immediate, no retry. (The deadline-timer variant of this code appears only in the Part 2 map, never here.) |
| `KafkaException(_TRANSPORT)` | post-funnel from `_DESTROY*` | Java path-dependent `UnknownServerException` / `DisconnectException`; librdkafka uniform `_TRANSPORT`. |
| `KafkaException(SHARE_SESSION_NOT_FOUND` / `INVALID_SHARE_SESSION_EPOCH` / `SHARE_SESSION_LIMIT_REACHED)` (122/123/133) | surfaced **verbatim** | **Hidden in Java** (retried → `TimeoutException`). The Java callback javadoc does not even list these. |
| `KafkaException(_BAD_MSG` / `_UNDERFLOW)` | corrupt frame, stamped raw | Java `SchemaException` (background thread) → eventual per-partition `TimeoutException`. |
| `KafkaException(UNKNOWN_TOPIC_OR_PART)` (3) and any other broker code | no-whitelist pass-through | The Java callback javadoc advertises only `AuthorizationException`, `InvalidRecordStateException`, `NotLeaderOrFollowerException`, `DisconnectException`, `WakeupException`, `InterruptException`, `KafkaException`; librdkafka can deliver codes outside that set. |

Notes: flags are always `False` on this path (branch on `.code()`); the `offsets` dict is still delivered alongside a non-null `exception`; Java fires the callback "after any retries have been performed," which is the mechanism that hides the retriable codes above.

---

## DeserializingShareConsumer note

`DeserializingShareConsumer` does not raise on a deserialization failure. It catches the deserializer exception and marks the message with `msg.set_error(KafkaError(KafkaError._VALUE_DESERIALIZATION, ...))` (or `_KEY_DESERIALIZATION`), leaving the raw bytes intact so the record is still acknowledgeable. Detect it in the poll loop with `if msg.error():` and branch on `.code()` — the same loop you already use for per-record broker errors. This differs from the regular `DeserializingConsumer`, which *raises* `ValueDeserializationError` / `KeyDeserializationError`; the share consumer never raises those.
