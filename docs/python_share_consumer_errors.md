# Python Share Consumer Errors

Every exception and `KafkaError` a `ShareConsumer` / `DeserializingShareConsumer` application can receive. For the librdkafka↔Python↔Java mapping behind these, see [share_consumer_error_reference.md](share_consumer_error_reference.md).

Verified against source on 2026-06-18: confluent-kafka-python `dev_kip-932_queues-for-kafka`, librdkafka `dev_kip-932_error_translation`.

## How errors reach you

Almost everything is a single `KafkaError`, wrapped in a `KafkaException`. Read it back with `exc.args[0]` and branch on `.code()` / `.name()` — there is no typed exception hierarchy. `.fatal()` / `.retriable()` are only meaningful on `poll` / `commit_sync` (top-level) / `commit_async` / `close`; everywhere else they're always `False`, so branch on `.code()`.

## 1. Python exception classes raised

| Class | When |
|---|---|
| `KafkaException` | Primary channel — wraps a `KafkaError`. Raised by `poll`, `subscribe`, `unsubscribe`, `subscription`, `acknowledge`, `acknowledge_offset`, `commit_sync` (top-level), `commit_async` (enqueue), `close`. |
| `RuntimeError` | `"Share consumer closed"` — any method called after `close()`. |
| `TypeError` | Wrong argument types (e.g. `None` topic, bad arg shape). |
| `ValueError` | Invalid argument values; also the rejected config key `on_commit`. |
| `KeyboardInterrupt` | Ctrl-C (SIGINT) during `poll()`. |

The share consumer does **not** raise `ConsumeError`, `KeyDeserializationError`, or `ValueDeserializationError` — those belong to the regular `DeserializingConsumer`.

## 2. KafkaError codes — internal/local (negative; single leading `_`)

| Code | Where delivered | Trigger |
|---|---|---|
| `_STATE` (-172) | raised | not subscribed; explicit-ack misuse; record not in-flight / GAP / offset not found; callback re-entrancy |
| `_CONFLICT` (-173) | raised | concurrent call from another thread |
| `_INVALID_ARG` (-186) | raised | `acknowledge` null msg / partition < 0 / offset < 0 / type ∉ [1..3]; empty or invalid subscribe topic |
| `_MAX_POLL_EXCEEDED` (-147) | raised (`poll`) | app didn't poll within `max.poll.interval.ms` |
| `_FATAL` (-150) → resolved to concrete code | raised (`poll` / `commit_sync` / `close`) | a fatal client error; the app **never sees `-150`** — it's substituted to the concrete fatal code (a ShareGroupHeartbeat code: `GROUP_AUTHORIZATION_FAILED`, `GROUP_ID_NOT_FOUND`, `UNSUPPORTED_VERSION`, `_UNSUPPORTED_FEATURE`, `INVALID_REQUEST`, `GROUP_MAX_SIZE_REACHED`) |
| `_AUTHENTICATION` (-169) | `error_cb` only (if registered) | SASL/SSL auth failure; **not** raised from `poll()` — auth errors go to `rk_rep` and fire `error_cb` during a serving call. Transient, not fatal |
| `_TRANSPORT` (-195) | commit map / ack callback (post-funnel from `_DESTROY*`); also `error_cb` (if registered) | broker connection dropped |
| `_ALL_BROKERS_DOWN` (-187) | `error_cb` only (if registered) | all brokers down; otherwise only logged — the event is gated on an `error_cb` being set |
| `_TIMED_OUT` (-185), `_TIMED_OUT_QUEUE` (-166) | commit map / ack callback | **only** if the linked librdkafka lacks the translation funnel (otherwise → `REQUEST_TIMED_OUT`) |
| `_DESTROY` (-197), `_DESTROY_BROKER` (-137) | commit map / ack callback | **only** if the linked librdkafka lacks the funnel (otherwise → `_TRANSPORT`) |
| `_BAD_MSG` (-199), `_UNDERFLOW` (-155) | commit map / ack callback | corrupt or short wire frame on the ack path (surfaced raw — the in-source comment claiming `INVALID_RECORD_STATE` is a code bug) |
| `_KEY_DESERIALIZATION` (-160), `_VALUE_DESERIALIZATION` (-159) | `msg.error()` only | `DeserializingShareConsumer` — set Python-side when a key/value deserializer raises |

## 3. KafkaError codes — broker (positive)

| Code | Where delivered | Trigger |
|---|---|---|
| `INVALID_RECORD_STATE` (121) | commit map / ack callback | ack rejected (e.g. acquisition lock expired) |
| `SHARE_SESSION_NOT_FOUND` (122) | commit map / ack callback | share session lost (Java hides via retry) |
| `INVALID_SHARE_SESSION_EPOCH` (123) | commit map / ack callback | stale session epoch (Java hides) |
| `SHARE_SESSION_LIMIT_REACHED` (133) | commit map / ack callback | too many sessions (Java hides) |
| `NOT_LEADER_FOR_PARTITION` (6) | commit map / ack callback | leader moved |
| `UNKNOWN_TOPIC_OR_PART` (3) | commit map / ack callback | topic/partition gone |
| `UNKNOWN_TOPIC_ID` (100) | commit map / ack callback | topic id mismatch |
| `FENCED_LEADER_EPOCH` (74) | commit map / ack callback | stale leader epoch |
| `REQUEST_TIMED_OUT` (7) | commit map / ack callback | post-funnel timeout; also the `commit_sync` deadline (map only) |
| `INVALID_REQUEST` (42) | commit map / ack callback | malformed ack request |
| `KAFKA_STORAGE_ERROR` (56) | commit map / ack callback | broker storage error |
| `TOPIC_AUTHORIZATION_FAILED` (29) | raised (`poll`) / ack path | not authorized to the topic |
| `GROUP_AUTHORIZATION_FAILED` (30) | raised (`poll`) / ack path | not authorized to the share group |
| `TOPIC_EXCEPTION` (17) | raised (`poll`) | invalid topic in the subscription |
| `INVALID_MSG` (2) | `msg.error()` / fetch | corrupt record (Java `CorruptRecordException`) |

**No-whitelist pass-through:** the commit map and ack callback do not filter broker codes — **any** `KafkaError.<BROKER_CODE>` can appear there verbatim, not only the ones listed. Treat unknown codes as failures.

**Never appears:** `FENCED_STATE_EPOCH` (124) — librdkafka doesn't emit it on the share path (live only in Java). `_PARTITION_EOF` — share groups have no EOF concept.

## 4. Index by delivery channel

- **Raised `KafkaException`:** `_STATE`, `_CONFLICT`, `_INVALID_ARG`, `_MAX_POLL_EXCEEDED`, `_FATAL` (as the concrete fatal code, never `-150`), `TOPIC_EXCEPTION`, `TOPIC_AUTHORIZATION_FAILED`, `GROUP_AUTHORIZATION_FAILED`.
- **`error_cb` (only if an `error_cb` is registered; fires as a callback during a `poll` / `commit_sync` / `close` drain — not raised):** `_AUTHENTICATION`, `_TRANSPORT` (generic connection loss), `_ALL_BROKERS_DOWN`.
- **`commit_sync()` dict values and ack-callback `exception`:** `INVALID_RECORD_STATE`, session codes (122/123/133), `NOT_LEADER_FOR_PARTITION`, `UNKNOWN_TOPIC_OR_PART`, `UNKNOWN_TOPIC_ID`, `FENCED_LEADER_EPOCH`, `REQUEST_TIMED_OUT`, `INVALID_REQUEST`, `KAFKA_STORAGE_ERROR`, `_TRANSPORT`, `_BAD_MSG` / `_UNDERFLOW`, plus any other broker code.
- **`msg.error()` (per record in the poll list):** `_KEY_DESERIALIZATION`, `_VALUE_DESERIALIZATION` (DeserializingShareConsumer), plus any per-record broker error the core attaches.
- **Python built-ins:** `RuntimeError`, `TypeError`, `ValueError`, `KeyboardInterrupt`.

## 5. How the local codes compare to the Java share consumer

Java has no error codes — each client-side condition is a typed exception, silent internal handling, or an internal retry. So none of these is "returned the same" in form; what matters is the channel and the behavior.

- **Synchronous-API conditions match on channel.** `_STATE`, `_CONFLICT`, `_FATAL` (and `_INVALID_ARG` on `subscribe`) are raised from the offending method in Python, exactly where Java throws. The only difference is representation — Java uses a specific exception class; Python gives one `KafkaException` and you branch on `.code()`.
- **Data-path conditions diverge** — deserialization, max-poll, transport, timeout, and corrupt frames behave differently, sometimes a lot.

Verdict key: **≈** same channel (raised from the same method; differ only in Java-type vs Python-code) · **◑** partial · **✗** differs in channel or behavior.

| Python `KafkaError` (delivery) | Java equivalent (type · where) | Same? |
|---|---|---|
| `_STATE` (raised) | `IllegalStateException` from `poll` / `acknowledge` / `acquire()` — all four triggers collapse to this in Java too | ≈ typed vs code |
| `_CONFLICT` (raised) | `ConcurrentModificationException` from `acquire()` | ≈ but Java's is `java.util`, **not** a `KafkaException` |
| `_INVALID_ARG` (raised) | `subscribe`: `IllegalArgumentException`. `acknowledge` args: **not validated** (null → `NullPointerException`; bad partition/offset → `IllegalStateException`) | ◑ `subscribe` matches; `acknowledge` differs — Java doesn't validate |
| `_MAX_POLL_EXCEEDED` (raised, `poll`) | **No exception** — silently sends LeaveGroup, goes STALE, rejoins on next `poll()` | ✗ Java never surfaces it to the app |
| `_FATAL`→concrete code (raised, `poll`) | The specific fatal exception re-thrown from `poll()`; consumer poisoned for all later calls | ≈ same channel; both surface the concrete cause — Java as the precise type, Python as the concrete `KafkaError` code (never `-150`) |
| `_AUTHENTICATION` (`error_cb` only) | `AuthenticationException` from `poll()` | ✗ different channel — Java raises from `poll()`; Python only fires `error_cb` (if registered), never raises |
| `_TRANSPORT` (commit map / ack cb / `error_cb`) | Retried internally; surfaces only as `TimeoutException` after `default.api.timeout.ms`, else `poll()` returns empty | ✗ Java hides transient transport errors |
| `_ALL_BROKERS_DOWN` (`error_cb`) | No direct signal; retries → eventual timeout / empty poll | ✗ no equivalent in Java |
| `_TIMED_OUT` / `_TIMED_OUT_QUEUE` (commit map / ack cb) | `TimeoutException` from `commitSync()`; `poll()` returns empty rather than throwing | ✗ different channel — per-partition code vs thrown from `commitSync` |
| `_DESTROY` / `_DESTROY_BROKER` (commit map / ack cb) | `IllegalStateException("This consumer has already been closed.")` on reuse; in-flight ops wrapped in `KafkaException` during close | ✗ reuse-guard, not a per-operation code |
| `_BAD_MSG` / `_UNDERFLOW` (commit map / ack cb) | Wire underflow → `SchemaException` **swallowed** in the network thread; record CRC → `CorruptRecordException` from `poll()` | ✗ Java doesn't surface wire underflow at all |
| `_KEY_DESERIALIZATION` / `_VALUE_DESERIALIZATION` (`msg.error()` only — not raised) | `RecordDeserializationException` **thrown from `poll()`**; aborts the batch; failed record **auto-RELEASEd**; raw bytes carried in the exception | ✗ biggest divergence — different channel *and* record disposition |

Source (Apache Kafka): `ShareConsumerImpl.java` (`616`, `1122`, `1137`, `1143`, `1208`, `1226`), `AbstractHeartbeatRequestManager.java` (`171`, `270`, `476`), `ShareCompletedFetch.java` (`326–354`), `NetworkClientDelegate.java` (`443`), `ConsumerNetworkThread.java` (`163`). Broker-code (positive) Java mappings are in [share_consumer_error_reference.md](share_consumer_error_reference.md) (Parts 2 and 3).

### Porting from Java — watch for these

- **Deserialization:** Java's `try/except poll()` catches `RecordDeserializationException`; in Python the bad record comes back with `msg.error()` set (raw bytes intact, still acknowledgeable) and nothing is raised — a Java-shaped catch block does nothing here.
- **Max-poll:** Java handles it invisibly; Python raises `_MAX_POLL_EXCEEDED`.
- **Auth / transport / all-brokers-down:** Java raises (auth) or retries and hides (transport) these; Python surfaces `_AUTHENTICATION` / `_TRANSPORT` / `_ALL_BROKERS_DOWN` via `error_cb` **only if an `error_cb` is registered** (otherwise only logged) — never raised from `poll()`.
- **Corrupt wire frame:** Java swallows it in the network thread; Python reports `_BAD_MSG` / `_UNDERFLOW`.
- **`_CONFLICT`:** Java's `ConcurrentModificationException` is not in the `KafkaException` tree; Python's is — a `catch (KafkaException)` in Java would miss it.
