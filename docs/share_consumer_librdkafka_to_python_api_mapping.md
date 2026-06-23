# Share Consumer: librdkafka → Python API Mapping

Reference for editing the **KIP-932 - Python - Public Interfaces** Confluence page
(<https://confluentinc.atlassian.net/wiki/spaces/~6242932dfd5e45007042c332/pages/5557224633>).

That doc is a *Python* public-interfaces spec but currently references librdkafka C
symbols (functions, types, error constants) and CPython-internal symbols throughout.
This table maps each C symbol the doc references to the Python API that should replace
it. Line numbers are where the symbol appears in the page body (as of the 2026-06-15
fetch). Mappings verified against the C binding (`src/confluent_kafka/src/ShareConsumer.c`),
the type stubs (`cimpl.pyi`), and librdkafka.

---

## 1. librdkafka functions → Python public API

Wrapped 1:1 by the binding. Use the Python method in the doc; keep the C name only as an
optional "(internally calls …)" aside.

| librdkafka function (in doc) | Doc lines | Python API that should replace it |
|---|---|---|
| `rd_kafka_share_consumer_new()` | 209 | `ShareConsumer(config)` (constructor) |
| `rd_kafka_share_subscribe()` | — | `ShareConsumer.subscribe(topics)` |
| `rd_kafka_share_unsubscribe()` | 314 | `ShareConsumer.unsubscribe()` |
| `rd_kafka_share_subscription()` | 351 | `ShareConsumer.subscription()` |
| `rd_kafka_share_consume_batch()` | 397, 410, 854 | `ShareConsumer.poll(timeout)` |
| `rd_kafka_share_acknowledge()` (always ACCEPT) | 459 | `ShareConsumer.acknowledge(message)` |
| `rd_kafka_share_acknowledge_type()` | 460 | `ShareConsumer.acknowledge(message, ack_type=...)` |
| `rd_kafka_share_acknowledge_offset()` | 461, 522, 557, 587 | `ShareConsumer.acknowledge_offset(topic, partition, offset, ack_type=...)` |
| `rd_kafka_share_commit_sync()` | 41, 651 | `ShareConsumer.commit_sync(timeout)` |
| `rd_kafka_share_commit_async()` | (prose) | `ShareConsumer.commit_async()` |
| `rd_kafka_share_consumer_close()` + `rd_kafka_share_destroy()` | 768–769, 789, 815 | `ShareConsumer.close()` (one call bundles both) |
| ⚠️ `rd_kafka_conf_set_share_acknowledgement_commit_cb()` | 853 | `ShareConsumer.set_acknowledgement_commit_callback(callback)` |
| `rd_kafka_share_set_log_queue()` | 24 | *No public method* — wired automatically when a `log_cb`/`logger` is in the config dict |
| `rd_kafka_share_consumer_closed_err()` | 357 | *No public API* — surfaces as `RuntimeError("Share consumer closed")` |

> ⚠️ The callback row is doubly wrong. The conf-time function
> `rd_kafka_conf_set_share_acknowledgement_commit_cb()` **does not exist** in librdkafka —
> the real C is the *runtime* `rd_kafka_share_set_acknowledgement_commit_cb()`, and the
> Python surface is the **method** `set_acknowledgement_commit_callback(cb)` where
> `cb(offsets: Dict[TopicPartition, Set[int]], exc: Optional[KafkaException])`.

---

## 2. librdkafka C types → Python types

| C type (in doc) | Doc lines | Python equivalent |
|---|---|---|
| `rd_kafka_conf_t` | 158, 196, 231, 234 | `dict` (the config dict) |
| `rd_kafka_message_t *` | 428 | `Message` |
| `rd_kafka_topic_partition_list_t` | 645, 670 | `list[TopicPartition]` (and `Dict[TopicPartition, Optional[KafkaError]]` for the `commit_sync()` result) |
| `rd_kafka_resp_err_t` | 207, 313, 329, 534, 587 | `KafkaError` (carried inside `KafkaException`) |
| `rd_kafka_error_t` / `rd_kafka_error_t*` | 409, 650, 720 | `KafkaError` (carried inside `KafkaException`) |
| `rd_kafka_share_t *` | 145, 154, 235 | *Opaque* — the `ShareConsumer` object itself; no handle exposed |
| `rd_kafka_t *` | 25 | *Not exposed in Python* (no accessor; the doc itself notes this option was rejected) |

---

## 3. librdkafka error constants → `KafkaError` codes

All verified present on `confluent_kafka.KafkaError`.

| C constant (in doc) | Doc lines | Python constant | Value |
|---|---|---|---|
| `RD_KAFKA_RESP_ERR__INVALID_ARG` | 269, 535 | `KafkaError._INVALID_ARG` | -186 |
| `RD_KAFKA_RESP_ERR__STATE` | 411, 537 | `KafkaError._STATE` | -172 |
| `RD_KAFKA_RESP_ERR__UNKNOWN_GROUP` | 352 | `KafkaError._UNKNOWN_GROUP` | -179 |
| `RD_KAFKA_RESP_ERR__TIMED_OUT` | 355 | `KafkaError._TIMED_OUT` | -185 |

Inspect via `exc.args[0].code()`, e.g. `if e.args[0].code() == KafkaError._STATE:`.

(Deserialization paths additionally use `KafkaError._KEY_DESERIALIZATION` (-160) and
`KafkaError._VALUE_DESERIALIZATION` (-159), already Python-side.)

---

## 4. Internal C / CPython symbols — no public Python API

Not librdkafka public APIs and not callable by users. In a *public interfaces* doc these
are noise; strip them or move to a clearly-labeled "Implementation Notes" aside.

| Symbol | Doc lines | What to say instead (Python-visible effect) |
|---|---|---|
| `ShareConsumerHandle` (struct) | 29, 147, 154 | "the `ShareConsumer` type is opaque" — drop the struct |
| `Handle` (base struct) | 144, 155, 160 | internal base; drop |
| `common_conf_setup()` | 158, 197, 231 | "the config `dict` is parsed/validated internally" |
| `rd_kafka_conf_set()` | 208 | a rejected property raises `KafkaException` (or `ValueError`) |
| `PyErr_CheckSignals()` | 39 | "`poll()` checks for pending signals between chunks and raises `KeyboardInterrupt`" |
| `CallState` / `CallState_begin` / `CallState_end` | 795, 810 | "the GIL is released during the blocking call" |
| `ShareConsumer_dealloc` | 815 | "if garbage-collected without `close()`, the handle is destroyed via the GC/`__del__` path" |

---

## Suggested rewrite pattern for the doxygen `@raises` blocks

Today they read, e.g.:

> `@raises KafkaException wrapping the rd_kafka_resp_err_t from rd_kafka_share_acknowledge_offset()`

For a Python audience, rewrite as:

> Raises `KafkaException`; inspect `e.args[0].code()` for the `KafkaError`
> (e.g. `KafkaError._STATE`). *(internally calls `rd_kafka_share_acknowledge_offset`)*
