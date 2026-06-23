# Share Consumer (KIP-932) — Errors Exposed to Python Applications

This document describes how the `confluent_kafka` Python client surfaces the
**Share Consumer** (KIP-932) errors that originate in the underlying librdkafka
C library, across the **three** points from which errors reach an application:

1. **API responses** — raised exceptions / return values of `ShareConsumer` methods.
2. **`commit_sync` per-partition map** — the dict returned by `ShareConsumer.commit_sync()`.
3. **Acknowledgement commit callback** — the callable registered with `set_acknowledgement_commit_callback()`.

> The underlying librdkafka error catalog (every `rd_kafka_resp_err_t` that can
> be produced at each point) is documented separately in the librdkafka repo at
> `SHARE_CONSUMER_ERRORS.md`. This document covers the **Python binding layer**:
> how those C errors are translated and what the application actually sees.
>
> Branch: `dev_kip-932_queues-for-kafka_leak_tests`. Binding:
> `src/confluent_kafka/src/ShareConsumer.c`; generic machinery:
> `src/confluent_kafka/src/confluent_kafka.c` / `.h`.

## The Python-facing API

```python
class ShareConsumer:
    def __init__(self, config: Dict[str, Any]) -> None: ...
    def subscribe(self, topics: List[str]) -> None: ...
    def unsubscribe(self) -> None: ...
    def subscription(self) -> List[str]: ...
    def poll(self, timeout: float = -1) -> List[Message]: ...
    def acknowledge(self, message: Message, ack_type: AcknowledgeType = ...) -> None: ...
    def acknowledge_offset(self, topic, partition, offset, ack_type=...) -> None: ...
    def commit_sync(self, timeout: float = 60) -> Dict[TopicPartition, Optional[KafkaError]]: ...
    def commit_async(self) -> None: ...
    def set_acknowledgement_commit_callback(self, callback: Optional[AcknowledgementCommitCallback]) -> None: ...
    def close(self) -> None: ...
```

---

## The exposure machinery

### `KafkaError` (`cimpl`)

A real exception subclass (`confluent_kafka.c:64-84`, `Py_TPFLAGS_BASE_EXC_SUBCLASS`)
wrapping a librdkafka error code plus flags:

| Attribute / method | Returns | Source |
|--------------------|---------|--------|
| `.code()` | `int` — the `rd_kafka_resp_err_t` value | `confluent_kafka.c:89` |
| `.str()` | `str` — human-readable message (`rd_kafka_err2str` fallback) | `confluent_kafka.c:93` |
| `.name()` | `str` — the enum symbol (`rd_kafka_err2name`, e.g. `"INVALID_RECORD_STATE"`) | `confluent_kafka.c:100` |
| `.fatal()` | `bool` | `confluent_kafka.c:105` |
| `.retriable()` | `bool` | `confluent_kafka.c:111` |
| `.txn_requires_abort()` | `bool` | `confluent_kafka.c:117` |

Every librdkafka code is also exposed as a **class constant** (auto-generated
from `rd_kafka_get_err_descs`, `confluent_kafka.c:3640`): broker codes as
`KafkaError.<NAME>` (e.g. `KafkaError.INVALID_RECORD_STATE`,
`KafkaError.GROUP_AUTHORIZATION_FAILED`), local codes keep the leading
underscore (e.g. `KafkaError._STATE`, `KafkaError._TIMED_OUT`,
`KafkaError._MAX_POLL_EXCEEDED`). `KafkaError` compares equal to a raw int code,
so `err.code() == KafkaError.INVALID_RECORD_STATE` works.

### `KafkaException`

Wraps a `KafkaError`; the application extracts it via **`exc.args[0]`**
(`confluent_kafka.c:3780`).

### Translation helpers and **flag fidelity**

The most important subtlety: whether `.fatal()` / `.retriable()` survive the
translation depends on which helper the binding uses, which in turn depends on
whether the librdkafka API returns a rich `rd_kafka_error_t*` or a bare
`rd_kafka_resp_err_t`.

| Helper | Input | Produces | fatal/retriable preserved? | Location |
|--------|-------|----------|----------------------------|----------|
| `cfl_PyErr_from_error_destroy(error)` | `rd_kafka_error_t*` | raised `KafkaException` | **YES** (via `rd_kafka_error_is_fatal/_is_retriable`) | `confluent_kafka.h:208`, `confluent_kafka.c:430` |
| `cfl_PyErr_Format(err, fmt, …)` | `rd_kafka_resp_err_t` | raised `KafkaException` | **NO** (always `False`) | `confluent_kafka.h:198` |
| `KafkaError_new_or_None(err, str)` | `rd_kafka_resp_err_t` | `KafkaError` or `None` (non-raising) | **NO** (always `False`) | `confluent_kafka.c:416` |

### Non-Kafka exceptions

* **Closed handle** → plain `RuntimeError("Share consumer closed")` on every method (`confluent_kafka.h:451`).
* **Bad arguments** → `TypeError` / `ValueError` (from `PyArg_*` or explicit `PyErr_SetString`).
* **Allocation failure** → `MemoryError`.
* **`KeyboardInterrupt`** can propagate out of `poll()`.

---

## Point 1 — API responses

Every method also raises `RuntimeError` on a closed handle and `TypeError` on bad
args (omitted from rows below for brevity). "Flag-less" / "rich" refers to the
[flag-fidelity](#translation-helpers-and-flag-fidelity) of the raised `KafkaException`.

| Method | Returns | On error | Exception (helper) | librdkafka source |
|--------|---------|----------|--------------------|-------------------|
| `ShareConsumer(config)` | instance | rejected config key `on_commit` | `ValueError` | `ShareConsumer.c:1032` |
| | | invalid config / missing `group.id` | `KafkaException` / `TypeError` (from `common_conf_setup`) | `ShareConsumer.c:1066` |
| | | handle creation failed | `KafkaException` **flag-less** (`cfl_PyErr_Format(rd_kafka_last_error())`) | `rd_kafka_share_consumer_new` — `ShareConsumer.c:1077` |
| `subscribe(topics)` | `None` | subscribe failed | `KafkaException` **flag-less** (`cfl_PyErr_Format`) | `rd_kafka_share_subscribe` (`resp_err_t`) — `ShareConsumer.c:155` |
| `unsubscribe()` | `None` | unsubscribe failed | `KafkaException` **flag-less** | `rd_kafka_share_unsubscribe` — `ShareConsumer.c:182` |
| `subscription()` | `List[str]` (topic names) | query failed | `KafkaException` **flag-less** | `rd_kafka_share_subscription` — `ShareConsumer.c:210` |
| `poll(timeout)` | `List[Message]` (empty on timeout) | batch error | `KafkaException` **rich** (fatal **or** retriable) (`cfl_PyErr_from_error_destroy`) | `rd_kafka_share_consume_batch` (`error_t*`) — `ShareConsumer.c:326` |
| | | per-record error *(latent)* | `Message` whose `.error()` is a **flag-less** `KafkaError` (not raised) | `rkm->err` via `Message_new0` — `confluent_kafka.c:1080` |
| `acknowledge(msg, ack_type)` | `None` | ack failed | `KafkaException` **flag-less** | `rd_kafka_share_acknowledge_offset` — `ShareConsumer.c:427` |
| `acknowledge_offset(t,p,o,ack_type)` | `None` | ack failed | `KafkaException` **flag-less** | `rd_kafka_share_acknowledge_offset` — `ShareConsumer.c:469` |
| `commit_sync(timeout)` | `Dict[TopicPartition, KafkaError\|None]` | **top-level** error | `KafkaException` **rich** | `rd_kafka_share_commit_sync` (`error_t*`) — `ShareConsumer.c:521` |
| `commit_async()` | `None` | enqueue error | `KafkaException` **rich** | `rd_kafka_share_commit_async` — `ShareConsumer.c:570` |
| `close()` | `None` (idempotent) | close/destroy error | `KafkaException` **rich** | `rd_kafka_share_consumer_close` / `_destroy` — `ShareConsumer.c:743` |

### `poll()` — error dichotomy

`poll()` returns a **list of `Message`** objects. There are two channels:

* **Raised `KafkaException`** — in the current librdkafka, `consume_batch`
  surfaces every error as a single top-level `rd_kafka_error_t*`, which `poll()`
  raises as a **rich** `KafkaException` (fatal or retriable per the underlying
  error). This is the channel actually exercised today.
* **`Message.error()`** — the generic machinery supports per-record error
  `Message`s (flag-less `KafkaError` via `Message_new0`), but librdkafka does
  not currently mix an error op into a record batch, so this channel is latent.

```python
try:
    messages = sc.poll(timeout=1.0)
except KafkaException as e:
    err = e.args[0]                 # KafkaError
    if err.fatal():
        raise                       # terminate
    # else retriable: back off and re-poll
else:
    for msg in messages:
        if msg.error():             # informational, flag-less KafkaError
            log.warning("%s", msg.error())
            continue
        handle(msg)
```

---

## Point 2 — `commit_sync` per-partition map

```python
results: Dict[TopicPartition, Optional[KafkaError]] = sc.commit_sync(timeout=10)
```

* Built by `c_parts_to_dict_topic_partition_to_error` (`confluent_kafka.c:1603`).
* **Value** per partition = `KafkaError_new_or_None(rktpar->err, NULL)` → `None`
  if that partition committed OK, else a **flag-less** `KafkaError` (`confluent_kafka.c:1617`).
* **Key** `TopicPartition` *also* carries the same code on its `.error`
  attribute (`TopicPartition_setup`, `confluent_kafka.c:1313`) — redundant with the value.
* `{}` (empty dict) when no acknowledgements were pending (`ShareConsumer.c:527`).
* The **top-level** error (e.g. invalid argument, closed) is **raised** as a rich
  `KafkaException` and never appears in the dict (`ShareConsumer.c:521`).

```python
results = sc.commit_sync(timeout=10)
for tp, err in results.items():
    if err is not None:
        log.warning("commit failed for %s[%d]: %s (%s)",
                    tp.topic, tp.partition, err.str(), err.name())
```

---

## Point 3 — Acknowledgement commit callback

Registered **only** at runtime — there is **no config-dict key**:

```python
sc.set_acknowledgement_commit_callback(callback)   # or None to clear
```

The application callback receives exactly **two positional arguments**
(`cimpl.pyi:53`, `_types.py:39`):

```python
def callback(offsets: Dict[TopicPartition, Set[int]],
             exception: Optional[KafkaException]) -> None:
    ...
```

* `offsets` — dict mapping each `TopicPartition` to a `set[int]` of acknowledged
  offsets (built by `c_share_partition_offsets_list_to_py_dict`, `confluent_kafka.c:1644`).
* `exception` — a `KafkaException` (**flag-less**, wrapping `KafkaError_new_or_None(err)`)
  on failure, or `None` on success (`ShareConsumer.c:611-623`).

**Argument order is `(offsets, exception)`** — the *opposite* of the regular
`Consumer`'s `on_commit(err, partitions)` (noted in `cimpl.pyi:54`).

### librdkafka → Python shape change

librdkafka fires this callback **once per partition** and delivers that
partition's result in the C-level top-level `err`. The binding maps that single
`err` to the **`exception` argument** — *not* to a per-`TopicPartition.error`.
So in Python:

* each invocation normally carries one `TopicPartition` key in `offsets`;
* success/failure is read from the `exception` argument, not per-key;
* the offsets-dict **values are plain `set[int]`** with no error attached
  (unlike `commit_sync`, where the error rides the dict value).

### Dispatch, re-entrancy, and exceptions

* Always dispatched on the **application thread**, from inside `poll()`,
  `commit_sync()`, or `close()` (`ShareConsumer.c:954`). `commit_async()` returns
  immediately; its results arrive on a later serving call.
* Calling any `ShareConsumer` method from inside the callback raises
  `KafkaError._STATE` (re-entrancy guard, `ShareConsumer.c:968`).
* If the callback **raises**, the exception is stashed and **re-raised out of the
  serving method** (`poll`/`commit_*`/`close`) via the `CallState` crash
  mechanism (`ShareConsumer.c:638-646`; `confluent_kafka.c:3114-3168`).

```python
def on_ack(offsets, exc):
    for tp, acked in offsets.items():
        if exc is None:
            log.info("acked %s[%d]: %s", tp.topic, tp.partition, sorted(acked))
        else:
            kerr = exc.args[0]      # KafkaError
            log.warning("ack failed %s[%d]: %s", tp.topic, tp.partition, kerr.name())

sc.set_acknowledgement_commit_callback(on_ack)
```

---

## The error list propagated to Python

Translation is **mechanical**: every librdkafka `rd_kafka_resp_err_t` from the
share-consumer paths reaches Python unchanged as a `KafkaError` whose `.code()`
is the same integer and `.name()` is the same symbol. The full set of codes per
point is in the librdkafka `SHARE_CONSUMER_ERRORS.md`. The Python-visible
representation is summarized here:

| librdkafka code | Python constant | Reaches app via |
|-----------------|-----------------|-----------------|
| **Local (negative)** | | |
| `RD_KAFKA_RESP_ERR__STATE` | `KafkaError._STATE` | poll/acknowledge*/subscribe raise; ack-callback re-entrancy |
| `RD_KAFKA_RESP_ERR__CONFLICT` | `KafkaError._CONFLICT` | API raise (concurrent thread) |
| `RD_KAFKA_RESP_ERR__INVALID_ARG` | `KafkaError._INVALID_ARG` | acknowledge*/subscribe/close_queue raise; per-partition |
| `RD_KAFKA_RESP_ERR__TIMED_OUT` | `KafkaError._TIMED_OUT` | subscription raise; per-partition |
| `RD_KAFKA_RESP_ERR__TRANSPORT` | `KafkaError._TRANSPORT` | per-partition (map/callback) |
| `RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED` | `KafkaError._MAX_POLL_EXCEEDED` | poll raise (retriable) |
| `RD_KAFKA_RESP_ERR__FATAL` | resolved to specific code | poll raise (fatal) |
| `RD_KAFKA_RESP_ERR__DESTROY` / `__DESTROY_BROKER` | `KafkaError._DESTROY` / `_DESTROY_BROKER` | close raise; per-partition |
| `RD_KAFKA_RESP_ERR__NOT_CONFIGURED`, `__CRIT_SYS_RESOURCE` | `KafkaError._NOT_CONFIGURED`, `_CRIT_SYS_RESOURCE` | (config/SASL setters) |
| **Broker (positive)** | | |
| `INVALID_RECORD_STATE` (121) | `KafkaError.INVALID_RECORD_STATE` | commit_sync map / ack-callback |
| `SHARE_SESSION_NOT_FOUND` (122) | `KafkaError.SHARE_SESSION_NOT_FOUND` | commit_sync map / ack-callback |
| `INVALID_SHARE_SESSION_EPOCH` (123) | `KafkaError.INVALID_SHARE_SESSION_EPOCH` | commit_sync map / ack-callback |
| `SHARE_SESSION_LIMIT_REACHED` (133) | `KafkaError.SHARE_SESSION_LIMIT_REACHED` | commit_sync map / ack-callback |
| `NOT_LEADER_OR_FOLLOWER`, `FENCED_LEADER_EPOCH`, `UNKNOWN_TOPIC_OR_PART`, `UNKNOWN_TOPIC_ID` | same names | commit_sync map / ack-callback |
| `TOPIC_AUTHORIZATION_FAILED` (29) | `KafkaError.TOPIC_AUTHORIZATION_FAILED` | poll raise; subscription-topic error |
| `GROUP_AUTHORIZATION_FAILED` (30) | `KafkaError.GROUP_AUTHORIZATION_FAILED` | poll raise (fatal, heartbeat) |
| `GROUP_MAX_SIZE_REACHED`, `INVALID_REQUEST`, `UNSUPPORTED_VERSION`, `GROUP_ID_NOT_FOUND` | same names | poll raise (fatal, heartbeat) |
| `REQUEST_TIMED_OUT` (7) | `KafkaError.REQUEST_TIMED_OUT` | commit_sync map (deadline) |
| `TOPIC_EXCEPTION` | `KafkaError.TOPIC_EXCEPTION` | poll raise (subscription) |

> Any broker code can in principle reach the commit_sync map / ack-callback,
> because librdkafka stamps per-partition and propagated errors verbatim; the set
> above reflects normal operation.

---

## Caveats & work-in-progress

* **`.fatal()` / `.retriable()` are only meaningful on errors from `poll`,
  `commit_sync` (top-level), `commit_async`, and `close`.** These are the only
  four share APIs librdkafka declares as returning a rich `rd_kafka_error_t*`
  (which carries the fatal/retriable flags), and the binding preserves them via
  `cfl_PyErr_from_error_destroy`. **Everywhere else the underlying librdkafka
  value is a bare `rd_kafka_resp_err_t` that carries no flags to begin with** —
  `subscribe`, `unsubscribe`, `subscription`, `acknowledge`, `acknowledge_offset`
  (declared as returning `rd_kafka_resp_err_t`), and the `commit_sync` dict
  values, the ack-callback `exception`, and `Message.error()` (whose source is a
  `rd_kafka_topic_partition_t.err` / a callback `err` param, both bare
  `rd_kafka_resp_err_t`). At those points `.fatal()`/`.retriable()` are always
  `False`. This **faithfully mirrors librdkafka's mixed API surface — it is NOT a
  binding-layer downgrade** (no flagged error is being stripped). Surfacing
  richer per-partition error information would require a librdkafka API change,
  anticipated by the `TODO KIP-932` at `rdkafka_fetcher.c:2042` to move
  per-partition results from `rd_kafka_resp_err_t` to `rd_kafka_error_t`.
* **commit_sync vs. ack-callback asymmetry.** `commit_sync` puts the
  per-partition error in the dict **value**; the ack-callback puts it in the
  **`exception` argument** and leaves the offsets-dict values as plain `set[int]`.
* **No config-dict key** for the acknowledgement-commit callback — runtime setter only.
* **`poll()` per-record `Message.error()`** is supported by the machinery but not
  currently emitted by librdkafka (errors come as a raised `KafkaException`).
* Open `TODO KIP-932` items in the binding: `commit_sync` keys are `TopicPartition`
  (no topic UUID) pending a future `TopicIdPartition` (`cimpl.pyi:589`); the
  provisional error-mapping note that all codes currently map to `KafkaException`
  (`ShareConsumer.c:849-865`).

---

### Source files

`src/confluent_kafka/src/ShareConsumer.c`,
`src/confluent_kafka/src/confluent_kafka.c`,
`src/confluent_kafka/src/confluent_kafka.h`,
`src/confluent_kafka/cimpl.pyi`, `src/confluent_kafka/_types.py`,
`src/confluent_kafka/error.py`, `examples/share_consumer.py`,
`tests/integration/share_consumer/test_share_consumer_ack.py`.
