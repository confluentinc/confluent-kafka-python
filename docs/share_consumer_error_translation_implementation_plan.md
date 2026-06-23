# Share Consumer (KIP-932) — Error Translation Implementation Plan

Status: **code implemented; test updates pending** (per "do tests next"). The C
extension compiles clean and passes a broker-free smoke test; the test changes
in §6 are the remaining work. Decisions C (closed guard → IllegalStateException)
and D (init path through the helper) were taken as recommended.

Goal: stop funneling every librdkafka error from the share consumer into a blind
`KafkaException`, and instead raise the Python exception a user porting from the
Java client would expect. Scope is the **share consumer only** — `Producer`,
`Consumer`, and `Admin` keep their current behavior untouched.

Background analysis lives in the sibling reference docs
(`share_consumer_error_mapping.md`, `share_consumer_error_reference.md`,
`python_share_consumer_errors.md`). This document is the build plan.

---

## 1. Finalized decisions

1. **Attach the `KafkaError`.** Every translated exception still carries a
   `KafkaError` as `args[0]` (so `e.args[0].code()` / `.str()` / `.fatal()` keep
   working), exactly like `KafkaException` does today. Only the *class* changes.
2. **New exception classes subclass `RuntimeError`**, not `KafkaException` —
   matching Java (`java.lang.IllegalStateException`,
   `java.util.ConcurrentModificationException`). So `except KafkaException` no
   longer swallows these control-flow errors.
3. Types are defined in `cimpl` (next to `KafkaException`) and re-exported at
   `confluent_kafka.*`. They are general types; only the *wiring* is share-scoped
   for now, so `Consumer`/`Producer` can adopt the same translation later.
4. This is a **breaking change** for the (experimental) share consumer: code
   catching `KafkaException` for these conditions must switch to the new
   classes / `ValueError`.

## 2. The mapping (verified against librdkafka `dev_kip-932_error_translation` + Java)

| Java | librdkafka code | Python target | Base class | `args[0]` |
|---|---|---|---|---|
| `IllegalStateException` | `RD_KAFKA_RESP_ERR__STATE` | **`IllegalStateException`** (new) | `RuntimeError` | `KafkaError(_STATE)` |
| `ConcurrentModificationException` | `RD_KAFKA_RESP_ERR__CONFLICT` | **`ConcurrentModificationException`** (new) | `RuntimeError` | `KafkaError(_CONFLICT)` |
| `IllegalArgumentException` | `RD_KAFKA_RESP_ERR__INVALID_ARG` | `ValueError` | builtin | `KafkaError(_INVALID_ARG)` |
| `IllegalArgumentException` | (CPython arg-type checks) | `TypeError` | builtin | — *(already correct, no change)* |
| `KafkaException` + everything else | any other code | `KafkaException` | unchanged | `KafkaError(code)` |

Evidence: `rdkafka.h:2272-2276` (`set_acknowledgement_commit_cb`: `__STATE` in-callback/closed,
`__CONFLICT` concurrent access), `rdkafka.h:3362-3366` (`acknowledge*`: `__INVALID_ARG`
bad input, `__STATE` wrong mode/GAP), `rdkafka.h:5232-5236` (`subscribe`: `__INVALID_ARG`
empty/dup names); Java `ShareConsumerImpl.java:1126/1141/1146/1211/1228`.

## 3. Design

A single selector decides the wrapping class; both helpers always build and
attach a `KafkaError`. The rich-error variant goes through
`KafkaError_new_from_error_destroy()` so the `fatal`/`retriable`/`txn` flags are
preserved on the attached `KafkaError`.

```c
/* ShareConsumer.c — static, keeps the shared cfl_PyErr_* macros untouched.

   Map a librdkafka error code to the Python exception class a Java
   share-consumer user expects. The KafkaError is attached as args[0] in every
   case, so only the wrapping class differs from today's behavior. */
static PyObject *ShareConsumer_exc_type(rd_kafka_resp_err_t code) {
        switch (code) {
        case RD_KAFKA_RESP_ERR__STATE:       return IllegalStateException;
        case RD_KAFKA_RESP_ERR__CONFLICT:    return ConcurrentModificationException;
        case RD_KAFKA_RESP_ERR__INVALID_ARG: return PyExc_ValueError;
        default:                             return KafkaException;
        }
}

/* code-based path — replaces cfl_PyErr_Format() at share call sites */
static void ShareConsumer_set_py_error(rd_kafka_resp_err_t code,
                                       const char *fmt, ...) {
        char buf[512];
        va_list ap; va_start(ap, fmt); vsnprintf(buf, sizeof(buf), fmt, ap); va_end(ap);
        PyObject *eo = KafkaError_new0(code, "%s", buf);
        if (eo) {
                PyErr_SetObject(ShareConsumer_exc_type(code), eo);
                Py_DECREF(eo);   /* SetObject took its own ref; see note R1 */
        }
}

/* rich-error path — replaces cfl_PyErr_from_error_destroy() at share call sites.
   KafkaError_new_from_error_destroy() destroys `error` and keeps its flags. */
static void ShareConsumer_set_py_error_destroy(rd_kafka_error_t *error) {
        rd_kafka_resp_err_t code = rd_kafka_error_code(error);
        PyObject *eo = KafkaError_new_from_error_destroy(error);
        if (eo) {
                PyErr_SetObject(ShareConsumer_exc_type(code), eo);
                Py_DECREF(eo);
        }
}
```

Mechanics: `PyErr_SetObject(T, kafka_error)` normalizes to `T(kafka_error)` when
`kafka_error` is not already an instance of `T`, giving `args[0] == kafka_error`.
This is exactly how `KafkaException` is raised today (`confluent_kafka.h:198`), so
`str(exc)` (the `KafkaError{code=...,str="..."}` form from `KafkaError_str0`) and
`exc.args[0].code()` behave identically — only `type(exc)` changes.

## 4. Step-by-step changes

### Step 1 — `src/confluent_kafka/src/confluent_kafka.h`
Declare the globals next to `extern PyObject *KafkaException;` (line 187):
```c
extern PyObject *IllegalStateException;
extern PyObject *ConcurrentModificationException;
```

### Step 2 — `src/confluent_kafka/src/confluent_kafka.c`
- Define the two globals near `PyObject *KafkaException;` (line 51).
- In module init, right after `KafkaException` is created
  (lines 3780-3793), create and register both as `RuntimeError` subclasses:
```c
IllegalStateException = PyErr_NewExceptionWithDoc(
    "cimpl.IllegalStateException",
    "Raised by ShareConsumer for an illegal-state error (librdkafka _STATE),\n"
    "mirroring Java's IllegalStateException. ``args[0]`` is a KafkaError.\n",
    PyExc_RuntimeError, NULL);
Py_INCREF(IllegalStateException);
PyModule_AddObject(m, "IllegalStateException", IllegalStateException);

ConcurrentModificationException = PyErr_NewExceptionWithDoc(
    "cimpl.ConcurrentModificationException",
    "Raised by ShareConsumer on concurrent multi-threaded access\n"
    "(librdkafka _CONFLICT), mirroring Java's ConcurrentModificationException.\n"
    "``args[0]`` is a KafkaError.\n",
    PyExc_RuntimeError, NULL);
Py_INCREF(ConcurrentModificationException);
PyModule_AddObject(m, "ConcurrentModificationException",
                   ConcurrentModificationException);
```

### Step 3 — `src/confluent_kafka/src/ShareConsumer.c`
1. Add the two static helpers + selector from §3 near the top of the file.
2. Swap the librdkafka-error call sites:

   | Method | Line(s) | Current | New |
   |---|---|---|---|
   | `subscribe` | 156 | `cfl_PyErr_Format` | `ShareConsumer_set_py_error` |
   | `unsubscribe` | 181 | `cfl_PyErr_Format` | `ShareConsumer_set_py_error` |
   | `subscription` | 209 | `cfl_PyErr_Format` | `ShareConsumer_set_py_error` |
   | `acknowledge` | 410 | `cfl_PyErr_Format` | `ShareConsumer_set_py_error` |
   | `acknowledge_offset` | 450 | `cfl_PyErr_Format` | `ShareConsumer_set_py_error` |
   | `poll` | 316 | `cfl_PyErr_from_error_destroy` | `ShareConsumer_set_py_error_destroy` |
   | `commit_sync` (fn-level) | 498 | `cfl_PyErr_from_error_destroy` | `ShareConsumer_set_py_error_destroy` |
   | `commit_async` | 547 | `cfl_PyErr_from_error_destroy` | `ShareConsumer_set_py_error_destroy` |
   | `set_acknowledgement_commit_callback` | 670 | `cfl_PyErr_from_error_destroy` | `ShareConsumer_set_py_error_destroy` |
   | `close` | 726 | `cfl_PyErr_from_error_destroy` | `ShareConsumer_set_py_error_destroy` |
   | `init` (create) | 1049 | `cfl_PyErr_Format(rd_kafka_last_error())` | `ShareConsumer_set_py_error` — *(see review point D)* |
   | `init` (log queue) | 1064 | `cfl_PyErr_Format(rd_kafka_error_code(error))` | `ShareConsumer_set_py_error` |
   | `init` (oauth) | 1091 | `cfl_PyErr_from_error_destroy` | `ShareConsumer_set_py_error_destroy` |

3. Delete the now-implemented TODO block at lines 825-841.
4. Update the docstrings in `ShareConsumer_methods` so `:raises:` reflects reality,
   e.g. `subscribe` → `:raises ValueError:` for empty/duplicate names;
   `poll`/`acknowledge*` → `:raises IllegalStateException:` for the `_STATE` cases;
   add `:raises ConcurrentModificationException:` for concurrent access.
5. **Optional consistency (review point C):** upgrade the closed-consumer guard
   (`ERR_MSG_SHARE_CONSUMER_CLOSED`, raised ~10x as `PyExc_RuntimeError`) to raise
   `IllegalStateException` wrapping `KafkaError(_STATE, "Share consumer closed")`.
   Backward-compatible (subclass of `RuntimeError`); matches Java's
   `IllegalStateException("This consumer has already been closed.")`.

### Step 4 — `src/confluent_kafka/cimpl.pyi`
Add stubs near `class KafkaException` (line 274):
```python
class IllegalStateException(RuntimeError): ...
class ConcurrentModificationException(RuntimeError): ...
```

### Step 5 — `src/confluent_kafka/__init__.py`
Import both from `.cimpl` (the `from .cimpl import (...)` block, line 32) and add
to `__all__` (line 57). Optionally re-export from `confluent_kafka.error` for
symmetry with `KafkaError`/`KafkaException`.

### Step 6 — Tests
Because `args[0]` is still a `KafkaError`, **only the `pytest.raises(<type>)`
class needs to change** — the inner `.args[0].code()` assertions stay valid.

Unit (`tests/test_ShareConsumer.py`):
- `_STATE` → `IllegalStateException`: 192-201, 351-353
- `_INVALID_ARG` → `ValueError`: 359-365, 410-412, 438-440, 445-447, 470-474
- `_CONFLICT` → `ConcurrentModificationException`: 552-604
- closed-consumer `pytest.raises(RuntimeError)` (245, 270-313): keep passing as-is;
  tighten to `IllegalStateException` only if review point C is taken.
- `TypeError` tests (39, 329-333, 376-381, 396-432, 482-496) and config `ValueError`
  tests (101-104): unchanged.

Integration (`tests/integration/share_consumer/`):
- `_STATE` → `IllegalStateException`: `test_share_consumer.py:662`,
  `..._commit.py:420/459/613`, `..._ack.py:54/252/408-433/577/716`
- `_INVALID_ARG` → `ValueError`: `..._ack.py:444`
- `_CONFLICT` → `ConcurrentModificationException`: `..._ack.py:1230`
- Re-entrancy (calling a share method *inside* the ack callback returns `_STATE`):
  `..._ack.py:1183-1185` → `IllegalStateException`.
- **Verify, do not blindly flip:** `..._commit.py:594/703` assert
  `code() in (_STATE, INVALID_RECORD_STATE)` on *per-partition / callback* values —
  those stay `KafkaException`/`KafkaError` (see §5). Confirm each site's channel.
- Add positive tests asserting `isinstance(exc, RuntimeError)` for the two new
  types and that `exc.args[0].code()` matches the expected code.

### Step 7 — Docs
Update the "Provisional error → exception mapping" sections that currently say
this is unimplemented: `share_consumer_error_mapping.md:177`,
`share_consumer_error_reference.md:16`, `SHARE_CONSUMER_ERRORS.md:267`. Add a
CHANGELOG entry flagging the breaking change.

## 5. What deliberately stays `KafkaException` / `KafkaError`

- Per-partition values in `commit_sync`'s result dict
  (`c_parts_to_dict_topic_partition_to_error`) — mirrors Java
  `Map<TopicIdPartition, Optional<KafkaException>>`.
- The `exception` argument delivered to the user's ack-commit callback
  (`ShareConsumer.c:591`) — broker results; already `KafkaException(KafkaError)`.
- The internal `__FAIL` "Unable to build callback args" crash path (line 603).

The translation applies only to errors raised as the **result of a direct
ShareConsumer method call**.

## 6. Notes / review points

- **R1 (refcount):** the new helpers `Py_DECREF(eo)` after `PyErr_SetObject`
  (correct ownership). The existing `cfl_PyErr_Format` macro omits this (a small
  pre-existing leak on error paths). Calling out the intentional difference; the
  shared macro is left alone.
- **C (closed guard):** upgrade to `IllegalStateException`? Recommended for Java
  fidelity; backward-compatible. Separable from the core change.
- **D (init create path, line 1049):** `rd_kafka_share_consumer_new` failure via
  `rd_kafka_last_error()`. Most config errors are already rejected earlier as
  `ValueError` (by `common_conf_setup` / `reject_incompatible_config`). Routing it
  through the helper is consistent, but construction failures are arguably better
  left as `KafkaException`. Recommend: route through the helper for uniformity.
- **ValueError carrying a KafkaError:** `ValueError.args[0]` becomes a `KafkaError`
  (so `str()` is the `KafkaError{...}` form). This is per decision #1 and keeps
  `.code()` working; flagging because it's slightly unconventional for a builtin.

## 7. Build & validate

- Rebuild/relink the C extension against the librdkafka
  `dev_kip-932_error_translation` branch (currently checked out at
  `~/projects/librdkafka`) — it provides the share `_STATE`/`_CONFLICT`/`_INVALID_ARG`
  contract this relies on.
- Unit tests are broker-free: `PYTHONPATH=src /usr/bin/python3 -m pytest
  tests/test_ShareConsumer.py tests/test_ShareConsumer_callbacks.py
  tests/test_DeserializingShareConsumer.py`.
- Integration tests need the repo-pinned `trivup==0.14.0` and Docker (Schema
  Registry); only `INVALID_RECORD_STATE` (via lock expiry) is reproducible for the
  ack-commit error path.
- Style: run `style-format.sh` with `.venv/bin` ahead on `PATH`, scoped to the diff.

## 8. Suggested sequencing

1. Steps 1-2 (new types) + Step 5 (export) — compiles, no behavior change yet.
2. Step 3 (helpers + call-site swaps) — the core change.
3. Step 6 (tests) alongside Step 3.
4. Steps 4 + 7 (stubs + docs).
5. Decide review points C and D before merging.
