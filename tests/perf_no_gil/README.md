# Free-threading manual perf runs

Standalone scripts for manually comparing Producer/Consumer
throughput between a standard CPython 3.14 build and a free-threaded (3.14t,
`--disable-gil`) build. These are **not** pytest tests -- nothing here is
collected by `pytest`; run each script directly with the interpreter you
want to measure.

## Files

- `common.py` -- shared argparse/config/topic-creation/reporting helpers.
- `producer_perf.py` -- Producer runs 1-5.
- `consumer_perf.py` -- Consumer runs 1-5.
- `aio_producer_perf.py` -- AIOProducer runs 1-5.
- `aio_consumer_perf.py` -- AIOConsumer runs 1-4.

## Prerequisites

1. A reachable Kafka cluster -- bring your own; these scripts never start one themselves. `-b/--bootstrap-servers` is passed straight through to librdkafka's `bootstrap.servers`, so it accepts either a plain `host:port` CSV (`localhost:9092`) or a scheme-prefixed one, e.g. what a trivup `KafkaCluster` hands back: `PLAINTEXT://localhost:55920,PLAINTEXT://localhost:55925,PLAINTEXT://localhost:55930`.
2. Two interpreters to compare, each with this package installed:
   ```
   python3.14  -m pip install -e .
   python3.14t -m pip install -e .
   ```
3. Every script also takes repeatable `--extra-conf KEY=VALUE` for anything else your cluster needs (SASL/SSL, etc).

The command tables below assume you've exported your cluster's address once:
```
export BROKERS="PLAINTEXT://localhost:55920,PLAINTEXT://localhost:55925,PLAINTEXT://localhost:55930"
```

Each script prints, along with elapsed time and throughput, the interpreter build and whether the GIL is actually disabled at runtime (`sys._is_gil_enabled()`), so a run's output is self-describing -- you don't have to trust that you invoked the interpreter you meant to.

## How "mode" maps to the run topology

For Producer and Consumer, the requested runs differ along two independent axes: which CPython build you use, and how threads share (or don't share) a client instance. The scripts only need to encode the second axis (`--mode`); the first is just which `python3.14` / `python3.14t` binary you invoke.

### Producer (`producer_perf.py`)

| Run | Mode         | Interpreter | Command |
|-----|--------------|-------------|---------|
| 1   | `single`     | 3.14        | `python3.14  producer_perf.py -b "$BROKERS" --mode single -d 10` |
| 2   | `single`     | 3.14t       | `python3.14t producer_perf.py -b "$BROKERS" --mode single -d 10` |
| 3   | `shared`     | 3.14        | `python3.14  producer_perf.py -b "$BROKERS" --mode shared -d 10 -t 8` |
| 4   | `shared`     | 3.14t       | `python3.14t producer_perf.py -b "$BROKERS" --mode shared -d 10 -t 8` |
| 5   | `per-thread` | 3.14t       | `python3.14t producer_perf.py -b "$BROKERS" --mode per-thread -d 10 -t 8` |

- `single`: one Producer, one thread, producing for `--duration`/`-d` seconds (default 10).
- `shared`: one Producer instance, `-t` threads all calling `produce()`/`poll()` on it concurrently, each for the same `-d` seconds.
- `per-thread`: `-t` independent Producer instances, one per thread, each producing for `-d` seconds.

Every mode produces for the fixed wall-clock duration (not a fixed message count), then reports how many messages it actually got fully delivered (and the resulting throughput) in that time -- so the reported message count is an output, not an input.

Every invocation creates its own scratch topic -- a random name (`perf-no-gil-producer-<uuid>`) with `--partitions`/`-p` partitions (default 3) -- so runs never collide and there's no `--topic` flag to pass. Each message carries a distinct, per-caller-prefixed key (so concurrent callers in `shared` mode never emit the same key at the same time) and the producer is configured with `partitioner: consistent_random` (librdkafka's default), so messages are hash-partitioned evenly across those partitions rather than all landing on one.

`per-thread` mode also avoids ever sharing a Python object across threads: `produce_until` builds its own payload, topic string, and deadline float, and each worker builds its own `Producer`/`DeliveryTracker` and binds `tracker.on_delivery` once rather than re-accessing it every call. This isn't just style -- profiling with [samply](https://github.com/mstange/samply) traced a real, large throughput gap here: a `per-thread` run with these objects built once by the caller and handed to every worker ran at roughly half the throughput of the same number of independent OS *processes*, even though each thread already had its own producer instance and its own broker connections. The profile showed `_Py_DecRefShared` -- CPython 3.14's free-threaded biased reference counting taking its slow atomic path, which happens whenever a thread other than an object's *creator* touches its refcount -- at a meaningful share of samples. Eliminating every instance of "object built by the caller, used by a worker thread" (payload, topic, deadline, producer, tracker, and critically the bound-method object `tracker.on_delivery`, which was being reallocated on every single call) dropped `_Py_DecRefShared` to zero and took `per-thread` from roughly half of the multi-process throughput to matching or exceeding it. None of this is a librdkafka or OS limitation -- it was ordinary Python object-sharing that happened to be invisible on a GIL build and expensive on a free-threaded one. `shared` mode can't benefit from this: it deliberately has one producer instance genuinely accessed from multiple threads, which no amount of avoiding *incidental* sharing changes.

The local send queue uses librdkafka's own (large, multi-GB) defaults, so a producer that can enqueue faster than the broker can ack it will happily build an in-memory backlog during the timed window rather than ever hitting `BufferError`. `flush()` then has to drain whatever backlog accumulated before `elapsed` is measured -- if you see a "(of which drain: ...)" line in the output, that's what's happening, and how big that backlog is (and so how long the drain takes) can vary run to run based on scheduling/network jitter rather than the code under test. Treat a run with a large drain line as noisier than one without; rerunning or watching for it is more reliable than trusting a single sample when it appears.

Producer uses librdkafka's own thread-safe `rk` handle underneath (guarded only by a lightweight active-call/closing gate, not a global serializing lock), so `shared` mode exercises real concurrent access -- this is where free-threading has the most room to help.

### Consumer (`consumer_perf.py`)

The topic must already have plenty of messages in it before you run this -- generously more than either run below could consume in `--duration`, or the run will exhaust the topic partway through and understate throughput (its `elapsed`/`messages/calls` idling out the rest of the window rather than measuring steady-state consumption).

| Run | Mode         | Interpreter | Command |
|-----|--------------|-------------|---------|
| 1   | `single`     | 3.14        | `python3.14  consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode single -d 10` |
| 2   | `single`     | 3.14t       | `python3.14t consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode single -d 10` |
| 3   | `per-thread` | 3.14        | `python3.14  consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode per-thread -d 10 -t 6` |
| 4   | `per-thread` | 3.14t       | `python3.14t consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode per-thread -d 10 -t 6` |

- `single`: one Consumer, one thread, consuming for `--duration`/`-d` seconds (default 10).
- `per-thread`: `-t` independent Consumer instances sharing one consumer group (a fresh random `group.id` per run by default), so the broker splits partitions across them -- the normal way to scale consumption.

Like Producer, each mode consumes for the fixed wall-clock duration (not a fixed message count), then reports how many messages it actually got in that time.

There's no `shared` mode (a single Consumer instance polled concurrently from multiple threads) here on purpose: unlike Producer/Admin, the Consumer/ShareConsumer C implementation serializes all access through its own reentrancy gate (only one thread may be inside Consumer C code at a time; others spin-wait), so a shared-instance run would only measure that gate's overhead/fairness, not real concurrent consumption -- a contention question, not a throughput one. `per-thread` is the mode that shows genuine multi-core consumer scaling, and each of its per-thread `Consumer` instances is now built on the thread that uses it (not by the main thread and handed over), for the same free-threading cross-thread-refcount reason documented for Producer's `per-thread` mode above; `poll_timeout` gets a thread-owned copy inside `drain()` for the same reason `deadline` does there, since it's read on every loop iteration.

Each consumer run waits for its initial partition assignment(s) before starting the clock, so rebalance time isn't counted. Re-running with the same `--group-id` before the previous run's offsets would need to be reset; the default random group ID avoids that entirely by always reading from `earliest`.

### AIOProducer (`aio_producer_perf.py`)

| Run | Mode         | Interpreter | Command |
|-----|--------------|-------------|---------|
| 1   | `single`     | 3.14        | `python3.14  aio_producer_perf.py -b "$BROKERS" --mode single -d 10` |
| 2   | `single`     | 3.14t       | `python3.14t aio_producer_perf.py -b "$BROKERS" --mode single -d 10` |
| 3   | `concurrent` | 3.14        | `python3.14  aio_producer_perf.py -b "$BROKERS" --mode concurrent -d 10 -t 8` |
| 4   | `concurrent` | 3.14t       | `python3.14t aio_producer_perf.py -b "$BROKERS" --mode concurrent -d 10 -t 8` |
| 5   | `per-loop`   | 3.14t       | `python3.14t aio_producer_perf.py -b "$BROKERS" --mode per-loop -d 10 -t 8` |

`AIOProducer`'s concurrency model doesn't map onto Producer's `single`/`shared`/`per-thread` split -- `produce()` just buffers a message in memory and returns almost immediately (dispatching to librdkafka, via a background `ThreadPoolExecutor`, only once `batch_size` messages have queued up or `buffer_timeout` elapses), and there's no supported pattern for calling one instance's methods from a thread other than the one running its event loop. So there's no "shared across OS threads" mode here -- the idiomatic way async code adds concurrency is more coroutines on the *same* event loop, not more threads calling in from outside it.

- `single`: one coroutine, one event loop, sequential `produce()`+await, for `-d` seconds.
- `concurrent`: `-t` asyncio tasks (`asyncio.gather`) all calling `produce()` on *one* `AIOProducer` instance within *one* event loop (one OS thread) -- the idiomatic way real async code scales. `--executor-workers` controls the actual OS-thread parallelism available to it: the `ThreadPoolExecutor` running librdkafka's blocking calls is the only place real parallelism can happen here, since the event loop itself is single-threaded.
- `per-loop`: `-t` independent OS threads, each running its own event loop with its own `AIOProducer` instance, producing independently -- genuine OS-level parallelism across fully independent instances, the direct analog of Producer's `per-thread` mode.

`per-loop` applies the same object-ownership lesson `per-thread` needed for the sync Producer: each thread's event loop builds its own `AIOProducer`, payload, and topic-string copy rather than reusing ones built by the main thread, avoiding the same cross-thread reference-counting cost documented above. `single` and `concurrent` never needed this in the first place -- everything in both runs entirely on one OS thread (asyncio tasks interleave cooperatively, they don't run in parallel), so there's no second thread that could ever touch an object it doesn't own.

Delivery errors are tracked via a done-callback (`future.add_done_callback`) attached to each `produce()` call's returned future rather than a `DeliveryTracker`-style callback registered with the client -- asyncio guarantees a future's done-callbacks run on its own event loop's thread, so within any single loop (even `concurrent` mode, where many tasks share one instance) they're already serialized without needing a lock. `set_result()`/`set_exception()` only *schedule* those callbacks rather than running them synchronously, though, so a plain `flush()` isn't proof every callback has actually executed -- `wait_for_pending` explicitly counts down to 0 before the run reports its error count.

`single` mode measured roughly 4x lower throughput than the sync Producer's `single` mode on the same broker, and that gap is architectural, not something this script can route around: with only one coroutine, `produce()`'s periodic batch dispatch (`await self._flush_buffer()`, once every `--batch-size` messages) blocks the only coroutine that exists, so the event loop sits genuinely idle for the entire dispatch -- there's nothing else scheduled to run. On top of that, `produce_batch()`'s API takes a list of dicts, so every message pays a per-message string-keyed dict lookup inside the C extension (the same class of cost `producer_perf.py`'s positional-args fix eliminated for the plain `Producer.produce()`, just unavoidable here since the batch API's contract is dict-shaped). `--batch-size` (default 1000, matching `AIOProducer`'s own default) has a real, non-monotonic optimum around that default: below roughly 500 throughput collapses (too few messages per dispatch to amortize the executor round-trip -- 18x worse at `batch_size=1`), and above it throughput mildly declines as more of the per-message dict-lookup cost accumulates per dispatch before any of it can be sent. `concurrent` mode's higher throughput than `single` is exactly this serialization gap being partly closed: other tasks can keep buffering while one is blocked on a dispatch.

### AIOConsumer (`aio_consumer_perf.py`)

The topic must already have plenty of messages in it, same requirement as `consumer_perf.py` above -- generously more than either run could consume in `--duration`.

| Run | Mode         | Interpreter | Command |
|-----|--------------|-------------|---------|
| 1   | `single`     | 3.14        | `python3.14  aio_consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode single -d 10` |
| 2   | `single`     | 3.14t       | `python3.14t aio_consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode single -d 10` |
| 3   | `per-loop`   | 3.14        | `python3.14  aio_consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode per-loop -d 10 -t 6` |
| 4   | `per-loop`   | 3.14t       | `python3.14t aio_consumer_perf.py -b "$BROKERS" --topic perf-no-gil-consumer --mode per-loop -d 10 -t 6` |

- `single`: one coroutine, one event loop, sequential `poll()`/`consume()`+await, for `-d` seconds.
- `per-loop`: `-t` independent OS threads, each running its own event loop with its own `AIOConsumer` instance, sharing one consumer group so the broker splits partitions across them -- genuine OS-level parallelism across fully independent instances, the direct analog of Consumer's `per-thread` mode.

There's no `concurrent` mode here, deliberately: `AIOConsumer` serializes all Consumer C-code access through the same per-instance gate the sync Consumer uses, bridged for async reentrancy by a `ReentryContext` plus a fresh `asyncio.Lock` per rebalance-callback invocation (so an `async def on_assign` can safely `await consumer.assign(...)` without deadlocking against the gate its own triggering `poll()` call is holding). Many tasks sharing one `AIOConsumer`/one event loop would mostly measure that lock-and-gate machinery serializing them against each other -- a contention/fairness question, not a throughput one -- and its correctness is already covered by `tests/integration/consumer/test_aio_consumer_*.py`. `per-loop` applies the same object-ownership fix documented for Producer/Consumer above: each thread's event loop builds its own `AIOConsumer`, and `on_assign` explicitly calls `await consumer.assign(partitions)` (the documented/tested way to accept an `AIOConsumer` rebalance) rather than relying on it being applied automatically.

`--consume-batch-size` (default 1, meaning `poll()`) is the same trade-off as AIOProducer's `--batch-size`: `AIOConsumer` dispatches every call to its executor individually with no buffering of its own, so `poll()` pays that dispatch overhead once per message while `consume(N, ...)` amortizes it over up to `N` messages per round trip. Unlike AIOProducer's batch size, there's no per-message dict-lookup cost pulling the other way here, so this one trended monotonically in testing: `--consume-batch-size 100` measured roughly 9x the throughput of the `poll()` default (1) in a `single` run against the same broker -- worth sweeping on your own workload before trusting either end of that range as representative.

## Interpreting results

- Compare Run 1 vs Run 2 (and the equivalents for Consumer/AIOProducer/AIOConsumer) first: this isolates single-threaded overhead/regression from the free-threaded build itself, independent of any GIL-removal benefit.
- Compare `shared`/`per-thread`/`per-loop` throughput scaling on 3.14 (GIL held) vs 3.14t (GIL disabled) to see whether free-threading actually improves multi-core throughput for that client.
- For Consumer/AIOConsumer, expect `per-thread`/`per-loop` throughput to scale with threads on both builds (each consumer's poll loop is independent).
- For AIOProducer, expect `concurrent` to scale with `--executor-workers` (up to whatever the broker/network can absorb) since that's the only real parallelism inside one event loop; a `concurrent` run that doesn't improve when you raise `--executor-workers` points at contention in the executor path worth investigating. Expect `per-loop` to scale with `-t` similarly to Producer's `per-thread`, for the same reason.
- For AIOConsumer, also try sweeping `--consume-batch-size` before comparing builds -- it changes the picture more than GIL vs no-GIL does on its own.
