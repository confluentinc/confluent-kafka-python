# Verifiable Clients for Apache Kafka System Tests

Two Python programs that implement Apache Kafka's "verifiable client"
contract so the Ducktape-based Python system tests in `apache/kafka` can
exercise confluent-kafka-python the same way they exercise the Java client:

- `verifiable_producer.py` — produces messages with optional throttling
- `verifiable_share_consumer.py` — KIP-932 share consumer

Each program accepts a standardized CLI and emits newline-delimited JSON
events on stdout. The Python harness in
`apache/kafka/tests/kafkatest/services/verifiable_*.py` parses those events
to track progress and verify correctness.

Both clients share `verifiable_client.py`, which provides event emission,
signal handling, and Java -> librdkafka config translation.

## Events

Both clients emit `startup_complete` on start and `shutdown_complete` after
a clean SIGTERM/SIGINT. Beyond that:

**Producer**
- `producer_send_success` `{topic, partition, offset, key, value}`
- `producer_send_error` `{topic, key, value, message, exception}`
- `tool_data` `{sent, acked, target_throughput, avg_throughput}` (final summary)

**Share consumer**
- `records_consumed` `{count, partitions:[{topic, partition, count, offsets:[]}]}`
- `offsets_acknowledged` `{count, partitions:[...], success, error?}`
- `record_data` `{topic, partition, offset, key, value}` (only with `--verbose`)
- `offset_reset_strategy_set` `{offsetResetStrategy}` (only with `--offset-reset-strategy`)

All events also carry a `timestamp` field (epoch milliseconds).

## CLI

Both accept `--bootstrap-server` / `--broker-list`, `--command-config <file>`
(Java properties), `-X key=value` (raw librdkafka properties), and `--debug
<flags>`.

**Producer:** `--topic`, `--max-messages` (-1=infinite), `--throughput`
(-1=unlimited), `--acks`, `--value-prefix`, `--repeating-keys`,
`--message-create-time`, `--producer.config` (deprecated alias of
`--command-config`).

**Share consumer:** `--topic`, `--group-id`, `--max-messages` (-1=infinite),
`--acknowledgement-mode auto|sync|async`, `--offset-reset-strategy
earliest|latest`, `--verbose`.

The three acknowledgement modes all run with
`share.acknowledgement.mode=implicit` (poll() auto-accepts the previous
batch); they differ only in how they commit:

| mode  | commit                | offsets_acknowledged source         |
|-------|-----------------------|-------------------------------------|
| auto  | implicit (next poll)  | acknowledgement-commit callback     |
| sync  | `commit_sync()`       | acknowledgement-commit callback     |
| async | `commit_async()`      | acknowledgement-commit callback     |

`offsets_acknowledged` is always emitted from the acknowledgement-commit
callback (`ShareConsumer.set_acknowledgement_commit_callback`), so async/auto
acks carry real broker per-partition results, not optimistic guesses.

## How System Tests Work

Apache Kafka's system tests run on
[Ducktape](https://github.com/confluentinc/ducktape). A driver container
orchestrates worker containers over SSH: it spins up brokers, starts the
producer/consumer programs, and parses their stdout events.

To plug in a custom client, point Ducktape at a `--globals` file. The Python
harness resolves `VerifiableProducer` / `VerifiableShareConsumer` to
`VerifiableClientApp`, which on each worker:

1. Runs the `deploy` script once before the first test.
2. Invokes `exec_cmd` (with the harness-supplied flags appended) and parses
   its stdout as JSON events.

This directory's `globals.json` wires both clients to
`python -m confluent_kafka.kafkatest.verifiable_*` running from a virtualenv
built by `deploy.sh`. Paths assume the confluent-kafka-python repo is mounted
at `/confluent-kafka-python` and a librdkafka checkout at `/librdkafka` inside
each worker.

`deploy.sh` apt-installs build deps, builds and installs the **mounted**
librdkafka (this branch needs a newer librdkafka than any distro
`librdkafka-dev` package provides — KIP-932 share APIs, IncrementalAlterConfigs,
the modern admin types — so the apt package is intentionally not used), creates
a virtualenv at `/opt/cfk-python/venv`, and `pip install -e`'s the repo into it,
linking the C extension against the just-installed librdkafka. It honors
`REPO_DIR` (default `/confluent-kafka-python`), `LIBRDKAFKA_DIR` (default
`/librdkafka`), `VENV_DIR` (default `/opt/cfk-python/venv`), and
`DEPLOY_SKIP_APT=1` for pre-provisioned images. It uses a flock'd sentinel so
concurrent deploys sharing the librdkafka mount don't race the build.

## Running Share Consumer Tests

Assumes Docker is running with ~10 GB of free RAM for containers.

### Prerequisites

- A checkout of `confluentinc/confluent-kafka-python` (this repo)
- A checkout of `confluentinc/librdkafka` with the KIP-932 share APIs
- A checkout of `apache/kafka`

```bash
export CFK_PYTHON_DIR=/path/to/confluent-kafka-python
export LIBRDKAFKA_DIR=/path/to/librdkafka
export KAFKA_DIR=/path/to/apache/kafka
```

### 1. Compile apache/kafka's system-test libraries

```bash
cd "$KAFKA_DIR"
./gradlew systemTestLibs
```

First run takes ~10 minutes; subsequent runs are fast.

### 2. Patch ducker-ak to mount confluent-kafka-python and librdkafka

Open `$KAFKA_DIR/tests/docker/ducker-ak`, find the `docker_run()` function,
and add `-v` flags for both repos:

```diff
     must_do -v ${container_runtime} run --init --privileged \
         -d -t -h "${node}" --network ducknet "${expose_ports}" \
         --memory=${docker_run_memory_limit} ... \
-        -v "${kafka_dir}:/opt/kafka-dev" --name "${node}" -- "${image_name}"
+        -v "${kafka_dir}:/opt/kafka-dev" \
+        ${CFK_PYTHON_DIR:+-v "${CFK_PYTHON_DIR}:/confluent-kafka-python"} \
+        ${LIBRDKAFKA_DIR:+-v "${LIBRDKAFKA_DIR}:/librdkafka"} \
+        --name "${node}" -- "${image_name}"
```

This is a one-time local edit.

### 3. Make `deploy.sh` executable

```bash
chmod +x "$CFK_PYTHON_DIR/src/confluent_kafka/kafkatest/deploy.sh"
```

### 4. Bring up the cluster and run the tests

```bash
cd "$KAFKA_DIR"
CFK_PYTHON_DIR=$CFK_PYTHON_DIR LIBRDKAFKA_DIR=$LIBRDKAFKA_DIR ./tests/docker/ducker-ak up

CFK_PYTHON_DIR=$CFK_PYTHON_DIR LIBRDKAFKA_DIR=$LIBRDKAFKA_DIR ./tests/docker/ducker-ak test \
  tests/kafkatest/tests/client/share_consumer_test.py \
  -- --globals /confluent-kafka-python/src/confluent_kafka/kafkatest/globals.json
```

The harness runs `deploy.sh` once per worker before the first test; that
script builds the virtualenv and installs the client. The first test starts a
few minutes after launch (deploy time); subsequent tests reuse the venv.

### 5. View results

```bash
cat "$KAFKA_DIR/results/latest/report.txt"
```

Per-test stdout (debugging):

```bash
find "$KAFKA_DIR/results/latest" -name "verifiable_*.stdout"
```

### 6. Tear down

```bash
cd "$KAFKA_DIR"
./tests/docker/ducker-ak down
```

## Stand-alone usage

The clients can be run directly for local debugging:

```bash
python -m confluent_kafka.kafkatest.verifiable_producer \
    --topic t --bootstrap-server localhost:9092 --max-messages 10

python -m confluent_kafka.kafkatest.verifiable_share_consumer \
    --topic t --group-id g --bootstrap-server localhost:9092 \
    --acknowledgement-mode sync --offset-reset-strategy earliest \
    --max-messages 10
```
