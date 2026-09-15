# Soak testing client

The soak testing clients should be run for prolonged periods
of time, typically 2+ weeks, to vet out any resource leaks, etc.

The soak testing client is made up of a producer, producing messages to
the configured topic, and a consumer, consuming the same messages back.

OpenTelemetry reporting supported through OTLP.

# Installation

1. Edit `ccloud.config`

2. Edit `otel-config.yaml`

3. the first time:
```bash
./bootstrap.sh <python-branch/tag> <librdkafka-branch/tag>
```
4. next times:
```bash
./build.sh <python-branch/tag> <librdkafka-branch/tag>
```

5. 
```bash
. venv/bin/activate

5. Run some tests
```bash
TESTID=<testid> ./run.sh ccloud.config
```

## Share Consumer 

To run with a share consumer instead of the regular consumer:
```bash
SHARE=true TESTID=<testid> ./run.sh ccloud.config
```
Requires KIP-932 compatible librdkafka and broker.

Requires KIP-932 compatible librdkafka and broker.

## Free-threaded multi-replica testing

To soak test the client with free threading (GIL disabled), run N (>1)
complete producer+consumer replicas concurrently in a single process via
`soak_multi.py`:

```bash
MULTI_REPLICAS=<n> TESTID=<testid> ./run.sh ccloud.config
```

All replicas share one topic and one consumer group, so they behave like a
normally scaled-out consumer group. This requires the venv's Python to
already be a free-threaded build with the GIL disabled. `soak_multi.py`
refuses to start if the GIL is enabled. `soak_multi.py` doesn't support the share consumer.