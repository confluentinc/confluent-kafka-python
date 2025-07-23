# Soak testing client

The soak testing clients should be run for prolonged periods
of time, typically 2+ weeks, to vet out any resource leaks, etc.

The soak testing client is made up of a producer, producing messages to
the configured topic, and a consumer, consuming the same messages back.

OpenTelemetry reporting supported through OTLP.

# Installation

1. Edit `ccloud.config`

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