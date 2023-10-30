# Soak testing client

The soak testing clients should be run for prolonged periods
of time, typically 2+ weeks, to vet out any resource leaks, etc.

The soak testing client is made up of a producer, producing messages to
the configured topic, and a consumer, consuming the same messages back.

OpenTelemetry reporting supported through OTLP.

# Installation

TESTID=normal \
LK_VERSION=v2.2.0 \
CKPY_VERSION=v2.2.0 \
CC_BOOSTRAP_SERVERS=_ \
CC_USERNAME=_ \
CC_PASSWORD=_ \
DOCKER_REPOSITORY=_ ./install.sh
