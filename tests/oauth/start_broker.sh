#!/bin/bash
# Start a local Kafka broker configured for OAUTHBEARER integration tests.
#
# Usage:
#   ./start_broker.sh            # foreground (Ctrl+C to stop)
#   ./start_broker.sh -daemon    # background (use stop_broker.sh to stop)
#
# Env:
#   KAFKA_HOME      Path to Apache Kafka source/dist (default: $HOME/projects/kafka)
#   CLUSTER_ID      Override the formatted cluster ID (default: random uuid)
set -e

KAFKA_HOME="${KAFKA_HOME:-$HOME/projects/kafka}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROPS="$SCRIPT_DIR/server.properties"

if [ ! -x "$KAFKA_HOME/bin/kafka-storage.sh" ]; then
    echo "ERROR: kafka-storage.sh not found under $KAFKA_HOME/bin." >&2
    echo "Set KAFKA_HOME to a built Apache Kafka tree." >&2
    exit 1
fi

CLUSTER_ID="${CLUSTER_ID:-$(uuidgen)}"

# Format log.dirs (idempotent thanks to --ignore-formatted)
"$KAFKA_HOME/bin/kafka-storage.sh" format \
    -t "$CLUSTER_ID" \
    -c "$PROPS" \
    --ignore-formatted

# Run broker. -daemon (if passed) makes kafka-server-start.sh fork into the
# background and write logs to $KAFKA_HOME/logs/.
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$@" "$PROPS"
