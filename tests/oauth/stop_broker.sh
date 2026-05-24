#!/bin/bash
# Stop the local Kafka broker started by start_broker.sh and optionally
# wipe its storage directory.
#
# Usage:
#   ./stop_broker.sh            # stop only; preserve /tmp/kraft-oauth-test-logs
#   ./stop_broker.sh --clean    # stop and wipe log.dirs
#
# Env:
#   KAFKA_HOME      Path to Apache Kafka source/dist (default: $HOME/projects/kafka)
set -e

KAFKA_HOME="${KAFKA_HOME:-$HOME/projects/kafka}"
LOG_DIR="/tmp/kraft-oauth-test-logs"

# kafka-server-stop.sh signals the Kafka process; returns non-zero if no
# broker is running, which is fine for tests.
"$KAFKA_HOME/bin/kafka-server-stop.sh" 2>/dev/null || true

if [ "${1:-}" = "--clean" ]; then
    echo "Removing $LOG_DIR"
    rm -rf "$LOG_DIR"
fi
