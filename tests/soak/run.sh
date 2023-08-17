#!/bin/bash
#

set -e
source venv/bin/activate

librdkafka_version=$(python3 -c 'from confluent_kafka import libversion; print(libversion()[0])')

if [[ -z $librdkafka_version ]]; then
    echo "No librdkafka version found.."
    exit 1
fi

set -u
topic="pysoak-$librdkafka_version"

echo "Starting soak client using topic $topic"
set +x
time opentelemetry-instrument confluent-kafka-python/tests/soak/soakclient.py -t $topic -r 80 -f  confluent-kafka-python/ccloud.config 2>&1
ret=$?
echo "Python client exited with status $ret"
exit $ret


