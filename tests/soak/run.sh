#!/bin/bash
#

set -e
source venv/bin/activate

librdkafka_version=$(python3 -c 'from confluent_kafka import libversion; print(libversion()[0])')

if [[ -z $librdkafka_version ]]; then
    echo "No librdkafka version found.."
    exit 1
fi

if [[ -z $STY ]]; then
    echo "This script should be run from inside a screen session"
    exit 1
fi

set -u
topic="pysoak-$librdkafka_version"
logfile="${topic}.log.bz2"

echo "Starting soak client using topic $topic with logs written to $logfile"
set +x
time confluent-kafka-python/tests/soak/soakclient.py -t $topic -r 80 -f  confluent-kafka-python/ccloud.config 2>&1 \
    | tee /dev/stderr | bzip2 > $logfile
ret=$?
echo "Python client exited with status $ret"
exit $ret


