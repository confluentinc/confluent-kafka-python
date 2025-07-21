#!/bin/bash

source venv/bin/activate

librdkafka_version=$(python3 -c 'from confluent_kafka import libversion; print(libversion()[0])')

if [[ -z $librdkafka_version ]]; then
    echo "No librdkafka version found.."
    exit 1
fi

set -u
topic="pysoak-$TESTID-$librdkafka_version"
logfile="${TESTID}.log.bz2"
export HOSTNAME=$(hostname)
echo "Starting soak client using topic $topic. Logging to $logfile."
set +x
# Ignore SIGINT in children (inherited)
trap "" SIGINT
time opentelemetry-instrument confluent-kafka-python/tests/soak/soakclient.py -i $TESTID -t $topic -r 80 -f $1 |& tee /dev/tty | bzip2 > $logfile &
PID=$!
# On SIGINT kill only the first process of the pipe
onsigint() {
        # List children of $PID only
        ps --ppid $PID -f | grep soakclient.py | grep -v grep | awk '{print $2}' | xargs kill
}
trap onsigint SIGINT
# Await the result
wait $PID
ret=$?
echo "Python client exited with status $ret"
echo "Ending soak client using topic $topic. Logging to $logfile."
exit $ret
