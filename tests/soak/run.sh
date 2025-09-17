#!/bin/bash

source venv/bin/activate

testdir=$PWD
export LD_LIBRARY_PATH=$testdir/librdkafka-installation/lib
librdkafka_version=$(python3 -c 'from confluent_kafka import libversion; print(libversion()[0])')

if [[ -z $librdkafka_version ]]; then
    echo "No librdkafka version found.."
    exit 1
fi

set -u
run=true
topic="pysoak-$TESTID-$librdkafka_version"
logfile="${TESTID}.log.bz2"
limit=$((50 * 1024 * 1024)) # 50MB
export HOSTNAME=$(hostname)
echo "Starting soak client using topic $topic. Logging to $logfile."
set +x
while [ "$run" = true ]; do
    # Ignore SIGINT in children (inherited)
    trap "" SIGINT
    time opentelemetry-instrument $testdir/soakclient.py -i $TESTID -t $topic -r 80 -f $1 |& tee /dev/tty | bzip2 > $logfile &
    PID=$!
    terminate_last() {
        # List children of $PID only
        ps --ppid $PID -f | grep soakclient.py | grep -v grep | awk '{print $2}' | xargs kill
    }
    # On SIGINT kill only the first process of the pipe
    onsigint() {
        echo "Terminating soak client using topic $topic. Logging to $logfile."
        terminate_last
        run=false
    }
    trap onsigint SIGINT
    # Await the result
    sleep 1
    size=$(stat -c%s "$logfile")
    while (( size < limit )); do
        echo "Log file size is $size bytes, less than limit $limit"
        sleep 3600
        size=$(stat -c%s "$logfile")
    done
    echo "Rolling log file: $logfile"
    terminate_last
    wait $PID
    ret=$?
    echo "Python client exited with status $ret"
    mv $logfile "${TESTID}.log.prev.bz2" || true
done
echo "Ending soak client using topic $topic. Logging to $logfile."
exit $ret
