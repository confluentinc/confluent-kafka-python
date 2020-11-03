# Running Apache Kafka's client system-tests (kafkatests) with the Python client

Apache Kafka's system-tests (also called kafkatests) allow the Java
Producer and Consumer to be replaced with an alternate client using the
pluggable VerifiableClient mixin.

These instructions walks you through the required steps to run
the Confluent Kafka Python client with kafkatests.

## Pre-requisites

The assumption is that the host system is running Linux with Docker installed,
and that the kafkatest environment is set up according to the instructions in
the Kafka repository's `tests/README.md` file.

Only the Vagrant-based kafkatest runs are supported.
(Ducker (docker-based) kafkatest runs are currently not supported due
to deployment issues.)

 1. Clone kafka into a directory of your choice
    (hereby after referred to as `$KAFKA_DIR`)
 2. Build Kafka
 3. Set up the Vagrant-based kafkatest environmnent according
    to `$KAFKA_DIR/tests/README.md`
 4. Run `vagrant status` to verify that workers are runnning.



## Create self-contained wheels

To ease deployment of the Python client and its dependencies to
the kafkatest worker instances, we'll build self-contained binary linux
wheels.

From the confluent-kafka-python top-level directory, run:

    $ CIBW_SKIP="cp3* *i686*" tools/cibuildwheel-build.sh wheels

After about 5 minutes the resulting Python wheels should be available in
the wheels/ directory.

The CIBW_SKIP part makes sure only Python 2.7-x64 packages are built.


## Prepare deploy directory

The Python wheels and a deploy script needs to be copied to a location reachable
from the kafkatest worker instances (which is anywhere in `$KAFKA_DIR`).
Run the provided script to take care of this:

    $ confluent_kafka/kafkatests/deploy.sh --prepare $KAFKA_DIR wheels

Synchronize shared directory with workers:

    $ cd $KAFKA_DIR
    $ vagrant rsync


## Verify Python client setup

    $ cd $KAFKA_DIR
    $ ducktape --globals tests/confluent-kafka-python/globals.json tests/kafkatest/tests/client/pluggable_test.py
    $ grep confluent-kafka-python results/latest/PluggableConsumerTest/test_start_stop/1/test_log.debug

The test should PASS and the grep command should return some output.


## Run kafkatests

    $ cd $KAFKA_DIR

To run a sub-set of tests:

    $ ducktape --globals tests/confluent-kafka-python/globals.json tests/kafkatest/tests/client/consumer_test.py::AssignmentValidationTest

To run the full kafkatest client test-suite:

    $ ducktape --globals tests/confluent-kafka-python/globals.json tests/kafkatest/tests/client




## Stand-alone usage (fwiw)

The kafkatest client can be ran directly with:

   python -m confluent_kafka.kafkatest.verifiable_consumer <options>

   python -m confluent_kafka.kafkatest.verifiable_producer <options>
