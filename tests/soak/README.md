# Soak testing client

The soak testing clients should be run for prolonged periods
of time, typically 2+ weeks, to vet out any resource leaks, etc.

The soak testing client is made up of a producer, producing messages to
the configured topic, and a consumer, consuming the same messages back.

DataDog reporting supported by setting datadog.api_key a and datadog.app_key
in the soak client configuration file.


There are some convenience script to get you started.

On the host (ec2) where you aim to run the soaktest, do:

$ git clone https://github.com/confluentinc/librdkafka
$ git clone https://github.com/confluentinc/confluent-kafka-python

# Build librdkafka and python
$ ~/confluent-kafka-python/tests/soak/build.sh <librdkafka-version> <cfl-python-version>

# Set up config:
$ cp ~/confluent-kafka-python/tests/soak/ccloud.config.example ~/confluent-kafka-python/ccloud.config

# Start a screen session
$ screen bash

# Within the screen session, run the soak client
(screen)$ ~/run.sh
(screen)$ Ctrl-A d  # to detach
