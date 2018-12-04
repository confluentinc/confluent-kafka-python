# Soak testing client

The soak testing clients should be run for prolonged periods
of time, typically 2+ weeks, to vet out any resource leaks, etc.

The soak testing client is made up of a producer, producing messages to
the configured topic, and a consumer, consuming the same messages back.

DataDog reporting supported by setting datadog.api_key a and datadog.app_key
in the soak client configuration file.


Use ubuntu-bootstrap.sh in this directory set up the environment (e.g., on ec2).
