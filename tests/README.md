Unit tests
==========

From top-level directory run:

    $ tox

**NOTE**: This requires `tox` ( please install with `pip install tox` ) and several supported versions of Python.

Integration tests
=================

**NOTE**: Integration tests require an existing Kafka cluster and a `testconf.json` configuration file. Any value provided
in `testconf.json` prefixed with '$' will be treated as an environment variable and automatically resolved.

At a minimum you must specify `bootstrap.servers` and `topic` within `testconf.json`. Please reference [tests/testconf-example.json](tests/testconf-example.json) for formatting.

**WARNING**: These tests will create new topics and consumer groups.

To run all of the integration test `modes` uncomment the following line from `tox.ini` and provide the location to your `testconf.json`

    #python examples/integration_test.py --conf <testconf.json>

You can also run the integration tests outside of `tox` by running this command from the source root directory

    python examples/integration_test.py --conf <testconf.json>

To run individual integration test `modes` use the following syntax

    python examples/integration_test.py --<test mode> --conf <testconf.json>

For example:

    python examples/integration_test.py --producer --conf testconf.json

To get a list of modes you can run the integration test manually with the `--help` flag

    python examples/integration_tests.py --help


Throttle Callback test
======================
The throttle_cb integration test requires an additional step and as such is not included in the default test modes.
In order to execute the throttle_cb test you must first set a throttle for the client 'throttled_client' with the command below:

    kafka-configs  --zookeeper <zookeeper host>:<zookeeper port> \
        --alter --add-config 'request_percentage=01' \
        --entity-name throttled_client --entity-type clients

Once the throttle has been set you can proceed with the following command:

    python examples/integration_test.py --throttle --conf testconf.json


To remove the throttle you can execute the following

    kafka-configs  --zookeeper <zookeeper host>:<zookeeper port> \
        --alter --delete-config 'request_percentage' \
        --entity-name throttled_client --entity-type clients


HTTPS Schema Registry test
==========================

HTTPS tests require access to a Schema Registry instance configured to with at least one HTTPS listener.

For instructions on how to configure the Schema Registry please see the Confluent documentation:

[Schema Registry documentation](https://docs.confluent.io/current/schema-registry/docs/security.html#configuring-the-rest-api-for-http-or-https)

If client authentication has been enabled you will need to provide both the client certificate, `schema.registry.ssl.certificate.location`,
and the client's private key, `schema.registry.ssl.key.location`

    python examples/integration_test.py --avro-https --conf testconf.json