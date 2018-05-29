Unit tests
==========

From top-level directory run:

    $ tox

**NOTE**: This requires `tox` ( please install with `pip install tox` ) and several supported versions of Python.

Integration tests
=================

**WARNING**: These tests require an active Kafka cluster and will create new topics.


To run all of the integration test `modes` uncomment the following line from `tox.ini` and add the addresses to your Kafka and Confluent Schema Registry instances.

    #python examples/integration_test.py {posargs} <bootstrap-servers> confluent-kafka-testing [<schema-registry-url>]

You can also run the integration tests outside of `tox` by running this command from the source root directory

    examples/integration_test.py <kafka-broker> [<test-topic>] [<schema-registry>]

To run individual integration test `modes` with `tox` use the following syntax

    tox -- --<test mode>

For example:

    tox -- --producer

To get a list of modes you can run the integration test manually with the `--help` flag

    examples/integration_tests.py --help


The throttle_cb integration test requires an additional step and as such is not included in the default test modes.
In order to execute the throttle_cb test you must first set a throttle for the client 'throttled_client' with the command below:

    kafka-configs  --zookeeper <zookeeper host>:<zookeeper port> \
        --alter --add-config 'request_percentage=01' \
        --entity-name throttled_client --entity-type clients

Once the throttle has been set you can proceed with the following command:

    tox -- --throttle


    To remove the throttle you can execute the following


    kafka-configs  --zookeeper <zookeeper host>:<zookeeper port> \
        --alter --delete-config 'request_percentage' \
        --entity-name throttled_client --entity-type clients
