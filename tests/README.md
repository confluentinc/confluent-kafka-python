Unit tests
==========

From top-level directory run:

    $ tox

**NOTE**: This requires `tox` ( please install with `pip install tox` ) and several supported versions of Python.

Integration tests
==========

To run all of the integration test `modes` uncomment the following line from `tox.ini` and add the paths to your Kafka and Confluent Schema Registry instances. 
You can also run the integration tests outside of `tox` by running this command from the source root.

    examples/integration_test.py <kafka-broker> [<test-topic>] [<schema-registry>]

To run individual integration test `modes` with `tox` use the following syntax.

    tox -- --<test mode>

The throttle_cb integration test require an additiona step and as such are not included in the default test mode.
In order to execute the throttle_cb test you must first set a throttle for the client 'throttled_client' with the command below:

    kafka-configs  --zookeeper <zookeeper host>:<zookeeper port> \
        --alter --add-config 'request_percentage=01' \
        --entity-name throttled_client --entity-type clients

Once the throttle has been set you can proceed with the following command:

    tox -- --throttle
