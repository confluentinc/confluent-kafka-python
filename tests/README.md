# Conventions Used in This Document
Unless otherwise noted all commands, file and directory references are relative to the *source root* directory.

## Terminology
 - modes: Collection of integration tests to be run; a mode(singular) represents an individual integration testing unit
 - testconf: [JSON](https://tools.ietf.org/html/rfc8259) formatted configuration file.
        Example: [tests/testconf-example.json](./tests/testconf-example.json) for formatting.

Unit tests
==========
From top-level directory run:

    $ tox

**NOTE**: This requires `tox` ( please install with `pip install tox` ) and several supported versions of Python.

Integration tests
=================

## Introduction
Integration tests, `tests` henceforth, are broken up into two distinct categories.
   1. Default
   2. Ancillary

Default modes can be executed against any unsecured cluster with a minimally configured [testconf](#configuration).

    Default Modes: consumer, producer, avro, performance, admin

Ancillary modes require additional steps to be taken prior to `mode` execution. Per `mode` instructions are covered in
within sections sharing the same name.

Ancillary Modes: [throttle](#throttle), [avro-https](#avro-https), [avro-basic-auth](#avro-basic-auth)

## Prerequisites
All test modes require access to a running Kafka cluster configured with at least one PLAINTEXT listener.

Test modes prefixed with `avro` require a running [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html)

**Note** Test mode `avro` is included in the default modes list.

In order to keep things contained resources have been provided to deploy a dockerized cluster which developers are encouraged to leverage.
See the [Docker](#docker) section below for additional requirements and details.

All tests require some degree of configuration to run.
See [#Configuration](#configuration) for instructions on configuring tests.

## Running tests
**WARNING**: These tests will create new topics and consumer groups.

To run all of the default test `modes` uncomment the following line from `tox.ini` and provide the location to your `testconf`

    #python examples/integration_test.py --conf testconf.json

You can also run the integration tests outside of `tox` with the command below.

    python examples/integration_test.py --conf testconf.json

To specify which `mode` or `modes` to run use the following syntax

    python examples/integration_test.py --<test mode 1> --<test mode n...> --conf <testconf>

For example:

    python examples/integration_test.py --producer --conf testconf.json

To get a list of modes you can run the integration test manually with the `--help` flag

    python examples/integration_tests.py --help

## Configuration
Tests are configured with a JSON encoded file referred to as a `testconf` to be provided as an argument upon test execution.

Test `mode` configuration can be broken up into 3 main categories.
  1. Global, minimal client configuration values required by every test mode.
     1. bootstrap.servers
     2. topic
  2. Default, client configurations required to execute schema registry tests.
     1. schema.registry.url
  3. Ancillary, client configurations required to execute a specific test mode. These are covered in their own sections
     * See the [Ancillary](#ancillary-modes) section for configuration requirements

The following basic conventions should be followed when constructing the `testconf` file.
- Global configuration values should be defined as members of the root json object.
- Default configuration values should also be defined as members of the root object.
- Ancillary mode configuration values should be defined as object members named the same as the mode.

Values can be defined literally or prefixed with '$'.
Values prefixed with '$' represent environment variables which will be resolved at runtime.

See [../tests/testconf-example.json](../tests/testconf-example.json) for an example.

## Docker

A complete docker cluster setup is included within the directory `./docker`.
This is the preferred method for executing integration tests but standalone cluster will do as well.

#### Prerequisites:

 1. docker-compose 3.0 +
 2. docker-engine 1.13.0+

To set up a cluster with the default settings execute the following commands:
```bash
    source ./docker/.env
    # If running HTTPS enabled tests
    ./docker/bin/certify
    docker-compose -f ./docker/docker-compose.yaml up -d
```

Use the `testconf` provided in the ./docker/conf directory for all tests.
Unless noted elsewhere the default configuration should suffice.

When tests have completed you can destroy the cluster by executing the command
```bash
    docker-compose -f ./docker/docker-compose.yaml down -v --remove-orphans
```

## Ancillary Modes

### Throttle

The throttle_cb integration test requires aggressive throttling to be configured to simulate heavy load.

No cluster or testconf changes are required however the following preliminary steps must be executed prior to test execution.

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


### Avro Https

#### Cluster Requirements
1. Schema Registry must be configured with a HTTPS listener and optionally TLS mutual auth.

For instructions on how to configure the Schema Registry please see the [Confluent Schema Registry documentation](https://docs.confluent.io/current/schema-registry/docs/security.html#configuring-the-rest-api-for-http-or-https)

**Docker:** Prior to starting the cluster you must first generate the certificates by running the following.

    ./docker/bin/certify

#### Testconf Requirements
1. Add `avro-https` to the root object with the following members
    1. `schema.registry.url`; for the HTTPS listener
    2. `schema.registry.ssl.ca.location`; location of the CA certificate used to verify the Schema Registry certificate,
2. **Optional:** configuring tls client auth:
    1. `schema.registry.ssl.certificate.location`; client certificate location
    2. `schema.registry.ssl.key.location`; client private key location

#### Execute test
    python examples/integration_test.py --avro-https --conf testconf.json

### Avro Basic Auth

#### Cluster Requirements
1. A running Schema Registry configured with http basic authentication enabled.

**Docker:**  REST_AUTHENTICATION_METHOD=BASIC docker-compose -f ./docker/docker-compose.yaml up -d

#### Testconf Requirements
1. schema.registry.url must include user information in the url
  * `"schema.registry.url" : "http://ckp_tester:test_secret@schema_registry:8081"`

2. add `avro-basic-auth` to the root object with the following members
    1. `schema.registry.basic.auth.user.info`
    2. `sasl.username`
    3. `sasl.password`

Configuration details can be found at the following links: 
1. [schema.registry.*](https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#basic-auth-security)
2. [sasl.*](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

The [Docker testconf](../docker/conf/testconf.json) and [Docker compose file](../docker/docker-compose.yaml) can be referenced as an example.

**Note** The userinfo provided in the url and testconf members must have valid credentials as configured with the Schema Registry.

#### Execute test
     python examples/integration_test.py --avro-basic-auth --conf testconf.json
