# Conventions Used in This Document
Unless otherwise noted all commands, file and directory references are relative to the *source root* directory.

## Terminology
 - modes: Collection of integration tests to be run
 - testconf: [JSON](https://tools.ietf.org/html/rfc8259) formatted configuration file.
        Example: [tests/testconf-example.json](./tests/testconf-example.json) for formatting.

Unit tests
==========
From top-level directory run:

    $ ./tests/run.sh tox

**NOTE**: This requires `tox` ( please install with `pip install tox` ) and several supported versions of Python.

If tox is not installed:

    $ ./tests/run.sh unit

Integration tests
=================

### Requirements
 1. docker-compose 3.0 +
 2. docker-engine 1.13.0+
 3. **Optional:** tox (enables testing against multiple interpreter versions)

### Cluster setup
**Note** Manual cluster set up is not required when using ./tests/run.sh

    $ ./tests/docker/bin/cluster_up.sh

### Cluster teardown
**Note** Manual cluster teardown is not required when using ./tests/run.sh

    $ ./tests/docker/bin/cluster_down.sh

### Configuration
Tests are configured with a JSON configuration file referred to as `testconf.json` to be provided as the last argument upon test execution.

Advanced users can reference the provided configuration file, [testconf.json](integration/testconf.json), if modification is required.
Most developers however should use the defaults.

### Running tests
To run the entire test suite:

From the source root directory ...

- With tox installed (will run against all supported interpreters)
  1. Uncomment the following line from [tox.ini](../tox.ini)
    - ```#python tests/integration/integration_test.py```
  2. Execute the following script
    - ```$ ./tests/run.sh tox```

- Without tox (will run against current interpreter)
  - ```$ ./tests/run.sh all```

To run just the unit tests

    $ ./tests/run.sh unit

To run a specific integration test `mode` or set of `modes` use the following syntax

    $ ./tests/run.sh <test mode 1> <test mode 2>..

For example:

    $ ./tests/run.sh --producer --consumer

To get a list of integration test `modes` simply supply the `help` option

    $ ./tests/run.sh --help
