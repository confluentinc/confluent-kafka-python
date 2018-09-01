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

### Requirements
 1. docker-compose 3.0 +
 2. docker-engine 1.13.0+
 3. **Optional:** tox

### Cluster setup
**Note** Manual cluster set up is not required when using ./tests/run_all.sh

    ./docker/bin/cluster_up.sh

### Cluster teardown
**Note** Manual cluster teardown is not required when using ./tests/run_all.sh

    ./docker/bin/cluster_down.sh

### Configuration
Tests are configured with a JSON encoded file referred to as a `testconf` to be provided as the last argument upon test execution.

Advanced users can reference the provided configuration file, [testconf.json](../docker/conf/testconf.json), if modification is required.
Most developers however should just use the defaults.

### Running tests
To run the entire test suite:

- With tox installed (will run against all supported interpreters)
  1. Uncomment the following line from [tox.ini](../tox.ini)
    - ```#python examples/integration_test.py ./docker/conf/testconf.json```
  2. Execute the following script
    - ```./tests/run.sh tox [options]```

- Without tox (will run against current interpreter)
  - ```./tests/run_all.sh```

To run a specific `mode` or set of `modes` use the following syntax

    ./tests/run.sh [test mode 1] [test mode n...]

For example:

    ./tests/run.sh producer consumer

To get a list of `modes` simply supply the `help` option

    ./tests/run.sh help
