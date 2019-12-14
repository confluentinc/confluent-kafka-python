# Conventions Used in This Document
Unless otherwise noted all commands, file and directory references are relative to the *source root* directory.

Unit tests
==========

Execute tests with each configured interpreter(must be installed on local system). 

    $ tox

Execute a specific test with each configured interpreter.  

    $ tox -- tests/confluent_kafka/test_producer.py


Execute tests with a single interpreter(must be present in tox.ini)

    $ tox -e py27

Execute a specific test with a specific interpreter. 

    $ tox -e py27 -- tests/confluent_kafka/test_producer.py

**NOTE**: This requires `tox` ( please install with `pip install tox` ) and several supported versions of Python.

If tox is not installed:

    $ pytest

Integration tests
=================

### Requirements
 - docker

Execute integration tests with each configured interpreter. 

    $ tox -- tests/integration/



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
