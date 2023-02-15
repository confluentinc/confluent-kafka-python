# Overview

Test summary:

- `./tests/test_*` (pytest): Unit tests. These tests don't bring up any external processes.
- `./tests/schema_registry/*` (pytest): Tests related to Schema Registry that utilize a mock client and don't bring up any external processes.
- `./tests/avro/*` (pytest): Tests for the old Avro API. These tests don't bring up any external processes.
- `./tests/integration/integration_test.py`: Older integration tests, not built on pytest.
- `./tests/integration/*`, excluding `integration_test.py` (pytest): Integration tests that depend on Kafka.
- `./tests/system`: System tests.
- `./tests/soak`: Soak test.

# Running the Tests

**Note:** Unless otherwise stated, all command, file and directory references are relative to the *repo's root* directory.

A python3 env suitable for running tests:

    $ python3 -m venv venv_test
    $ source venv_test/bin/activate
    $ pip install -r tests/requirements.txt
    $ python setup.py build
    $ python setup.py install

When you're finished with it:

    $ deactivate

## Unit tests

"Unit" tests are the ones directly in the `./test` directory. These tests do
not require an active Kafka cluster.

You can run them selectively like so:

    $ pytest -s -v tests/test_Producer.py

Or run them all with:

    $ pytest -s -v tests/test_*.py

Note that the -v flag enables verbose output and -s flag disables capture of stderr and stdout (so that you see it on the console).

You can also use ./tests/run.sh to run the unit tests:

    $ ./tests/run.sh unit


## Integration tests

Integration tests are currently transitioning from one framework to another.

### The Old Way

The original integration tests do not utilise `pytest` and are all specified in `./test/integration_test.py`. These tests expect a Kafka cluster and Schema Registry instances to already be running.

The easiest way to arrange for this is:

    ./tests/docker/bin/cluster_up.sh

And also:

    source ./tests/docker/.env.sh

which sets environment variables referenced by `./tests/integration/testconf.json`.

You can then run the tests as follows:

    python ./tests/integration/integration_test.py ./tests/integration/testconf.json

Or selectively using via specifying one or more options ("modes"). You can see all of these via:

    python ./tests/integration/integration_test.py --help


### The New Way

The newer integration tests utilise `pytest` and define the `kafka_cluster` fixture (in `./tests/integration/conftest.py`) which uses [trivup](https://github.com/edenhill/trivup) to bring up a Kafka Cluster and Schema Registry instance automatically.

You can run these tests selectively like so:

    pytest -v -s ./tests/integration/consumer/test_consumer_error.py

#### Bring your own cluster

If you would like to avoid creating / destroying a cluster each time you run a
test and you have a test cluster running, you can set the BROKERS environment
variable which will automatically make the integration tests use those brokers
as the `bootstrap.servers` instead of creating a new cluster for each test
run, e.g.:

```bash

$ export BROKERS=localhost:9092
# SR_URL is optional and only required for Schema-registry tests
$ export SR_URL=http://localhost:1234/
```


#### Troubleshooting

If for some reason these tests aren't working, you can add `'debug': True` to the config property list in `./tests/integration/conftest.py` to debug the cause.

Note that the following error is benign:

```
tests/integration/consumer/test_consumer_error.py::test_consume_error [2020-12-02 12:09:15.649905] KafkaBrokerApp-2: Failed to set RLIMIT_NOFILE(9223372036854775807,9223372036854775807): current limit exceeds maximum limit
```


### Running with Tox

Tox can be used to test against various supported Python versions (py27, py36, py38):

1. You need to have tox installed:

    ```pip install tox```

2. Uncomment the following line in [tox.ini](../tox.ini)

    ```#python tests/integration/integration_test.py```

3. From top-level directory run:

    ```$ ./tests/run.sh tox```

