# Conventions Used in This Document
Although not strictly required the use of [pyenv](https://pypi.org/project/pyenv/) and [tox-pyenv](https://pypi.org/project/tox-pyenv/) are highly encourage.

This simplifies the act of installing and using multiple interpreters when using [tox]( [tox](https://pypi.org/project/tox/)). 


Unit tests
==========

Execute tests with each interpreter configured in [tox.ini](../tox.ini(must be installed on local system). 

    $ tox


Using tox
=========

Execute a specific test with each configured interpreter.  

    $ tox -- tests/confluent_kafka/test_producer.py


Execute tests with a single interpreter(must be present in tox.ini)

    $ tox -e py27

Execute a specific test with a specific interpreter. 

    $ tox -e py27 -- tests/confluent_kafka/test_producer.py


Execute tests against your current interpreter without the use of [tox](https://pypi.org/project/tox/):

    $ pytest

Integration tests
=================

### Requirements
 - docker(for schema registry)
 - trivup(not required for external clusters)

Execute integration tests with each configured interpreter. 

    $ tox -- tests/integration/


### Configuration
By default integration tests are executed against an embedded cluster which requires no additional configuration. 

To point the integration tests at an external cluster uncomment and set the values under the `dev`
property in [tox.ini](../tox.ini)
