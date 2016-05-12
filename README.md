Confluent's Apache Kafka client for Python
==========================================


Prerequisites
===============

    librdkafka >=0.9.1 (or master>=2016-04-13)
    py.test  (pip install pytest)


Build
=====

    python setup.by build



Install
=======
Preferably in a virtualenv:

    pip install .


Run unit-tests
==============

    py.test


Run integration tests
=====================
**WARNING**: These tests require an active Kafka cluster and will make use of a topic named 'test'.

    examples/integration_test.py <kafka-broker>



Generate documentation
======================
Install sphinx and sphinx_rtd_theme packages and then:

    make docs

or:

    python setup.by build_sphinx


Documentation will be generated in `docs/_build/`


Examples
========

See [examples](examples)
