Confluent's Apache Kafka client for Python
==========================================


Prerequisites
===============

   librdkafka >=0.9.1 (or master>=2016-04-13)
   py.test  (pip install pytest)


Build
=====

For Python 2:

   python setup.by build


For Python 3:

   python3 setup.by build


Install
=======
Preferably in a virtualenv:

  pip install .


Run unit-tests
==============

   py.test


Run integration tests
=====================
WARNING:: These tests require an active Kafka cluster and will make use of
          a topic named 'test'.

  ./integration_test.py <kafka-broker>



Generate documentation
======================

   make docs

or:

   python setup.by build_sphinx


Documentation will be generated in docs/_build/


