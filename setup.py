#!/usr/bin/env python

from setuptools import setup
from distutils.core import Extension


module = Extension('confluent_kafka.cimpl',
                    include_dirs = ['/usr/local/include'],
                    libraries= ['rdkafka'],
                    sources=['confluent_kafka/cimpl/confluent_kafka.c', 'confluent_kafka/cimpl/Producer.c', 'confluent_kafka/cimpl/Consumer.c'])

setup (name='confluent-kafka',
       version='0.9.1',
       description='Confluent\'s Apache Kafka client for Python',
       author='Confluent Inc',
       author_email='support@confluent.io',
       url='https://github.com/confluentinc/confluent-kafka-python',
       ext_modules=[module],
       packages=['confluent_kafka', 'confluent_kafka.cimpl', 'confluent_kafka.kafkatest'])
