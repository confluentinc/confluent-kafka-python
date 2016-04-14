#!/usr/bin/env python

from setuptools import setup
from distutils.core import Extension


module = Extension('confluent_kafka',
                    include_dirs = ['/usr/local/include'],
                    libraries= ['rdkafka'],
                    sources=['confluent_kafka.c', 'Producer.c', 'Consumer.c'])

setup (name='confluent-kafka',
       version='0.9.1',
       description='Confluent\'s Apache Kafka client for Python',
       author='Confluent Inc',
       author_email='support@confluent.io',
       url='https://github.com/confluentinc/confluent-kafka-python',
       ext_modules=[module])

