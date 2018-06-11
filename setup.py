#!/usr/bin/env python

from setuptools import setup, find_packages
from distutils.core import Extension
import sys

INSTALL_REQUIRES = list()

if sys.version_info[0] < 3:
    avro = 'avro'
    INSTALL_REQUIRES.extend(['futures', 'enum34'])
else:
    avro = 'avro-python3'

module = Extension('confluent_kafka.cimpl',
                   libraries=['rdkafka'],
                   sources=['confluent_kafka/src/confluent_kafka.c',
                            'confluent_kafka/src/Producer.c',
                            'confluent_kafka/src/Consumer.c',
                            'confluent_kafka/src/Metadata.c',
                            'confluent_kafka/src/AdminTypes.c',
                            'confluent_kafka/src/Admin.c'])

setup(name='confluent-kafka',
      version='0.11.5rc0',
      description='Confluent\'s Apache Kafka client for Python',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      ext_modules=[module],
      packages=find_packages(exclude=("tests", "tests.*")),
      data_files=[('', ['LICENSE.txt'])],
      install_requires=INSTALL_REQUIRES,
      extras_require={
          'avro': ['fastavro', 'requests', avro]
      })
