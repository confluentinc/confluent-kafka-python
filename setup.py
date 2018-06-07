#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from distutils.core import Extension
import sys
if sys.version_info[0] < 3:
    avro = 'avro'
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


def get_install_requirements(path):
    content = open(os.path.join(os.path.dirname(__file__), path)).read()
    return [
        req
        for req in content.split("\n")
        if req != '' and not req.startswith('#')
    ]


setup(name='confluent-kafka',
      version='0.11.5rc0',
      description='Confluent\'s Apache Kafka client for Python',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      ext_modules=[module],
      packages=find_packages(exclude=("tests", "tests.*")),
      data_files=[('', ['LICENSE.txt'])],
      install_requires=[
          'futures;python_version<"3.0"',
          'enum34;python_version<"3.0"'
      ],
      extras_require={
          'avro': ['fastavro', 'requests', avro],
          'dev': get_install_requirements("test-requirements.txt")
      })
