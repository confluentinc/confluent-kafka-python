#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from distutils.core import Extension
import platform

INSTALL_REQUIRES = [
    'futures;python_version<"3.2"',
    'enum34;python_version<"3.4"',
    'requests;python_version<"3.2"'
]

AVRO_REQUIRES = [
    'fastavro',
    'requests',
    'avro;python_version<"3.0"',
    'avro-python3;python_version>"3.0"'
]

TEST_REQUIRES = [
    'pytest==4.6.4;python_version<"3.0"',
    'pytest;python_version>="3.0"',
    'pytest-timeout',
    'flake8'
]

# On Un*x the library is linked as -lrdkafka,
# while on windows we need the full librdkafka name.
if platform.system() == 'Windows':
    librdkafka_libname = 'librdkafka'
else:
    librdkafka_libname = 'rdkafka'

module = Extension('confluent_kafka.cimpl',
                   libraries=[librdkafka_libname],
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
      version='1.2.0',
      description='Confluent\'s Python client for Apache Kafka',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      ext_modules=[module],
      packages=find_packages(exclude=("tests", "tests.*")),
      data_files=[('', ['LICENSE.txt'])],
      install_requires=INSTALL_REQUIRES,
      extras_require={
          'avro': AVRO_REQUIRES,
          'dev': TEST_REQUIRES + AVRO_REQUIRES
      })
