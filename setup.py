#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from distutils.core import Extension
import platform

work_dir = os.path.dirname(os.path.realpath(__file__))
mod_dir = os.path.join(work_dir, 'confluent_kafka')
ext_dir = os.path.join(mod_dir, 'src')

with open(os.path.join(mod_dir, 'requirements.txt')) as f:
    INSTALL_REQUIRES = f.read().split()

with open(os.path.join(mod_dir, 'avro', 'requirements.txt')) as f:
    AVRO_REQUIRES = f.read().splitlines()

with open(os.path.join(work_dir, 'tests', 'requirements.txt')) as f:
    TEST_REQUIRES = f.read().splitlines()

with open(os.path.join(work_dir, 'docs', 'requirements.txt')) as f:
    DOC_REQUIRES = f.read().splitlines()

# Temp fix until a better packaging strategy can be rolled out
SCHEMA_REGISTRY_REQUIRES = ['requests']
JSON_REQUIRES = ['jsonschema'] + SCHEMA_REGISTRY_REQUIRES

# On Un*x the library is linked as -lrdkafka,
# while on windows we need the full librdkafka name.
if platform.system() == 'Windows':
    librdkafka_libname = 'librdkafka'
else:
    librdkafka_libname = 'rdkafka'

module = Extension('confluent_kafka.cimpl',
                   libraries=[librdkafka_libname],
                   sources=[os.path.join(ext_dir, 'confluent_kafka.c'),
                            os.path.join(ext_dir, 'Producer.c'),
                            os.path.join(ext_dir, 'Consumer.c'),
                            os.path.join(ext_dir, 'Metadata.c'),
                            os.path.join(ext_dir, 'AdminTypes.c'),
                            os.path.join(ext_dir, 'Admin.c')])


def get_install_requirements(path):
    content = open(os.path.join(os.path.dirname(__file__), path)).read()
    return [
        req
        for req in content.split("\n")
        if req != '' and not req.startswith('#')
    ]


setup(name='confluent-kafka',
      # Make sure to bump CFL_VERSION* in confluent_kafka/src/confluent_kafka.h
      # and version and release in docs/conf.py.
      version='1.4.0',
      description='Confluent\'s Python client for Apache Kafka',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      ext_modules=[module],
      packages=find_packages(exclude=("tests", "tests.*")),
      data_files=[('', [os.path.join(work_dir, 'LICENSE.txt')])],
      install_requires=INSTALL_REQUIRES,
      extras_require={
          'schema-registry': SCHEMA_REGISTRY_REQUIRES,
          'avro': AVRO_REQUIRES,
          'json': JSON_REQUIRES,
          'dev': TEST_REQUIRES + AVRO_REQUIRES,
          'doc': DOC_REQUIRES + AVRO_REQUIRES
      })
