#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from distutils.core import Extension
import platform

work_dir = os.path.dirname(os.path.realpath(__file__))
mod_dir = os.path.join(work_dir, 'src', 'confluent_kafka')
ext_dir = os.path.join(mod_dir, 'src')

INSTALL_REQUIRES = [
    'futures;python_version<"3.2"',
    'enum34;python_version<"3.4"',
]

TEST_REQUIRES = [
    'pytest==4.6.4;python_version<"3.0"',
    'pytest;python_version>="3.0"',
    'pytest-timeout',
    'flake8'
]

DOC_REQUIRES = ['sphinx', 'sphinx-rtd-theme']

SCHEMA_REGISTRY_REQUIRES = ['requests']

AVRO_REQUIRES = ['fastavro>=0.23.0,<1.0;python_version<"3.0"',
                 'fastavro>=1.0;python_version>"3.0"',
                 'avro>=1.11.1,<2',
                 ] + SCHEMA_REGISTRY_REQUIRES

JSON_REQUIRES = ['pyrsistent==0.16.1;python_version<"3.0"',
                 'pyrsistent;python_version>"3.0"',
                 'jsonschema'] + SCHEMA_REGISTRY_REQUIRES

PROTO_REQUIRES = ['protobuf'] + SCHEMA_REGISTRY_REQUIRES

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


trove_classifiers = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(name='confluent-kafka',
      # Make sure to bump CFL_VERSION* in confluent_kafka/src/confluent_kafka.h
      # and version in docs/conf.py.
      version='2.4.0',
      description='Confluent\'s Python client for Apache Kafka',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      ext_modules=[module],
      packages=find_packages('src'),
      package_dir={'': 'src'},
      data_files=[('', [os.path.join(work_dir, 'LICENSE.txt')])],
      install_requires=INSTALL_REQUIRES,
      classifiers=trove_classifiers,
      extras_require={
          'schema-registry': SCHEMA_REGISTRY_REQUIRES,
          'avro': AVRO_REQUIRES,
          'json': JSON_REQUIRES,
          'protobuf': PROTO_REQUIRES,
          'dev': TEST_REQUIRES + AVRO_REQUIRES,
          'doc': DOC_REQUIRES + AVRO_REQUIRES
      })
