#!/usr/bin/env python

import os

from setuptools import setup, find_packages
from distutils.core import Extension
from distutils.command.build import build
import sys
if sys.version_info[0] < 3:
    avro = 'avro'
else:
    avro = 'avro-python3'


module = Extension('confluent_kafka.cimpl',
                   include_dirs=['confluent_kafka/librdkafka/src/'],
                   sources=['confluent_kafka/src/confluent_kafka.c',
                            'confluent_kafka/src/Producer.c',
                            'confluent_kafka/src/Consumer.c'],
                   extra_objects=['confluent_kafka/librdkafka/src/librdkafka.a'],
                   )


class LibrdkafkaBuilder(build):
    def run(self):
        here = os.path.abspath(os.getcwd())
        try:
            os.chdir("./confluent_kafka/librdkafka")
            os.system("./configure")
            os.system("make libs")
        finally:
            os.chdir(here)

        with open("confluent_kafka/librdkafka/Makefile.config") as f:
            for line in f:
                if line.startswith("LIBS+="):
                    _, libraries = line.split('=')
                    module.libraries.extend(map(lambda l: l.replace('-l', ''), libraries.split()))
                    break
        build.run(self)

setup(name='confluent-kafka',
      version='0.9.2',
      description='Confluent\'s Apache Kafka client for Python',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      cmdclass={'build': LibrdkafkaBuilder},
      ext_modules=[module],
      packages=find_packages(exclude=("tests",)),
      data_files=[('', ['LICENSE'])],
      extras_require={
          'avro': ['fastavro', 'requests', avro]
      })
