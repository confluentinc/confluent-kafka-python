#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limit
#

import logging

from confluent_kafka import Consumer, Producer


class CountingFilter(logging.Filter):
    def __init__(self, name):
        self.name = name
        self.cnt = 0

    def filter(self, record):
        print(self.name, record.getMessage())
        self.cnt += 1
        print(record)


def test_logging_consumer():
    """ Tests that logging works """

    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    f = CountingFilter('consumer')
    logger.addFilter(f)

    kc = Consumer({'group.id': 'test',
                   'debug': 'all'},
                  logger=logger)
    while f.cnt == 0:
        kc.poll(timeout=0.5)

    print('%s: %d log messages seen' % (f.name, f.cnt))

    kc.close()


def test_logging_producer():
    """ Tests that logging works """

    logger = logging.getLogger('producer')
    logger.setLevel(logging.DEBUG)
    f = CountingFilter('producer')
    logger.addFilter(f)

    p = Producer({'debug': 'all'}, logger=logger)

    while f.cnt == 0:
        p.poll(timeout=0.5)

    print('%s: %d log messages seen' % (f.name, f.cnt))


def test_logging_constructor():
    """ Verify different forms of constructors """

    for how in ['dict', 'dict+kwarg', 'kwarg']:
        logger = logging.getLogger('producer: ' + how)
        logger.setLevel(logging.DEBUG)
        f = CountingFilter(logger.name)
        logger.addFilter(f)

        if how == 'dict':
            p = Producer({'debug': 'all', 'logger': logger})
        elif how == 'dict+kwarg':
            p = Producer({'debug': 'all'}, logger=logger)
        elif how == 'kwarg':
            conf = {'debug': 'all', 'logger': logger}
            p = Producer(**conf)
        else:
            raise RuntimeError('Not reached')

        print('Test %s with %s' % (p, how))

        while f.cnt == 0:
            p.poll(timeout=0.5)

        print('%s: %s: %d log messages seen' % (how, f.name, f.cnt))
