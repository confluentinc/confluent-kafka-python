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
# limitations under the License.
#


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#
import os
import os.path
import random

NAMES = ['stefan', 'melanie', 'nick', 'darrel', 'kent', 'simon']
AGES = list(range(1, 10)) + [None]


def get_schema_path(fname):
    dname = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dname, fname)


avsc_dir = os.path.dirname(os.path.realpath(__file__))


def create_basic_item(i):
    return {
        'name': random.choice(NAMES) + '-' + str(i),
        'number': random.choice(AGES)
    }


BASIC_ITEMS = map(create_basic_item, range(1, 20))


def create_adv_item(i):
    friends = map(create_basic_item, range(1, 3))
    family = map(create_basic_item, range(1, 3))
    basic = create_basic_item(i)
    basic['family'] = dict(map(lambda bi: (bi['name'], bi), family))
    basic['friends'] = dict(map(lambda bi: (bi['name'], bi), friends))
    return basic


ADVANCED_ITEMS = map(create_adv_item, range(1, 20))


def cleanup(files):
    for f in files:
        try:
            os.remove(f)
        except OSError:
            pass
