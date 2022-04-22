#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
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
from datetime import datetime, timezone

from avro import schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

NAMES = ['stefan', 'melanie', 'nick', 'darrel', 'kent', 'simon']
AGES = list(range(1, 10)) + [None]


def get_schema_path(fname):
    dname = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dname, fname)


def load_schema_file(fname):
    fname = get_schema_path(fname)
    with open(fname) as f:
        return f.read()


avsc_dir = os.path.dirname(os.path.realpath(__file__))

BASIC_SCHEMA = load_schema_file(os.path.join(avsc_dir, 'basic_schema.avsc'))


def create_basic_item(i):
    return {
        'name': random.choice(NAMES) + '-' + str(i),
        'number': random.choice(AGES)
    }


BASIC_ITEMS = list(map(create_basic_item, range(1, 20)))

ADVANCED_SCHEMA = load_schema_file(os.path.join(avsc_dir, 'adv_schema.avsc'))


def create_adv_item(i):
    friends = map(create_basic_item, range(1, 3))
    family = map(create_basic_item, range(1, 3))
    basic = create_basic_item(i)
    basic['family'] = dict(map(lambda bi: (bi['name'], bi), family))
    basic['friends'] = dict(map(lambda bi: (bi['name'], bi), friends))
    basic['timestamp'] = datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
    return basic


ADVANCED_ITEMS = list(map(create_adv_item, range(1, 20)))


def _write_items(base_name, schema_str, items):
    avro_schema = schema.Parse(schema_str)
    avro_file = base_name + '.avro'
    with DataFileWriter(open(avro_file, "w"), DatumWriter(), avro_schema) as writer:
        for i in items:
            writer.append(i)
    writer.close
    return (avro_file)


def write_basic_items(base_name):
    return _write_items(base_name, BASIC_SCHEMA, BASIC_ITEMS)


def write_advanced_items(base_name):
    return _write_items(base_name, ADVANCED_SCHEMA, ADVANCED_ITEMS)


def cleanup(files):
    for f in files:
        try:
            os.remove(f)
        except OSError:
            pass


if __name__ == "__main__":
    write_advanced_items("advanced")
