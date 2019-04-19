#!/usr/bin/env python
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

import argparse
import json
import sys
from produce_spec_runner import execute_produce_spec
from trogdor_utils import update_trogdor_error
from trogdor_utils import trogdor_log

parser = argparse.ArgumentParser(description='Python Trogdor Producer.')
parser.add_argument('--spec', dest='spec', required=False)
args = parser.parse_args()


def execute_task(workload):
    try:
        spec_class = workload["class"]
        if spec_class == 'org.apache.kafka.trogdor.workload.ProduceBenchSpec':
            execute_produce_spec(workload)
        else:
            update_trogdor_error(spec_class + " is not supported.")
    except Exception as exp:
        update_trogdor_error("Exception:{}".format(exp))


def get_task_from_specfile(specFile):
    """
    :param specFile: One Trogdor ExternalCommandSpec JSON file
    :return specFile["spec"]["workload"] or None
    """
    specString = ""
    workload = None
    with open(specFile) as f:
        for line in f:
            if line.startswith('#'):
                continue
            specString += line
    if (specString == ""):
        return None
    task = json.loads(specString)
    if task.get("class", "") == "org.apache.kafka.trogdor.workload.ExternalCommandSpec" and "workload" in task:
        workload = task["workload"]
    return workload


def get_task_from_input():
    """
    Waiting for a new task on stdin, formatted as {"id":<task ID string>, "workload":<configured workload JSON object>}
    :return: spec["workload"]
    """
    for line in sys.stdin:
        try:
            comm = json.loads(line)
            if "workload" in comm and "id" in comm:
                trogdor_log("Received one workload:{}".format(comm))
                return comm["workload"]
        except ValueError:
            trogdor_log("Input line " + line + " is not a valid external runner command.")


if __name__ == '__main__':
    spec = None
    if args.spec:
        spec = get_task_from_specfile(args.spec)
        if spec is None:
            update_trogdor_error("Unrecognized task:{}".format(args.spec))
            exit(1)
    else:
        spec = get_task_from_input()
    execute_task(spec)
