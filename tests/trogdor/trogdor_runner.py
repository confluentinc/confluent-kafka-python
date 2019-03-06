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

# TrogdorRunner executes one Trogdor ExternalCommandSpec task.
#
# One example of the ExternalCommandSpec spec:
# {
#     "id": "$TASK_ID",
#
#     "spec": {
#         "class": "org.apache.kafka.trogdor.workload.ExternalCommandSpec",
#         "command": ["python", "./tests/bin/ExternalCommandExample.py"],
#         "durationMs": 10000000,
#         "commandNode": "node0",
#         "workload":{
#             "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
#             "bootstrapServers": "localhost:9092",
#             "targetMessagesPerSec": 10000,
#             "maxMessages": 50000,
#             "activeTopics": {
#                 "foo[1-3]": {
#                     "numPartitions": 10,
#                     "replicationFactor": 1
#                 }
#             },
#             "inactiveTopics": {
#                 "foo[4-5]": {
#                     "numPartitions": 10,
#                     "replicationFactor": 1
#                 }
#             }
#         }
#     }
# }

import argparse
import json
import sys
from produce_spec_runner import execute_produce_spec, ProduceSpecRunner
from trogdor_utils import update_trogdor_status
from trogdor_utils import update_trogdor_error
from trogdor_utils import trogdor_log

# if passed with --spec spec.json, TrogdorProducer directly executes the task,
# otherwise, TrogdorProduder waits on the task start command.
parser = argparse.ArgumentParser(description='Python Trogdor Producer.');
parser.add_argument('--spec', dest='spec', required=False);
args = parser.parse_args()


def execute_task(task_spec):
    try:
        workload = task_spec["workload"]
        spec_class = workload["class"]
        if spec_class == 'org.apache.kafka.trogdor.workload.ProduceBenchSpec':
            execute_produce_spec(task_spec)
        else:
            update_trogdor_error(spec_class + " is not supported.")
    except Exception as exp:
        update_trogdor_error("Exception: " + exp)



def get_task_from_specfile(specFile):
    specString = "";
    with open(specFile) as f:
        for line in f:
            if line.startswith('#'):
                continue;
            specString += line;
        f.close();
    if (specString == ""):
        return None;
    task = json.loads(specString);
    return task

def get_task_from_input():
    for line in sys.stdin:
        try:
            comm = json.loads(line)
            if "action" in comm:
                action = comm["action"]
                if action == "stop":
                    exit(0)
                elif action == "start":
                    if "spec" in comm:
                        task = comm["spec"]
                        update_trogdor_status("Started to execute the workload.")
                        return comm["spec"]
                    else:
                        update_trogdor_error("No spec in command " + line)
                else:
                    trogdor_log("Unknown command action " + action)
            else:
                update_trogdor_error("Unknown control command " + line)
        except ValueError:
            trogdor_log("Input line " + line + " is not a valid external runner command.")


if __name__ == '__main__':
    spec = None;
    if args.spec:
        spec = get_task_from_specfile(args.spec);
    else:
        spec = get_task_from_input()
    execute_task(spec);
