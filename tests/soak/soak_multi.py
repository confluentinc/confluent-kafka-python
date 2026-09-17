#!/usr/bin/env python
#
# Copyright 2026 Confluent Inc.
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
# Multi-instance wrapper around soakclient.SoakClient, for soak testing
# free threading support. The below script always spawns an equal number of
# producers and consumers. For a single instance (1 producer and 1 consumer)
# use soakclient.py directly. Requires the GIL to be disabled.
#
# Usage:
#  tests/soak/soak_multi.py -i <testid> -t <topic> -r <produce-rate> -f <client-conf-file>
#      --num-replicas <n>
#

import argparse
import sys
import time
import traceback

from soakclient import SoakClient


def gil_is_enabled():
    return getattr(sys, "_is_gil_enabled", lambda: True)()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Multi-instance (free-threaded) client soak test')
    parser.add_argument('-i', dest='testid', type=str, required=True, help='Test id (suffixed -0.. per replica)')
    parser.add_argument('-b', dest='brokers', type=str, default=None, help='Bootstrap servers')
    parser.add_argument('-t', dest='topic', type=str, required=True, help='Topic to use')
    parser.add_argument('-r', dest='rate', type=float, default=10, help='Message produce rate per second, per replica')
    parser.add_argument(
        '-f', dest='conffile', type=argparse.FileType('r'), help='Configuration file (configprop=value format)'
    )
    parser.add_argument(
        '--num-replicas',
        dest='num_replicas',
        type=int,
        default=2,
        help='Number of full Producer+Consumer SoakClient replicas to run concurrently in this process',
    )

    args = parser.parse_args()

    if args.num_replicas <= 1:
        parser.error('--num-replicas must be > 1 (use soakclient.py directly for a single instance)')

    if gil_is_enabled():
        parser.error(
            'soak_multi.py requires a free-threaded Python interpreter with the GIL '
            'disabled (run with PYTHON_GIL=0 or -X gil=0)'
        )

    base_conf = dict()
    if args.conffile is not None:
        # Parse client configuration file.
        # Standard "key=value" format.
        for line in args.conffile:
            line = line.strip()
            if len(line) == 0 or line[0] == '#':
                continue

            i = line.find('=')
            if i <= 0:
                raise ValueError("Configuration lines must be `name=value..`, not {}".format(line))

            name = line[:i]
            value = line[i + 1 :]

            base_conf[name] = value

    if args.brokers is not None:
        # Overwrite any brokers specified in configuration file with
        # brokers from -b command line argument
        base_conf['bootstrap.servers'] = args.brokers

    # We don't care about partition EOFs
    base_conf['enable.partition.eof'] = False

    # Create one SoakClient replica per -n. All replicas
    # land in the same consumer group.
    soaks = []
    for i in range(args.num_replicas):
        conf = dict(base_conf)
        testid = "{}-{}".format(args.testid, i)
        soaks.append(SoakClient(testid, args.topic, args.rate, conf))

    # Get initial resource usage for every replica
    for soak in soaks:
        soak.get_rusage()

    # Run until interrupted
    try:
        while any(soak.run for soak in soaks):
            time.sleep(10)
            for soak in soaks:
                soak.get_rusage()

        print("Soak client(s) aborted", file=sys.stderr)

    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
    except Exception as ex:
        print("Fatal exception {}\n{}".format(ex, traceback.format_exc()), file=sys.stderr)

    # Terminate every replica
    for soak in soaks:
        soak.terminate()
