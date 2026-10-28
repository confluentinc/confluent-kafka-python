#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
import sys
import sysconfig
import warnings

# test_dlq.py imports celpy; test_avro_serializers.py and
# test_associated_cluster_id.py import fastavro (through the Avro serdes).
# celpy ships no free-threaded wheel; fastavro ships one but has not declared
# itself GIL-safe. Neither is installed on free-threaded builds (see
# requirements-tests-install-nogil.txt), so all of them are excluded here.
# On Python 3.15 the rules extra (celpy needs google-re2) is not installed
# either (see requirements-tests-install-py315.txt), so test_dlq.py is excluded
# there. TODO: drop that once tink and google-re2 publish 3.15 wheels.
FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

collect_ignore = []
if FREE_THREADED_BUILD:
    collect_ignore = [
        "_async/test_associated_cluster_id.py",
        "_async/test_avro_serializers.py",
        "_async/test_dlq.py",
        "_sync/test_associated_cluster_id.py",
        "_sync/test_avro_serializers.py",
        "_sync/test_dlq.py",
    ]
    warnings.warn(
        "free-threaded build: skipping collection of {} schema_registry "
        "integration test modules requiring celpy (no free-threaded wheel) "
        "or fastavro (not declared GIL-safe)".format(len(collect_ignore)),
        RuntimeWarning,
    )
elif sys.version_info >= (3, 15):
    collect_ignore = [
        "_async/test_dlq.py",
        "_sync/test_dlq.py",
    ]
    warnings.warn(
        "Python 3.15: skipping collection of {} schema_registry integration "
        "test modules requiring the rules extra (celpy needs google-re2), "
        "which ships no Python 3.15 wheels yet".format(len(collect_ignore)),
        RuntimeWarning,
    )
