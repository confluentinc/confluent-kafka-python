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
import sysconfig
import warnings

# test_dlq.py imports celpy (via CelExecutor) at the top of the file. celpy
# ships no free-threaded wheel, so it is not installed on free-threaded
# builds (see requirements-tests-install-nogil.txt) and importing this
# module would fail at collection; exclude it there. On regular builds the
# dep is expected to be installed, so a missing dep stays a loud collection
# error instead of a silent skip.
FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

collect_ignore = []
if FREE_THREADED_BUILD:
    collect_ignore = [
        "_async/test_dlq.py",
        "_sync/test_dlq.py",
    ]
    warnings.warn(
        "free-threaded build: skipping collection of {} schema_registry "
        "integration test modules requiring celpy, which ships no "
        "free-threaded wheel".format(len(collect_ignore)),
        RuntimeWarning,
    )
