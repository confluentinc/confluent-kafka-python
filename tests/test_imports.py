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

import subprocess
import sys

# Imports every confluent_kafka submodule, prints the names of those imported and a
# traceback for each one that fails. A module is skipped only when its import fails because
# an optional dependency is not installed: a ModuleNotFoundError for a top-level package
# that cannot be found. A symbol missing from an installed dependency, or a missing
# submodule of an installed package, still fails.
_IMPORT_ALL_MODULES = '''
import importlib
import importlib.util
import pkgutil
import sys
import traceback

import confluent_kafka

failed = False
for module in pkgutil.walk_packages(confluent_kafka.__path__, 'confluent_kafka.', onerror=lambda name: None):
    try:
        importlib.import_module(module.name)
    except ModuleNotFoundError as e:
        top = e.name.partition('.')[0] if e.name else None
        if top and top != 'confluent_kafka' and importlib.util.find_spec(top) is None:
            continue
        failed = True
        traceback.print_exc()
    except ImportError:
        failed = True
        traceback.print_exc()
    except Exception:
        failed = True
        traceback.print_exc()
    else:
        print(module.name)
sys.exit(int(failed))
'''


def test_import_all_modules():
    """Every submodule can be imported.

    Runs in a fresh interpreter, so the result does not depend on what other tests have
    already imported.
    """
    result = subprocess.run([sys.executable, '-c', _IMPORT_ALL_MODULES], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    # confluent_kafka.aio has no optional dependencies, so it must not have been skipped.
    assert 'confluent_kafka.aio' in result.stdout.splitlines()
