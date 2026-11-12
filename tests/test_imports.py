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
# an optional dependency is not installed.
_IMPORT_ALL_MODULES = '''
import importlib
import pkgutil
import sys
import traceback

import confluent_kafka

# Never an optional dependency: confluent_kafka itself, typing_extensions (required on
# Python < 3.11, and not needed at runtime on later versions) and, on Python 3.10+, the
# standard library.
NOT_OPTIONAL = {'confluent_kafka', 'typing_extensions', *getattr(sys, 'stdlib_module_names', ())}


def is_namespace_package(name):
    spec = getattr(sys.modules.get(name), '__spec__', None)
    return spec is not None and spec.origin is None and spec.submodule_search_locations is not None


def not_installed(e):
    """Whether the ImportError e only means that an optional dependency is not installed."""
    if e.name is None and isinstance(e.__cause__, ImportError):
        # Re-raised by a dependency, as tink does for its extras.
        return not_installed(e.__cause__)
    if not e.name or e.name.partition('.')[0] in NOT_OPTIONAL:
        return False
    if isinstance(e, ModuleNotFoundError):
        # e.name is the first module of the import that was not found. A top-level module, or
        # a portion of a namespace package such as azure or google.cloud, is installed by a
        # distribution of its own, which is therefore missing. A missing submodule of an
        # installed regular package is an error.
        parent = e.name.rpartition('.')[0]
        return not parent or is_namespace_package(parent)
    # "from package import name" raises a plain ImportError naming the package when name is
    # neither an attribute nor a submodule of it: a portion that is not installed if package
    # is a namespace package, and a missing symbol otherwise.
    return is_namespace_package(e.name)


failed = False
for module in pkgutil.walk_packages(confluent_kafka.__path__, 'confluent_kafka.', onerror=lambda name: None):
    try:
        importlib.import_module(module.name)
    except Exception as e:
        if isinstance(e, ImportError) and not_installed(e):
            continue
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
