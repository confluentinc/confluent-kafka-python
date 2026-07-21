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

"""
Shared test infrastructure for tests/parallel/.

Tests here deliberately race Producer/Consumer methods against each other,
so a real regression can segfault the process instead of just failing an
assertion. subprocess_isolated() below runs a decorated test in a fresh
`python -m pytest` subprocess, so a crash only fails that one test instead
of taking down the whole suite.

This is a plain module, not conftest.py: conftest.py is auto-loaded by
pytest's own plugin machinery and isn't meant to be imported as a regular
module -- doing so (`from tests.parallel.conftest import ...`) is not
guaranteed to resolve the same way from a re-invoked subprocess as it does
under pytest's own collection, and failed with
"ModuleNotFoundError: No module named 'tests.parallel.conftest'" in
exactly that scenario.
"""

import functools
import os
import subprocess
import sys

_SUBPROCESS_MARKER_ENV = "_PARALLEL_TESTS_SUBPROCESS"
_SUBPROCESS_TIMEOUT_SECONDS = 120


def subprocess_isolated(test_func):
    """
    Run `test_func` in a fresh `python -m pytest` subprocess instead of
    in-process. A clean run passes normally; a crash (e.g. segfault) shows
    up as a non-zero/negative subprocess return code, which is turned into
    a normal assertion failure here -- so it fails only this test rather
    than taking down the whole run.
    """

    @functools.wraps(test_func)
    def wrapper(*args, **kwargs):
        if os.environ.get(_SUBPROCESS_MARKER_ENV):
            # Already inside the re-invoked subprocess: run the real body.
            return test_func(*args, **kwargs)

        test_file = sys.modules[test_func.__module__].__file__
        node_id = f"{os.path.relpath(test_file)}::{test_func.__name__}"
        env = dict(os.environ, **{_SUBPROCESS_MARKER_ENV: "1"})

        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", node_id],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
            env=env,
        )
        assert result.returncode == 0, (
            f"{test_func.__name__} crashed/failed in subprocess "
            f"(returncode={result.returncode}):\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    return wrapper
