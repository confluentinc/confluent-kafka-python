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

import pytest

FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


if FREE_THREADED_BUILD:
    @pytest.fixture(scope="module", autouse=True)
    def gil_stays_disabled():
        """
        Runs in adaptive GIL mode (PYTHON_GIL unset) so the suite tests the
        configuration users actually run. Module-scoped so the warning points
        at the test file whose imports or tests re-enabled the GIL; the
        interpreter's own RuntimeWarning names the offending extension module.

        TODO FTS: replace the warnings with the commented asserts in the same
        PR that declares Py_MOD_GIL_NOT_USED in cimpl. Until then a re-enable
        is expected (cimpl itself triggers it) and must not fail the suite.
        """
        if sys._is_gil_enabled():
            warnings.warn("the GIL was re-enabled before this test module "
                          "started", RuntimeWarning)
        # assert not sys._is_gil_enabled(), \
        #     "the GIL was re-enabled before this test module started"
        yield
        if sys._is_gil_enabled():
            warnings.warn("the GIL was re-enabled by this test module",
                          RuntimeWarning)
        # assert not sys._is_gil_enabled(), \
        #     "the GIL was re-enabled by this test module"
