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
"""
Prefer httpx2 (httpx's maintained successor, Python >= 3.10), falling back to
httpx. Import httpx from here so we use whichever Authlib uses, avoiding its
deprecation warning and a redundant second dependency.
"""

try:
    import httpx2 as httpx
except ImportError:
    import httpx  # type: ignore[no-redef]  # noqa: F401

Response = httpx.Response
BasicAuth = httpx.BasicAuth

__all__ = ['httpx', 'Response', 'BasicAuth']
