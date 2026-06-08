#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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

from unittest import mock

import pytest

from confluent_kafka.schema_registry.common import json_schema


# Numeric hosts so resolution needs no network; schemes other than http(s)
# are rejected before any lookup.
@pytest.mark.parametrize("uri", [
    "http://169.254.169.254/latest/meta-data/",
    "http://127.0.0.1:8080/internal",
    "http://10.0.0.5/schema",
    "http://[::1]/schema",
    "file:///etc/passwd",
    "gopher://127.0.0.1/x",
])
def test_retrieve_via_httpx_refuses_non_public(uri):
    with mock.patch.object(json_schema.httpx, "get") as get:
        with pytest.raises(ValueError):
            json_schema._retrieve_via_httpx(uri)
        get.assert_not_called()


def test_retrieve_via_httpx_allows_public_host():
    response = mock.Mock()
    response.json.return_value = {"type": "object"}

    def gai(host, port, *args, **kwargs):
        return [(2, 1, 6, "", ("93.184.216.34", port))]

    with mock.patch.object(json_schema.socket, "getaddrinfo", side_effect=gai), \
            mock.patch.object(json_schema.httpx, "get", return_value=response) as get:
        json_schema._retrieve_via_httpx("http://schemas.example.com/draft-07/schema")
        get.assert_called_once()
