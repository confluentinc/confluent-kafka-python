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
Tests for the JSON codec used by the JSON serializer/deserializer.

The codec prefers orjson but falls back to the stdlib json module when orjson
is unavailable (e.g. on free-threaded CPython builds without orjson wheels).
The fallback branch is otherwise never exercised in environments that ship
orjson, so these tests force it explicitly.
"""

import contextlib
import importlib
import sys

import pytest

from confluent_kafka.schema_registry.common import json_schema


def _orjson_importable() -> bool:
    # Mirror the codec module's own logic: availability is driven by whether
    # `import orjson` actually succeeds, not merely whether a spec is findable
    # (a compiled extension can have a spec yet fail to import/initialize).
    try:
        import orjson  # noqa: F401
    except Exception:
        return False
    return True


_ORJSON_AVAILABLE = _orjson_importable()

# A representative payload: nested structures, every JSON scalar type, key
# ordering to verify it is preserved, and a non-ASCII character.
_SAMPLE = {"b": 1, "a": "é", "n": None, "f": 1.5, "t": True, "list": [1, "x", None]}


@contextlib.contextmanager
def _orjson_disabled():
    """Reload the codec module with orjson forced unavailable.

    Setting ``sys.modules['orjson'] = None`` makes ``import orjson`` raise
    ImportError, so reloading the module rebinds ``_json_loads``/``_json_dumps``
    to the stdlib fallback. Note that ``importlib.reload`` mutates the module
    object in place, so the module-level ``json_schema`` reference is also
    switched to the fallback for the duration of this context. The original
    orjson-backed codec is restored on exit so the rest of the suite is
    unaffected.
    """
    sentinel = object()
    saved = sys.modules.get("orjson", sentinel)
    sys.modules["orjson"] = None  # type: ignore[assignment]  # => ImportError on import
    try:
        importlib.reload(json_schema)
        assert json_schema._HAS_ORJSON is False
        yield json_schema
    finally:
        if saved is sentinel:
            sys.modules.pop("orjson", None)
        else:
            sys.modules["orjson"] = saved
        importlib.reload(json_schema)  # restore the orjson-backed codec


@pytest.fixture
def stdlib_codec():
    with _orjson_disabled() as mod:
        yield mod


def test_has_orjson_reflects_availability():
    assert json_schema._HAS_ORJSON is _ORJSON_AVAILABLE


def test_stdlib_fallback_is_active(stdlib_codec):
    assert stdlib_codec._HAS_ORJSON is False


def test_stdlib_fallback_roundtrip(stdlib_codec):
    encoded = stdlib_codec._json_dumps(_SAMPLE)
    assert isinstance(encoded, str)
    assert stdlib_codec._json_loads(encoded) == _SAMPLE


def test_stdlib_fallback_loads_accepts_str_bytes_bytearray(stdlib_codec):
    raw = '{"x": 1}'
    expected = {"x": 1}
    assert stdlib_codec._json_loads(raw) == expected
    assert stdlib_codec._json_loads(raw.encode("utf-8")) == expected
    assert stdlib_codec._json_loads(bytearray(raw.encode("utf-8"))) == expected


def test_stdlib_fallback_output_is_compact_and_unicode_preserving(stdlib_codec):
    # Compact separators (no whitespace) and non-ASCII left as UTF-8, matching
    # orjson's wire output so serialized bytes stay consistent across backends.
    assert stdlib_codec._json_dumps({"a": 1, "b": 2}) == '{"a":1,"b":2}'
    assert stdlib_codec._json_dumps({"k": "é"}) == '{"k":"é"}'


@pytest.mark.skipif(
    not _ORJSON_AVAILABLE,
    reason="orjson not installed; cannot compare backends",
)
def test_stdlib_and_orjson_produce_identical_output():
    # Capture the orjson output FIRST, while the module is still orjson-backed.
    # importlib.reload (used by _orjson_disabled) mutates json_schema in place,
    # so once the fallback is active there is no live orjson codec to compare
    # against -- the orjson value must be taken before entering the context.
    assert json_schema._HAS_ORJSON is True
    orjson_out = json_schema._json_dumps(_SAMPLE)

    with _orjson_disabled() as stdlib_codec:
        stdlib_out = stdlib_codec._json_dumps(_SAMPLE)

    assert stdlib_out == orjson_out
