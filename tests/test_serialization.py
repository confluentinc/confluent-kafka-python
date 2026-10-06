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

import pytest

from confluent_kafka.serialization import SerializationError, StringDeserializer, StringSerializer


@pytest.mark.parametrize(
    "codec, value",
    [
        ("ascii", "caf\u00e9"),
        ("latin_1", "\u20ac"),
        ("utf_8", "\ud800"),
        ("utf_16", "\udc00"),
        ("idna", "a" * 64),
    ],
)
def test_string_serializer_wraps_unicode_errors(codec, value):
    with pytest.raises(UnicodeError) as original:
        value.encode(codec)

    with pytest.raises(SerializationError) as wrapped:
        StringSerializer(codec)(value)

    assert str(wrapped.value) == str(original.value)
    assert type(wrapped.value.__context__) is type(original.value)


@pytest.mark.parametrize(
    "codec, value",
    [
        ("utf_8", b"\xff"),
        ("utf_8", b"\xe2\x82"),
        ("ascii", b"\x80"),
        ("utf_16_le", b"a"),
        ("utf_32_le", b"\x00\xd8\x00\x00"),
        ("idna", b"xn--"),
    ],
)
def test_string_deserializer_wraps_unicode_errors(codec, value):
    with pytest.raises(UnicodeError) as original:
        value.decode(codec)

    with pytest.raises(SerializationError) as wrapped:
        StringDeserializer(codec)(value)

    assert str(wrapped.value) == str(original.value)
    assert type(wrapped.value.__context__) is type(original.value)


@pytest.mark.parametrize(
    "codec, value",
    [
        ("utf_8", None),
        ("utf_8", ""),
        ("utf_8", "hello"),
        ("utf_8", "caf\u00e9\x00"),
        ("ascii", "hello"),
        ("latin_1", "caf\u00e9"),
        ("utf_16", ""),
        ("utf_16", "hello\u4e16\u754c"),
        ("utf_32", "hello\u4e16\u754c"),
    ],
)
def test_string_serialization_round_trip(codec, value):
    encoded = StringSerializer(codec)(value)

    assert encoded == (None if value is None else value.encode(codec))
    assert StringDeserializer(codec)(encoded) == value


@pytest.mark.parametrize(
    "serde, value",
    [
        (StringSerializer("unknown_codec"), "hello"),
        (StringDeserializer("unknown_codec"), b"hello"),
    ],
)
def test_string_serialization_does_not_wrap_unknown_codec(serde, value):
    with pytest.raises(LookupError):
        serde(value)
