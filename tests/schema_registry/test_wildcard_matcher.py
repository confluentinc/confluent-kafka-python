#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 20244Confluent Inc.
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


def test_wildcard_matcher():
    from confluent_kafka.schema_registry.wildcard_matcher import wildcard_match

    assert not wildcard_match("", "Foo")
    assert not wildcard_match("Foo", "")
    assert wildcard_match("", "")
    assert wildcard_match("Foo", "Foo")
    assert wildcard_match("", "*")
    assert not wildcard_match("", "?")
    assert wildcard_match("Foo", "Fo*")
    assert wildcard_match("Foo", "Fo?")
    assert wildcard_match("Foo Bar and Catflag", "Fo*")
    assert wildcard_match("New Bookmarks", "N?w ?o?k??r?s")
    assert not wildcard_match("Foo", "Bar")
    assert wildcard_match("Foo Bar Foo", "F*o Bar*")
    assert wildcard_match("Adobe Acrobat Installer", "Ad*er")
    assert wildcard_match("Foo", "*Foo")
    assert wildcard_match("BarFoo", "*Foo")
    assert wildcard_match("Foo", "Foo*")
    assert wildcard_match("FooBar", "Foo*")
    assert not wildcard_match("FOO", "*Foo")
    assert not wildcard_match("BARFOO", "*Foo")
    assert not wildcard_match("FOO", "Foo*")
    assert not wildcard_match("FOOBAR", "Foo*")
    assert wildcard_match("eve", "eve*")
    assert wildcard_match("alice.bob.eve", "a*.bob.eve")
    assert not wildcard_match("alice.bob.eve", "a*")
    assert wildcard_match("alice.bob.eve", "a**")
    assert not wildcard_match("alice.bob.eve", "alice.bob*")
    assert wildcard_match("alice.bob.eve", "alice.bob**")