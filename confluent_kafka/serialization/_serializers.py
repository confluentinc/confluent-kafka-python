#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2020 Confluent Inc.
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

import struct

from sys import version_info as interpreter_version


def _py3_str_serializer(value):
    """
    Converts Python String to UTF-8 encoded bytes.

    :param str value: string to be encoded
    :returns: utf-8 encoded byte array
    :rtype: bytes
    """
    return value.encode('utf-8')


def _py3_str_deserailizer(data):
    """
    Converts UTF-8 encoded bytes to a string.

    :param bytes data: UTF-8 encoded string
    :returns: decoded string
    :rtype: str
    """
    return str(data, 'utf-8')


def _py2_str_serializer(value):
    """
    Python 2 strings are synonymous with bytes.

    Coercing encoding on non-ascii characters here will result in an exception.
    Although character  encoding can be controlled by setting the source encoding
    this has no effect on the behavior of bytes.encode().

    See PEP-263 on setting the encoding.

    :param str value: string to be encoded
    :returns: encoded string
    :rtype: bytes
    """
    return value


def _py2_str_deserializer(data):
    """
    Decodes utf-8 encoded stream.

    See PEP-263 on setting the encoding.

    :param bytes data: utf-8 encoded string
    :returns: decoded string
    :rtype: str
    """
    return data.decode('utf-8')


if interpreter_version >= (3, 4):
    string_serializer = _py3_str_serializer
    string_deserializer = _py3_str_deserailizer
else:
    string_serializer = _py2_str_serializer
    string_deserializer = _py2_str_deserializer


def double_serializer(value):
    """
    Convert Python float to C type double bytes.

    :param float value: float to be encoded
    :returns: C type double bytes
    :rtype: bytes
    """
    return struct.pack('>d', value)


def double_deserializer(data):
    """
    Convert C type double to Python float.

    :param bytes data: C type double bytes
    :returns: decoded float
    :rtype: float
    """
    return struct.unpack('>d', data)[0]


def long_serializer(value):
    """
    Convert Python integer to C type long long bytes.

    :param int value: integer to be encoded
    :returns: C type long long bytes
    :rtype: bytes
    """
    return struct.pack('>q', value)


def long_deserializer(data):
    """
    Convert C type long long bytes to Python integer.

    :param bytes data: C type long long bytes
    :returns: decoded integer
    :rtype: integer
    """
    return struct.unpack('>q', data)[0]


def float_serializer(value):
    """
    Convert a Python float to C type float bytes.

    :param float value: float to be encoded
    :returns: C type float bytes
    :rtype: bytes
    """
    return struct.pack('>f', value)


def float_deserializer(data):
    """
    Convert C type float bytes to Python float.

    :param bytes data: C type float bytes
    :returns: decoded float
    :rtype: float
    """
    return struct.unpack('>f', data)[0]


def int_serializer(value):
    """
    Converts Python integer to C type int bytes.

    :param int value: integer to be encoded
    :returns: C type int bytes
    :rtype: bytes
    """
    return struct.pack('>i', value)


def int_deserializer(data):
    """
    Convert C type int bytes to Python integer.

    :param bytes data: C type int bytes
    :returns: decoded integer
    :rtype: int
    """
    return struct.unpack('>i', data)[0]


def short_serializer(value):
    """
    Converts Python integer to C type short bytes.

    :param int value: integer to be encoded
    :returns: C type short bytes
    :rtype: bytes
    """
    return struct.pack(">h", value)


def short_deserializer(data):
    """
    Converts C type short to Python integer.

    :param bytes data: C short bytes
    :returns: decoded integer
    :rtype: int
    """
    return struct.unpack(">h", data)[0]
