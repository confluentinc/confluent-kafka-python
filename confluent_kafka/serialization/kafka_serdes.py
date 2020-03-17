#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from .error import SerializationError


class Serializer(object):
    """
    Extensible class from which all Serializer implementations derive.
    Serializers instruct kafka clients on how convert Python objects to bytes.

    Note:
        This class is not directly instantiable. The derived classes must be
        used instead.

    The following implementations are provided by this module.

    Note:
        Unless noted elsewhere all numeric types are signed and big-endian.

    .. list-table::
        :header-rows: 1

        * - Name
          - Type
          - Binary Format
        * - DoubleSerializer
          - float
          - IEEE 764 binary64
        * - ShortSerializer
          - int
          - int16
        * - LongSerializer
          - int
          - int64
        * - IntegerSerializer
          - int
          - int32
        * - StringSerializer
          - unicode
          - unicode(encoding)

    """
    def __call__(self, datum, ctx):
        """
        Converts datum to bytes.

        Args:
            datum (object): object to be serialized
            ctx (SerializationContext): Serialization context

        Raises:
            SerializerError if an error occurs daring serialization

        Returns:
            bytes if datum is not None, otherwise None

        """
        raise NotImplementedError


class Deserializer(object):
    """
    Extensible class from which all Deserializer implementations derive.
    Deserializers instruct kafka clients on how convert bytes to objects.

    Note:
        This class is not directly instantiable. The derived classes must be
        used instead.

    The following implementations are provided by this module.

    Note:
        Unless noted elsewhere all numeric types are signed and big-endian.

    .. list-table::
        :header-rows: 1

        * - Name
          - Type
          - Binary Format
        * - DoubleDeserializer
          - float
          - IEEE 764 binary64
        * - ShortDeserializer
          - int
          - int16
        * - LongDeserializer
          - int
          - int64
        * - IntegerDeserializer
          - int
          - int32
        * - StringDeserializer
          - unicode
          - unicode(encoding)

    """
    def __call__(self, data, ctx):
        """
        Convert bytes to object

        Args:
            data (bytes): bytes to be deserialized
            ctx (SerializationContext): Serialization context

        Raises:
            SerializerError if an error occurs daring deserialization

        Returns:
            object if data is not None, otherwise None

        """
        raise NotImplementedError


class DoubleSerializer(Serializer):
    """
    Serializes float to IEEE 764 binary64.

    .. _DoubleSerializer:
        https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/DoubleSerializer.html

    """  # noqa: E501
    def __call__(self, datum, ctx):
        """
        Serializes float as IEEE 764 binary64 bytes.

        Args:
            datum (float): float to be serialized
            ctx (SerializationContext): Serialization context

        Raises:
            SerializerError if an error occurs daring serialization.

        Returns:
            IEEE 764 binary64 bytes if datum is not None, otherwise None

        """
        if datum is None:
            return None

        try:
            return struct.pack('>d', datum)
        except struct.error as e:
            raise SerializationError(str(e))


class DoubleDeserializer(Deserializer):
    """
    Deserializes float to IEEE 764 binary64.

    .. _DoubleDeserializer:
        https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/DoubleDeserializer.html

    """  # noqa: E501
    def __call__(self, data, ctx):
        """
        Deserializes float from IEEE 764 binary64 bytes.

        Args:
            data (bytes): IEEE 764 binary64 bytes
            ctx (SerializationContext): Serialization context

        Raises:
            SerializerError if an error occurs daring deserialization.

        Returns:
            float if data is not None, otherwise None

        """
        if data is None:
            return None

        try:
            return struct.unpack('>d', data)[0]
        except struct.error as e:
            raise SerializationError(str(e))


class IntegerSerializer(Serializer):
    """
    Serializes int to int32 bytes.

    .. _IntegerSerializer:
        https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/IntegerSerializer.html

    """  # noqa: E501
    def __call__(self, datum, ctx):
        """
        Serializes int as int32 bytes.

        Args:
            datum (int): int to be serialized.
            ctx (SerializationContext): Serialization context

        Raises:
            SerializerError if an error occurs daring serialization

        Returns:
            int32 bytes if datum is not None, else None

        """
        if datum is None:
            return None

        try:
            return struct.pack('>i', datum)
        except struct.error as e:
            raise SerializationError(str(e))


class IntegerDeserializer(Deserializer):
    """
    Deserializes int to int32 bytes.

    .._IntegerDeserializer:
        https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/IntegerDeserializer.html

    """  # noqa: E501
    def __call__(self, data, ctx):
        """
        Deserializes int from int32 bytes.

        Args:
            data(bytes): int32 bytes
            ctx (SerializationContext): Serialization context

        Raises:
            SerializerError if an error occurs daring deserialization.

        Returns:
            int if data is not None, otherwise None

        """
        if data is None:
            return None

        try:
            return struct.unpack('>i', data)[0]
        except struct.error as e:
            raise SerializationError(str(e))


class StringSerializer(Serializer):
    """
    Serializes unicode to bytes per the configured codec. Defaults to ``utf_8``.

    Keyword Args:
        - codec (str, optional): encoding scheme. Defaults to utf_8

    .. _StandardEncodings:
        https://docs.python.org/3/library/codecs.html#standard-encodings

    .. _StringSerializer:
        https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/StringSerializer.html

    """  # noqa: E501
    def __init__(self, codec='utf_8'):
        self.codec = codec

    def __call__(self, datum, ctx):
        """
        Serializes a str(py2:unicode) to bytes.

        Compatibility Note:
            Python 2 str objects must be converted to unicode objects.
            Python 3 all str objects are already unicode objects.

        Args:
            datum (unicode): Unicode object to serialize
            ctx (SerializationContext): Serialization context

        Raises:
            SerializerError if an error occurs daring serialization.

        Returns:
            serialized bytes if datum is not None, otherwise None

        """
        if datum is None:
            return None

        try:
            return datum.encode(self.codec)
        except struct.error as e:
            raise SerializationError(str(e))


class StringDeserializer(Deserializer):
    """
    Deserializes utf-8 bytes to str(py2:unicode) object.

    .. _StringDeserializer:
        https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/StringDeserializer.html

    """  # noqa: E501
    def __init__(self, codec='utf_8'):
        self.codec = codec

    def __call__(self, data, ctx):
        """
        Deserializes utf-8 bytes to str(py2:unicode)

        Compatibility Note:
            Python 2 str objects must be converted to unicode objects.
            Python 3 all str objects are already unicode objects.

        Args:
            data (bytes): bytes to be deserialized
            ctx (SerializationContext): Serialization Context

        Raises:
            SerializerError if an error occurs daring deserialization.

        Returns:
            unicode if data is not None, otherwise None

        """
        if data is None:
            return None

        try:
            return data.decode(self.codec)
        except struct.error as e:
            raise SerializationError(str(e))
