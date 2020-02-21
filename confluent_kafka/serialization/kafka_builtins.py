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

from .error import SerializerError


class DoubleSerializer(object):
    __slots__ = []

    def __init__(self):
        """
        Apache Kafka conformant serializer/deserializer.

        See the javadocs for additional details:
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/DoubleSerializer.html
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/DoubleDeserializer.html
        """
        pass

    @staticmethod
    def serialize(datum, ctx):
        """
        Convert float to C type double bytes.

        :param float datum: float to be encoded
        :param SerializationContext ctx: N/A

        :raises: SerializerError

        :returns: C type double bytes
        :rtype: bytes
        """

        if datum is None:
            return None

        try:
            return struct.pack('>d', datum)
        except struct.error as e:
            raise SerializerError(e)

    @staticmethod
    def deserialize(data, ctx):
        """
        Convert C type double to float.

        :param bytes data: bytes to be decoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :returns: decoded float
        :rtype: float
        """

        if data is None:
            return None

        try:
            return struct.unpack('>d', data)[0]
        except struct.error as e:
            raise SerializerError(e)


class FloatSerializer(object):
    __slots__ = []

    def __init__(self):
        """
        Apache Kafka conformant serializer/deserializer.

        See the javadocs for additional  details:
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/FloatSerializer.html
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/FloatDeserializer.html
        """
        pass

    @staticmethod
    def serialize(datum, ctx):
        """
        Convert a float to C type float bytes.

        Note: Internally python stores as double precision floats. Java however
        stores single-point floats. As a result the approximation encoded/decoded
        by python will be slightly different than java's.

        reference:
            https://docs.python.org/2/tutorial/floatingpoint.html

        :param float datum: float to be encoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :return: C type float bytes
        :rtype: bytes
        """

        if datum is None:
            return None

        try:
            return struct.pack('>f', datum)
        except struct.error as e:
            raise SerializerError(e)

    @staticmethod
    def deserialize(data, ctx):
        """
        Convert C type float bytes to float

        Note: Internally python stores as double precision floats. Java however
        stores single-point floats. As a result the approximation encoded/decoded
        by python will be slightly different than java's.

        reference:
            https://docs.python.org/2/tutorial/floatingpoint.html

        :param bytes data: data to be decoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :returns: decoded float
        :rtype: float
        """

        if data is None:
            return None

        try:
            return struct.unpack('>f', data)[0]
        except struct.error as e:
            raise SerializerError(e)


class LongSerializer(object):
    __slots__ = []

    def init__(self):
        """
        Apache Kafka conformant serializer/deserializer.

        See the javadocs for additional details:
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/LongSerializer.html
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/LongDeserializer.html
        """
        pass

    @staticmethod
    def serialize(datum, ctx):
        """
        Convert int to C type long long

        :param int datum: int to be encoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :return: C type long long bytes
        :rtype: bytes
        """
        if datum is None:
            return None

        try:
            return struct.pack('>q', datum)
        except struct.error as e:
            raise SerializerError(e)

    @staticmethod
    def deserialize(data, ctx):
        """
        Convert C type long long bytes to int

        :param bytes data: bytes to be decoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :return: decoded int
        :rtype: int
        """

        if data is None:
            return None

        try:
            return struct.unpack('>q', data)[0]
        except struct.error as e:
            raise SerializerError(e)


class IntegerSerializer(object):
    __slots__ = []

    def __init__(self):
        """
        Apache Kafka conformant serializer/deserializer.

        See the javadocs for additional  details:
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/IntegerSerializer.html
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/IntegerDeserializer.html
        """
        pass

    @staticmethod
    def serialize(datum, ctx):
        """
        Converts int to C type int bytes.

        :param int datum: int to be encoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :return: C type int bytes
        :rtype: bytes
        """
        if datum is None:
            return None

        try:
            return struct.pack('>i', datum)
        except struct.error as e:
            raise SerializerError(e)

    @staticmethod
    def deserialize(data, ctx):
        """
        Convert C type int bytes to int

        :param bytes data: data to be decoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :returns: decoded int
        :rtype: int
        """

        if data is None:
            return None

        try:
            return struct.unpack('>i', data)[0]
        except struct.error as e:
            raise SerializerError(e)


class ShortSerializer(object):
    __slots__ = []

    def __init__(self):
        """
        Apache Kafka conformant serializer/deserializer.

        See the javadocs for additional  details:
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/ShortSerializer.html
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/ShortDeserializer.html
        """
        pass

    @staticmethod
    def serialize(datum, ctx):
        """
        Converts int to C type short bytes.

        :param int datum: int to be encoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :return: C type int bytes
        :rtype: bytes
        """

        if datum is None:
            return None

        try:
            return struct.pack('>h', datum)
        except struct.error as e:
            raise SerializerError(e)

    @staticmethod
    def deserialize(data, ctx):
        """
        Convert C type short bytes to int

        :param bytes data: data to be decoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :returns: decoded int
        :rtype: int
        """

        if data is None:
            return None

        try:
            return struct.unpack('>h', data)[0]
        except struct.error as e:
            raise SerializerError(e)


class StringSerializer(object):
    __slots__ = ['codec']

    def __init__(self, codec='utf_8'):
        """
        Apache Kafka conformant serializer/deserializer.

        See the javadocs for additional  details:
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/StringSerializer.html
            https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/StringDeserializer.html

        :param str codec: encoding scheme. See
            https://docs.python.org/3/library/codecs.html#standard-encodings for
            a list of applicable values

        """
        self.codec = codec

    def serialize(self, datum, ctx):
        """
        Encode str to bytes.

        Compatibility Note:
            Python 2 str objects must be converted to unicode objects.
            Python 3 all str objects are already unicode objects

        :param unicode(py2)|str datum: unicode object to decode
        :param SerializationContext ctx: N/A

        :raises: SerializerError

        :returns: encoded string
        :rtype: bytes
        """

        if datum is None:
            return None

        try:
            return datum.encode(self.codec)
        except struct.error as e:
            raise SerializerError(e)

    def deserialize(self, data, ctx):
        """
        Decode bytes to str

        :param bytes data: data to be decoded
        :param SerializationContext ctx:

        :raises: SerializerError

        :returns: decoded string
        :rtype: str
        """

        if data is None:
            return None

        try:
            return data.decode(self.codec)
        except struct.error as e:
            raise SerializerError(e)
