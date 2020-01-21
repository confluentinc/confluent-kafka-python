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

from confluent_kafka.serialization._serializers import double_deserializer, float_deserializer, \
    long_deserializer, int_deserializer, short_deserializer, string_deserializer, \
    double_serializer, float_serializer, long_serializer, int_serializer, short_serializer, string_serializer

__all__ = [double_deserializer, float_deserializer,
           long_deserializer, int_deserializer, short_deserializer, string_deserializer,
           double_serializer, float_serializer,
           long_serializer, int_serializer, short_serializer, string_serializer]


"""
Collection of built-in serializers designed to be compatible with org/apache/kafka/common/serialization/Serializer.

**Note** Strings are a bit tricky in Python 2 and require special handling to get right as they are already stored
internally as bytes. How those bytes are stored is contingent upon your source file encoding (PEP-263). This puts the
library at a bit of a disadvantage it is unable to coerce the interpreter to represent strings in the encoding of it's
choosing; UTF-8 in this case. Keeping this limitation in mind we recommend that all Python 2 targeted implementations
set their source encoding to `UTF-8`.

This can be achieved by adding the following header to the application's source files.

```
#!/usr/bin/env python
# -*- coding: utf-8 -*-
....
```

JavaDocs for reference.
Serializers:

DoubleSerializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/DoubleSerializer.html
FloatSerializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/FloatSerializer.html
LongSerializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/LongSerializer.html
IntegerSerializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/IntegerSerializer.html
ShortSerializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/ShortSerializer.html
StringSerializer https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/StringSerializer.html

Deserializers:

DoubleSerializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/DoubleDeserializer.html
FloatDeserializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/FloatDeserializer.html
LongDeserializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/LongDeserializer.html
IntegerDeserializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/IntegerDeserializer.html
ShortDeserializer: https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/ShortDeserializer.html
StringDeserializer https://kafka.apache.org/20/javadoc/org/apache/kafka/common/serialization/StringDeserializer.html
"""
