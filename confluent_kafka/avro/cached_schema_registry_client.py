#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Confluent Inc.
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

import warnings

# Python 2 considers int an instance of str
try:
    string_type = basestring  # noqa
except NameError:
    string_type = str

from confluent_kafka.schema_registry.cached_schema_registry_client import CachedSchemaRegistryClient as _RegistryClient

__all__ = ['CachedSchemaRegistryClient']


warnings.warn(
    "CachedSchemaRegistryClient has been repackaged under confluent_kafka.schema_registry."
    "This package will be removed in a future version",
    category=DeprecationWarning, stacklevel=2)


class CachedSchemaRegistryClient(object):
    """
    CachedSchemaRegistryClient shim to maintain compatibility during transitional period.
    """
    def __new__(cls, url, max_schemas_per_subject=1000, ca_location=None, cert_location=None, key_location=None):
        conf = url
        if not isinstance(url, dict):
            conf = {
                'url': url,
                'ssl.ca.location': ca_location,
                'ssl.certificate.location': cert_location,
                'ssl.key.location': key_location
            }
            warnings.warn(
                "CachedSchemaRegistry constructor is being deprecated. "
                "Use CachedSchemaRegistryClient(dict: config) instead. "
                "Existing params ca_location, cert_location and key_location will be replaced with their "
                "librdkafka equivalents as keys in the conf dict: `ssl.ca.location`, `ssl.certificate.location` and "
                "`ssl.key.location` respectively",
                category=DeprecationWarning, stacklevel=2)
        url = conf.get('url', '')
        if not isinstance(url, string_type):
            raise TypeError("URL must be of type str")
        return _RegistryClient(conf)
