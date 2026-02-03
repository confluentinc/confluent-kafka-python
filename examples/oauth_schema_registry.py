#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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

# Examples of setting up Schema Registry with OAuth with static token,
# Client Credentials, and custom functions


# CUSTOM OAuth configuration takes in a custom function, config for that
# function, and returns the fields shown below. All fields must be returned
# for OAuth authentication to work


from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient


def main():
    static_oauth_config = {
        'url': 'https://psrc-123456.us-east-1.aws.confluent.cloud',
        'bearer.auth.credentials.source': 'STATIC_TOKEN',
        'bearer.auth.token': 'static-token',
        'bearer.auth.logical.cluster': 'lsrc-12345',
        'bearer.auth.identity.pool.id': 'pool-abcd',
    }
    static_oauth_sr_client = SchemaRegistryClient(static_oauth_config)
    print(static_oauth_sr_client.get_subjects())

    client_credentials_oauth_config = {
        'url': 'https://psrc-123456.us-east-1.aws.confluent.cloud',
        'bearer.auth.credentials.source': 'OAUTHBEARER',
        'bearer.auth.client.id': 'client-id',
        'bearer.auth.client.secret': 'client-secret',
        'bearer.auth.scope': 'schema_registry',
        'bearer.auth.issuer.endpoint.url': 'https://yourauthprovider.com/v1/token',
        'bearer.auth.logical.cluster': 'lsrc-12345',
        'bearer.auth.identity.pool.id': 'pool-abcd',
    }

    client_credentials_oauth_sr_client = SchemaRegistryClient(client_credentials_oauth_config)
    print(client_credentials_oauth_sr_client.get_subjects())

    def custom_oauth_function(config):
        return config

    custom_config = {
        'bearer.auth.token': 'example-token',
        'bearer.auth.logical.cluster': 'lsrc-12345',
        'bearer.auth.identity.pool.id': 'pool-abcd',
    }

    custom_sr_config = {
        'url': 'https://psrc-123456.us-east-1.aws.confluent.cloud',
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.function': custom_oauth_function,
        'bearer.auth.custom.provider.config': custom_config,
    }

    custom_sr_client = SchemaRegistryClient(custom_sr_config)
    print(custom_sr_client.get_subjects())

    # Example: Using union-of-pools with comma-separated pool IDs
    union_of_pools_config = {
        'url': 'https://psrc-123456.us-east-1.aws.confluent.cloud',
        'bearer.auth.credentials.source': 'STATIC_TOKEN',
        'bearer.auth.token': 'multi-pool-token',
        'bearer.auth.logical.cluster': 'lsrc-12345',
        'bearer.auth.identity.pool.id': 'pool-abc,pool-def,pool-ghi',
    }

    union_sr_client = SchemaRegistryClient(union_of_pools_config)
    print(union_sr_client.get_subjects())

    # Example: Omitting identity pool for auto pool mapping
    auto_pool_config = {
        'url': 'https://psrc-123456.us-east-1.aws.confluent.cloud',
        'bearer.auth.credentials.source': 'OAUTHBEARER',
        'bearer.auth.client.id': 'client-id',
        'bearer.auth.client.secret': 'client-secret',
        'bearer.auth.scope': 'schema_registry',
        'bearer.auth.issuer.endpoint.url': 'https://yourauthprovider.com/v1/token',
        'bearer.auth.logical.cluster': 'lsrc-12345',
        # bearer.auth.identity.pool.id is omitted - SR will use auto pool mapping
    }

    auto_pool_sr_client = SchemaRegistryClient(auto_pool_config)
    print(auto_pool_sr_client.get_subjects())


if __name__ == '__main__':
    main()
