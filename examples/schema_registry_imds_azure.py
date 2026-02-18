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

# Examples of setting up Schema Registry with OAuth with static token,
# Client Credentials, Azure IMDS, and custom functions


# CUSTOM OAuth configuration takes in a custom function, config for that
# function, and returns the fields shown below. All fields must be returned
# for OAuth authentication to work


from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient


def main():
    BOOTSTRAP = "api://bootstrapid"
    UAMI_CLIENT_ID = "uamiid"

    endpoint_query = f"resource=${BOOTSTRAP}&client_id=${UAMI_CLIENT_ID}"

    azure_imds_oauth_config = {
        'url': 'https://psrc-123456.us-east-1.aws.confluent.cloud',
        'bearer.auth.credentials.source': 'OAUTHBEARER_AZURE_IMDS',
        'bearer.auth.issuer.endpoint.query': endpoint_query,
        'bearer.auth.logical.cluster': 'lsrc-12345',
        'bearer.auth.identity.pool.id': 'pool-abcd',
    }

    azure_imds_oauth_sr_client = SchemaRegistryClient(azure_imds_oauth_config)
    print(azure_imds_oauth_sr_client.get_subjects())


if __name__ == '__main__':
    main()
