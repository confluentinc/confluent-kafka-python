# -*- coding: utf-8 -*-
# Copyright 2023 Confluent Inc.
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

import pytest
import concurrent
from confluent_kafka.admin import UserScramCredentialsDescription, UserScramCredentialUpsertion, \
                                  UserScramCredentialDeletion, ScramCredentialInfo, \
                                  ScramMechanism
from confluent_kafka.error import KafkaException, KafkaError


def test_user_scram_credentials(kafka_cluster):
    """
    Tests for the alter and describe SASL/SCRAM credential operations.
    """

    admin_client = kafka_cluster.admin()

    newuser = "non-existent"
    mechanism = ScramMechanism.SCRAM_SHA_256
    iterations = 10000
    password = b"password"
    salt = b"salt"

    futmap = admin_client.describe_user_scram_credentials([newuser])
    assert isinstance(futmap, dict)
    assert len(futmap) == 1
    assert newuser in futmap
    fut = futmap[newuser]
    with pytest.raises(KafkaException) as ex:
        result = fut.result()
    assert ex.value.args[0] == KafkaError.RESOURCE_NOT_FOUND

    # Insert new user with SCRAM_SHA_256: 10000
    futmap = admin_client.alter_user_scram_credentials([UserScramCredentialUpsertion(newuser,
                                                        ScramCredentialInfo(mechanism, iterations),
                                                        password, salt)])
    fut = futmap[newuser]
    result = fut.result()
    assert result is None

    # Try upsertion for newuser,SCRAM_SHA_256 and add newuser,SCRAM_SHA_512
    futmap = admin_client.alter_user_scram_credentials([UserScramCredentialUpsertion(
                                                            newuser,
                                                            ScramCredentialInfo(
                                                                mechanism, iterations),
                                                            password, salt),
                                                        UserScramCredentialUpsertion(
                                                            newuser,
                                                            ScramCredentialInfo(
                                                                ScramMechanism.SCRAM_SHA_512, 10000),
                                                            password)
                                                        ])
    fut = futmap[newuser]
    result = fut.result()
    assert result is None

    # Delete newuser,SCRAM_SHA_512
    futmap = admin_client.alter_user_scram_credentials([UserScramCredentialDeletion(
                                                            newuser,
                                                            ScramMechanism.SCRAM_SHA_512)
                                                        ])
    fut = futmap[newuser]
    result = fut.result()
    assert result is None

    # Describe all users
    for args in [[], [None]]:
        f = admin_client.describe_user_scram_credentials(*args)
        assert isinstance(f, concurrent.futures.Future)
        results = f.result()
        assert newuser in results
        description = results[newuser]
        assert isinstance(description, UserScramCredentialsDescription)
        for scram_credential_info in description.scram_credential_infos:
            assert ((scram_credential_info.mechanism == mechanism) and
                    (scram_credential_info.iterations == iterations))

    # Describe specific user
    futmap = admin_client.describe_user_scram_credentials([newuser])
    assert isinstance(futmap, dict)
    assert len(futmap) == 1
    assert newuser in futmap
    description = futmap[newuser].result()
    assert isinstance(description, UserScramCredentialsDescription)
    for scram_credential_info in description.scram_credential_infos:
        assert ((scram_credential_info.mechanism == mechanism) and
                (scram_credential_info.iterations == iterations))

    # Delete newuser
    futmap = admin_client.alter_user_scram_credentials([UserScramCredentialDeletion(newuser, mechanism)])
    assert isinstance(futmap, dict)
    assert len(futmap) == 1
    assert newuser in futmap
    fut = futmap[newuser]
    result = fut.result()
    assert result is None

    # newuser isn't found
    futmap = admin_client.describe_user_scram_credentials([newuser])
    assert isinstance(futmap, dict)
    assert len(futmap) == 1
    assert newuser in futmap
    fut = futmap[newuser]
    with pytest.raises(KafkaException) as ex:
        result = fut.result()
    assert ex.value.args[0] == KafkaError.RESOURCE_NOT_FOUND
