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

from .. import cimpl

from enum import Enum


class ScramMechanism(Enum):
    UNKNOWN = cimpl.SCRAM_MECHANISM_UNKNOWN
    SCRAM_SHA_256 = cimpl.SCRAM_MECHANISM_SHA_256
    SCRAM_SHA_512 = cimpl.SCRAM_MECHANISM_SHA_512

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value


class ScramCredentialInfo:
    def __init__(self, mechanism: ScramMechanism, iterations: int):
        self.mechanism = mechanism
        self.iterations = iterations


class UserScramCredentialsDescription:
    def __init__(self, user: str, scram_credential_infos: list):
        self.user = user
        self.scram_credential_infos = scram_credential_infos


class UserScramCredentialAlteration:
    def __init__(self, user: str):
        self.user = user


class UserScramCredentialUpsertion(UserScramCredentialAlteration):
    def __init__(self, user: str, scram_credential_info: ScramCredentialInfo, salt: bytes, password: bytes):
        super(UserScramCredentialUpsertion, self).__init__(user)
        self.scram_credential_info = scram_credential_info
        self.salt = salt
        self.password = password


class UserScramCredentialDeletion(UserScramCredentialAlteration):
    def __init__(self, user: str, mechanism: ScramMechanism):
        super(UserScramCredentialDeletion, self).__init__(user)
        self.mechanism = mechanism
