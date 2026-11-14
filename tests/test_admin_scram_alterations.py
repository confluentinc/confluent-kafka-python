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

"""
Input checks of the C alter_user_scram_credentials().

AdminClient.alter_user_scram_credentials() validates the alterations before
passing them to the C method, so these tests call the C method directly. It
used to ignore the result of reading each attribute: a missing one crashed it
or left an exception set while it carried on. Each case runs in a subprocess
so that a crash fails only that case.
"""

import subprocess
import sys

import pytest

_SETUP = """
import concurrent.futures

from confluent_kafka.admin import (
    AdminClient,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialAlteration,
    UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
)

info = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096)
upsertion = UserScramCredentialUpsertion("user", info, b"password", b"salt")
deletion = UserScramCredentialDeletion("user", ScramMechanism.SCRAM_SHA_256)


class Unclassifiable:
    user = "user"

    @property
    def __class__(self):
        raise RuntimeError("no class")
"""

_CALL = """
admin = AdminClient({"socket.timeout.ms": 10})
try:
    super(AdminClient, admin).alter_user_scram_credentials([alteration], concurrent.futures.Future())
except Exception as e:
    print(f"{type(e).__name__}: {e}")
else:
    print("accepted")
"""

CASES = {
    'valid-upsertion': ("alteration = upsertion", "accepted"),
    'valid-deletion': ("alteration = deletion", "accepted"),
    'no-user': ("alteration = deletion; del alteration.user", "TypeError: Required attribute .user missing"),
    'no-scram_credential_info': (
        "alteration = upsertion; del alteration.scram_credential_info",
        "TypeError: Required attribute .scram_credential_info missing",
    ),
    'no-iterations': (
        "alteration = upsertion; del info.iterations",
        "TypeError: Required attribute .iterations missing",
    ),
    'str-iterations': (
        "alteration = upsertion; info.iterations = '4096'",
        "TypeError: Expected .iterations to be int type, not str",
    ),
    'no-mechanism': (
        "alteration = upsertion; del info.mechanism",
        "TypeError: Required attribute .mechanism missing",
    ),
    'no-password': (
        "alteration = upsertion; del alteration.password",
        "TypeError: Required attribute .password missing",
    ),
    'no-salt': ("alteration = upsertion; del alteration.salt", "TypeError: Required attribute .salt missing"),
    'deletion-no-mechanism': (
        "alteration = deletion; del alteration.mechanism",
        "TypeError: Required attribute .mechanism missing",
    ),
    'neither-upsertion-nor-deletion': (
        "alteration = UserScramCredentialAlteration('user')",
        "TypeError: Alteration 0: should be a UserScramCredentialUpsertion or UserScramCredentialDeletion,"
        " got UserScramCredentialAlteration",
    ),
    'isinstance-error': ("alteration = Unclassifiable()", "RuntimeError: no class"),
}


@pytest.mark.parametrize('case', sorted(CASES))
def test_alter_user_scram_credentials_checks_attributes(case):
    statement, expected = CASES[case]
    result = subprocess.run(
        [sys.executable, '-c', _SETUP + statement + "\n" + _CALL],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, f'returncode={result.returncode}\nstderr:\n{result.stderr}'
    assert result.stdout.strip() == expected
