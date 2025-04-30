# Copyright 2024 Confluent Inc.
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

import base64
import logging
import time
from typing import Optional, Tuple, Any

from tink import aead, daead, KmsClient, kms_client_from_uri, \
    register_kms_client, TinkError
from tink.core import Registry
from tink.proto import tink_pb2, aes_siv_pb2

from confluent_kafka.schema_registry import SchemaRegistryError, RuleMode, \
    _MAGIC_BYTE_V0
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.rules.encryption.dek_registry.dek_registry_client import \
    DekRegistryClient, Kek, KekId, DekId, Dek, DekAlgorithm
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import \
    get_kms_driver, KmsDriver
from confluent_kafka.schema_registry.serde import RuleContext, \
    FieldRuleExecutor, FieldTransform, RuleError, FieldContext, FieldType


log = logging.getLogger(__name__)


aead.register()
daead.register()

ENCRYPT_KEK_NAME = "encrypt.kek.name"
ENCRYPT_KMS_KEY_ID = "encrypt.kms.key.id"
ENCRYPT_KMS_TYPE = "encrypt.kms.type"
ENCRYPT_DEK_ALGORITHM = "encrypt.dek.algorithm"
ENCRYPT_DEK_EXPIRY_DAYS = "encrypt.dek.expiry.days"

MILLIS_IN_DAY = 24 * 60 * 60 * 1000


class Clock(object):
    def now(self) -> int:
        return int(round(time.time() * 1000))


class FieldEncryptionExecutor(FieldRuleExecutor):

    def __init__(self, clock: Clock = Clock()):
        self.client = None
        self.config = None
        self.clock = clock

    def configure(self, client_conf: dict, rule_conf: dict):
        if client_conf:
            if self.client:
                if self.client.config() != client_conf:
                    raise RuleError("executor already configured")
            else:
                self.client = DekRegistryClient.new_client(client_conf)

        if self.config:
            if rule_conf:
                for key, value in rule_conf.items():
                    v = self.config.get(key)
                    if v is not None:
                        if v != value:
                            raise RuleError(f"rule config key already set: {key}")
                    else:
                        self.config[key] = value
        else:
            self.config = rule_conf if rule_conf else {}

    def type(self) -> str:
        return "ENCRYPT"

    def new_transform(self, ctx: RuleContext) -> FieldTransform:
        cryptor = self._get_cryptor(ctx)
        kek_name = self._get_kek_name(ctx)
        dek_expiry_days = self._get_dek_expiry_days(ctx)
        transform = FieldEncryptionExecutorTransform(
            self, cryptor, kek_name, dek_expiry_days)
        return transform.transform

    def close(self):
        if self.client is not None:
            self.client.__exit__()

    def _get_cryptor(self, ctx: RuleContext) -> 'Cryptor':
        dek_algorithm = DekAlgorithm.AES256_GCM
        dek_algorithm_str = ctx.get_parameter(ENCRYPT_DEK_ALGORITHM)
        if dek_algorithm_str is not None:
            dek_algorithm = DekAlgorithm[dek_algorithm_str]
        cryptor = Cryptor(dek_algorithm)
        return cryptor

    def _get_kek_name(self, ctx: RuleContext) -> str:
        kek_name = ctx.get_parameter(ENCRYPT_KEK_NAME)
        if kek_name is None:
            raise RuleError("no kek name found")
        if kek_name == "":
            raise RuleError("empty kek name")
        return kek_name

    def _get_dek_expiry_days(self, ctx: RuleContext) -> int:
        dek_expiry_days_str = ctx.get_parameter(ENCRYPT_DEK_EXPIRY_DAYS)
        if dek_expiry_days_str is None:
            return 0
        try:
            dek_expiry_days = int(dek_expiry_days_str)
        except ValueError:
            raise RuleError("invalid expiry days")
        if dek_expiry_days < 0:
            raise RuleError("negative expiry days")
        return dek_expiry_days

    @classmethod
    def register(cls):
        RuleRegistry.register_rule_executor(FieldEncryptionExecutor())

    @classmethod
    def register_with_clock(cls, clock: Clock) -> 'FieldEncryptionExecutor':
        executor = FieldEncryptionExecutor(clock)
        RuleRegistry.register_rule_executor(executor)
        return executor


class Cryptor:
    EMPTY_AAD = b""

    def __init__(self, dek_format: DekAlgorithm):
        self.dek_format = dek_format
        self.is_deterministic = dek_format == DekAlgorithm.AES256_SIV
        self.registry = Registry()

        if dek_format is DekAlgorithm.AES128_GCM:
            self.key_template = aead.aead_key_templates.AES128_GCM_RAW
        elif dek_format is DekAlgorithm.AES256_GCM:
            self.key_template = aead.aead_key_templates.AES256_GCM_RAW
        elif dek_format is DekAlgorithm.AES256_SIV:
            # Construct AES256_SIV_RAW since it doesn't exist in Tink
            key_format = aes_siv_pb2.AesSivKeyFormat(
                # Generate 2 256-bit keys
                key_size=64,
            )
            self.key_template = tink_pb2.KeyTemplate(
                type_url=daead.deterministic_aead_key_templates.AES256_SIV.type_url,
                output_prefix_type=tink_pb2.RAW,
                value=key_format.SerializeToString(),
            )
        else:
            raise RuleError("invalid dek algorithm")

    def generate_key(self) -> bytes:
        key_data = self.registry.new_key_data(self.key_template)
        return key_data.value

    def encrypt(self, dek: bytes, plaintext: bytes, associated_data: bytes) -> bytes:
        key_data = tink_pb2.KeyData(
            type_url=self.key_template.type_url,
            value=dek,
            key_material_type=tink_pb2.KeyData.SYMMETRIC
        )
        if self.is_deterministic:
            primitive = self.registry.primitive(key_data, daead.DeterministicAead)
            return primitive.encrypt_deterministically(plaintext, associated_data)
        else:
            primitive = self.registry.primitive(key_data, aead.Aead)
            return primitive.encrypt(plaintext, associated_data)

    def decrypt(self, dek: bytes, ciphertext: bytes, associated_data: bytes) -> bytes:
        key_data = tink_pb2.KeyData(
            type_url=self.key_template.type_url,
            value=dek,
            key_material_type=tink_pb2.KeyData.SYMMETRIC
        )
        if self.is_deterministic:
            primitive = self.registry.primitive(key_data, daead.DeterministicAead)
            return primitive.decrypt_deterministically(ciphertext, associated_data)
        else:
            primitive = self.registry.primitive(key_data, aead.Aead)
            return primitive.decrypt(ciphertext, associated_data)


class FieldEncryptionExecutorTransform(object):

    def __init__(self, executor: FieldEncryptionExecutor, cryptor: Cryptor, kek_name: str, dek_expiry_days: int):
        self._executor = executor
        self._cryptor = cryptor
        self._kek_name = kek_name
        self._kek = None
        self._dek_expiry_days = dek_expiry_days

    def _is_dek_rotated(self):
        return self._dek_expiry_days > 0

    def _get_kek(self, ctx: RuleContext) -> Kek:
        if self._kek is None:
            self._kek = self._get_or_create_kek(ctx)
        return self._kek

    def _get_or_create_kek(self, ctx: RuleContext) -> Kek:
        is_read = ctx.rule_mode == RuleMode.READ
        kms_type = ctx.get_parameter(ENCRYPT_KMS_TYPE)
        kms_key_id = ctx.get_parameter(ENCRYPT_KMS_KEY_ID)
        kek_id = KekId(self._kek_name, False)
        kek = self._retrieve_kek_from_registry(kek_id)
        if kek is None:
            if is_read:
                raise RuleError(f"no kek found for {self._kek_name} during consume")
            if not kms_type:
                raise RuleError(f"no kms type found for {self._kek_name} during produce")
            if not kms_key_id:
                raise RuleError(f"no kms key id found for {self._kek_name} during produce")
            kek = self._store_kek_to_registry(kek_id, kms_type, kms_key_id, False)
            if kek is None:
                # handle conflicts (409)
                kek = self._retrieve_kek_from_registry(kek_id)
            if kek is None:
                raise RuleError(f"no kek found for {self._kek_name} during produce")
        if kms_type and kek.kms_type != kms_type:
            raise RuleError(f"found {self._kek_name} with kms type {kek.kms_type} "
                            f"which differs from rule kms type {kms_type}")
        if kms_key_id and kek.kms_key_id != kms_key_id:
            raise RuleError(f"found {self._kek_name} with kms key id {kek.kms_key_id} "
                            f"which differs from rule kms key id {kms_key_id}")
        return kek

    def _retrieve_kek_from_registry(self, kek_id: KekId) -> Optional[Kek]:
        try:
            return self._executor.client.get_kek(kek_id.name, kek_id.deleted)
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 404:
                return None
            raise RuleError(f"could not get kek {kek_id.name}") from e

    def _store_kek_to_registry(
        self, kek_id: KekId, kms_type: str,
        kms_key_id: str, shared: bool
    ) -> Optional[Kek]:
        try:
            return self._executor.client.register_kek(kek_id.name, kms_type, kms_key_id, shared)
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 409:
                return None
            raise RuleError(f"could not register kek {kek_id.name}") from e

    def _get_or_create_dek(self, ctx: RuleContext, version: Optional[int]) -> Dek:
        kek = self._get_kek(ctx)
        is_read = ctx.rule_mode == RuleMode.READ
        if version is None or version == 0:
            version = 1
        dek_id = DekId(
            kek.name,
            ctx.subject,
            version,
            self._cryptor.dek_format,
            is_read
        )
        dek = self._retrieve_dek_from_registry(dek_id)
        is_expired = self._is_expired(ctx, dek)
        primitive = None
        if dek is None or is_expired:
            if is_read:
                raise RuleError(f"no dek found for {dek_id.kek_name} during consume")
            encrypted_dek = None
            if not kek.shared:
                primitive = self._get_aead(self._executor.config, self._kek)
                raw_dek = self._cryptor.generate_key()
                encrypted_dek = primitive.encrypt(raw_dek, self._cryptor.EMPTY_AAD)
            new_version = dek.version + 1 if is_expired else 1
            try:
                dek = self._create_dek(dek_id, new_version, encrypted_dek)
            except RuleError as e:
                if dek is None:
                    raise e
                log.warning("failed to create dek for %s, subject %s, version %d, using existing dek",
                            kek.name, ctx.subject, new_version)
        key_bytes = dek.get_key_material_bytes()
        if key_bytes is None:
            if primitive is None:
                primitive = self._get_aead(self._executor.config, self._kek)
            encrypted_dek = dek.get_encrypted_key_material_bytes()
            raw_dek = primitive.decrypt(encrypted_dek, self._cryptor.EMPTY_AAD)
            dek.set_key_material(raw_dek)
        return dek

    def _create_dek(self, dek_id: DekId, new_version: Optional[int], encrypted_dek: Optional[bytes]) -> Dek:
        new_dek_id = DekId(
            dek_id.kek_name,
            dek_id.subject,
            new_version,
            dek_id.algorithm,
            dek_id.deleted,
        )
        dek = self._store_dek_to_registry(new_dek_id, encrypted_dek)
        if dek is None:
            # handle conflicts (409)
            dek = self._retrieve_dek_from_registry(dek_id)
        if dek is None:
            raise RuleError(f"no dek found for {dek_id.kek_name} during produce")
        return dek

    def _retrieve_dek_from_registry(self, key: DekId) -> Optional[Dek]:
        try:
            version = key.version
            if not version:
                version = 1
            dek = self._executor.client.get_dek(
                key.kek_name, key.subject, key.algorithm, version, key.deleted)
            return dek if dek and dek.encrypted_key_material else None
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 404:
                return None
            raise RuleError(f"could not get dek for kek {key.kek_name}, subject {key.subject}") from e

    def _store_dek_to_registry(self, key: DekId, encrypted_dek: Optional[bytes]) -> Optional[Dek]:
        try:
            encrypted_dek_str = base64.b64encode(encrypted_dek).decode("utf-8") if encrypted_dek else None
            dek = self._executor.client.register_dek(
                key.kek_name, key.subject, encrypted_dek_str, key.algorithm, key.version)
            return dek
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 409:
                return None
            raise RuleError(f"could not register dek for kek {key.kek_name}, subject {key.subject}") from e

    def _is_expired(self, ctx: RuleContext, dek: Optional[Dek]) -> bool:
        now = self._executor.clock.now()
        return (ctx.rule_mode != RuleMode.READ
                and self._dek_expiry_days > 0
                and dek is not None
                and (now - dek.ts) / MILLIS_IN_DAY > self._dek_expiry_days)

    def transform(self, ctx: RuleContext, field_ctx: FieldContext, field_value: Any) -> Any:
        if field_value is None:
            return None
        if ctx.rule_mode == RuleMode.WRITE:
            plaintext = self._to_bytes(field_ctx.field_type, field_value)
            if plaintext is None:
                raise RuleError(f"type {field_ctx.field_type} not supported for encryption")
            version = None
            if self._is_dek_rotated():
                version = -1
            dek = self._get_or_create_dek(ctx, version)
            key_material_bytes = dek.get_key_material_bytes()
            ciphertext = self._cryptor.encrypt(key_material_bytes, plaintext, Cryptor.EMPTY_AAD)
            if self._is_dek_rotated():
                ciphertext = self._prefix_version(dek.version, ciphertext)
            if field_ctx.field_type == FieldType.STRING:
                return base64.b64encode(ciphertext).decode("utf-8")
            else:
                return self._to_object(field_ctx.field_type, ciphertext)
        elif ctx.rule_mode == RuleMode.READ:
            if field_ctx.field_type == FieldType.STRING:
                ciphertext = base64.b64decode(field_value)
            else:
                ciphertext = self._to_bytes(field_ctx.field_type, field_value)
            if ciphertext is None:
                return field_value

            version = None
            if self._is_dek_rotated():
                version, ciphertext = self._extract_version(ciphertext)
                if version is None:
                    raise RuleError("no version found in ciphertext")
            dek = self._get_or_create_dek(ctx, version)
            key_material_bytes = dek.get_key_material_bytes()
            plaintext = self._cryptor.decrypt(key_material_bytes, ciphertext, Cryptor.EMPTY_AAD)
            return self._to_object(field_ctx.field_type, plaintext)
        else:
            raise RuleError(f"unsupported rule mode {ctx.rule_mode}")

    def _prefix_version(self, version: int, ciphertext: bytes) -> bytes:
        return bytes([_MAGIC_BYTE_V0]) + version.to_bytes(4, byteorder="big") + ciphertext

    def _extract_version(self, ciphertext: bytes) -> Tuple[Optional[int], bytes]:
        if len(ciphertext) < 5:
            return None, ciphertext
        version = int.from_bytes(ciphertext[1:5], byteorder="big")
        return version, ciphertext[5:]

    def _to_bytes(self, field_type: FieldType, value: Any) -> Optional[bytes]:
        if field_type == FieldType.STRING:
            return value.encode("utf-8")
        elif field_type == FieldType.BYTES:
            return value
        return None

    def _to_object(self, field_type: FieldType, value: bytes) -> Any:
        if field_type == FieldType.STRING:
            return value.decode("utf-8")
        elif field_type == FieldType.BYTES:
            return value
        return None

    def _get_aead(self, config: dict, kek: Kek) -> aead.Aead:
        kek_url = kek.kms_type + "://" + kek.kms_key_id
        kms_client = self._get_kms_client(config, kek_url)
        return kms_client.get_aead(kek_url)

    def _get_kms_client(self, config: dict, kek_url: str) -> KmsClient:
        driver = get_kms_driver(kek_url)
        try:
            client = kms_client_from_uri(kek_url)
        except TinkError:
            client = self._register_kms_client(driver, config, kek_url)
        return client

    def _register_kms_client(self, kms_driver: KmsDriver, config: dict, kek_url: str) -> KmsClient:
        kms_client = kms_driver.new_kms_client(config, kek_url)
        register_kms_client(kms_client)
        return kms_client
