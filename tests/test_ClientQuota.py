from concurrent.futures import Future

import pytest

from confluent_kafka.admin import (
    AdminClient,
    ClientQuotaAlteration,
    ClientQuotaAlterationOp,
    ClientQuotaEntity,
    ClientQuotaFilter,
    ClientQuotaFilterComponent,
    ClientQuotaMatchType,
)
from confluent_kafka.cimpl import _AdminClientImpl


def _assert_embedded_nul_rejected(func, *args):
    with pytest.raises(ValueError, match="embedded NUL"):
        func(*args)


def test_client_quota_entity_equality_and_hash():
    first = ClientQuotaEntity({"user": "alice", "client-id": None})
    second = ClientQuotaEntity({"client-id": None, "user": "alice"})
    assert first == second
    assert hash(first) == hash(second)


def test_client_quota_entity_entries_cannot_mutate_hash_key():
    entity = ClientQuotaEntity({"user": "alice"})
    keyed = {entity: "result"}

    entries = entity.entries
    entries["user"] = "bob"

    assert entity.entries == {"user": "alice"}
    assert keyed[entity] == "result"


def test_client_quota_filter_validation():
    valid = ClientQuotaFilter([ClientQuotaFilterComponent("user", ClientQuotaMatchType.EXACT, "alice")], strict=True)
    AdminClient._check_client_quota_filter(valid)

    with pytest.raises(ValueError):
        AdminClient._check_client_quota_filter(
            ClientQuotaFilter([ClientQuotaFilterComponent("user", ClientQuotaMatchType.EXACT)])
        )
    with pytest.raises(ValueError):
        AdminClient._check_client_quota_filter(
            ClientQuotaFilter([ClientQuotaFilterComponent("user", ClientQuotaMatchType.ANY, "alice")])
        )
    with pytest.raises(ValueError):
        AdminClient._check_client_quota_filter(
            ClientQuotaFilter(
                [
                    ClientQuotaFilterComponent("user", ClientQuotaMatchType.ANY),
                    ClientQuotaFilterComponent("user", ClientQuotaMatchType.DEFAULT),
                ]
            )
        )


@pytest.mark.parametrize(
    "component",
    [
        ClientQuotaFilterComponent("user\x00suffix", ClientQuotaMatchType.ANY),
        ClientQuotaFilterComponent("user", ClientQuotaMatchType.EXACT, "alice\x00suffix"),
    ],
)
def test_client_quota_filter_rejects_embedded_nul(component):
    _assert_embedded_nul_rejected(AdminClient._check_client_quota_filter, ClientQuotaFilter([component]))


@pytest.mark.parametrize(
    "component",
    [
        ClientQuotaFilterComponent("user\x00suffix", ClientQuotaMatchType.ANY),
        ClientQuotaFilterComponent("user", ClientQuotaMatchType.EXACT, "alice\x00suffix"),
    ],
)
def test_client_quota_filter_c_boundary_rejects_embedded_nul(component):
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:1"})
    _assert_embedded_nul_rejected(
        _AdminClientImpl.describe_client_quotas, admin, ClientQuotaFilter([component]), Future()
    )


def test_client_quota_alteration_validation():
    entity = ClientQuotaEntity({"user": "alice"})
    valid = [ClientQuotaAlteration(entity, [ClientQuotaAlterationOp("producer_byte_rate", 1024.0)])]
    AdminClient._check_client_quota_alterations(valid)

    with pytest.raises(ValueError):
        AdminClient._check_client_quota_alterations([])
    with pytest.raises(ValueError):
        AdminClient._check_client_quota_alterations(
            [
                ClientQuotaAlteration(entity, [ClientQuotaAlterationOp("producer_byte_rate", 1.0)]),
                ClientQuotaAlteration(entity, [ClientQuotaAlterationOp("consumer_byte_rate", 1.0)]),
            ]
        )
    with pytest.raises(ValueError):
        AdminClient._check_client_quota_alterations(
            [
                ClientQuotaAlteration(
                    entity,
                    [
                        ClientQuotaAlterationOp("producer_byte_rate", 1.0),
                        ClientQuotaAlterationOp("producer_byte_rate", None),
                    ],
                )
            ]
        )


@pytest.mark.parametrize(
    "entity,op",
    [
        (ClientQuotaEntity({"user\x00suffix": "alice"}), ClientQuotaAlterationOp("producer_byte_rate", 1.0)),
        (ClientQuotaEntity({"user": "alice\x00suffix"}), ClientQuotaAlterationOp("producer_byte_rate", 1.0)),
        (ClientQuotaEntity({"user": "alice"}), ClientQuotaAlterationOp("producer_byte_rate\x00suffix", 1.0)),
    ],
)
def test_client_quota_alteration_rejects_embedded_nul(entity, op):
    _assert_embedded_nul_rejected(AdminClient._check_client_quota_alterations, [ClientQuotaAlteration(entity, [op])])


@pytest.mark.parametrize(
    "entity,op",
    [
        (ClientQuotaEntity({"user\x00suffix": "alice"}), ClientQuotaAlterationOp("producer_byte_rate", 1.0)),
        (ClientQuotaEntity({"user": "alice\x00suffix"}), ClientQuotaAlterationOp("producer_byte_rate", 1.0)),
        (ClientQuotaEntity({"user": "alice"}), ClientQuotaAlterationOp("producer_byte_rate\x00suffix", 1.0)),
    ],
)
def test_client_quota_alteration_c_boundary_rejects_embedded_nul(entity, op):
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:1"})
    _assert_embedded_nul_rejected(
        _AdminClientImpl.alter_client_quotas, admin, [ClientQuotaAlteration(entity, [op])], Future()
    )
