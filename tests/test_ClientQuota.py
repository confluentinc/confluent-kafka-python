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
