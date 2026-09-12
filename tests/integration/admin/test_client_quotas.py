import time
import uuid

from confluent_kafka.admin import (
    ClientQuotaAlteration,
    ClientQuotaAlterationOp,
    ClientQuotaEntity,
    ClientQuotaFilter,
    ClientQuotaFilterComponent,
    ClientQuotaMatchType,
)


def _describe_user(admin_client, user):
    quota_filter = ClientQuotaFilter([ClientQuotaFilterComponent("user", ClientQuotaMatchType.EXACT, user)])
    return admin_client.describe_client_quotas(quota_filter).result(timeout=15)


def test_client_quotas(kafka_cluster):
    admin_client = kafka_cluster.admin()
    user = "confluent-kafka-python-{}".format(uuid.uuid4().hex)
    entity = ClientQuotaEntity({"user": user})
    key = "producer_byte_rate"

    validate_only = ClientQuotaAlteration(entity, [ClientQuotaAlterationOp(key, 111111.0)])
    assert admin_client.alter_client_quotas([validate_only], validate_only=True)[entity].result(timeout=15) is None
    assert entity not in _describe_user(admin_client, user)

    alteration = ClientQuotaAlteration(entity, [ClientQuotaAlterationOp(key, 222222.0)])
    assert admin_client.alter_client_quotas([alteration])[entity].result(timeout=15) is None

    for _ in range(6):
        result = _describe_user(admin_client, user)
        if result.get(entity, {}).get(key) == 222222.0:
            break
        time.sleep(0.2)
    assert result[entity][key] == 222222.0

    removal = ClientQuotaAlteration(entity, [ClientQuotaAlterationOp(key, None)])
    assert admin_client.alter_client_quotas([removal])[entity].result(timeout=15) is None
    for _ in range(6):
        result = _describe_user(admin_client, user)
        if entity not in result:
            break
        time.sleep(0.2)
    assert entity not in result
