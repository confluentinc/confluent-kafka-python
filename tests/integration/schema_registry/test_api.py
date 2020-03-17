from uuid import uuid1

from confluent_kafka.schema_registry import CompatibilityType, SchemaRegistryClient
from confluent_kafka.schema_registry.config import SchemaRegistryConfig


def _subject_name(prefix):
    return "-".join([prefix, str(uuid1())])


def test_api_get_schema(kafka_cluster, load_avsc):
    """
    Registers a schema then retrieves it using the schema id returned from the
    call to register the Schema.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the Schema retrieved by ID does not match the expected
        Schema
    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    schema = load_avsc('basic_schema.avsc')

    version = sr.register_schema(str(uuid1()), schema)
    schema2 = sr.get_schema(version.schema_id)

    assert schema2 == schema


def test_api_get_subjects(kafka_cluster, load_avsc):
    """
    Populates KafkaClusterFixture SR instance with a fixed number of subjects
    then verifies the response includes them all.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if all registered subjects are not returned
    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    avscs = ['basic_schema.avsc', 'primitive_string.avsc',
             'primitive_bool.avsc', 'primitive_float.avsc']

    for avsc in avscs:
        sr.register_schema(_subject_name(avsc), load_avsc(avsc))

    registered = sr.get_subjects()
    assert len(registered) >= 4


def test_api_register_schema(kafka_cluster, load_avsc):
    """
    Registers a schema

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if all registered subjects are not returned
    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    avsc = 'basic_schema.avsc'

    version = sr.register_schema(_subject_name(avsc), load_avsc(avsc))
    assert version.schema == sr.get_schema(version.schema_id)


def test_api_get_subject_versions(kafka_cluster, load_avsc):
    """
    Registers a Schema with a subject, lists the versions associated with that
    subject and ensures the versions and their schemas match what was
    registered.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the versions or schemas do not match what was
        registered.

    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    subject = _subject_name("list-version-test")
    sr.set_compatibility(level=CompatibilityType.NONE)

    avscs = ['basic_schema.avsc', 'primitive_string.avsc',
             'primitive_bool.avsc', 'primitive_float.avsc']

    for avsc in avscs:
        sr.register_schema(subject, load_avsc(avsc))

    assert len(sr.list_versions(subject)) == len(avscs)


def test_api_delete_subject(kafka_cluster, load_avsc):
    """
    Registers a Schema under a specific subject then deletes it.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the version number is not returned in the response or
        if the subject has not been deleted.

    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    schema = load_avsc('basic_schema.avsc')
    subject = _subject_name("test-delete")
    version = sr.register_schema(subject, schema)
    sr.delete_subject(version.subject)

    assert version.subject not in sr.get_subjects()


def test_api_get_subject_version(kafka_cluster, load_avsc):
    """
    Registers a schema, fetches that schema by it's subject version id.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the returned schema and id do not match the
        registration schema and schema id.

    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    schema = load_avsc('basic_schema.avsc')
    subject = _subject_name('test-get_subject')

    version1 = sr.register_schema(subject, schema)

    version2 = sr.get_version(subject, version1.version)
    assert version2.schema_id == version1.schema_id
    assert version2.schema == version1.schema


def test_api_post_subject_registration(kafka_cluster, load_avsc):
    """
    Registers a schema, fetches that schema by it's subject version id.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the returned schema and id do not match the
        registration schema and schema id.

    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    schema = load_avsc('basic_schema.avsc')
    subject = _subject_name('test_registration')

    version = sr.register_schema(subject, schema)
    version2 = sr.get_registration(subject, version.schema)

    assert version.schema == version2.schema
    assert version2.subject == version2.subject
    assert version.version == version2.version


def test_api_delete_subject_version(kafka_cluster, load_avsc):
    """
    Registers a Schema under a specific subject then deletes it.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the version number is not returned in the response or
        if the subject has not been deleted.

    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    schema = load_avsc('basic_schema.avsc')
    subject = str(uuid1())

    sr.register_schema(subject, schema)
    sr.delete_version(subject, 1)

    assert subject not in sr.get_subjects()


def test_api_subject_config_update(kafka_cluster, load_avsc):
    """
    Updates a subjects compatibility policy then ensures the same policy
    is returned when queried.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the compatibility policy returned by the schema
        registry fixture does not match the updated value.

    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    schema = load_avsc('basic_schema.avsc')
    subject = str(uuid1())

    sr.register_schema(subject, schema)
    sr.set_compatibility(subject_name=subject, level=CompatibilityType.FULL_TRANSITIVE)

    assert sr.get_compatibility(subject_name=subject) == CompatibilityType.FULL_TRANSITIVE


def test_api_config_update(kafka_cluster, load_avsc):
    """
    Updates a global compatibility policy then ensures the same policy
    is returned when queried.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the compatibility policy returned by the schema
        registry fixture does not match the updated value.

    """
    sr_conf = SchemaRegistryConfig(kafka_cluster.schema_registry())
    sr = SchemaRegistryClient(sr_conf)

    sr.set_compatibility(level=CompatibilityType.FORWARD_TRANSITIVE)

    assert sr.get_compatibility() == CompatibilityType.FORWARD_TRANSITIVE
