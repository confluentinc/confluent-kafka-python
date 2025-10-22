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

"""
Type stubs for confluent_kafka.cimpl

This combines automatic stubgen output (constants, functions) with
manual class definitions based on runtime introspection and domain knowledge.

⚠️ WARNING: MAINTENANCE REQUIRED ⚠️
This stub file must be kept in sync with the C extension source code in src/:
- src/Admin.c (AdminClientImpl methods)
- src/Producer.c (Producer class)
- src/Consumer.c (Consumer class)
- src/AdminTypes.c (NewTopic, NewPartitions classes)
- src/confluent_kafka.c (KafkaError, Message, TopicPartition, Uuid classes)

When modifying C extension interfaces (method signatures, parameters, defaults),
you MUST update the corresponding type definitions in this file.
Failure to do so will result in incorrect type hints and mypy errors.

TODO: Consider migrating to Cython in the future to eliminate this dual
maintenance burden and get type hints directly from the implementation.
"""

from typing import Any, Optional, Callable, List, Tuple, Dict, Union, overload, TYPE_CHECKING
from typing_extensions import Self, Literal
import builtins

from ._types import HeadersType

if TYPE_CHECKING:
    from confluent_kafka.admin._metadata import ClusterMetadata, GroupMetadata

# Callback types with proper class references (defined locally to avoid circular imports)
DeliveryCallback = Callable[[Optional['KafkaError'], 'Message'], None]
RebalanceCallback = Callable[['Consumer', List['TopicPartition']], None]

# ===== CLASSES (Manual - stubgen missed these) =====

class KafkaError:
    _KEY_DESERIALIZATION: int
    _KEY_SERIALIZATION: int
    _VALUE_DESERIALIZATION: int
    _VALUE_SERIALIZATION: int

    def __init__(self, code: int, str: Optional[str] = None, fatal: bool = False) -> None: ...
    def code(self) -> int: ...
    def name(self) -> builtins.str: ...
    def str(self) -> builtins.str: ...
    def fatal(self) -> bool: ...
    def retriable(self) -> bool: ...
    def txn_requires_abort(self) -> bool: ...
    def __str__(self) -> builtins.str: ...
    def __bool__(self) -> bool: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: Union['KafkaError', int]) -> bool: ...
    def __le__(self, other: Union['KafkaError', int]) -> bool: ...
    def __gt__(self, other: Union['KafkaError', int]) -> bool: ...
    def __ge__(self, other: Union['KafkaError', int]) -> bool: ...

class KafkaException(Exception):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    args: Tuple[Any, ...]

class Message:
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def key(self) -> Optional[bytes]: ...
    def value(self) -> Optional[bytes]: ...
    def headers(self) -> Optional[HeadersType]: ...
    def error(self) -> Optional[KafkaError]: ...
    def timestamp(self) -> Tuple[int, int]: ...  # (timestamp_type, timestamp)
    def latency(self) -> Optional[float]: ...
    def leader_epoch(self) -> Optional[int]: ...
    def set_headers(self, headers: HeadersType) -> None: ...
    def set_key(self, key: Any) -> None: ...
    def set_value(self, value: Any) -> None: ...
    def __len__(self) -> int: ...

class TopicPartition:
    def __init__(self, topic: str, partition: int = -1, offset: int = -1001) -> None: ...
    topic: str
    partition: int
    offset: int
    leader_epoch: int
    metadata: Optional[str]
    error: Optional[KafkaError]
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __lt__(self, other: 'TopicPartition') -> bool: ...

class Uuid:
    def __init__(self, uuid_str: Optional[str] = None) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __int__(self) -> int: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...

class Producer:
    def __init__(self, config: Dict[str, Union[str, int, float, bool]]) -> None: ...
    def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: int = -1,
        callback: Optional[DeliveryCallback] = None,
        on_delivery: Optional[DeliveryCallback] = None,
        timestamp: int = 0,
        headers: Optional[HeadersType] = None
    ) -> None: ...
    def produce_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        partition: int = -1,
        callback: Optional[DeliveryCallback] = None,
        on_delivery: Optional[DeliveryCallback] = None
    ) -> int: ...
    def poll(self, timeout: float = -1) -> int: ...
    def flush(self, timeout: float = -1) -> int: ...
    def purge(
        self,
        in_queue: bool = True,
        in_flight: bool = True,
        blocking: bool = True
    ) -> None: ...
    def abort_transaction(self, timeout: float = -1) -> None: ...
    def begin_transaction(self) -> None: ...
    def commit_transaction(self, timeout: float = -1) -> None: ...
    def init_transactions(self, timeout: float = -1) -> None: ...
    def send_offsets_to_transaction(
        self,
        positions: List[TopicPartition],
        group_metadata: Any,  # ConsumerGroupMetadata
        timeout: float = -1
    ) -> None: ...
    def list_topics(self, topic: Optional[str] = None, timeout: float = -1) -> Any: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...

class Consumer:
    def __init__(self, config: Dict[str, Union[str, int, float, bool, None]]) -> None: ...
    def subscribe(
        self,
        topics: List[str],
        on_assign: Optional[RebalanceCallback] = None,
        on_revoke: Optional[RebalanceCallback] = None,
        on_lost: Optional[RebalanceCallback] = None
    ) -> None: ...
    def unsubscribe(self) -> None: ...
    def poll(self, timeout: float = -1) -> Optional[Message]: ...
    def consume(self, num_messages: int = 1, timeout: float = -1) -> List[Message]: ...
    def assign(self, partitions: List[TopicPartition]) -> None: ...
    def unassign(self) -> None: ...
    def assignment(self) -> List[TopicPartition]: ...
    @overload
    def commit(
        self,
        message: Optional['Message'] = None,
        offsets: Optional[List[TopicPartition]] = None,
        asynchronous: Literal[True] = True
    ) -> None: ...
    @overload
    def commit(
        self,
        message: Optional['Message'] = None,
        offsets: Optional[List[TopicPartition]] = None,
        asynchronous: Literal[False] = False
    ) -> List[TopicPartition]: ...
    def get_watermark_offsets(
        self,
        partition: TopicPartition,
        timeout: float = -1,
        cached: bool = False
    ) -> Tuple[int, int]: ...
    def pause(self, partitions: List[TopicPartition]) -> None: ...
    def resume(self, partitions: List[TopicPartition]) -> None: ...
    def seek(self, partition: TopicPartition) -> None: ...
    def position(self, partitions: List[TopicPartition]) -> List[TopicPartition]: ...
    def store_offsets(
        self,
        message: Optional['Message'] = None,
        offsets: Optional[List[TopicPartition]] = None
    ) -> None: ...
    def committed(
        self,
        partitions: List[TopicPartition],
        timeout: float = -1
    ) -> List[TopicPartition]: ...
    def close(self) -> None: ...
    def list_topics(self, topic: Optional[str] = None, timeout: float = -1) -> Any: ...
    def offsets_for_times(
        self,
        partitions: List[TopicPartition],
        timeout: float = -1
    ) -> List[TopicPartition]: ...
    def incremental_assign(self, partitions: List[TopicPartition]) -> None: ...
    def incremental_unassign(self, partitions: List[TopicPartition]) -> None: ...
    def consumer_group_metadata(self) -> Any: ...  # ConsumerGroupMetadata
    def memberid(self) -> str: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def __bool__(self) -> bool: ...

class _AdminClientImpl:
    def __init__(self, config: Dict[str, Union[str, int, float, bool]]) -> None: ...
    def create_topics(
        self,
        topics: List['NewTopic'],
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        operation_timeout: float = -1
    ) -> None: ...
    def delete_topics(
        self,
        topics: List[str],
        future: Any,
        request_timeout: float = -1,
        operation_timeout: float = -1
    ) -> None: ...
    def create_partitions(
        self,
        topics: List['NewPartitions'],
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        operation_timeout: float = -1
    ) -> None: ...
    def describe_topics(
        self,
        future: Any,
        topic_names: List[str],
        request_timeout: float = -1,
        include_authorized_operations: bool = False
    ) -> None: ...
    def describe_cluster(
        self,
        future: Any,
        request_timeout: float = -1,
        include_authorized_operations: bool = False
    ) -> None: ...
    def list_topics(
        self,
        topic: Optional[str] = None,
        timeout: float = -1
    ) -> ClusterMetadata: ...
    def list_groups(
        self,
        group: Optional[str] = None,
        timeout: float = -1
    ) -> List[GroupMetadata]: ...
    def describe_consumer_groups(
        self,
        group_ids: List[str],
        future: Any,
        request_timeout: float = -1,
        include_authorized_operations: bool = False
    ) -> None: ...
    def list_consumer_groups(
        self,
        future: Any,
        states_int: Optional[List[int]] = None,
        types_int: Optional[List[int]] = None,
        request_timeout: float = -1
    ) -> None: ...
    def list_consumer_group_offsets(
        self,
        request: Any,  # ConsumerGroupTopicPartitions
        future: Any,
        require_stable: bool = False,
        request_timeout: float = -1
    ) -> None: ...
    def alter_consumer_group_offsets(
        self,
        requests: Any,  # List[ConsumerGroupTopicPartitions]
        future: Any,
        request_timeout: float = -1
    ) -> None: ...
    def delete_consumer_groups(
        self,
        group_ids: List[str],
        future: Any,
        request_timeout: float = -1
    ) -> None: ...
    def create_acls(
        self,
        acls: List[Any],  # List[AclBinding]
        future: Any,
        request_timeout: float = -1
    ) -> None: ...
    def describe_acls(
        self,
        acl_binding_filter: Any,  # AclBindingFilter
        future: Any,
        request_timeout: float = -1
    ) -> None: ...
    def delete_acls(
        self,
        acls: List[Any],  # List[AclBindingFilter]
        future: Any,
        request_timeout: float = -1
    ) -> None: ...
    def describe_configs(
        self,
        resources: List[Any],  # List[ConfigResource]
        future: Any,
        request_timeout: float = -1,
        broker: int = -1
    ) -> None: ...
    def alter_configs(
        self,
        resources: List[Any],  # List[ConfigResource]
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        broker: int = -1
    ) -> None: ...
    def incremental_alter_configs(
        self,
        resources: List[Any],  # List[ConfigResource]
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        broker: int = -1
    ) -> None: ...
    def describe_user_scram_credentials(
        self,
        users: Optional[List[str]],
        future: Any,
        request_timeout: float = -1
    ) -> None: ...
    def alter_user_scram_credentials(
        self,
        alterations: List[Any],  # List[UserScramCredentialAlteration]
        future: Any,
        request_timeout: float = -1
    ) -> None: ...
    def list_offsets(
        self,
        topic_partitions: List[TopicPartition],
        future: Any,
        isolation_level_value: Optional[int] = None,
        request_timeout: float = -1
    ) -> None: ...
    def delete_records(
        self,
        topic_partition_offsets: List[TopicPartition],
        future: Any,
        request_timeout: float = -1,
        operation_timeout: float = -1
    ) -> None: ...
    def elect_leaders(
        self,
        election_type: int,
        partitions: Optional[List[TopicPartition]],
        future: Any,
        request_timeout: float = -1,
        operation_timeout: float = -1
    ) -> None: ...
    def poll(self, timeout: float = -1) -> int: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...

class NewTopic:
    def __init__(
        self,
        topic: str,
        num_partitions: int = -1,
        replication_factor: int = -1,
        replica_assignment: Optional[List[List[int]]] = None,
        config: Optional[Dict[str, str]] = None
    ) -> None: ...
    topic: str
    num_partitions: int
    replication_factor: int
    replica_assignment: Optional[List[List[int]]]
    config: Optional[Dict[str, str]]
    def __str__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: 'NewTopic') -> bool: ...
    def __le__(self, other: 'NewTopic') -> bool: ...
    def __gt__(self, other: 'NewTopic') -> bool: ...
    def __ge__(self, other: 'NewTopic') -> bool: ...

class NewPartitions:
    def __init__(
        self,
        topic: str,
        new_total_count: int,
        replica_assignment: Optional[List[List[int]]] = None
    ) -> None: ...
    topic: str
    new_total_count: int
    replica_assignment: Optional[List[List[int]]]
    def __str__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: 'NewPartitions') -> bool: ...
    def __le__(self, other: 'NewPartitions') -> bool: ...
    def __gt__(self, other: 'NewPartitions') -> bool: ...
    def __ge__(self, other: 'NewPartitions') -> bool: ...

# ===== MODULE FUNCTIONS (From stubgen) =====

def libversion() -> Tuple[str, int]: ...
def version() -> Tuple[str, int]: ...

# ===== CONSTANTS (From stubgen) =====

ACL_OPERATION_ALL: int
ACL_OPERATION_ALTER: int
ACL_OPERATION_ALTER_CONFIGS: int
ACL_OPERATION_ANY: int
ACL_OPERATION_CLUSTER_ACTION: int
ACL_OPERATION_CREATE: int
ACL_OPERATION_DELETE: int
ACL_OPERATION_DESCRIBE: int
ACL_OPERATION_DESCRIBE_CONFIGS: int
ACL_OPERATION_IDEMPOTENT_WRITE: int
ACL_OPERATION_READ: int
ACL_OPERATION_UNKNOWN: int
ACL_OPERATION_WRITE: int
ACL_PERMISSION_TYPE_ALLOW: int
ACL_PERMISSION_TYPE_ANY: int
ACL_PERMISSION_TYPE_DENY: int
ACL_PERMISSION_TYPE_UNKNOWN: int
ALTER_CONFIG_OP_TYPE_APPEND: int
ALTER_CONFIG_OP_TYPE_DELETE: int
ALTER_CONFIG_OP_TYPE_SET: int
ALTER_CONFIG_OP_TYPE_SUBTRACT: int
CONFIG_SOURCE_DEFAULT_CONFIG: int
CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG: int
CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG: int
CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG: int
CONFIG_SOURCE_GROUP_CONFIG: int
CONFIG_SOURCE_STATIC_BROKER_CONFIG: int
CONFIG_SOURCE_UNKNOWN_CONFIG: int
CONSUMER_GROUP_STATE_COMPLETING_REBALANCE: int
CONSUMER_GROUP_STATE_DEAD: int
CONSUMER_GROUP_STATE_EMPTY: int
CONSUMER_GROUP_STATE_PREPARING_REBALANCE: int
CONSUMER_GROUP_STATE_STABLE: int
CONSUMER_GROUP_STATE_UNKNOWN: int
CONSUMER_GROUP_TYPE_CLASSIC: int
CONSUMER_GROUP_TYPE_CONSUMER: int
CONSUMER_GROUP_TYPE_UNKNOWN: int
ELECTION_TYPE_PREFERRED: int
ELECTION_TYPE_UNCLEAN: int
ISOLATION_LEVEL_READ_COMMITTED: int
ISOLATION_LEVEL_READ_UNCOMMITTED: int
OFFSET_BEGINNING: int
OFFSET_END: int
OFFSET_INVALID: int
OFFSET_SPEC_EARLIEST: int
OFFSET_SPEC_LATEST: int
OFFSET_SPEC_MAX_TIMESTAMP: int
OFFSET_STORED: int
RESOURCE_ANY: int
RESOURCE_BROKER: int
RESOURCE_GROUP: int
RESOURCE_PATTERN_ANY: int
RESOURCE_PATTERN_LITERAL: int
RESOURCE_PATTERN_MATCH: int
RESOURCE_PATTERN_PREFIXED: int
RESOURCE_PATTERN_UNKNOWN: int
RESOURCE_TOPIC: int
RESOURCE_TRANSACTIONAL_ID: int
RESOURCE_UNKNOWN: int
SCRAM_MECHANISM_SHA_256: int
SCRAM_MECHANISM_SHA_512: int
SCRAM_MECHANISM_UNKNOWN: int
TIMESTAMP_CREATE_TIME: int
TIMESTAMP_LOG_APPEND_TIME: int
TIMESTAMP_NOT_AVAILABLE: int
