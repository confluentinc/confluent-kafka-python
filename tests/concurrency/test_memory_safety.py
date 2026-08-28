#!/usr/bin/env python
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
Free-threading memory-safety tests for the borrowed-reference / stale-count
bug class.

The affected C code takes a caller-supplied Python container, captures its
length once and/or borrows its items via PyList_GET_ITEM without Py_INCREF,
then dereferences those borrowed items -- often casting them to a C struct and
handing an internal char* to librdkafka. With the GIL disabled, another thread
that mutates the same container concurrently causes:

  * a freed element -> the borrowed pointer / its char* fields dangle
    (use-after-free), and/or
  * a shrunk list  -> the captured count reads past the array (out-of-bounds),
    and for Producer.produce_batch a *grown* list overflows a pre-sized buffer.

Each test races victim threads (calling the API on a shared container) against
mutator threads (mutating that same container). On a vulnerable build the
process segfaults. On a memory-safe build the process survives and the test passes.
"""

import os
import threading
import time

import pytest

from confluent_kafka import (
    Consumer,
    ConsumerGroupState,
    ConsumerGroupType,
    Producer,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin import (
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
    ResourcePatternType,
    ResourceType,
    ScramMechanism,
    UserScramCredentialDeletion,
)
from tests.concurrency._subprocess_isolation import subprocess_isolated

_CONF = {'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}
_CONSUMER_CONF = dict(_CONF, **{'group.id': 'memref-test'})

# A large list gives many borrowed-item derefs per call, so a concurrent
# mutation reliably lands inside the C parse loop.
LIST_SIZE = 2000
NUM_VICTIMS = 6
NUM_MUTATORS = 3
DURATION_SECONDS = 6


def _race(call, shared, mutate, num_victims=NUM_VICTIMS, num_mutators=NUM_MUTATORS, duration=DURATION_SECONDS):
    """
    Race ``call(shared)`` on ``num_victims`` threads against ``mutate(shared)``
    on ``num_mutators`` threads for ``duration`` seconds.

    ``mutate`` must be a plain Python mutation of ``shared`` (never another
    client method) -- see the module docstring for why.
    """
    stop = threading.Event()
    start = threading.Barrier(num_victims + num_mutators)

    def victim():
        start.wait()
        while not stop.is_set():
            try:
                call(shared)
            except Exception:  # noqa: BLE001 - broker-less/value errors are expected; only crashes matter
                pass

    def mutator():
        start.wait()
        while not stop.is_set():
            try:
                mutate(shared)
            except Exception:  # noqa: BLE001 - concurrent-mutation ops may raise; that is fine
                pass

    threads = [threading.Thread(target=victim) for _ in range(num_victims)]
    threads += [threading.Thread(target=mutator) for _ in range(num_mutators)]
    for t in threads:
        t.start()

    time.sleep(duration)
    stop.set()
    for t in threads:
        t.join(timeout=10)
    assert all(not t.is_alive() for t in threads), "a worker thread hung"


def _churn(make_items):
    """
    Return a mutator that first replaces every element (dropping the old items'
    last reference so they are freed -> use-after-free) and then shrinks the
    list (-> stale captured count reads out of bounds).
    """

    def mutate(shared):
        shared[:] = make_items()
        del shared[len(shared) // 2 :]

    return mutate


def _shrink_hard(make_items):
    """
    Mutator for the captured-count + unchecked-PyList_GET_ITEM sites: grow the
    list to full length (so the C code may capture a large count), then realloc
    it far down. A stale count then indexes well past the reallocated array, so
    the out-of-bounds slots are genuine heap garbage -- not the NULLs a plain
    ``del`` leaves behind. This matters for the sites whose borrowed value goes
    through NULL-tolerant PyObject_Str (delete_topics, *_consumer_groups,
    describe_user_scram_credentials), where a NULL slot would not fault; a
    garbage pointer dereferenced into invalid memory reliably segfaults.
    """

    def mutate(shared):
        items = make_items()
        shared[:] = items  # grow to full length
        shared[:] = items[:8]  # realloc far down -> stale count reads OOB garbage

    return mutate


# --- item factories ---------------------------------------------------------


def _new_topics():
    return [NewTopic('t%d' % i, num_partitions=1, replication_factor=1) for i in range(LIST_SIZE)]


def _new_partitions():
    return [NewPartitions('t%d' % i, 2) for i in range(LIST_SIZE)]


def _topic_names():
    return ['t%d' % i for i in range(LIST_SIZE)]


def _messages():
    return [{'value': b'value', 'key': b'key'} for _ in range(LIST_SIZE)]


def _headers():
    return [('h%d' % i, b'value') for i in range(LIST_SIZE)]


def _config_resources():
    return [ConfigResource(ResourceType.TOPIC, 't%d' % i) for i in range(LIST_SIZE)]


def _incremental_config_resources():
    # incremental_alter_configs reads each resource's incremental_configs while
    # parsing, so a bare ConfigResource would make the C loop error out on the
    # first (in-bounds) element before it ever reaches an out-of-bounds index.
    return [
        ConfigResource(
            ResourceType.TOPIC,
            't%d' % i,
            incremental_configs=[ConfigEntry('cleanup.policy', 'compact', incremental_operation=AlterConfigOpType.SET)],
        )
        for i in range(LIST_SIZE)
    ]


def _acl_bindings():
    return [
        AclBinding(
            ResourceType.TOPIC,
            't%d' % i,
            ResourcePatternType.LITERAL,
            'User:p',
            '*',
            AclOperation.READ,
            AclPermissionType.ALLOW,
        )
        for i in range(LIST_SIZE)
    ]


def _acl_binding_filters():
    return [
        AclBindingFilter(
            ResourceType.TOPIC,
            't%d' % i,
            ResourcePatternType.LITERAL,
            'User:p',
            '*',
            AclOperation.READ,
            AclPermissionType.ALLOW,
        )
        for i in range(LIST_SIZE)
    ]


def _group_ids():
    return ['g%d' % i for i in range(LIST_SIZE)]


def _user_names():
    return ['u%d' % i for i in range(LIST_SIZE)]


def _scram_alterations():
    return [UserScramCredentialDeletion('u%d' % i, ScramMechanism.SCRAM_SHA_256) for i in range(LIST_SIZE)]


def _group_states():
    return [ConsumerGroupState.STABLE.value for _ in range(LIST_SIZE)]


def _group_types():
    return [ConsumerGroupType.CLASSIC.value for _ in range(LIST_SIZE)]


def _admin_list_crash(call, make_items):
    """Shared body for the AdminClient list-parse crash tests. Each of these
    methods captures the list length once and iterates with the unchecked
    PyList_GET_ITEM macro (same pattern as create_topics), so a concurrent
    shrink reads an out-of-bounds pointer slot -> garbage pointer dereferenced
    -> segfault. (Written as individual functions rather than @pytest.mark.
    parametrize because @subprocess_isolated keys its child off the function
    name and would otherwise re-run every parametrization per case.)"""
    admin = AdminClient(_CONF)
    _race(lambda s: call(admin, s), make_items(), _shrink_hard(make_items))
    os._exit(
        0
    )  # skip cleanup -- draining the backlog of requests still queued against the unreachable broker can take minutes


###############################################################################
# Producer
###############################################################################


@subprocess_isolated
def test_produce_batch_races_message_list_mutation():
    """Producer.produce_batch() parsing the message list vs. mutation of it.

    Producer_produce_batch() reads the list length twice (once to size the
    rkmessages/msgstates arrays, once inside the parse helper) and borrows each
    message dict -- a grow races a buffer overflow, a shrink/replace an
    out-of-bounds/use-after-free.
    """
    producer = Producer(_CONF)
    _race(lambda s: producer.produce_batch('memref', s), _messages(), _churn(_messages))


@subprocess_isolated
def test_produce_headers_list_races_mutation():
    """Producer.produce(headers=[...]) -> py_headers_list_to_c() captures the
    list length and borrows each (key, value) tuple via PyList_GET_ITEM."""
    producer = Producer(_CONF)
    _race(lambda s: producer.produce('memref', value=b'value', headers=s), _headers(), _churn(_headers))


###############################################################################
# AdminClient
###############################################################################


@subprocess_isolated
def test_create_topics_races_list_mutation():
    """AdminClient.create_topics() -- the issue #2319 site: borrowed NewTopic
    cast to a C struct, newt->topic handed to rd_kafka_NewTopic_new()."""
    admin = AdminClient(_CONF)
    _race(lambda s: admin.create_topics(s), _new_topics(), _churn(_new_topics))
    os._exit(
        0
    )  # skip cleanup -- draining the backlog of requests still queued against the unreachable broker can take minutes


@subprocess_isolated
def test_create_partitions_races_list_mutation():
    """AdminClient.create_partitions() -- borrowed NewPartitions cast to a C
    struct, newp->topic handed to rd_kafka_NewPartitions_new()."""
    admin = AdminClient(_CONF)
    _race(lambda s: admin.create_partitions(s), _new_partitions(), _churn(_new_partitions))
    os._exit(
        0
    )  # skip cleanup -- draining the backlog of requests still queued against the unreachable broker can take minutes


@subprocess_isolated
def test_describe_topics_races_topic_names_mutation():
    """AdminClient.describe_topics() -- Admin_describe_topics() stores a char*
    pointing directly into each borrowed str item and passes the array to
    rd_kafka_TopicCollection_of_topic_names(). The wrapper forwards
    TopicCollection.topic_names straight to C, so mutating that list races it.
    """
    admin = AdminClient(_CONF)
    collection = TopicCollection(_topic_names())

    def mutate(coll):
        coll.topic_names[:] = _topic_names()
        del coll.topic_names[len(coll.topic_names) // 2 :]

    _race(lambda coll: admin.describe_topics(coll), collection, mutate)
    os._exit(
        0
    )  # skip cleanup -- draining the backlog of requests still queued against the unreachable broker can take minutes


@subprocess_isolated
def test_delete_topics_races_list_mutation():
    """AdminClient.delete_topics() -- Admin_delete_topics() borrows each topic
    str item (Admin.c:759)."""
    _admin_list_crash(lambda a, s: a.delete_topics(s), _topic_names)


@subprocess_isolated
def test_describe_configs_races_list_mutation():
    """AdminClient.describe_configs() -- borrows each ConfigResource
    (Admin.c:1010)."""
    _admin_list_crash(lambda a, s: a.describe_configs(s), _config_resources)


@subprocess_isolated
def test_alter_configs_races_list_mutation():
    """AdminClient.alter_configs() -- borrows each ConfigResource + nested
    set_config_dict (Admin.c:1312)."""
    _admin_list_crash(lambda a, s: a.alter_configs(s), _config_resources)


@subprocess_isolated
def test_incremental_alter_configs_races_list_mutation():
    """AdminClient.incremental_alter_configs() -- borrows each ConfigResource +
    nested incremental_configs (Admin.c:1153)."""
    _admin_list_crash(lambda a, s: a.incremental_alter_configs(s), _incremental_config_resources)


@subprocess_isolated
def test_create_acls_races_list_mutation():
    """AdminClient.create_acls() -- borrows each AclBinding (Admin.c:1463)."""
    _admin_list_crash(lambda a, s: a.create_acls(s), _acl_bindings)


@subprocess_isolated
def test_delete_acls_races_list_mutation():
    """AdminClient.delete_acls() -- borrows each AclBindingFilter
    (Admin.c:1710)."""
    _admin_list_crash(lambda a, s: a.delete_acls(s), _acl_binding_filters)


@subprocess_isolated
def test_describe_consumer_groups_races_list_mutation():
    """AdminClient.describe_consumer_groups() -- borrows each group_id str
    (Admin.c:2418)."""
    _admin_list_crash(lambda a, s: a.describe_consumer_groups(s), _group_ids)


@subprocess_isolated
def test_delete_consumer_groups_races_list_mutation():
    """AdminClient.delete_consumer_groups() -- borrows each group_id str
    (Admin.c:2773)."""
    _admin_list_crash(lambda a, s: a.delete_consumer_groups(s), _group_ids)


@subprocess_isolated
def test_describe_user_scram_credentials_races_list_mutation():
    """AdminClient.describe_user_scram_credentials() -- borrows each user str
    (Admin.c:1981)."""
    _admin_list_crash(lambda a, s: a.describe_user_scram_credentials(s), _user_names)


@subprocess_isolated
def test_alter_user_scram_credentials_races_list_mutation():
    """AdminClient.alter_user_scram_credentials() -- borrows each alteration
    across many GetAttr calls (Admin.c:2164)."""
    _admin_list_crash(lambda a, s: a.alter_user_scram_credentials(s), _scram_alterations)


@subprocess_isolated
def test_list_consumer_groups_states_races_list_mutation():
    """AdminClient.list_consumer_groups() -- Admin_list_consumer_groups()
    borrows each states_int item (Admin.c:1999). Not reachable through the
    public states= API (the wrapper always builds a fresh, private list each
    call), so this bypasses it via super() the same way as the non-list-request
    tests below."""
    _admin_list_crash(
        lambda a, s: super(AdminClient, a).list_consumer_groups(AdminClient._create_future(), states_int=s),
        _group_states,
    )


@subprocess_isolated
def test_list_consumer_groups_types_races_list_mutation():
    """AdminClient.list_consumer_groups() -- Admin_list_consumer_groups()
    borrows each types_int item (Admin.c:2027). Not reachable through the
    public types= API (the wrapper always builds a fresh, private list each
    call), so this bypasses it via super() the same way as the non-list-request
    tests below."""
    _admin_list_crash(
        lambda a, s: super(AdminClient, a).list_consumer_groups(AdminClient._create_future(), types_int=s), _group_types
    )


###############################################################################
# Consumer
###############################################################################


@subprocess_isolated
def test_assign_torn_read_from_list_mutation():
    """Consumer.assign() -> py_to_c_parts() must apply a coherent snapshot
    of the partition list.

    py_to_c_parts iterates the caller's list non-atomically and borrows each
    TopicPartition (reading tp->topic / tp->metadata char* without Py_INCREF),
    so it is both memory-unsafe and non-atomic under concurrent mutation.
    Race assign() against a mutator that swaps the shared list between two disjoint
    topic namespaces ('a*' vs 'b*'), and the resulting assignment() can contain a
    *mix* of both -- a torn read that no single state of the list ever contained.

    A Py_BEGIN_CRITICAL_SECTION over the parse loop makes a concurrent list
    slice-assignment wait (same per-object lock), so every assignment is
    entirely one namespace."""
    consumer = Consumer(_CONSUMER_CONF)

    def make(ns):
        return [TopicPartition('%s%d' % (ns, i), i % 8) for i in range(LIST_SIZE)]

    shared = make('a')
    stop = threading.Event()
    start = threading.Barrier(1 + NUM_MUTATORS)
    torn = []  # list.append is atomic under free-threading

    def victim():
        start.wait()
        while not stop.is_set():
            try:
                consumer.assign(shared)
                namespaces = {tp.topic[:1] for tp in consumer.assignment()}
            except Exception:  # noqa: BLE001 - value errors during mutation are fine
                continue
            if len(namespaces) > 1:
                torn.append(sorted(namespaces))

    def mutator():
        start.wait()
        names = ('a', 'b')
        k = 0
        while not stop.is_set():
            try:
                k ^= 1
                shared[:] = make(names[k])
            except Exception:  # noqa: BLE001
                pass

    threads = [threading.Thread(target=victim)]
    threads += [threading.Thread(target=mutator) for _ in range(NUM_MUTATORS)]
    for t in threads:
        t.start()
    time.sleep(DURATION_SECONDS)
    stop.set()
    for t in threads:
        t.join(timeout=10)

    assert not torn, (
        f"assign() applied a torn (non-atomic) snapshot of the partition list: "
        f"{len(torn)} of the assignments mixed both topic namespaces "
        f"(e.g. {torn[:3]}). py_to_c_parts must hold a critical section over "
        f"its parse loop."
    )


@subprocess_isolated
def test_subscribe_races_topic_list_mutation():
    """Consumer.subscribe() borrows each topic str item across
    cfl_PyObject_Unistr(); a concurrent mutation frees the item mid-parse."""
    consumer = Consumer(_CONSUMER_CONF)
    _race(lambda s: consumer.subscribe(s), _topic_names(), _churn(_topic_names))


@subprocess_isolated
def test_list_consumer_group_offsets_non_list_request_is_rejected():
    admin = AdminClient(_CONF)
    internal_future = AdminClient._create_future()
    with pytest.raises((TypeError, ValueError)):
        # super() bypasses AdminClient's own _check_* to reach the cimpl parse.
        super(AdminClient, admin).list_consumer_group_offsets({'not': 'a list'}, internal_future)


@subprocess_isolated
def test_alter_consumer_group_offsets_non_list_request_is_rejected():
    admin = AdminClient(_CONF)
    internal_future = AdminClient._create_future()
    with pytest.raises((TypeError, ValueError)):
        super(AdminClient, admin).alter_consumer_group_offsets({'not': 'a list'}, internal_future)
