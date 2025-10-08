### KIP-848 - Migration Guide

#### Overview

- **What changed:**

  The **Group Leader role** (consumer member) is removed. Assignments are calculated by the **Group Coordinator (broker)** and distributed via **heartbeats**.

- **Requirements:**

  - Broker version **4.0.0+**
  - confluent-kafka-python version **2.12.0+**: GA (production-ready)

- **Enablement (client-side):**

  - `group.protocol=consumer`
  - `group.remote.assignor=<assignor>` (optional; broker-controlled if `NULL`; default broker assignor is `uniform`)

#### Available Features

All KIP-848 features are supported including:

- Subscription to one or more topics, including **regular expression (regex) subscriptions**
- Rebalance callbacks (**incremental only**)
- Static group membership
- Configurable remote assignor
- Enforced max poll interval
- Upgrade from `classic` protocol or downgrade from `consumer` protocol
- AdminClient changes as per KIP

#### Contract Changes

##### Client Configuration changes

| Classic Protocol (Deprecated Configs in KIP-848) | KIP-848 / Next-Gen Replacement                        |
|--------------------------------------------------|-------------------------------------------------------|
| `partition.assignment.strategy`                  | `group.remote.assignor`                               |
| `session.timeout.ms`                             | Broker config: `group.consumer.session.timeout.ms`    |
| `heartbeat.interval.ms`                          | Broker config: `group.consumer.heartbeat.interval.ms` |
| `group.protocol.type`                            | Not used in the new protocol                          |

**Note:** The properties listed under “Classic Protocol (Deprecated Configs in KIP-848)” are **no longer used** when using the KIP-848 consumer protocol.

##### Rebalance Callback Changes

- The **protocol is fully incremental** in KIP-848.
- In the **rebalance callbacks**, you **must use**:
  - `consumer.incremental_assign(partitions)` to assign new partitions
  - `consumer.incremental_unassign(partitions)` to revoke partitions
- **Do not** use `consumer.assign()` or `consumer.unassign()` when using `group.protocol='consumer'` (KIP-848).
- ⚠️ The `partitions` list passed to `incremental_assign()` and `incremental_unassign()` contains only the **incremental changes** — partitions being **added** or **revoked** — **not the full assignment**, as was the case with `assign()` in the classic protocol.
- All assignors under KIP-848 are now **sticky**, including `range`, which was **not sticky** in the classic protocol.

##### Static Group Membership

- Duplicate `group.instance.id` handling:
  - **Newly joining member** is fenced with **UNRELEASED_INSTANCE_ID (fatal)**.
  - (Classic protocol fenced the **existing** member instead.)
- Implications:
  - Ensure only **one active instance per** `group.instance.id`.
  - Consumers must shut down cleanly to avoid blocking replacements until session timeout expires.

##### Session Timeout & Fetching

- **Session timeout is broker-controlled**:
  - If the Coordinator is unreachable, a consumer **continues fetching messages** but cannot commit offsets.
  - Consumer is fenced once a heartbeat response is received from the Coordinator.
- In the classic protocol, the client stopped fetching when session timeout expired.

##### Closing / Auto-Commit

- On `close()` or unsubscribe with auto-commit enabled:
  - Member retries committing offsets until a timeout expires.
  - Currently uses the **default remote session timeout**.
  - Future **KIP-1092** will allow custom commit timeouts.

##### Error Handling Changes

- `UNKNOWN_TOPIC_OR_PART` (**subscription case**):
  - No longer returned if a topic is missing in the **local cache** when subscribing; the subscription proceeds.
- `TOPIC_AUTHORIZATION_FAILED`:
  - Reported once per heartbeat or subscription change, even if only one topic is unauthorized.

##### Summary of Key Differences (Classic vs Next-Gen)

- **Assignment:** Classic protocol calculated by **Group Leader (consumer)**; KIP-848 calculated by **Group Coordinator (broker)**
- **Assignors:** Classic range assignor was **not sticky**; KIP-848 assignors are **sticky**, including range
- **Deprecated configs:** Classic client configs are replaced by `group.remote.assignor` and broker-controlled session/heartbeat configs
- **Static membership fencing:** KIP-848 fences **new member** on duplicate `group.instance.id`
- **Session timeout:** Classic enforced on client; KIP-848 enforced on broker
- **Auto-commit on close:** Classic stops at client session timeout; KIP-848 retries until remote timeout
- **Unknown topics:** KIP-848 does not return error on subscription if topic missing
- **Upgrade/Downgrade:** KIP-848 supports upgrade/downgrade from/to `classic` and `consumer` protocols

#### Minimal Example Config

##### Classic Protocol

``` properties
# Optional; default is 'classic'
group.protocol=classic

partition.assignment.strategy=<range,roundrobin,sticky>
session.timeout.ms=45000
heartbeat.interval.ms=15000
```

##### Next-Gen Protocol / KIP-848

``` properties
group.protocol=consumer

# Optional: select a remote assignor
# Valid options currently: 'uniform' or 'range'
#   group.remote.assignor=<uniform,range>
# If unset(NULL), broker chooses the assignor (default: 'uniform')

# Session & heartbeat now controlled by broker:
#   group.consumer.session.timeout.ms
#   group.consumer.heartbeat.interval.ms
```

#### Rebalance Callback Migration

##### Range Assignor (Classic)

``` python
# Rebalance Callback for Range Assignor (Classic Protocol)
def on_assign(consumer, partitions):
    # Full partition list is provided under the classic protocol
    print(f"[Classic] Assigned partitions: {partitions}")
    consumer.assign(partitions)

def on_revoke(consumer, partitions):
    print(f"[Classic] Revoked partitions: {partitions}")
    consumer.unassign()
```

##### Incremental Assignor (Including Range in Consumer / KIP-848, Any Protocol)

``` python
# Rebalance callback for incremental assignor
def on_assign(consumer, partitions):
    # Only incremental partitions are passed here (not full list)
    print(f"[KIP-848] Incrementally assigning: {partitions}")
    consumer.incremental_assign(partitions)

def on_revoke(consumer, partitions):
    print(f"[KIP-848] Incrementally revoking: {partitions}")
    consumer.incremental_unassign(partitions)
```

**Note:** The `partitions` list contains **only partitions being added or revoked**, not the full partition list as in the classic `consumer.assign()`.

#### Upgrade and Downgrade

- A group made up entirely of `classic` consumers runs under the classic protocol.
- The group is **upgraded to the consumer protocol** as soon as at least one `consumer` protocol member joins.
- The group is **downgraded back to the classic protocol** if the last `consumer` protocol member leaves while `classic` members remain.
- Both **rolling upgrade** (classic → consumer) and **rolling downgrade** (consumer → classic) are supported.

#### Migration Checklist (Next-Gen Protocol / KIP-848)

1.  Upgrade to **confluent-kafka-python ≥ 2.12.0** (GA release)
2.  Run against **Kafka brokers ≥ 4.0.0**
3.  Set `group.protocol=consumer`
4.  Optionally set `group.remote.assignor`; leave `NULL` for broker-controlled (default: `uniform`), valid options: `uniform` or `range`
5.  Replace deprecated configs with new ones
6.  Update rebalance callbacks to **incremental APIs only**
7.  Review static membership handling (`group.instance.id`)
8.  Ensure proper shutdown to avoid fencing issues
9.  Adjust error handling for unknown topics and authorization failures
