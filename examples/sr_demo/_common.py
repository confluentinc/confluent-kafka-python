#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Shared config + schemas for the Schema Registry demo.
#
# The three role scripts (schema_registry.py, producer.py, consumer.py) all
# import from here so the topic, connection coordinates, and — most importantly —
# the v1/v2 schema strings are guaranteed identical across roles.

import os

# Connection coordinates (override via env if you renamed things in the compose file).
BROKERS = os.environ.get("DEMO_BROKERS", "localhost:9092")
SR_URL = os.environ.get("DEMO_SR_URL", "http://localhost:8081")
TOPIC = os.environ.get("DEMO_TOPIC", "demo-user")
SUBJECT = f"{TOPIC}-value"

# Pin the subject-name strategy to TOPIC so the demo doesn't issue an ASSOCIATED
# lookup against an OSS Schema Registry (which would 404 and fall back anyway).
STRATEGY_CONF = {"subject.name.strategy.type": "TOPIC"}

# v1 — the original contract: name, favorite_number, favorite_color.
V1 = """
{"type": "record", "name": "User", "namespace": "io.confluent.demo", "fields": [
  {"name": "name", "type": "string"},
  {"name": "favorite_number", "type": "long"},
  {"name": "favorite_color", "type": "string"}
]}
"""

# v2 (GOOD) — adds 'email' WITH a default. A v2 reader can read old v1 data by
# filling the default, so this is BACKWARD compatible.
V2 = """
{"type": "record", "name": "User", "namespace": "io.confluent.demo", "fields": [
  {"name": "name", "type": "string"},
  {"name": "favorite_number", "type": "long"},
  {"name": "favorite_color", "type": "string"},
  {"name": "email", "type": "string", "default": ""}
]}
"""

# v2 (BAD) — adds a REQUIRED 'phone' with NO default. A v2 reader hitting old v1
# data has no value to use, so this is BACKWARD INCOMPATIBLE and the registry
# will reject it.
V2_BAD = """
{"type": "record", "name": "User", "namespace": "io.confluent.demo", "fields": [
  {"name": "name", "type": "string"},
  {"name": "favorite_number", "type": "long"},
  {"name": "favorite_color", "type": "string"},
  {"name": "phone", "type": "string"}
]}
"""

SCHEMAS = {"v1": V1, "v2": V2, "v2_bad": V2_BAD}


def schema_registry_client():
    """Build a SchemaRegistryClient pointed at the demo registry."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient({"url": SR_URL})
