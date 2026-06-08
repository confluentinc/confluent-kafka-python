#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Schema Registry / governance role for the demo.
#
# This script never produces or consumes — it only manages the *contract*.
# Run the steps in order:
#
#     python schema_registry.py status        # what's registered right now
#     python schema_registry.py register-v1   # publish the v1 contract + set BACKWARD
#     python schema_registry.py check-v2       # test good vs bad v2; watch the registry REJECT the bad one
#     python schema_registry.py register-v2    # publish the (compatible) v2 contract
#     python schema_registry.py loosen         # optional: set NONE, register the bad schema, then restore
#     python schema_registry.py reset          # delete the subject for a clean slate

import argparse

from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

from _common import SCHEMAS, SUBJECT, schema_registry_client


def status(sr):
    subjects = sr.get_subjects()
    print("subjects:", subjects)
    if SUBJECT in subjects:
        print("versions:", sr.get_versions(SUBJECT))
        print("compatibility:", sr.get_compatibility(SUBJECT))
        latest = sr.get_latest_version(SUBJECT)
        print(f"latest: v{latest.version} (id={latest.schema_id})")


def register_v1(sr):
    schema_id = sr.register_schema(SUBJECT, Schema(SCHEMAS["v1"], "AVRO"))
    sr.set_compatibility(SUBJECT, "BACKWARD")
    print(f"registered v1 under '{SUBJECT}' -> id {schema_id}")
    print("compatibility policy set to:", sr.get_compatibility(SUBJECT))


def check_v2(sr):
    good = sr.test_compatibility(SUBJECT, Schema(SCHEMAS["v2"], "AVRO"))
    print(f"  v2     (adds 'email' with default)   compatible? {good}")
    bad = sr.test_compatibility(SUBJECT, Schema(SCHEMAS["v2_bad"], "AVRO"))
    print(f"  v2_bad (adds required 'phone')        compatible? {bad}")

    print("\nNow trying to actually register the incompatible schema...")
    try:
        sr.register_schema(SUBJECT, Schema(SCHEMAS["v2_bad"], "AVRO"))
        print("  (unexpected) the registry accepted it")
    except SchemaRegistryError as e:
        print(f"  REJECTED by the registry -> {e}")


def register_v2(sr):
    schema_id = sr.register_schema(SUBJECT, Schema(SCHEMAS["v2"], "AVRO"))
    print(f"registered v2 -> id {schema_id}; versions now {sr.get_versions(SUBJECT)}")


def loosen(sr):
    print("setting compatibility to NONE...")
    sr.set_compatibility(SUBJECT, "NONE")
    schema_id = sr.register_schema(SUBJECT, Schema(SCHEMAS["v2_bad"], "AVRO"))
    print(f"the previously-rejected schema now registers -> id {schema_id}; versions {sr.get_versions(SUBJECT)}")
    sr.set_compatibility(SUBJECT, "BACKWARD")
    print("restored compatibility policy to BACKWARD")


def reset(sr):
    try:
        sr.delete_subject(SUBJECT)  # soft delete
        sr.delete_subject(SUBJECT, permanent=True)  # hard delete
        print(f"deleted subject '{SUBJECT}' (clean slate)")
    except SchemaRegistryError as e:
        print(f"nothing to delete ({e})")


COMMANDS = {
    "status": status,
    "register-v1": register_v1,
    "check-v2": check_v2,
    "register-v2": register_v2,
    "loosen": loosen,
    "reset": reset,
}


def main():
    parser = argparse.ArgumentParser(description="Schema Registry governance role for the demo")
    parser.add_argument("command", choices=COMMANDS, help="demo step to run")
    args = parser.parse_args()

    sr = schema_registry_client()
    COMMANDS[args.command](sr)


if __name__ == "__main__":
    main()
