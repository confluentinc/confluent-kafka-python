#!/usr/bin/env python3
"""
Generate KafkaError codes to be exposed in KafkaError stub class in cimpl.pyi

This script introspects the compiled C extension to extract all error codes
and generates the KafkaError class error codes

Usage:
    python3 tools/generate_kafka_error_stub_codes.py          # Prints the error codes expected
    python3 tools/generate_kafka_error_stub_codes.py --check  # Prints the expected vs actual diff
"""

import argparse
import os
import re
import sys


def get_kafka_error_constants():
    """Get all error code constants from the compiled cimpl module."""
    try:
        from confluent_kafka import cimpl
    except ImportError as e:
        print(f"Error: Cannot import confluent_kafka.cimpl: {e}")
        print("Make sure the package is built and installed.")
        sys.exit(1)

    # Get all KafkaError attributes
    all_attrs = dir(cimpl.KafkaError)

    # Methods that should not be treated as constants
    methods = {'code', 'fatal', 'name', 'retriable', 'str', 'txn_requires_abort'}

    # Filter to get only constants (not methods, not dunders)
    constants = sorted([
        attr for attr in all_attrs
        if not attr.startswith('__') and attr not in methods
    ])

    return constants


def check_cimpl_pyi(stub_path, constants):
    """Check if the KafkaError class in cimpl.pyi has all error codes.

    Args:
        stub_path: Path to the cimpl.pyi file
        constants: List of error code constant names

    Returns:
        bool: True if file is up to date, False if differences found
    """
    # Read existing stub file
    try:
        with open(stub_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: {stub_path} not found")
        sys.exit(1)

    # Find the KafkaError class definition
    # Match from "class KafkaError:" to the next class definition
    class_pattern = r'(class KafkaError:.*?)(\nclass \w+)'
    match = re.search(class_pattern, content, re.DOTALL)

    if not match:
        print(f"Error: Could not find KafkaError class in {stub_path}")
        sys.exit(1)

    # Extract the class body
    class_body = match.group(1)

    # Extract all error code constants from the stub file
    # Pattern matches lines like "    CONSTANT_NAME: int"
    stub_constants = []
    for line in class_body.split('\n'):
        # Match attribute definitions (not methods or special methods)
        attr_match = re.match(r'^\s+([A-Z_][A-Z0-9_]*)\s*:\s*int\s*$', line)
        if attr_match:
            stub_constants.append(attr_match.group(1))

    # For comparison
    expected_set = set(constants)
    actual_set = set(stub_constants)

    # Check for differences
    missing = expected_set - actual_set
    extra = actual_set - expected_set

    if missing or extra:
        if missing:
            print(f"\nMissing constants (in compiled module but not in stub):")
            for const in sorted(missing):
                print(f"  + {const}: int")
        if extra:
            print(f"\nExtra constants (in stub but not in compiled module):")
            for const in sorted(extra):
                print(f"  - {const}: int")
        return False

    return True



def main():
    parser = argparse.ArgumentParser(
        description='Generate KafkaError error codes for cimpl.pyi stub file'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check if cimpl.pyi has differences and print diff'
    )
    args = parser.parse_args()

    # Get error constants from compiled module
    constants = get_kafka_error_constants()

    if args.check:
        # Find the stub file path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        stub_path = os.path.join(project_root, 'src', 'confluent_kafka', 'cimpl.pyi')

        print("Checking if KafkaError stubs are up to date...")
        is_up_to_date = check_cimpl_pyi(stub_path, constants)

        if not is_up_to_date:
            print("\n⚠️  Differences found in KafkaError constants")
            print("\nPlease run this script again (without the --check flag) to generate the error codes "
                  "and update the KafkaError class in cimpl.pyi stub file")
            sys.exit(1)
        else:
            print("✅ KafkaError stubs are up to date!")
    else:
        # Default: Just print the error codes expected
        print("Expected KafkaError error code constants:")
        print()
        for const in constants:
            print(f"    {const}: int")
        print()
        print(f"Total: {len(constants)} error code constants")


if __name__ == '__main__':
    main()