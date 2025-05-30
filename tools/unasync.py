#!/usr/bin/env python

import os
import re
import sys
import argparse
import difflib

# List of directories to convert from async to sync
# Each tuple contains the async directory and its sync counterpart
# If you add a new _async directory and want the _sync directory to be
# generated, you must add it to this list.
ASYNC_TO_SYNC = [
    ("src/confluent_kafka/schema_registry/_async", "src/confluent_kafka/schema_registry/_sync"),
    ("tests/integration/schema_registry/_async", "tests/integration/schema_registry/_sync")
]

SUBS = [
    ('from confluent_kafka.schema_registry.common import asyncinit', ''),
    ('@asyncinit', ''),
    ('import asyncio', ''),
    ('asyncio.sleep', 'time.sleep'),

    ('Async([A-Z][A-Za-z0-9_]*)', r'\2'),
    ('_Async([A-Z][A-Za-z0-9_]*)', r'_\2'),
    ('async_([a-z][A-Za-z0-9_]*)', r'\2'),

    ('async def', 'def'),
    ('await ', ''),
    ('aclose', 'close'),
    ('__aenter__', '__enter__'),
    ('__aexit__', '__exit__'),
    ('__aiter__', '__iter__'),
]

COMPILED_SUBS = [
    (re.compile(r'(^|\b)' + regex + r'($|\b)'), repl)
    for regex, repl in SUBS
]

USED_SUBS = set()


def unasync_line(line):
    for index, (regex, repl) in enumerate(COMPILED_SUBS):
        old_line = line
        line = re.sub(regex, repl, line)
        if old_line != line:
            USED_SUBS.add(index)
    return line


def unasync_file(in_path, out_path):
    with open(in_path, "r") as in_file:
        with open(out_path, "w", newline="") as out_file:
            for line in in_file.readlines():
                line = unasync_line(line)
                out_file.write(line)


def unasync_file_check(in_path, out_path):
    """Check if the sync file matches the expected generated content.

    Args:
        in_path: Path to the async file
        out_path: Path to the sync file

    Returns:
        bool: True if files match, False if they don't

    Raises:
        ValueError: If there's an error reading the files
    """
    try:
        with open(in_path, "r") as in_file:
            async_content = in_file.read()
            expected_content = "".join(unasync_line(line) for line in async_content.splitlines(keepends=True))

        with open(out_path, "r") as out_file:
            actual_content = out_file.read()

        if actual_content != expected_content:
            diff = difflib.unified_diff(
                expected_content.splitlines(keepends=True),
                actual_content.splitlines(keepends=True),
                fromfile=in_path,
                tofile=out_path,
                n=3  # Show 3 lines of context
            )
            print(''.join(diff))
            return False
        return True
    except Exception as e:
        print(f"Error comparing {in_path} and {out_path}: {e}")
        raise ValueError(f"Error comparing {in_path} and {out_path}: {e}")


def check_sync_files(dir_pairs):
    """Check if all sync files match their expected generated content.
    Returns a list of files that have differences."""
    files_with_diff = []

    for async_dir, sync_dir in dir_pairs:
        for dirpath, _, filenames in os.walk(async_dir):
            for filename in filenames:
                if not filename.endswith('.py'):
                    continue
                rel_dir = os.path.relpath(dirpath, async_dir)
                async_path = os.path.normpath(os.path.join(async_dir, rel_dir, filename))
                sync_path = os.path.normpath(os.path.join(sync_dir, rel_dir, filename))

                if not os.path.exists(sync_path):
                    files_with_diff.append(sync_path)
                    continue

                if not unasync_file_check(async_path, sync_path):
                    files_with_diff.append(sync_path)

    return files_with_diff


def unasync_dir(in_dir, out_dir):
    # Create the output directory if it doesn't exist
    os.makedirs(out_dir, exist_ok=True)

    # Create README.md in the sync directory
    readme_path = os.path.join(out_dir, "README.md")
    readme_content = """# Auto-generated Directory

This directory contains auto-generated code. Do not edit these files directly.

To make changes:
1. Edit the corresponding files in the sibling `_async` directory
2. Run `python tools/unasync.py` to propagate the changes to this `_sync` directory
"""
    with open(readme_path, "w") as f:
        f.write(readme_content)

    for dirpath, _, filenames in os.walk(in_dir):
        for filename in filenames:
            if not filename.endswith('.py'):
                continue
            rel_dir = os.path.relpath(dirpath, in_dir)
            in_path = os.path.normpath(os.path.join(in_dir, rel_dir, filename))
            out_path = os.path.normpath(os.path.join(out_dir, rel_dir, filename))
            # Create the subdirectory if it doesn't exist
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            print(in_path, '->', out_path)
            unasync_file(in_path, out_path)


def unasync(dir_pairs=None, check=False):
    """Convert async code to sync code.

    Args:
        dir_pairs: List of (async_dir, sync_dir) tuples to process. If None, uses ASYNC_TO_SYNC.
        check: If True, only check if sync files are up to date without modifying them.
    """
    if dir_pairs is None:
        dir_pairs = ASYNC_TO_SYNC

    files_with_diff = []
    if check:
        files_with_diff = check_sync_files(dir_pairs)

    if files_with_diff:
        print("\n⚠️  Detected differences between async and sync files.")
        print("\nFiles that need to be regenerated:")
        for file in files_with_diff:
            print(f"  - {file}")
        print("\nPlease run this script again (without the --check flag) to regenerate the sync files.")
        sys.exit(1)
    else:
        print("\n✅ All _sync directories are up to date!")
    if not check:
        print("Converting async code to sync code...")
        for async_dir, sync_dir in dir_pairs:
            unasync_dir(async_dir, sync_dir)

        print("\n✅ Generated sync code from async code.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert async code to sync code')
    parser.add_argument(
        '--check',
        action='store_true',
        help='Exit with non-zero status if sync directory has any differences')
    args = parser.parse_args()
    unasync(check=args.check)
