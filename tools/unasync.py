#!/usr/bin/env python

import os
import re
import sys
import argparse
from pprint import pprint
import subprocess

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
    with open(in_path, "r") as in_file:
        with open(out_path, "r") as out_file:
            for in_line, out_line in zip(in_file.readlines(), out_file.readlines()):
                expected = unasync_line(in_line)
                if out_line != expected:
                    print(f'unasync mismatch between {in_path!r} and {out_path!r}')
                    print(f'Async code:         {in_line!r}')
                    print(f'Expected sync code: {expected!r}')
                    print(f'Actual sync code:   {out_line!r}')
                    sys.exit(1)


def unasync_dir(in_dir, out_dir, check_only=False):
    for dirpath, dirnames, filenames in os.walk(in_dir):
        for filename in filenames:
            if not filename.endswith('.py'):
                continue
            rel_dir = os.path.relpath(dirpath, in_dir)
            in_path = os.path.normpath(os.path.join(in_dir, rel_dir, filename))
            out_path = os.path.normpath(os.path.join(out_dir, rel_dir, filename))
            print(in_path, '->', out_path)
            if check_only:
                unasync_file_check(in_path, out_path)
            else:
                unasync_file(in_path, out_path)

def check_diff(sync_dir):
    """Check if there are any differences in the sync directory.
    Returns a list of files that have differences."""
    try:
        # Get the list of files in the sync directory
        result = subprocess.run(['git', 'ls-files', sync_dir], 
                              capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error listing files in {sync_dir}")
            return []
            
        files = result.stdout.strip().split('\n')
        if not files or (len(files) == 1 and not files[0]):
            print(f"No files found in {sync_dir}")
            return []

        # Check if any of these files have differences
        files_with_diff = []
        for file in files:
            if not file:  # Skip empty lines
                continue
            diff_result = subprocess.run(['git', 'diff', '--quiet', file], 
                                       capture_output=True, text=True)
            if diff_result.returncode != 0:
                files_with_diff.append(file)
        return files_with_diff
    except subprocess.CalledProcessError as e:
        print(f"Error checking differences: {e}")
        return []

def unasync(check=False):
    async_dirs = [
        "src/confluent_kafka/schema_registry/_async",
        "tests/integration/schema_registry/_async"
    ]
    sync_dirs = [
        "src/confluent_kafka/schema_registry/_sync",
        "tests/integration/schema_registry/_sync"
    ]

    print("Converting async code to sync code...")
    for async_dir, sync_dir in zip(async_dirs, sync_dirs):
        unasync_dir(async_dir, sync_dir, check_only=False)

    files_with_diff = []
    if check:
        for sync_dir in sync_dirs:
            files_with_diff.extend(check_diff(sync_dir))

    if files_with_diff:
        print("\n⚠️  Detected changes to a _sync directory that are uncommitted.")
        print("\nFiles with differences:")
        for file in files_with_diff:
            print(f"  - {file}")
        print("\nPlease either:")
        print("1. Commit the changes in the generated _sync files, or")
        print("2. Revert the changes in the original _async files")
        sys.exit(1)
    else:
        print("\n✅ Conversion completed successfully!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert async code to sync code')
    parser.add_argument('--check', action='store_true', 
                      help='Exit with non-zero status if sync directory has any differences')
    args = parser.parse_args()
    unasync(check=args.check)
