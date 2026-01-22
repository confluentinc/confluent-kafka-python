#!/usr/bin/env python

import argparse
import difflib
import os
import re
import subprocess
import sys
import tempfile

# List of directories to convert from async to sync
# Each tuple contains the async directory and its sync counterpart
# If you add a new _async directory and want the _sync directory to be
# generated, you must add it to this list.
ASYNC_TO_SYNC = [
    ("src/confluent_kafka/schema_registry/_async", "src/confluent_kafka/schema_registry/_sync"),
    ("tests/integration/schema_registry/_async", "tests/integration/schema_registry/_sync"),
    ("tests/schema_registry/_async", "tests/schema_registry/_sync"),
]

# Type hint patterns that should NOT have word boundaries (they contain brackets)
TYPE_HINT_SUBS = [
    (r'Coroutine\[Any, Any, ([^\]]+)\]', r'\1'),
    (r'Coroutine\[None, None, ([^\]]+)\]', r'\1'),
    (r'Awaitable\[([^\]]+)\]', r'\1'),
    (r'AsyncIterator\[([^\]]+)\]', r'Iterator[\1]'),
    (r'AsyncIterable\[([^\]]+)\]', r'Iterable[\1]'),
    (r'AsyncGenerator\[([^,]+), ([^\]]+)\]', r'Generator[\1, \2, None]'),
    (r'AsyncContextManager\[([^\]]+)\]', r'ContextManager[\1]'),
]

# Regular substitutions that need word boundaries
SUBS = [
    ('from confluent_kafka.schema_registry.common import asyncinit', ''),
    ('@asyncinit', ''),
    ('import asyncio', ''),
    ('asyncio.sleep', 'time.sleep'),
    ('._async.', '._sync.'),
    ('Async([A-Z][A-Za-z0-9_]*)', r'\2'),
    ('_Async([A-Z][A-Za-z0-9_]*)', r'_\2'),
    ('async_([a-z][A-Za-z0-9_]*)', r'\2'),
    ('async def', 'def'),
    ('await ', ''),
    ('aclose', 'close'),
    ('__aenter__', '__enter__'),
    ('__aexit__', '__exit__'),
    ('__aiter__', '__iter__'),
    (r'asyncio.run\((.*)\)', r'\2'),
]

# Import cleanup patterns - these need special handling to remove items from comma-separated lists
IMPORT_CLEANUP_SUBS = [
    # Remove Awaitable from import lists (with trailing comma and space)
    (r'Awaitable,\s*', ''),
    # Remove Awaitable from import lists (with leading comma and space)
    (r',\s*Awaitable', ''),
    # Remove Coroutine from import lists (with trailing comma and space)
    (r'Coroutine,\s*', ''),
    # Remove Coroutine from import lists (with leading comma and space)
    (r',\s*Coroutine', ''),
]

COMPILED_IMPORT_CLEANUP_SUBS = [(re.compile(regex), repl) for regex, repl in IMPORT_CLEANUP_SUBS]

# Compile type hint patterns without word boundaries
COMPILED_TYPE_HINT_SUBS = [(re.compile(regex), repl) for regex, repl in TYPE_HINT_SUBS]

# Compile regular patterns with word boundaries
COMPILED_SUBS = [(re.compile(r'(^|\b)' + regex + r'($|\b)'), repl) for regex, repl in SUBS]

USED_SUBS = set()


def unasync_line(line):
    # First apply type hint transformations (without word boundaries)
    for regex, repl in COMPILED_TYPE_HINT_SUBS:
        line = re.sub(regex, repl, line)

    # Then apply regular transformations (with word boundaries)
    for index, (regex, repl) in enumerate(COMPILED_SUBS):
        old_line = line
        line = re.sub(regex, repl, line)
        if old_line != line:
            USED_SUBS.add(index)

    # Finally apply import cleanup (to remove unused async-related imports)
    for regex, repl in COMPILED_IMPORT_CLEANUP_SUBS:
        line = re.sub(regex, repl, line)

    return line


def unasync_file(in_path, out_path):
    with open(in_path, "r") as in_file:
        with open(out_path, "w", newline="") as out_file:
            for line in in_file.readlines():
                line = unasync_line(line)
                out_file.write(line)


def unasync_file_check(in_path, out_path):
    """Check if the sync file matches the expected generated content.

    This function generates the expected sync content from the async file,
    formats it using isort and black (to match the project's style),
    and compares it to the actual sync file.

    Args:
        in_path: Path to the async file
        out_path: Path to the sync file

    Returns:
        bool: True if files match, False if they don't

    Raises:
        ValueError: If there's an error reading the files or formatting
    """
    try:
        with open(in_path, "r") as in_file:
            async_content = in_file.read()
            expected_content = "".join(unasync_line(line) for line in async_content.splitlines(keepends=True))

        # Format the expected content to match how it would be formatted after generation
        # Write to a temp file in project root so pyproject.toml is found, format it, then read it back
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        # Create temp file in project root so black/isort can find pyproject.toml
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', dir=project_root, delete=False) as tmp_file:
            tmp_path = tmp_file.name
            tmp_file.write(expected_content)

        try:
            # Format using isort and black (same as style-format.sh does)
            # Use relative path from project root so pyproject.toml config is found
            tmp_rel_path = os.path.relpath(tmp_path, project_root)
            subprocess.run(['python3', '-m', 'isort', tmp_rel_path], cwd=project_root, capture_output=True, check=False)
            subprocess.run(['python3', '-m', 'black', tmp_rel_path], cwd=project_root, capture_output=True, check=False)

            with open(tmp_path, "r") as formatted_file:
                expected_content = formatted_file.read()
        finally:
            os.unlink(tmp_path)

        with open(out_path, "r") as out_file:
            actual_content = out_file.read()

        if actual_content != expected_content:
            diff = difflib.unified_diff(
                expected_content.splitlines(keepends=True),
                actual_content.splitlines(keepends=True),
                fromfile=in_path,
                tofile=out_path,
                n=3,  # Show 3 lines of context
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

        print("Formatting generated _sync files...")
        # Format all generated _sync files
        sync_files = []
        for async_dir, sync_dir in dir_pairs:
            for root, dirs, files in os.walk(sync_dir):
                for file in files:
                    if file.endswith('.py'):
                        sync_files.append(os.path.join(root, file))
        if sync_files:
            # Find project root
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            style_script = os.path.join(script_dir, 'style-format.sh')
            if os.path.exists(style_script):
                # Make sure the script is executable
                os.chmod(style_script, 0o755)
                rel_sync_files = [os.path.relpath(f, project_root) if os.path.isabs(f) else f for f in sync_files]
                result = subprocess.run(
                    ['bash', style_script, '--fix'] + rel_sync_files, cwd=project_root, capture_output=True, text=True
                )
                if result.returncode == 0:
                    print("✅ Formatted generated _sync files.")
                else:
                    print(f"⚠️  Warning: Formatting had issues: {result.stderr}")
            else:
                print("⚠️  Warning: style-format.sh not found, skipping formatting.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert async code to sync code')
    parser.add_argument(
        '--check', action='store_true', help='Exit with non-zero status if sync directory has any differences'
    )
    parser.add_argument('--file', type=str, help='Convert a single file instead of all directories')
    args = parser.parse_args()

    if args.file:
        # Single file mode
        async_file = args.file
        if not os.path.exists(async_file):
            print(f"Error: File {async_file} does not exist")
            sys.exit(1)

        # Determine the sync file path
        sync_file = None
        for async_dir, sync_dir in ASYNC_TO_SYNC:
            if async_file.startswith(async_dir):
                sync_file = async_file.replace(async_dir, sync_dir, 1)
                break

        if not sync_file:
            print(f"Error: File {async_file} is not in a known async directory")
            print(f"Known async directories: {[d[0] for d in ASYNC_TO_SYNC]}")
            sys.exit(1)

        # Create the output directory if needed
        os.makedirs(os.path.dirname(sync_file), exist_ok=True)

        print(f"Converting: {async_file} -> {sync_file}")
        unasync_file(async_file, sync_file)
        print("✅ Done!")
    else:
        # Directory mode (original behavior)
        unasync(check=args.check)
