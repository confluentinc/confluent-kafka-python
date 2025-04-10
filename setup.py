#!/usr/bin/env python

import os
import platform
import re
import sys
from setuptools import setup
from setuptools import Extension
from setuptools.command.build_py import build_py as _build_py
from pprint import pprint

work_dir = os.path.dirname(os.path.realpath(__file__))
mod_dir = os.path.join(work_dir, 'src', 'confluent_kafka')
ext_dir = os.path.join(mod_dir, 'src')

# On Un*x the library is linked as -lrdkafka,
# while on windows we need the full librdkafka name.
if platform.system() == 'Windows':
    librdkafka_libname = 'librdkafka'
else:
    librdkafka_libname = 'rdkafka'

module = Extension('confluent_kafka.cimpl',
                   libraries=[librdkafka_libname],
                   sources=[os.path.join(ext_dir, 'confluent_kafka.c'),
                            os.path.join(ext_dir, 'Producer.c'),
                            os.path.join(ext_dir, 'Consumer.c'),
                            os.path.join(ext_dir, 'Metadata.c'),
                            os.path.join(ext_dir, 'AdminTypes.c'),
                            os.path.join(ext_dir, 'Admin.c')])

SUBS = [
    ('from .._backends.auto import AutoBackend', 'from .._backends.sync import SyncBackend'),
    ('import asyncio', ''),
    ('AsyncIterator', 'Iterator'),
    ('Async([A-Z][A-Za-z0-9_]*)', r'\2'),
    ('_Async([A-Z][A-Za-z0-9_]*)', r'_\2'),
    ('async_schema_registry', 'schema_registry'),
    ('async def', 'def'),
    ('async with', 'with'),
    ('async for', 'for'),
    ('await ', ''),
    ('handle_async_request', 'handle_request'),
    ('aclose', 'close'),
    ('aiter_stream', 'iter_stream'),
    ('aread', 'read'),
    ('asynccontextmanager', 'contextmanager'),
    ('__aenter__', '__enter__'),
    ('__aexit__', '__exit__'),
    ('__aiter__', '__iter__'),
    ('asyncio.sleep', 'time.sleep'),
    ('@asyncinit', ''),
    ('from confluent_kafka.schema_registry.async_util import asyncinit', ''),
    ('__await__', '__call__'),
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

def unasync():
    unasync_dir(
        "src/confluent_kafka/schema_registry/_async",
        "src/confluent_kafka/schema_registry",
        check_only=False
    )
    unasync_dir(
        "tests/integration/schema_registry/_async",
        "tests/integration/schema_registry",
        check_only=False
    )


    if len(USED_SUBS) != len(SUBS):
        unused_subs = [SUBS[i] for i in range(len(SUBS)) if i not in USED_SUBS]

        print("These patterns were not used:")
        pprint(unused_subs)


class build_py(_build_py):
    """
    Subclass build_py from setuptools to modify its behavior.

    Convert files in _async dir from being asynchronous to synchronous
    and saves them to the specified output directory.
    """

    def run(self):
        self._updated_files = []

        # Base class code
        if self.py_modules:
            self.build_modules()
        if self.packages:
            self.build_packages()
            self.build_package_data()

        # Our modification
        unasync()

        # Remaining base class code
        self.byte_compile(self.get_outputs(include_bytecode=0))

    def build_module(self, module, module_file, package):
        outfile, copied = super().build_module(module, module_file, package)
        if copied:
            self._updated_files.append(outfile)
        return outfile, copied


setup(
    ext_modules=[module],
    cmdclass={
        'build_py': build_py,
    }
)
