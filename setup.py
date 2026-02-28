#!/usr/bin/env python

import os
import platform
import shutil

from setuptools import Extension, setup

work_dir = os.path.dirname(os.path.realpath(__file__))
mod_dir = os.path.join(work_dir, 'src', 'confluent_kafka')
ext_dir = os.path.join(mod_dir, 'src')

# On Un*x the library is linked as -lrdkafka,
# while on windows we need the full librdkafka name.
if platform.system() == 'Windows':
    librdkafka_libname = 'librdkafka'
else:
    librdkafka_libname = 'rdkafka'

# Use LIBRDKAFKA_DIR environment variable on Windows ARM64
include_dirs = []
library_dirs = []

librdkafka_dir = os.environ.get('LIBRDKAFKA_DIR')
if platform.system() == 'Windows' and librdkafka_dir:
    include_dirs = [os.path.join(librdkafka_dir, 'include', 'librdkafka'),
                    os.path.join(librdkafka_dir, 'include')]
    library_dirs = [os.path.join(librdkafka_dir, 'lib')]
    
    # Auto-detect library name[CMake uses 'rdkafka', NuGet uses 'librdkafka']
    lib_dir = library_dirs[0]
    if os.path.exists(os.path.join(lib_dir, 'rdkafka.lib')):
        librdkafka_libname = 'rdkafka'
    
    # Copy DLLs to package directory for bundling
    dll_dir = os.path.join(librdkafka_dir, 'bin')
    if os.path.exists(dll_dir):
        for dll_file in os.listdir(dll_dir):
            if dll_file.endswith('.dll'):
                src = os.path.join(dll_dir, dll_file)
                dst = os.path.join(mod_dir, dll_file)
                if not os.path.exists(dst) or os.path.getmtime(src) > os.path.getmtime(dst):
                    print(f"Copying {dll_file} to package directory")
                    shutil.copy2(src, dst)

module = Extension(
    'confluent_kafka.cimpl',
    libraries=[librdkafka_libname],
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    sources=[
        os.path.join(ext_dir, 'confluent_kafka.c'),
        os.path.join(ext_dir, 'Producer.c'),
        os.path.join(ext_dir, 'Consumer.c'),
        os.path.join(ext_dir, 'Metadata.c'),
        os.path.join(ext_dir, 'AdminTypes.c'),
        os.path.join(ext_dir, 'Admin.c'),
    ],
)

setup(ext_modules=[module])
