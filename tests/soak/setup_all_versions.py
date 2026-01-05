#!/usr/bin/env python3
import os
import subprocess

PYTHON_SOAK_TEST_BRANCH = 'master'

LIBRDKAFKA_VERSIONS = [
    '2.13.0',
    '2.12.1',
    '2.11.1',
    '2.10.1',
    '2.8.0',
    '2.6.1',
    '2.5.3',
    '2.4.0',
    '2.3.0',
    '2.2.0',
    '2.1.1',
    '2.0.2',
]

PYTHON_VERSIONS = ['2.13.0', '2.12.1', '2.11.1', '2.10.1', '2.8.0', '2.6.1', '2.5.3', '2.4.0', '2.3.0', '2.2.0', '2.1.1', '2.0.2']


def run(command):
    print(f'Running command: {command}')
    env = os.environ.copy()
    env['MAKEFLAGS'] = 'SHELL=/bin/bash -j8'  # For parallel make jobs
    result = subprocess.run(command, shell=True, env=env)
    if result.returncode != 0:
        raise Exception(f'Command failed: {command}, result: {result.returncode}')


if __name__ == '__main__':
    for i, librdkafka_version in enumerate(LIBRDKAFKA_VERSIONS):
        python_version = PYTHON_VERSIONS[i]
        librdkafka_version_folder_name = librdkafka_version.replace('.', '_')
        folder_name = f'confluent-kafka-python_{librdkafka_version_folder_name}'
        if os.path.exists(folder_name):
            print(f'Skipping \'{folder_name}\', already exists.')
            continue

        run('git clone https://github.com/confluentinc/confluent-kafka-python.git')
        run(f'mv confluent-kafka-python {folder_name}')
        run(f'cd {folder_name} && git checkout {PYTHON_SOAK_TEST_BRANCH}')
        run(f'cd {folder_name}/tests/soak && ./build.sh v{librdkafka_version} v{python_version}')
        run(f'cp config/*.config {folder_name}/tests/soak')
