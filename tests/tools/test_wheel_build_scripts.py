import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(sys.platform == 'win32', reason='Linux wheel scripts require POSIX shell tools')


def command(directory, name, body):
    path = directory / name
    path.write_text('#!/bin/sh\n' + body + '\n')
    path.chmod(0o755)


@pytest.mark.parametrize('machine,arch', [('x86_64', 'x64'), ('aarch64', 'arm64')])
@pytest.mark.parametrize('libc', ['glibc', 'musl'])
def test_prepare_selects_the_matching_libc_without_touching_glibc_builds(tmp_path, machine, arch, libc):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    command(bin_dir, 'ldd', 'echo ' + libc)
    command(bin_dir, 'uname', 'echo ' + machine)
    native = tmp_path / 'dest' / 'runtimes' / ('linux-' + arch) / 'native'
    native.mkdir(parents=True)
    for name, data in [('alpine-librdkafka.so', b'musl'), ('librdkafka.so', b'glibc'), ('librdkafka.so.1', b'glibc')]:
        (native / name).write_bytes(data)

    subprocess.run(
        ['sh', str(PROJECT / 'tools/wheels/prepare-musllinux.sh')],
        cwd=tmp_path,
        env=dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH']),
        check=True,
    )

    expected = b'musl' if libc == 'musl' else b'glibc'
    assert (native / 'librdkafka.so').read_bytes() == expected
    assert (native / 'librdkafka.so.1').read_bytes() == expected


def test_missing_musl_library_fails_instead_of_linking_glibc(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    command(bin_dir, 'ldd', 'echo musl')
    command(bin_dir, 'uname', 'echo x86_64')
    native = tmp_path / 'dest/runtimes/linux-x64/native'
    native.mkdir(parents=True)
    (native / 'librdkafka.so').write_bytes(b'glibc')

    result = subprocess.run(
        ['sh', str(PROJECT / 'tools/wheels/prepare-musllinux.sh')],
        cwd=tmp_path,
        env=dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH']),
        capture_output=True,
    )

    assert result.returncode != 0
    assert (native / 'librdkafka.so').read_bytes() == b'glibc'


def test_verification_requires_the_free_threaded_artifact_too(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    command(bin_dir, 'docker', 'exit 0')
    wheels = tmp_path / 'wheels'
    wheels.mkdir()
    for abi in ('cp38', 'cp39', 'cp310', 'cp311', 'cp312', 'cp313', 'cp314'):
        (wheels / ('confluent_kafka-2.16.1-' + abi + '-' + abi + '-musllinux_1_2_aarch64.whl')).touch()

    result = subprocess.run(
        ['bash', str(PROJECT / 'tools/test-musllinux.sh'), str(wheels)],
        env=dict(
            os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH'], ARCH='arm64', LIBRDKAFKA_VERSION='v2.16.0'
        ),
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert 'Expected exactly one cp314t' in result.stderr
