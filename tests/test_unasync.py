import os
import tempfile

import pytest

from tools.unasync import unasync, unasync_file_check, unasync_line


@pytest.fixture
def temp_dirs():
    """Create temporary directories for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create async and sync directories
        async_dir = os.path.join(temp_dir, "async")
        sync_dir = os.path.join(temp_dir, "sync")
        os.makedirs(async_dir)
        os.makedirs(sync_dir)
        yield async_dir, sync_dir


def test_unasync_line():
    """Test the unasync_line function with various inputs."""
    test_cases = [
        ("async def test():", "def test():"),
        ("await some_func()", "some_func()"),
        ("from confluent_kafka.schema_registry.common import asyncinit", ""),
        ("@asyncinit", ""),
        ("import asyncio", ""),
        ("asyncio.sleep(1)", "time.sleep(1)"),
        ("class AsyncTest:", "class Test:"),
        ("class _AsyncTest:", "class _Test:"),
        ("async_test_func", "test_func"),
    ]

    for input_line, expected in test_cases:
        assert unasync_line(input_line) == expected


def test_unasync_file_check(temp_dirs):
    """Test the unasync_file_check function with various scenarios."""
    async_dir, sync_dir = temp_dirs

    # Test case 1: Files match
    async_file = os.path.join(async_dir, "test1.py")
    sync_file = os.path.join(sync_dir, "test1.py")
    os.makedirs(os.path.dirname(sync_file), exist_ok=True)

    with open(async_file, "w") as f:
        f.write(
            """async def test():
    await asyncio.sleep(1)
"""
        )

    with open(sync_file, "w") as f:
        f.write(
            """def test():
    time.sleep(1)
"""
        )

    # This should return True
    assert unasync_file_check(async_file, sync_file) is True

    # Test case 2: Files don't match
    async_file = os.path.join(async_dir, "test2.py")
    sync_file = os.path.join(sync_dir, "test2.py")
    os.makedirs(os.path.dirname(sync_file), exist_ok=True)

    with open(async_file, "w") as f:
        f.write(
            """async def test():
    await asyncio.sleep(1)
"""
        )

    with open(sync_file, "w") as f:
        f.write(
            """def test():
    # This is wrong
    asyncio.sleep(1)
"""
        )

    # This should return False
    assert unasync_file_check(async_file, sync_file) is False

    # Test case 3: Files have different lengths
    async_file = os.path.join(async_dir, "test3.py")
    sync_file = os.path.join(sync_dir, "test3.py")
    os.makedirs(os.path.dirname(sync_file), exist_ok=True)

    with open(async_file, "w") as f:
        f.write(
            """async def test():
    await asyncio.sleep(1)
    return "test"
"""
        )

    with open(sync_file, "w") as f:
        f.write(
            """def test():
    time.sleep(1)
"""
        )

    # This should return False
    assert unasync_file_check(async_file, sync_file) is False

    # Test case 4: File not found
    with pytest.raises(ValueError, match="Error comparing"):
        unasync_file_check("nonexistent.py", "also_nonexistent.py")


def test_unasync_generation(temp_dirs):
    """Test the unasync generation functionality."""
    async_dir, sync_dir = temp_dirs

    # Create a test async file
    test_file = os.path.join(async_dir, "test.py")
    with open(test_file, "w") as f:
        f.write(
            """async def test_func():
    await asyncio.sleep(1)
    return "test"

class AsyncTest:
    async def test_method(self):
        await self.some_async()
"""
        )

    # Run unasync with test directories
    dir_pairs = [(async_dir, sync_dir)]
    unasync(dir_pairs=dir_pairs, check=False)

    # Check if sync file was created
    sync_file = os.path.join(sync_dir, "test.py")
    assert os.path.exists(sync_file)

    # Check content
    with open(sync_file, "r") as f:
        content = f.read()
        assert "async def" not in content
        assert "await" not in content
        assert "AsyncTest" not in content
        assert "Test" in content

    # Check if README was created
    readme_file = os.path.join(sync_dir, "README.md")
    assert os.path.exists(readme_file)
    with open(readme_file, "r") as f:
        readme_content = f.read()
        assert "Auto-generated Directory" in readme_content
        assert "Do not edit these files directly" in readme_content


def test_unasync_check(temp_dirs):
    """Test the unasync check functionality."""
    async_dir, sync_dir = temp_dirs

    # Create a test async file
    test_file = os.path.join(async_dir, "test.py")
    with open(test_file, "w") as f:
        f.write(
            """async def test_func():
    await asyncio.sleep(1)
    return "test"
"""
        )

    # Create an incorrect sync file
    sync_file = os.path.join(sync_dir, "test.py")
    os.makedirs(os.path.dirname(sync_file), exist_ok=True)
    with open(sync_file, "w") as f:
        f.write(
            """def test_func():
    time.sleep(1)
    return "test"
    # Extra line that shouldn't be here
"""
        )

    # Run unasync check with test directories
    dir_pairs = [(async_dir, sync_dir)]
    with pytest.raises(SystemExit) as excinfo:
        unasync(dir_pairs=dir_pairs, check=True)
    assert excinfo.value.code == 1


def test_unasync_missing_sync_file(temp_dirs):
    """Test unasync check with missing sync files."""
    async_dir, sync_dir = temp_dirs

    # Create a test async file
    test_file = os.path.join(async_dir, "test.py")
    with open(test_file, "w") as f:
        f.write(
            """async def test_func():
    await asyncio.sleep(1)
    return "test"
"""
        )

    # Run unasync check with test directories
    dir_pairs = [(async_dir, sync_dir)]
    with pytest.raises(SystemExit) as excinfo:
        unasync(dir_pairs=dir_pairs, check=True)
    assert excinfo.value.code == 1
