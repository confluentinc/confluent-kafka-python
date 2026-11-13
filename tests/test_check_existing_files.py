import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import pytest

# The script has a dash in its name, so it can't be imported with a regular import statement.
_SCRIPT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "tools", "check-existing-files.py"))
_spec = importlib.util.spec_from_file_location("check_existing_files", _SCRIPT)
assert _spec is not None
_loader = _spec.loader
assert _loader is not None
check = importlib.util.module_from_spec(_spec)
_loader.exec_module(check)

WHEEL_312 = "confluent_kafka-1.0.0-cp312-cp312-manylinux_2_28_x86_64.whl"
WHEEL_314 = "confluent_kafka-1.0.0-cp314-cp314-manylinux_2_28_x86_64.whl"
WHEEL_314T = "confluent_kafka-1.0.0-cp314t-cp314t-manylinux_2_28_x86_64.whl"
SDIST = "confluent_kafka-1.0.0.tar.gz"


def _response(payload):
    return io.BytesIO(json.dumps(payload).encode())


def _run_main(files, existing, include_sdist="True", python_versions="all", index="pypi", differences=()):
    """Run main() on a wheelhouse holding `files`, with `existing` as the files published on the index
    and `differences` as what source_differences() finds.

    Returns the exit code (None if main() returned normally) and what it printed.
    """
    with tempfile.TemporaryDirectory() as wheelhouse:
        for name in files:
            Path(wheelhouse, name).touch()
        argv = ["check-existing-files.py", wheelhouse, index, "1.0.0", include_sdist, python_versions]
        out = io.StringIO()
        code = None
        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            check, "published_files", return_value=existing
        ), mock.patch.object(check, "source_differences", return_value=list(differences)), redirect_stdout(out):
            try:
                check.main()
            except SystemExit as e:
                code = e.code
        return code, out.getvalue()


def test_published_files_returns_filenames():
    payload = {"urls": [{"filename": WHEEL_314}, {"filename": SDIST}]}
    with mock.patch.object(check.urllib.request, "urlopen", return_value=_response(payload)) as urlopen:
        assert check.published_files("pypi", "1.0.0") == {WHEEL_314, SDIST}
    urlopen.assert_called_once_with("https://pypi.org/pypi/confluent-kafka/1.0.0/json", timeout=30)


def test_published_files_uses_test_index():
    with mock.patch.object(check.urllib.request, "urlopen", return_value=_response({"urls": []})) as urlopen:
        assert check.published_files("test", "1.0.0") == set()
    urlopen.assert_called_once_with("https://test.pypi.org/pypi/confluent-kafka/1.0.0/json", timeout=30)


def test_published_files_returns_none_for_unknown_release():
    error = urllib.error.HTTPError("url", 404, "Not Found", {}, None)
    with mock.patch.object(check.urllib.request, "urlopen", side_effect=error):
        assert check.published_files("pypi", "1.0.0") is None


def test_published_files_raises_on_other_http_errors():
    error = urllib.error.HTTPError("url", 500, "Server Error", {}, None)
    with mock.patch.object(check.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError):
            check.published_files("pypi", "1.0.0")


def test_empty_wheelhouse_fails():
    code, _ = _run_main([], existing=None)
    assert "Nothing to upload" in code


def test_missing_sdist_fails():
    code, out = _run_main([WHEEL_314], existing=None, include_sdist="True")
    assert "Nothing was uploaded" in code
    assert "the sdist (INCLUDE_SDIST=True)" in out


def test_no_wheels_with_all_fails():
    code, out = _run_main([SDIST], existing=None, python_versions="all")
    assert "Nothing was uploaded" in code
    assert "any wheel (PYTHON_VERSIONS=all)" in out


def test_missing_requested_python_version_fails():
    code, out = _run_main([WHEEL_314, SDIST], existing=None, python_versions="3.13, 3.14")
    assert "Nothing was uploaded" in code
    assert "a wheel for Python 3.13" in out
    assert "a wheel for Python 3.14" not in out


def test_trailing_comma_in_python_versions_is_ignored():
    code, out = _run_main([WHEEL_314, SDIST], existing=None, python_versions="3.14,")
    assert code is None
    assert "does not have" not in out


def test_free_threaded_wheel_counts_for_its_version():
    code, _ = _run_main([WHEEL_314T], existing={SDIST}, include_sdist="False", python_versions="3.14")
    assert code is None


def test_new_release_with_sdist_passes():
    code, out = _run_main([WHEEL_314, SDIST], existing=None)
    assert code is None
    assert "is not on pypi yet" in out
    assert "WARNING" not in out


def test_new_release_without_sdist_fails():
    code, _ = _run_main([WHEEL_314], existing=None, include_sdist="False")
    assert "INCLUDE_SDIST=False on a new release" in code


def test_new_release_with_some_python_versions_warns():
    code, out = _run_main([WHEEL_314, SDIST], existing=None, python_versions="3.14")
    assert code is None
    assert "WARNING: new release 1.0.0 will only get wheels for Python 3.14" in out


def test_new_release_with_no_wheels_warns():
    code, out = _run_main([SDIST], existing=None, python_versions="none")
    assert code is None
    assert "WARNING: new release 1.0.0 will get no wheels, only the sdist" in out
    assert "only get wheels for Python" not in out


def test_existing_release_with_clashing_wheel_fails():
    code, out = _run_main([WHEEL_312, WHEEL_314], existing={WHEEL_312}, include_sdist="False")
    assert "Nothing was uploaded" in code
    assert WHEEL_312 in out
    assert WHEEL_314 not in out


def test_existing_release_with_clashing_sdist_fails():
    code, out = _run_main([WHEEL_314, SDIST], existing={SDIST})
    assert "Nothing was uploaded" in code
    assert SDIST in out


def test_existing_release_without_clash_passes():
    code, out = _run_main([WHEEL_314], existing={WHEEL_312, SDIST}, include_sdist="False", python_versions="3.14")
    assert code is None
    assert "none of the 1 files to upload is there yet" in out


def test_existing_release_with_different_source_fails():
    code, out = _run_main(
        [WHEEL_314], existing={WHEEL_312, SDIST}, include_sdist="False", differences=["src/confluent_kafka/a.py"]
    )
    assert "Nothing was uploaded" in code
    assert "differs from tag v1.0.0" in out
    assert "src/confluent_kafka/a.py" in out


def test_different_source_is_reported_before_clashes():
    code, out = _run_main([WHEEL_312], existing={WHEEL_312}, include_sdist="False", differences=["setup.py"])
    assert "Nothing was uploaded" in code
    assert "setup.py" in out
    assert "already published" not in out


def test_new_release_skips_source_check():
    # A new release has no tag to compare with yet, so differences must not fail it.
    code, out = _run_main([WHEEL_314, SDIST], existing=None, differences=["src/confluent_kafka/a.py"])
    assert code is None
    assert "differs from tag" not in out


SEMAPHORE_YML = "global_job_config:\n  env_vars:\n    - name: LIBRDKAFKA_VERSION\n      value: {}\n"


def _git(repo, *args):
    # Override settings from the user's git config that would make commit or tag need input.
    config = ["-c", "user.name=test", "-c", "user.email=test@example.com"]
    config += ["-c", "commit.gpgSign=false", "-c", "tag.gpgSign=false"]
    subprocess.run(["git", "-C", repo, *config, *args], check=True, capture_output=True)


def _commit(repo, files, message):
    for name, content in files.items():
        Path(repo, name).parent.mkdir(parents=True, exist_ok=True)
        Path(repo, name).write_text(content)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


@pytest.fixture
def release_repo(tmp_path):
    """A clone of a repo whose origin has tag v1.0.0, checked out at that tag."""
    origin, clone = str(tmp_path / "origin"), str(tmp_path / "clone")
    _git(str(tmp_path), "init", "-q", origin)
    _commit(
        origin,
        {
            "src/confluent_kafka/a.py": "a = 1\n",
            "pyproject.toml": 'version = "1.0.0"\n',
            "tools/build.sh": "build\n",
            ".semaphore/semaphore.yml": SEMAPHORE_YML.format("v2.0.0"),
        },
        "1.0.0",
    )
    _git(origin, "tag", "v1.0.0")
    _git(str(tmp_path), "clone", "-q", "--no-tags", origin, clone)
    return clone


def test_librdkafka_version_is_read_from_semaphore_yml():
    assert check.librdkafka_version(SEMAPHORE_YML.format("v2.16.0")) == "v2.16.0"
    assert check.librdkafka_version("blocks: []\n") is None


def test_source_same_as_tag_has_no_differences(release_repo):
    assert check.source_differences("1.0.0", repo=release_repo) == []


def test_build_and_ci_changes_are_not_differences(release_repo):
    _commit(
        release_repo,
        {"tools/build.sh": "build 3.15\n", ".semaphore/semaphore.yml": SEMAPHORE_YML.format("v2.0.0") + "# 3.15\n"},
        "Add 3.15",
    )
    assert check.source_differences("1.0.0", repo=release_repo) == []


def test_package_file_changes_are_differences(release_repo):
    _commit(
        release_repo, {"src/confluent_kafka/a.py": "a = 2\n", "pyproject.toml": 'version = "1.0.0"\n# x\n'}, "Change"
    )
    assert check.source_differences("1.0.0", repo=release_repo) == ["pyproject.toml", "src/confluent_kafka/a.py"]


def test_librdkafka_version_change_is_a_difference(release_repo):
    _commit(release_repo, {".semaphore/semaphore.yml": SEMAPHORE_YML.format("v2.1.0")}, "Bump librdkafka")
    differences = check.source_differences("1.0.0", repo=release_repo)
    assert differences == ["LIBRDKAFKA_VERSION in .semaphore/semaphore.yml (v2.0.0 in v1.0.0, v2.1.0 here)"]


def test_missing_tag_fails(release_repo):
    with pytest.raises(SystemExit) as e:
        check.source_differences("9.9.9", repo=release_repo)
    assert "could not fetch tag v9.9.9" in str(e.value.code)
