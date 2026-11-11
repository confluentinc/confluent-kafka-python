import importlib.util
import io
import json
import os
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
SDIST = "confluent_kafka-1.0.0.tar.gz"


def _response(payload):
    return io.BytesIO(json.dumps(payload).encode())


def _run_main(files, existing, include_sdist="True", python_versions="all", index="pypi"):
    """Run main() on a wheelhouse holding `files`, with `existing` as the files published on the index.

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
        ), redirect_stdout(out):
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
