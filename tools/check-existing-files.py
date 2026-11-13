#!/usr/bin/env python3
"""
Check what is about to be uploaded against what is already published, so a
mistake fails before `twine upload` starts and not halfway through it.

- Wheelhouse does not match the parameters: fail if INCLUDE_SDIST=True but
  there is no sdist, if a requested Python version has no wheel, or if
  PYTHON_VERSIONS=all finds no wheels at all.
- Release already on the index: fail if this build's package files differ
  from the release's tag (v<package-version>), so files added to a release
  are built from that release's code, and fail if any file to upload is
  already there.
- Release not on the index yet: fail if the sdist is not included, since the
  first upload of a release must have a source distribution. Warn if only
  some Python versions are published.

Usage: check-existing-files.py <wheelhouse-dir> <pypi|test> <package-version>
                               <include-sdist> <python-versions>
"""

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

INDEX_URLS = {
    "pypi": "https://pypi.org/pypi/confluent-kafka/{}/json",
    "test": "https://test.pypi.org/pypi/confluent-kafka/{}/json",
}

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Everything that ends up in the sdist or the wheels. .semaphore/ and tools/
# are left out, since adding a Python version to a release means changing them.
PACKAGE_PATHS = ["src", "setup.py", "pyproject.toml", "requirements", "MANIFEST.in", "README.md", "LICENSE"]

# The wheels bundle librdkafka, whose version is set in this file.
SEMAPHORE_YML = ".semaphore/semaphore.yml"


def published_files(index, version):
    """Return the set of filenames on the index, or None if no such release."""
    url = INDEX_URLS[index].format(version)
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    return {f["filename"] for f in data["urls"]}


def missing_files(to_upload, include_sdist, python_versions):
    """Return what the parameters ask for but the wheelhouse does not have."""
    missing = []
    if include_sdist == "True" and not any(f.endswith(".tar.gz") for f in to_upload):
        missing.append("the sdist (INCLUDE_SDIST=True)")
    wheels = [f for f in to_upload if f.endswith(".whl")]
    if python_versions in ("all", ""):
        if not wheels:
            missing.append("any wheel (PYTHON_VERSIONS=all)")
    elif python_versions != "none":
        for v in filter(None, python_versions.replace(" ", "").split(",")):
            tag = "cp" + v.replace(".", "")
            if not any("-%s-" % tag in f or "-%st-" % tag in f for f in wheels):
                missing.append("a wheel for Python %s" % v)
    return missing


def git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True).stdout


def librdkafka_version(semaphore_yml):
    """Return the LIBRDKAFKA_VERSION value set in a semaphore.yml, or None."""
    match = re.search(r"- name: LIBRDKAFKA_VERSION\s+value: (\S+)", semaphore_yml)
    return match.group(1) if match else None


def source_differences(version, repo=REPO_ROOT):
    """Return what differs in the package files between tag v<version> and HEAD.

    Exits if the tag can't be fetched, since there is then nothing to compare with.
    """
    tag = "v" + version
    try:
        git(repo, "fetch", "--no-tags", "origin", "refs/tags/" + tag)
    except subprocess.CalledProcessError as e:
        sys.exit("ERROR: could not fetch tag %s to compare the source with:\n%s" % (tag, e.stderr.strip()))
    changed = git(repo, "diff", "--name-only", "FETCH_HEAD", "HEAD", "--", *PACKAGE_PATHS).split()
    released = librdkafka_version(git(repo, "show", "FETCH_HEAD:" + SEMAPHORE_YML))
    building = librdkafka_version(git(repo, "show", "HEAD:" + SEMAPHORE_YML))
    if released != building:
        changed.append("LIBRDKAFKA_VERSION in %s (%s in %s, %s here)" % (SEMAPHORE_YML, released, tag, building))
    return changed


def main():
    wheelhouse, index, version, include_sdist, python_versions = sys.argv[1:6]

    to_upload = sorted(os.listdir(wheelhouse))
    if not to_upload:
        sys.exit("Nothing to upload: %s is empty" % wheelhouse)

    missing = missing_files(to_upload, include_sdist, python_versions)
    if missing:
        print("ERROR: %s does not have:" % wheelhouse)
        for m in missing:
            print("  " + m)
        sys.exit("Check the build artifacts and the parameters. Nothing was uploaded.")

    existing = published_files(index, version)

    if existing is None:
        print("Release %s is not on %s yet" % (version, index))
        if include_sdist != "True":
            sys.exit(
                "ERROR: INCLUDE_SDIST=False on a new release. The first upload "
                "of a release must include the sdist. If this is meant to add "
                "wheels to an existing release, run it from a build whose "
                "pyproject.toml version is that release's version."
            )
        if python_versions == "none":
            print("WARNING: new release %s will get no wheels, only the sdist" % version)
        elif python_versions not in ("all", ""):
            print("WARNING: new release %s will only get wheels for Python %s" % (version, python_versions))
        return

    differences = source_differences(version)
    if differences:
        print("ERROR: release %s exists on %s, but this build differs from tag v%s in:" % (version, index, version))
        for d in differences:
            print("  " + d)
        sys.exit(
            "Files added to an existing release must be built from that release's code: branch "
            "from v%s and keep the files listed above as they are in that tag (see "
            "tools/RELEASE.md). Nothing was uploaded." % version
        )

    clashes = [f for f in to_upload if f in existing]
    if clashes:
        print("ERROR: these files are already published for %s on %s:" % (version, index))
        for f in clashes:
            print("  " + f)
        sys.exit(
            "Remove the matching Python versions from PYTHON_VERSIONS or set "
            "INCLUDE_SDIST=False. Nothing was uploaded."
        )
    print("Release %s exists on %s, none of the %d files to upload is there yet" % (version, index, len(to_upload)))


if __name__ == "__main__":
    main()
