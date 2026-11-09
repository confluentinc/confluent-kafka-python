#!/usr/bin/env python3
"""
Check what is about to be uploaded against what is already published, so a
mistake fails before `twine upload` starts and not halfway through it.

- Release already on the index: fail if any file to upload is already there.
- Release not on the index yet: fail if the sdist is not included, since the
  first upload of a release must have a source distribution. Warn if only
  some Python versions are published.

Usage: check-existing-files.py <wheelhouse-dir> <pypi|test> <package-version>
                               <include-sdist> <python-versions>
"""

import json
import os
import sys
import urllib.error
import urllib.request

INDEX_URLS = {
    "pypi": "https://pypi.org/pypi/confluent-kafka/{}/json",
    "test": "https://test.pypi.org/pypi/confluent-kafka/{}/json",
}


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


def main():
    wheelhouse, index, version, include_sdist, python_versions = sys.argv[1:6]

    to_upload = sorted(os.listdir(wheelhouse))
    if not to_upload:
        sys.exit("Nothing to upload: %s is empty" % wheelhouse)

    existing = published_files(index, version)

    if existing is None:
        print("Release %s is not on %s yet" % (version, index))
        if include_sdist != "True":
            sys.exit(
                "ERROR: INCLUDE_SDIST=False on a new release. The first upload "
                "of a release must include the sdist. If this is meant to add "
                "wheels to an existing release, run the workflow of that "
                "release's tag."
            )
        if python_versions not in ("all", ""):
            print("WARNING: new release %s will only get wheels for Python %s" % (version, python_versions))
        return

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
