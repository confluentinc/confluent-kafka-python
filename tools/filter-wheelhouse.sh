#!/bin/bash
#
# Keep only the wheels for the given Python versions (comma-separated, e.g.
# "3.13,3.14") in a wheelhouse directory. Other files, such as the sdist,
# are left untouched.
#
# Usage: filter-wheelhouse.sh <wheelhouse-dir> <python-versions>
#

set -e

if [[ -z "$1" || -z "$2" ]]; then
    echo "Usage: $0 <wheelhouse-dir> <python-versions>"
    exit 1
fi

wheelhouse="$1"
IFS=',' read -r -a versions <<< "$2"

find_args=()
for version in "${versions[@]}"; do
    version="${version// /}"
    if [[ ! "$version" =~ ^3\.[0-9]+$ ]]; then
        echo "Invalid Python version '$version', expected e.g. 3.14"
        exit 1
    fi
    tag="cp${version/./}"
    find_args+=(! -name "*-${tag}-*.whl" ! -name "*-${tag}t-*.whl")
done

find "$wheelhouse" -type f -name "*.whl" "${find_args[@]}" -delete

if ! compgen -G "$wheelhouse/*.whl" > /dev/null; then
    echo "No wheels found for Python $2 in $wheelhouse"
    exit 1
fi

echo "Publishing only Python $2 wheels:"
ls "$wheelhouse"
