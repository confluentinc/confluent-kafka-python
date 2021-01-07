#!/bin/bash
#

# Test wheels in given directory.
# Should preferably be run on OSX and requires Docker (for Linux tests).
# Must be run from the top-level project directory.

set -e

if [[ -z $1 ]]; then
    echo "Usage: $0 <wheel-directory>"
    exit 1
fi

if [[ ! -f tools/$(basename $0) ]]; then
    echo "Needs to be run from the top-level project directory"
    exit 1
fi


set -u

wheeldir="$1"

if [[ ! -d $wheeldir ]]; then
    echo "Wheel directory $wheeldir does not exist"
    exit 1
fi


echo "##################################"
echo "#### Testing packages locally ####"
echo "#### $wheeldir ####"
echo "##################################"

echo "# Smoke testing locally"
tools/smoketest.sh "$wheeldir"

if which docker ; then
    echo "# Smoke testing on many linux distros"
    tools/test-manylinux.sh "$wheeldir"
else
    echo "# Not smoke-testing on many linux distros: docker not available"
fi

echo "##################################"
echo "#### Tests passed for         ####"
echo "#### $wheeldir ####"
echo "##################################"

