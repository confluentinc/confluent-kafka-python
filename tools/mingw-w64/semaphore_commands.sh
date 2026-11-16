#!/bin/bash

set -e

export MAKE=mingw32-make  # so that Autotools can find it

python -m pip install cibuildwheel==4.3.5
