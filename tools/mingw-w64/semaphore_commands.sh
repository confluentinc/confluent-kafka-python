#!/bin/bash
pacman -S python --version 3.8.0

set -e

export PATH="$PATH;C:\Python38;C:\Python38\Scripts"
export MAKE=mingw32-make  # so that Autotools can find it

cmd /c mklink /D C:\Python38\python3.exe C:\Python38\python.exe

python3 -m pip install cibuildwheel==2.12.0
