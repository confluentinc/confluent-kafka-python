#!/bin/bash
pacman -S mingw-w64-x86_64-python38

set -e

export PATH="$PATH;C:\Python38;C:\Python38\Scripts"
export MAKE=mingw32-make  # so that Autotools can find it

cmd /c mklink /D C:\Python38\python3.exe C:\Python38\python.exe

python -m pip install cibuildwheel==2.12.0
