#!/bin/bash
set -e
pacman -S python --version 3.8.0
export PATH="$PATH;C:\Python38;C:\Python38\Scripts"
cmd /c mklink /D C:\Python38\python3.exe C:\Python38\python.exe
export MAKE=mingw32-make  # so that Autotools can find it
