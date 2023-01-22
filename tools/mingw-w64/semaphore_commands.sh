#!/bin/bash
set -e
pacman -S python --version 3.8.0
export PATH="/c/Python38:/c/Python38/Scripts:$PATH"
ln -s /c/Python38/python.exe /c/Python38/python3.exe
export MAKE=mingw32-make  # so that Autotools can find it
