@echo off
SETLOCAL ENABLEEXTENSIONS

rem x86 or x64
set ARCH=%1
rem win32 or win_amd64
set BW_ARCH=%2
rem librdkafka install destdir (relative path)
set DEST=%3
rem wheelhouse output dir
set WHEELHOUSE=%4

goto usage
