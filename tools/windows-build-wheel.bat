@echo off
rem Build wheels (using cibuildwheel) on Windows
set LIB_VER=%1
if [%LIB_VER%]==[] (
    set LIB_VER=%LIBRDKAFKA_NUGET_VERSION%
    if [%LIB_VER%]==[] (
        goto usage
    )
)

set LIB_DIR=%2
if [%LIB_DIR%]==[] (
    set LIB_DIR=dest
)

set OUT_DIR=%3
if [%OUT_DIR%]==[] (
    set OUT_DIR=wheelhouse
)

echo on
rem Download and install librdkafka from NuGet.
nuget install librdkafka.redist -version %LIB_VER% -OutputDirectory %LIB_DIR%

rem Build wheels (with tests)
call tools\wheels\build-wheels.bat x64 win_amd64 %LIB_DIR%/librdkafka.redist.%LIB_VER% %OUT_DIR% || goto :error
call tools\wheels\build-wheels.bat x86 win32     %LIB_DIR%/librdkafka.redist.%LIB_VER% %OUT_DIR% || goto :error

goto :eof

:usage
@echo Usage: %0 librdkafka-ver [librdkafka-dir [wheelhouse-dir]]
@echo   default librdkafka-ver: environment variable LIBRDKAFKA_NUGET_VERSION, if ommitted.
@echo   default librdkafka-dir: dest
@echo   default wheelhouse-dir: wheelhouse
exit /B 1

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%

