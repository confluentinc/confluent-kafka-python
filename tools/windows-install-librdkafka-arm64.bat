@echo off
setlocal enabledelayedexpansion

set VERSION=v2.13.0
set dest=C:\librdkafka-ARM64

if not "%~1"=="" set VERSION=%~1
if not "%~2"=="" set dest=%~2

if exist "%dest%" (
    set LIBRDKAFKA_DIR=%dest%
    exit /b 0
)

set TEMP_DIR=%TEMP%\librdkafka-build-%RANDOM%
mkdir "%TEMP_DIR%" 2>nul

cd /d "%TEMP_DIR%"

git clone --depth 1 --branch %VERSION% https://github.com/confluentinc/librdkafka.git
if errorlevel 1 goto :error

cd librdkafka
mkdir build-arm64
cd build-arm64

cmake .. -G "Visual Studio 17 2022" -A ARM64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=%dest% -DRDKAFKA_BUILD_STATIC=OFF -DRDKAFKA_BUILD_EXAMPLES=OFF -DRDKAFKA_BUILD_TESTS=OFF -DWITH_SSL=OFF -DWITH_ZLIB=OFF -DWITH_ZSTD=OFF -DWITH_SASL=OFF -DENABLE_LZ4_EXT=OFF
if errorlevel 1 goto :error

cmake --build . --config Release --parallel
if errorlevel 1 goto :error

cmake --install . --config Release
if errorlevel 1 goto :error

set LIBRDKAFKA_DIR=%dest%
goto :cleanup

:error
cd /d %TEMP%
if exist "%TEMP_DIR%" rd /s /q "%TEMP_DIR%" 2>nul
exit /b 1

:cleanup
cd /d %TEMP%
if exist "%TEMP_DIR%" rd /s /q "%TEMP_DIR%" 2>nul
exit /b 0
