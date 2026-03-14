rem Build wheels (using cibuildwheel) on Windows and manually
rem insert librdkafka in the built wheels.

echo on

rem For dumpbin
set PATH=%PATH%;c:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\bin
set

rem Download and install librdkafka from NuGet.
rem Check if LIBRDKAFKA_DIR is already set for Windows ARM64
if not defined LIBRDKAFKA_DIR (
    rem Download and install librdkafka from NuGet.
    call tools\windows-install-librdkafka.bat %LIBRDKAFKA_NUGET_VERSION% dest || exit /b 1
) else (
    call tools\windows-install-librdkafka-arm64.bat %LIBRDKAFKA_VERSION% %LIBRDKAFKA_DIR% || exit /b 1
)

pip install -r requirements\requirements-tests-install.txt || exit /b 1
pip install cibuildwheel==3.2.1 || exit /b 1

rem Build wheels (without tests)
if defined LIBRDKAFKA_DIR (
    cibuildwheel --platform windows --archs ARM64 --output-dir wheelhouse || exit /b 1
) else (
    cibuildwheel --platform windows --output-dir wheelhouse || exit /b 1
)
dir wheelhouse

rem cibuildwheel installs the generated packages, but they're not ready yet,
rem so remove them.
rem FIXME: this only covers python27 (default)
pip uninstall -y confluent_kafka[dev]

rem Only copy x86/x64 DLLs if building from NuGet
for %%A in (x86 x64 arm64) do (
    md stage\%%A\confluent_kafka 2>nul
)
if not defined LIBRDKAFKA_DIR (
    copy dest\librdkafka.redist.%LIBRDKAFKA_VERSION%\runtimes\win-x86\native\*.dll stage\x86\confluent_kafka\ || exit /b 1
    copy dest\librdkafka.redist.%LIBRDKAFKA_VERSION%\runtimes\win-x64\native\*.dll stage\x64\confluent_kafka\ || exit /b 1
)

rem Handle Windows ARM64 if LIBRDKAFKA_DIR is defined
if defined LIBRDKAFKA_DIR (
    copy %LIBRDKAFKA_DIR%\bin\*.dll stage\arm64\confluent_kafka\ || exit /b 1
)

rem Only process x86/x64 wheels if not ARM64 build
if not defined LIBRDKAFKA_DIR (
    cd stage\x86
    for %%W in (..\..\wheelhouse\*win32.whl) do (
        7z a -r %%~W confluent_kafka\*.dll || exit /b 1
        unzip -l %%~W
    )
    cd ..\x64
    for %%W in (..\..\wheelhouse\*amd64.whl) do (
        7z a -r %%~W confluent_kafka\*.dll || exit /b 1
        unzip -l %%~W
    )
    cd ..\..
) else (
    cd stage\arm64
    for %%W in (..\..\wheelhouse\*arm64.whl) do (
        7z a -r %%~W confluent_kafka\*.dll || exit /b 1
        unzip -l %%~W
    )
    cd ..\..
)

rem Basic testing
for %%W in (wheelhouse\confluent_kafka-*cp%PYTHON_SHORTVER%*win*%PYTHON_ARCH%.whl) do (
  python -c "import struct; print(struct.calcsize('P') * 8)"
  7z l %%~W
  pip install %%~W || exit /b 1
  pip install -r requirements\requirements-tests-install.txt

  python -c "from confluent_kafka import libversion ; print(libversion())" || exit /b 1

  python -m pytest --ignore=tests\schema_registry --ignore=tests\integration tests || exit /b 1
  pip uninstall -y confluent_kafka || exit /b 1
)

