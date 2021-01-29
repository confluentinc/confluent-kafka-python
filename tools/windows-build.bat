rem Build wheels (using cibuildwheel) on Windows and manually
rem insert librdkafka in the built wheels.

echo on

rem For dumpbin
set PATH=%PATH%;c:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\bin
set

rem Download and install librdkafka from NuGet.
call tools\windows-install-librdkafka.bat %LIBRDKAFKA_NUGET_VERSION% dest || exit /b 1

pip install -U -r tests/requirements.txt -r src/confluent_kafka/avro/requirements.txt
pip install cibuildwheel==0.12.0 || exit /b 1

rem Build wheels (without tests)
cibuildwheel --platform windows --output-dir wheelhouse || exit /b 1

dir wheelhouse

rem cibuildwheel installs the generated packages, but they're not ready yet,
rem so remove them.
rem FIXME: this only covers python27 (default)
pip uninstall -y confluent_kafka[dev]


rem Copy the librdkafka DLLs to a path structure that is identical to cimpl.pyd's location
md stage\x86\confluent_kafka
copy dest\librdkafka.redist.%LIBRDKAFKA_VERSION%\runtimes\win-x86\native\*.dll stage\x86\confluent_kafka\ || exit /b 1

md stage\x64\confluent_kafka
copy dest\librdkafka.redist.%LIBRDKAFKA_VERSION%\runtimes\win-x64\native\*.dll stage\x64\confluent_kafka\ || exit /b 1

rem For each wheel, add the corresponding x86 or x64 dlls to the wheel zip file
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

rem Basic testing
for %%W in (wheelhouse\confluent_kafka-*cp%PYTHON_SHORTVER%*win*%PYTHON_ARCH%.whl) do (
  python -c "import struct; print(struct.calcsize('P') * 8)"
  7z l %%~W
  pip install %%~W || exit /b 1
  pip install -r src\confluent_kafka\requirements.txt
  pip install -r src\confluent_kafka\avro\requirements.txt

  python -c "from confluent_kafka import libversion ; print(libversion())" || exit /b 1

  python -m pytest --ignore=tests\schema_registry --ignore=tests\integration tests || exit /b 1
  pip uninstall -y confluent_kafka || exit /b 1
)

