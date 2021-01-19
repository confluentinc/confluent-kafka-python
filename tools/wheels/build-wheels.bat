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

if [%WHEELHOUSE%]==[] goto usage
echo on

set CIBW_BUILD=cp27-%BW_ARCH% cp36-%BW_ARCH% cp37-%BW_ARCH% cp38-%BW_ARCH% cp39-%BW_WARCH%
set CIBW_BEFORE_BUILD=python -m pip install delvewheel==0.0.6
set CIBW_TEST_COMMAND=python {project}\test.py
rem set CIBW_BUILD_VERBOSITY=3
set INCLUDE_DIRS=%cd%\%DEST%\build\native\include
set DLL_DIR=%cd%\%DEST%\runtimes\win-%ARCH%\native
set LIB_DIRS=%cd%\%DEST%\build\native\lib\win\%ARCH%\win-%ARCH%-Release\v120
set CIBW_REPAIR_WHEEL_COMMAND=python -m delvewheel repair --add-path %DLL_DIR% -w {dest_dir} {wheel}

set PATH=%PATH%;c:\Program Files\Git\bin\

python -m pip install cibuildwheel==1.7.4

python -m cibuildwheel --output-dir %WHEELHOUSE% --platform windows

dir %WHEELHOUSE%

goto :eof

:usage
@echo "Usage: %0 x86|x64 win32|win_amd64 wheelhouse-dir"
exit /B 1
