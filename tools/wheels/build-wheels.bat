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

set CIBW_BUILD=cp36-%BW_ARCH% cp37-%BW_ARCH% cp38-%BW_ARCH% cp39-%BW_ARCH% cp310-%BW_ARCH% cp311-%BW_ARCH% cp312-%BW_ARCH%
set CIBW_BEFORE_BUILD=python -m pip install delvewheel==1.1.4
set CIBW_TEST_REQUIRES=-r tests/requirements.txt
set CIBW_TEST_COMMAND=pytest {project}\tests\test_Producer.py
rem set CIBW_BUILD_VERBOSITY=3
set include=%cd%\%DEST%\build\native\include
set lib=%cd%\%DEST%\build\native\lib\win\%ARCH%\win-%ARCH%-Release\v142
set DLL_DIR=%cd%\%DEST%\runtimes\win-%ARCH%\native
set CIBW_REPAIR_WHEEL_COMMAND=python -m delvewheel repair --add-path %DLL_DIR% -w {dest_dir} {wheel}

set PATH=%PATH%;c:\Program Files\Git\bin\

python3 -m cibuildwheel --output-dir %WHEELHOUSE% --platform windows || goto :error

goto :eof

:usage
@echo "Usage: %0 x86|x64 win32|win_amd64 librdkafka-dir wheelhouse-dir"
exit /B 1

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%
