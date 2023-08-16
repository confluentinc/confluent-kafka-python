rem Copy librdkafka headers and libs to Python paths for later use when building the module

set librdkafka_version=%1
set pypath=%2
set pypath64=%pypath%-x64

rem stdint.h and inttypes.h are only needed on Python2.7 which is using an older MSVC toolchain.
if exist %pypath% (
	if "%pypath%" == "c:\Python27" (
		copy stdint.h %pypath%\include\ || exit /b 1
		copy inttypes.h %pypath%\include\ || exit /b 1
	)
)

if exist %pypath64% (
	if "%pypath64%" == "c:\Python27-x64" (
		copy stdint.h %pypath64%\include\ || exit /b 1
		copy inttypes.h %pypath64%\include\ || exit /b 1
	)
)

rem Remove inttypes.h include from rdkafka.h until it has been removed from upstream
if exist %pypath% (
	md %pypath%\include\librdkafka
	findstr /V inttypes.h dest\librdkafka.redist.%librdkafka_version%\build\native\include\librdkafka\rdkafka.h > %pypath%\include\librdkafka\rdkafka.h
)

if exist %pypath64% (
	md %pypath64%\include\librdkafka
	findstr /V inttypes.h dest\librdkafka.redist.%librdkafka_version%\build\native\include\librdkafka\rdkafka.h > %pypath64%\include\librdkafka\rdkafka.h
)

rem Copy x86 libs and dlls
if exist %pypath% (
	echo A | xcopy /F dest\librdkafka.redist.%librdkafka_version%\build\native\lib\win\x86\win-x86-Release\v142\librdkafka.lib %pypath%\libs\* || exit /b 1
	echo A | xcopy /I /F /S dest\librdkafka.redist.%librdkafka_version%\runtimes\win-x86\native\* %pypath%\libs || exit /b 1
)

rem Copy x64 libs and dlls
if exist %pypath64% (
	echo A | xcopy /F dest\librdkafka.redist.%librdkafka_version%\build\native\lib\win\x64\win-x64-Release\v142\librdkafka.lib %pypath64%\libs\* || exit /b 1
	echo A | xcopy /I /F /S dest\librdkafka.redist.%librdkafka_version%\runtimes\win-x64\native\* %pypath64%\libs || exit /b 1
)
