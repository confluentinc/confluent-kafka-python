rem Download and install librdkafka.redist from NuGet.
rem
rem For each available Python version copy headers and libs.

echo on
set librdkafka_version=%1
set outdir=%2

nuget install librdkafka.redist -version %librdkafka_version% -OutputDirectory %outdir%


rem Download required (but missing) system includes
curl -s https://raw.githubusercontent.com/chemeris/msinttypes/master/inttypes.h -o inttypes.h || exit /b 1
curl -s https://raw.githubusercontent.com/chemeris/msinttypes/master/stdint.h -o stdint.h || exit /b 1

for %%V in (27, 35, 36, 37) do (
    call tools\windows-copy-librdkafka.bat %librdkafka_version% c:\Python%%~V || exit /b 1
)
