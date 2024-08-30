 @echo off
set chapter=Chapter%1
set dir=%~dp0%chapter%
:: Keep old directory before changing
if exist %dir%\ (
pushd .
cd %dir%

:: build and run our project
echo:
echo -------------------------------------------------------------
echo ^|                      Building %chapter%                    ^|
echo -------------------------------------------------------------
echo:
dotnet build
echo:
echo -------------------------------------------------------------
echo ^|                      Running %chapter%                     ^|
echo -------------------------------------------------------------
echo:
dotnet run
:: Return to original directory
popd
) else (
	echo Could not locate directory "%dir%"
)