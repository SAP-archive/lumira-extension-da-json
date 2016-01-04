@echo off
:: batch file to nuke all currently installed extensions and add newly built extension zip file without having to install

cd ..
cd target

:: save current directory in a variable
set BINDIR=%CD%

:: remove earlier extracted folders
rd /S /Q bundles\
rd /S /Q features\
rd /S /Q eclipse\

:: ensure only one zip file exists 
:: search for zip file extract and delete
for /r %%i in (*.zip) do (
	jar xf %%~nxi
	del %%~nxi
)

::copy the directories to lumira extension folder
pushd "C:\Users\I842060\.sapvi\extensions"
rd /S /Q bundles\
rd /S /Q features\
rd /S /Q eclipse\
xcopy /S /E /Y %BINDIR%\bundles bundles\
xcopy /S /E /Y %BINDIR%\features features\
xcopy /S /E /Y %BINDIR%\eclipse eclipse\

::deployed