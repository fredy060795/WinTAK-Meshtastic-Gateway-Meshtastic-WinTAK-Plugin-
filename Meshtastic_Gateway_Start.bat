@echo off
:: WinTAK Meshtastic Gateway - Starter
:: Sucht das Gateway (main_app.py oder Meshtastic_Gateway.exe), richtet beim ersten Start
:: eine eigene Python-Umgebung ein, installiert requirements.txt automatisch und startet.
:: Administratorrechte sind nicht noetig.
setlocal EnableExtensions EnableDelayedExpansion

set "BAT_DIR=%~dp0"
if "%BAT_DIR:~-1%"=="\" set "BAT_DIR=%BAT_DIR:~0,-1%"

echo ========================================
echo    TAK MESHTASTIC GATEWAY - START
echo ========================================
echo.

:: --- Gateway suchen -----------------------------------------------------------
:: Reihenfolge: Ordner dieser Datei, Unterordner, bekannte Installationsordner.
set "APP_DIR="
set "APP_EXE="
for %%D in (
    "%BAT_DIR%"
    "%BAT_DIR%\WinTAK-Meshtastic-Gateway-Meshtastic-WinTAK-Plugin-"
    "%BAT_DIR%\WinTAK-Meshtastic-Gateway-Meshtastic-WinTAK-Plugin--main"
    "%ProgramFiles%\WinTAK\Meshtastic Gateway"
    "%ProgramFiles%\WinTAK\Meshttastic Gateway"
) do (
    if not defined APP_DIR if exist "%%~D\main_app.py" set "APP_DIR=%%~D"
)
if defined APP_DIR goto :found_source

for %%D in (
    "%BAT_DIR%"
    "%BAT_DIR%\dist"
    "%ProgramFiles%\WinTAK\Meshtastic Gateway"
    "%ProgramFiles%\WinTAK\Meshttastic Gateway"
) do (
    if not defined APP_EXE if exist "%%~D\Meshtastic_Gateway.exe" set "APP_EXE=%%~D\Meshtastic_Gateway.exe"
)
if defined APP_EXE goto :run_exe

echo Das Gateway wurde nicht gefunden.
echo Gesucht wurde nach main_app.py bzw. Meshtastic_Gateway.exe in:
echo   %BAT_DIR%
echo   %BAT_DIR%\dist
echo   %ProgramFiles%\WinTAK\Meshtastic Gateway
echo.
echo Bitte diese Datei in den Ordner legen, in dem main_app.py liegt
echo (den kompletten Repository-Ordner entpacken, nicht nur die .bat).
goto :fail

:run_exe
echo Starte "%APP_EXE%" ...
for %%F in ("%APP_EXE%") do cd /d "%%~dpF"
"%APP_EXE%" %*
set "EXIT_CODE=!errorlevel!"
goto :finished

:found_source
cd /d "%APP_DIR%"
echo Gateway-Ordner: %APP_DIR%

:: --- Python-Umgebung ------------------------------------------------------------
:: Bevorzugt .venv im Gateway-Ordner; ist der Ordner schreibgeschuetzt (z. B. Program Files
:: ohne Admin), wird die Umgebung im Benutzerprofil angelegt.
set "VENV_DIR=%APP_DIR%\.venv"
if exist "%APP_DIR%\venv\Scripts\python.exe" if not exist "%VENV_DIR%\Scripts\python.exe" set "VENV_DIR=%APP_DIR%\venv"
if exist "%VENV_DIR%\Scripts\python.exe" goto :have_venv
set "WRITE_TEST=%APP_DIR%\.write_test_%RANDOM%"
(echo test> "%WRITE_TEST%") >nul 2>&1
if exist "%WRITE_TEST%" (
    del /q "%WRITE_TEST%" >nul 2>&1
) else (
    set "VENV_DIR=%LOCALAPPDATA%\MeshtasticGateway\venv"
)
if exist "%VENV_DIR%\Scripts\python.exe" goto :have_venv

set "BASE_PY="
py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)" >nul 2>&1
if not errorlevel 1 set "BASE_PY=py -3"
if defined BASE_PY goto :create_venv
:: "python" kann der Microsoft-Store-Platzhalter sein - deshalb wirklich ausfuehren.
python -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)" >nul 2>&1
if not errorlevel 1 set "BASE_PY=python"
if defined BASE_PY goto :create_venv

echo Python 3.9 oder neuer wurde nicht gefunden.
echo Bitte Python von https://www.python.org/downloads/ installieren
echo (beim Setup "Add python.exe to PATH" anhaken) und danach diese Datei erneut starten.
start "" "https://www.python.org/downloads/"
goto :fail

:create_venv
echo Erstelle Python-Umgebung in "%VENV_DIR%" ...
%BASE_PY% -m venv "%VENV_DIR%"
if errorlevel 1 (
    echo Python-Umgebung konnte nicht erstellt werden.
    goto :fail
)

:have_venv
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
echo Python: %VENV_PY%

:: --- Abhaengigkeiten installieren / aktualisieren ------------------------------
set "REQ_FILE=%APP_DIR%\requirements.txt"
set "REQ_STAMP=%VENV_DIR%\requirements.installed"
if not exist "%REQ_FILE%" goto :deps_check
if not exist "%REQ_STAMP%" goto :install_deps
fc /b "%REQ_FILE%" "%REQ_STAMP%" >nul 2>&1
if errorlevel 1 goto :install_deps

:deps_check
"%VENV_PY%" -c "import meshtastic, pubsub, serial, yaml, cryptography" >nul 2>&1
if not errorlevel 1 goto :deps_ok
if not exist "%REQ_FILE%" (
    echo requirements.txt fehlt - Abhaengigkeiten koennen nicht installiert werden.
    goto :fail
)

:install_deps
echo Installiere Abhaengigkeiten aus requirements.txt (einmalig, kann einige Minuten dauern) ...
"%VENV_PY%" -m pip install --disable-pip-version-check --upgrade pip >nul 2>&1
"%VENV_PY%" -m pip install --disable-pip-version-check -r "%REQ_FILE%"
if errorlevel 1 (
    echo.
    echo Installation der Abhaengigkeiten fehlgeschlagen. Internetverbindung pruefen und erneut starten.
    goto :fail
)
copy /y "%REQ_FILE%" "%REQ_STAMP%" >nul

:deps_ok
echo Abhaengigkeiten OK.
echo.

:: --- Gateway starten ----------------------------------------------------------
:: Das Gateway oeffnet die Browser-Oberflaeche (Port 5013) selbst.
echo Starte Gateway ... (Beenden mit Strg+C)
"%VENV_PY%" "%APP_DIR%\main_app.py" %*
set "EXIT_CODE=!errorlevel!"

:finished
echo.
if "%EXIT_CODE%"=="0" goto :finished_ok
echo Gateway wurde mit Fehlercode %EXIT_CODE% beendet.
if "%EXIT_CODE%"=="-1073741819" (
    echo Das ist ein Absturz in einem nativen Python-Modul ^(Zugriffsverletzung 0xC0000005^).
)
if defined APP_DIR if exist "%APP_DIR%\gateway_crash.log" (
    echo.
    echo Letzte Eintraege aus gateway_crash.log:
    echo ----------------------------------------
    powershell -NoProfile -Command "Get-Content -LiteralPath (Join-Path $env:APP_DIR 'gateway_crash.log') -Tail 40"
    echo ----------------------------------------
    echo Bitte diesen Text bzw. die Datei "%APP_DIR%\gateway_crash.log" weitergeben.
)
goto :fail

:finished_ok
echo Gateway beendet.
pause
exit /b 0

:fail
echo.
pause
exit /b 1
