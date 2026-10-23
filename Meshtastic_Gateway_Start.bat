@echo off
:: WinTAK Meshtastic Gateway - Starter
:: - laeuft aus dem Ordner, in dem diese Datei liegt (egal wo installiert)
:: - startet sich bei Bedarf mit Administratorrechten neu
:: - legt beim ersten Start eine eigene Python-Umgebung (.venv) an und installiert
::   requirements.txt automatisch; bei Aenderungen an requirements.txt erneut
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul 2>&1

set "APP_DIR=%~dp0"
if "%APP_DIR:~-1%"=="\" set "APP_DIR=%APP_DIR:~0,-1%"
cd /d "%APP_DIR%"

:: --- Administratorrechte (fuer Firewall/Ports von WinTAK) ----------------------
net session >nul 2>&1
if errorlevel 1 goto :elevate
goto :admin_ok

:elevate
if /i "%~1"=="--elevated" (
    echo Administratorrechte konnten nicht erlangt werden - starte ohne.
    goto :admin_ok
)
echo Fordere Administratorrechte an ...
set "BAT_PATH=%~f0"
powershell -NoProfile -Command "Start-Process -FilePath $env:ComSpec -ArgumentList '/c', ('\"\"' + $env:BAT_PATH + '\" --elevated\"') -Verb RunAs" 2>nul
if errorlevel 1 (
    echo Erhoehung abgelehnt - starte ohne Administratorrechte.
    goto :admin_ok
)
exit /b 0

:admin_ok
echo ========================================
echo    TAK MESHTASTIC GATEWAY - START
echo ========================================
echo Ordner: %APP_DIR%
echo.

:: --- Python finden ------------------------------------------------------------
set "VENV_DIR=%APP_DIR%\.venv"
if exist "%APP_DIR%\venv\Scripts\python.exe" if not exist "%VENV_DIR%\Scripts\python.exe" set "VENV_DIR=%APP_DIR%\venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
if exist "%VENV_PY%" goto :have_venv

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
set "APP_ARGS=%*"
if defined APP_ARGS set "APP_ARGS=!APP_ARGS:--elevated=!"
"%VENV_PY%" "%APP_DIR%\main_app.py" !APP_ARGS!
set "EXIT_CODE=%errorlevel%"
echo.
if not "%EXIT_CODE%"=="0" (
    echo Gateway wurde mit Fehlercode %EXIT_CODE% beendet.
    goto :fail
)
echo Gateway beendet.
pause
exit /b 0

:fail
echo.
pause
exit /b 1
