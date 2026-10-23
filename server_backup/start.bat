@echo off
:: Server-Backup - Starter
:: Richtet beim ersten Start eine eigene Python-Umgebung ein, installiert requirements.txt
:: und startet die Oberflaeche auf http://127.0.0.1:5042 (oeffnet sich im Browser).
:: Parameter: --autostart (ohne Browser, fuer den Windows-Autostart), --port N
setlocal EnableExtensions EnableDelayedExpansion

set "APP_DIR=%~dp0"
if "%APP_DIR:~-1%"=="\" set "APP_DIR=%APP_DIR:~0,-1%"
cd /d "%APP_DIR%"
title Server-Backup

echo ========================================
echo    SERVER-BACKUP - START
echo ========================================
echo.

if not exist "%APP_DIR%\backup_app.py" (
    echo backup_app.py wurde nicht gefunden.
    echo Bitte den kompletten Ordner "server_backup" entpacken, nicht nur die .bat.
    goto :fail
)

:: --- Python-Umgebung ------------------------------------------------------------
set "VENV_DIR=%APP_DIR%\.venv"
if exist "%VENV_DIR%\Scripts\python.exe" goto :have_venv
set "WRITE_TEST=%APP_DIR%\.write_test_%RANDOM%"
(echo test> "%WRITE_TEST%") >nul 2>&1
if exist "%WRITE_TEST%" (
    del /q "%WRITE_TEST%" >nul 2>&1
) else (
    set "VENV_DIR=%LOCALAPPDATA%\ServerBackup\venv"
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

:: --- Abhaengigkeiten installieren / aktualisieren ------------------------------
set "REQ_FILE=%APP_DIR%\requirements.txt"
set "REQ_STAMP=%VENV_DIR%\server_backup_requirements.installed"
if not exist "%REQ_STAMP%" goto :install_deps
fc /b "%REQ_FILE%" "%REQ_STAMP%" >nul 2>&1
if errorlevel 1 goto :install_deps
"%VENV_PY%" -c "import paramiko" >nul 2>&1
if not errorlevel 1 goto :deps_ok

:install_deps
echo Installiere Abhaengigkeiten (einmalig, kann etwas dauern) ...
"%VENV_PY%" -m pip install --disable-pip-version-check --upgrade pip >nul 2>&1
"%VENV_PY%" -m pip install --disable-pip-version-check -r "%REQ_FILE%"
if errorlevel 1 (
    echo.
    echo Installation fehlgeschlagen. Internetverbindung pruefen und erneut starten.
    goto :fail
)
copy /y "%REQ_FILE%" "%REQ_STAMP%" >nul

:deps_ok
echo Oberflaeche: http://127.0.0.1:5042
echo Dieses Fenster offen lassen - solange es laeuft, werden geplante Backups ausgefuehrt.
echo Beenden mit Strg+C oder in der Oberflaeche "Beenden".
echo.
"%VENV_PY%" "%APP_DIR%\backup_app.py" %*
set "EXIT_CODE=!errorlevel!"
if "%EXIT_CODE%"=="0" exit /b 0
if "%EXIT_CODE%"=="2" (
    echo Das Server-Backup laeuft bereits - die Oberflaeche wurde im Browser geoeffnet.
    timeout /t 5 >nul
    exit /b 0
)
echo Server-Backup wurde mit Fehlercode %EXIT_CODE% beendet.

:fail
echo.
pause
exit /b 1
