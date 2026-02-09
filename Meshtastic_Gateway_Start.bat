@echo off
:: Startet die Gateway-App im angegebenen Ordner; startet sich selbst als Admin falls nötig

:: Admin-Prüfung
net session >nul 2>&1
if %errorLevel% neq 0 (
    powershell -Command "Start-Process -FilePath '%~0' -Verb RunAs"
    exit /b
)

:: Wechsel in den expliziten Install-Ordner (achte auf exakte Schreibweise)
cd /d "C:\Program Files\WinTAK\Meshttastic Gateway"

echo ========================================
echo    TAK MESHTASTIC GATEWAY - START
echo ========================================
echo.

:: Verwende venv-Python wenn vorhanden, sonst py-Launcher
if exist ".\venv\Scripts\python.exe" (
    set "PYTHON_EXEC=.\\venv\\Scripts\\python.exe"
) else (
    where py >nul 2>&1
    if %errorlevel%==0 (
        set "PYTHON_EXEC=py -3"
    ) else (
        where python >nul 2>&1
        if %errorlevel%==0 (
            set "PYTHON_EXEC=python"
        ) else (
            set "PYTHON_EXEC="
        )
    )
)

if "%PYTHON_EXEC%"=="" (
    echo Python wurde nicht gefunden. Installiere Python 3.8+ oder erstelle eine venv.
    echo Oeffne Python-Downloadseite...
    start "" "https://www.python.org/downloads/"
    pause
    exit /b 1
)

echo Verwende: %PYTHON_EXEC%
%PYTHON_EXEC% main_app.py

echo.
echo Anwendung beendet.
pause