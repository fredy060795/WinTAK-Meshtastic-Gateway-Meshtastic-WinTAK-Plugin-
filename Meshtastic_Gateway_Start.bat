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

set "PYTHON_EXE="
set "PYTHON_ARGS="

:: Verwende venv-Python wenn vorhanden, sonst py-Launcher
if exist ".\venv\Scripts\python.exe" (
    set "PYTHON_EXE=.\venv\Scripts\python.exe"
) else (
    where py >nul 2>&1
    if %errorlevel%==0 (
        set "PYTHON_EXE=py"
        set "PYTHON_ARGS=-3"
    ) else (
        where python >nul 2>&1
        if %errorlevel%==0 (
            set "PYTHON_EXE=python"
        )
    )
)

if "%PYTHON_EXE%"=="" (
    echo Python wurde nicht gefunden. Installiere Python 3.8+ oder erstelle eine venv.
    echo Oeffne Python-Downloadseite...
    start "" "https://www.python.org/downloads/"
    pause
    exit /b 1
)

set "GATEWAY_URL="
for /f "usebackq delims=" %%I in (`call "%PYTHON_EXE%" %PYTHON_ARGS% -c "import main_app; print(main_app.build_web_ui_url(main_app.get_reachable_local_ip()))" 2^>nul`) do (
    if not defined GATEWAY_URL set "GATEWAY_URL=%%I"
)
if not defined GATEWAY_URL set "GATEWAY_URL=http://127.0.0.1:5013/"

echo Verwende: %PYTHON_EXE% %PYTHON_ARGS%
echo Browser-UI: %GATEWAY_URL%
echo Starte Python-Backend in eigenem Fenster...
start "TAK Meshtastic Gateway Backend" cmd /k ""%PYTHON_EXE%" %PYTHON_ARGS% main_app.py --all-ports"

echo Warte auf die Browser-UI und oeffne dann den Standardbrowser...
powershell -NoProfile -Command ^
  "$url='%GATEWAY_URL%';" ^
  "$deadline=(Get-Date).AddSeconds(30);" ^
  "do {" ^
  "  try { Invoke-WebRequest -UseBasicParsing -Uri $url -TimeoutSec 2 | Out-Null; Start-Process $url; exit 0 }" ^
  "  catch { Start-Sleep -Milliseconds 750 }" ^
  "} while ((Get-Date) -lt $deadline);" ^
  "Start-Process $url"

echo.
echo Backend gestartet. Falls der Browser nicht automatisch sichtbar ist:
echo   %GATEWAY_URL%
pause
