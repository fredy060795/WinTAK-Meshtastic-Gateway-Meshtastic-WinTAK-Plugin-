@echo off
:: Startet die Gateway-App im angegebenen Ordner; startet sich selbst als Admin falls nötig
setlocal EnableExtensions EnableDelayedExpansion

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

set "PYTHON_CMD="
if exist ".\venv\Scripts\python.exe" (
    set "PYTHON_CMD=""%CD%\venv\Scripts\python.exe"""
) else (
    where py >nul 2>&1
    if %errorlevel%==0 (
        set "PYTHON_CMD=py -3"
    ) else (
        where python >nul 2>&1
        if %errorlevel%==0 (
            set "PYTHON_CMD=python"
        )
    )
)

if not defined PYTHON_CMD (
    echo Python wurde nicht gefunden. Installiere Python 3.8+ oder erstelle eine venv.
    echo Oeffne Python-Downloadseite...
    start "" "https://www.python.org/downloads/"
    pause
    exit /b 1
)

echo Verwende: %PYTHON_CMD%

set "GATEWAY_IP="
for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "$udp = New-Object System.Net.Sockets.UdpClient; try { $udp.Connect('8.8.8.8',53); ($udp.Client.LocalEndPoint).Address.IPAddressToString } catch { '' } finally { $udp.Close() }"`) do (
    if not defined GATEWAY_IP set "GATEWAY_IP=%%I"
)
if not defined GATEWAY_IP (
    for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "$ip = [System.Net.Dns]::GetHostAddresses([System.Net.Dns]::GetHostName()) ^| Where-Object { $_.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork -and -not $_.IPAddressToString.StartsWith('127.') -and -not $_.IPAddressToString.StartsWith('169.254.') } ^| Select-Object -First 1 -ExpandProperty IPAddressToString; if ($ip) { $ip } else { '127.0.0.1' }"`) do (
        if not defined GATEWAY_IP set "GATEWAY_IP=%%I"
    )
)

set "UI_URL=http://%GATEWAY_IP%:5013/"

echo Starte Backend-Dienst...
start "WinTAK Meshtastic Gateway Backend" cmd /k %PYTHON_CMD% main_app.py --all-ports

echo HTML-UI wird erwartet unter: %UI_URL%
echo Warte auf Port 5013 und oeffne dann den Standard-Browser...

set "BROWSER_OPENED="
for /l %%N in (1,1,20) do (
    powershell -NoProfile -Command "$client = New-Object System.Net.Sockets.TcpClient; try { $iar = $client.BeginConnect('%GATEWAY_IP%', 5013, $null, $null); if ($iar.AsyncWaitHandle.WaitOne(750) -and $client.Connected) { exit 0 } else { exit 1 } } catch { exit 1 } finally { $client.Close() }" >nul 2>&1
    if !errorlevel! equ 0 (
        start "" "%UI_URL%"
        set "BROWSER_OPENED=1"
        goto :browser_done
    )
    timeout /t 1 /nobreak >nul
)

:browser_done
if not defined BROWSER_OPENED (
    echo Backend antwortet noch nicht auf Port 5013.
    echo Bitte oeffne spaeter manuell: %UI_URL%
)

echo.
echo Anwendung beendet.
pause
