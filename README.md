# WinTAK Meshtastic Gateway

A stable bridge between **Meshtastic** mesh radios and the **TAK ecosystem** (WinTAK, ATAK, iTAK).  
The gateway reads position data from Meshtastic nodes over a serial connection and forwards it as Cursor on Target (CoT) XML — both to a local WinTAK instance (UDP) and optionally to a remote TAK Server (TCP or UDP).

---

## Features

| Feature | Description |
|---|---|
| **Dual-Streaming** | Sends CoT data simultaneously to local WinTAK (UDP 4242) and a remote TAK Server (TCP/UDP). |
| **Automatic Reconnect** | Maintains the remote TAK Server connection with automatic retry on disconnect. |
| **All-Nodes Visibility** | All nodes are forwarded to TAK by default. Nodes with valid GPS (including phone GPS shared over mesh) appear at their real position; no-fix nodes are placed at configurable fallback coordinates and clearly marked. |
| **Config-Driven** | All settings (IPs, ports, callsign, COM port) are managed in a single `config.yaml`. |
| **Admin Startup Script** | Included `.bat` file auto-elevates to Administrator privileges on Windows. |

---

## Data Flow

```
Meshtastic Radio  ──serial──▶  Gateway (main_app.py)
                                  │
                                  ├──UDP──▶  Local WinTAK (127.0.0.1:4242)
                                  │
                                  └──TCP/UDP──▶  Remote TAK Server
```

---

## Prerequisites

- **Python 3.8+** (or the pre-built `.exe`, see below)
- A **Meshtastic** radio connected via USB (serial / COM port)
- **WinTAK** installed on the same machine (for local UDP reception)

### Python Dependencies

```
meshtastic
pypubsub
pyserial
pyyaml
colorlog   # optional – enables colored console output
```

Install all at once:

```bash
pip install meshtastic pypubsub pyserial pyyaml colorlog
```

---

## Configuration

Edit **`config.yaml`** in the same directory as `main_app.py`:

```yaml
gateway_callsign: MSHT-GW          # Callsign shown in logs
gateway_uid: GW-01                  # Unique gateway ID
meshtastic_port: COM7               # Serial port of the Meshtastic radio

tak_server_host: 123.123.123.123    # Remote TAK Server IP
tak_server_port: 8088               # Remote TAK Server port (8088 is the default bridge input port)
tak_server_protocol: TCP            # TCP or UDP

sync_interval_seconds: 300          # Full node re-sync interval (seconds)

# true  = show all nodes; no-fix nodes appear at park_lat/park_lon (recommended)
# false = only send nodes that have valid GPS coordinates
send_nodes_without_gps: true

# Fallback coordinates for nodes without a GPS fix.
# IMPORTANT: Set this to your base/site location when send_nodes_without_gps=true.
# Without this, no-fix nodes will be skipped.
# park_lat: 48.1351
# park_lon: 11.5820

# Optional: set/publish a fixed position for the local gateway node on startup
# (uses park_lat/park_lon as source coordinates)
# set_gateway_position_on_start: true
```

> **Tip:** If `meshtastic_port` is not set or the configured port is not found, the gateway will prompt you to choose a port interactively.

---

## Usage

### Option 1 — Batch Starter (recommended on Windows)

Double-click **`Meshtastic_Gateway_Start.bat`**.  
The script automatically requests Administrator privileges and launches the gateway.

### Option 2 — Run directly with Python

```bash
python main_app.py
```

Das Programm öffnet automatisch ein **GUI-Fenster**, das das reine Terminal-Fenster ersetzt:

| Bereich | Beschreibung |
|---|---|
| **Einstellungen** | Port(s) und Log-Level direkt im Fenster eintragen / auswählen |
| **▶ Start / ■ Stop** | Gateway starten und stoppen ohne Neustart |
| **Log-Ausgabe** | Alle Meldungen erscheinen live farbig im Fenster (DEBUG=grau, INFO=weiß, WARNING=gelb, ERROR=rot) |
| **Eingabe / Befehl** | Direkteingabe von Befehlen während der Gateway läuft |

#### Verfügbare Befehle im Eingabefeld

| Befehl | Wirkung |
|---|---|
| `sync` | Manuelle Vollsynchronisation aller Nodes |
| `log debug` / `log info` / `log warning` / `log error` | Log-Level sofort umschalten |
| `clear` | Log-Ausgabe leeren |
| `help` | Befehlsliste anzeigen |

```bash
# Terminal-Modus erzwingen (z. B. auf Server ohne Display)
python main_app.py --no-gui
```

Falls kein Display verfügbar ist (z. B. Server ohne Desktop), startet die Anwendung automatisch im Terminal-Modus.

### Option 3 — Build a standalone EXE with PyInstaller

```bash
pip install pyinstaller
pyinstaller --onefile --name "Meshtastic_Gateway" main_app.py
```

The resulting executable is located in the `dist/` folder.

---

## Important: Channel Routing for ATAK / iTAK

If Meshtastic nodes appear in your local WinTAK but **not** on ATAK/iTAK devices connected to the same TAK Server, the cause is usually missing **server-side channels**.

**How to fix it:**

1. Connect WinTAK to a TAK Server that has at least one active channel (e.g. the *Default* channel).
2. WinTAK will relay the locally received Meshtastic CoT data into that channel.
3. All other TAK clients subscribed to the same channel will then see the Meshtastic nodes.

---

## Notes on GPS Fixes

- All nodes are sent to TAK by default (`send_nodes_without_gps: true`).
- Nodes with valid GPS (including smartphones sharing their GPS position over the Meshtastic mesh) are placed at their real coordinates on the map.
- Nodes without any GPS fix are placed at configurable fallback coordinates (`park_lat` / `park_lon`) and marked as *"Listed (No GPS Fix)"* with precision source `USER`. **Set `park_lat` / `park_lon` in `config.yaml` to your own location**, otherwise no-fix nodes are skipped.
- Set `send_nodes_without_gps: false` if you only want nodes with confirmed GPS coordinates to appear.
- If you want your gateway node to announce a fixed location at startup, enable `set_gateway_position_on_start: true`.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| **Gateway cannot find the COM port** | Make sure the Meshtastic radio is connected via USB. Check Device Manager for the correct COM port number and update `meshtastic_port` in `config.yaml`. |
| **No connection to the remote TAK Server** | Verify `tak_server_host` and `tak_server_port` in `config.yaml`. Make sure the server is reachable and the port is open (firewall). |
| **Nodes visible in WinTAK but missing in ATAK/iTAK** | See [Channel Routing](#important-channel-routing-for-atak--itak) above. |
| **Certificate / connection errors after a Windows update** | Delete the TAK Server connection in WinTAK and re-add it. Certificates may need to be re-imported. |
| **Missing Python dependencies** | Run `pip install meshtastic pypubsub pyserial pyyaml colorlog`. |
| **No-fix nodes are missing** | Set `park_lat` and `park_lon` in `config.yaml` to your base or site coordinates. A startup warning is shown when this is misconfigured. |

---

## Project Structure

```
├── main_app.py                    # Gateway application
├── config.yaml                    # Configuration file
├── Meshtastic_Gateway_Start.bat   # Windows launcher (auto-admin)
├── Meshtastic_Gateway.spec        # PyInstaller build spec
└── README.md
```

---

## License

This project is provided as-is for use within the TAK community. See the repository for any license details.
