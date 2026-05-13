# WinTAK Meshtastic Gateway

A stable bridge between **Meshtastic** mesh radios and the **TAK ecosystem** (WinTAK, ATAK, iTAK).  
The gateway reads position data and text messages from Meshtastic nodes over a serial connection and forwards them as Cursor on Target (CoT) XML — both to a local WinTAK instance (UDP) and optionally to a remote TAK Server (TCP or UDP). When `pytak` is installed, the remote TAK uplink uses PyTAK for the TCP/UDP transport. It can also accept outgoing WinTAK CoT packets and send them into the Meshtastic mesh.

---

## Features

| Feature | Description |
|---|---|
| **Dual-Streaming** | Sends CoT data simultaneously to local WinTAK (UDP 4242) and a remote TAK Server (TCP/UDP). |
| **Chat Bridging** | Forwards Meshtastic text messages to TAK GeoChat and can relay GeoChat messages from WinTAK back into the mesh. |
| **CoT over Mesh** | Friendly position markers use official Meshtastic `ATAK_PLUGIN` PLI packets and other WinTAK CoT/marker events now prefer official `ATAK_PLUGIN` `detail=7` payloads; only oversized events fall back to zlib-compressed `ATAK_FORWARDER`, with mesh-safe `COTM:` fragments kept as the last-resort fallback. |
| **COM Relay Mode** | Incoming Meshtastic text from one selected COM port can be forwarded automatically to the other selected COM ports. |
| **Automatic Reconnect** | Maintains the remote TAK Server connection with automatic retry on disconnect. |
| **All-Nodes Visibility** | All nodes are forwarded to TAK by default. Nodes with valid GPS (including phone GPS shared over mesh) appear at their real position; nodes without current GPS use their last known position when available, otherwise configurable fallback coordinates. |
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
pytak      # recommended – remote TAK uplink uses PyTAK when available
colorlog   # optional – enables colored console output
```

Install all at once:

```bash
pip install meshtastic pypubsub pyserial pyyaml pytak colorlog
```

---

## Configuration

Edit **`config.yaml`** in the same directory as `main_app.py`:

```yaml
gateway_callsign: MSHT-GW          # Callsign shown in logs
gateway_uid: GW-01                  # Unique gateway ID
meshtastic_port: COM7               # Serial port of the Meshtastic radio

local_tak_ip: 127.0.0.1             # Local WinTAK IP
local_tak_port: 4242                # Local WinTAK UDP input for positions/chat
local_tak_chat_listen_port: 4242    # UDP input on this gateway for outgoing WinTAK GeoChat + other CoT (LPU5-style)
local_tak_tcp_listen_port: 8088     # Local TCP input for WinTAK/admin_map chat/CoT compatibility (LPU5-style)
tak_multicast_groups:               # Optional extra TAK multicast groups to listen on
  - 224.10.10.1:17012               # GeoChat multicast used by some TAK clients/setups
  - 239.2.3.1:6969                  # SA/CoT multicast used by common TAK setups
# tak_multicast_interface_ip: 0.0.0.0 # Optional NIC IP to use when joining multicast groups
# local_tak_chat_listen_ip: 0.0.0.0 # Optional bind IP for outgoing WinTAK CoT (default: all local interfaces)
# local_tak_tcp_listen_ip: 0.0.0.0 # Optional bind IP for local TCP chat/CoT input

tak_server_host: 123.123.123.123    # Remote TAK Server IP
tak_server_port: 8088               # Remote TAK Server port (8088 is the default bridge input port)
tak_server_protocol: TCP            # TCP or UDP (remote uplink uses PyTAK when installed)
relay_text_messages: true           # Relay incoming mesh text to the other selected COM ports
# relay_text_from_ports: COM7       # Optional: only relay texts received on these COM ports
# relay_text_to_ports:              # Optional: only relay texts to these COM ports
#   - COM3
#   - COM9

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

### WinTAK Chat / CoT Relay

- **Meshtastic → WinTAK:** incoming `TEXT_MESSAGE_APP` packets and direct Meshtastic `ATAK_PLUGIN` GeoChat packets (for example from ATAK/iTAK clients over mesh) are converted into TAK GeoChat events and sent to local WinTAK and the optional remote TAK target.
- **WinTAK → Meshtastic (GeoChat):** configure WinTAK to send outgoing GeoChat CoT via UDP to `local_tak_chat_listen_port` (default `4242`) on the gateway host. By default the UDP listener binds to `0.0.0.0`, so WinTAK can target either `127.0.0.1` or the gateway PC's LAN IP.
- **WinTAK TCP path:** the gateway also listens on `local_tak_tcp_listen_port` (default `8088`, bind IP `0.0.0.0`) so WinTAK can connect directly via TCP from the local PC or another system on the network. The gateway still listens on the normal `local_tak_port`, so outgoing WinTAK packets are recognized even when they are sent to the standard TAK UDP port.
- **Required in WinTAK:** create a **local server** with `127.0.0.1`, port `8088`, protocol `TCP`; otherwise no connection to the gateway is established.
- **Additional TAK inputs:** if your TAK setup distributes GeoChat/SA via multicast instead of direct unicast, configure `tak_multicast_groups` (defaults include `224.10.10.1:17012` and `239.2.3.1:6969`) so the gateway also joins those multicast streams. The gateway accepts common WinTAK GeoChat CoT variants including `<chat>` / `<__chat>` payloads with nested numbered message elements, normalizes multiline messages, collapses WinTAK transcript/history exports down to the newest typed message, and splits oversized TAK chat text into multiple mesh-safe messages when needed.
- **WinTAK TCP Monitor (GUI):** the gateway UI shows a dedicated **WinTAK-Nachrichten** panel that displays every GeoChat/text message received over the configured TCP listener in real time (timestamp, sender, message). The panel header/status now reflects the active bind IP and port (default `0.0.0.0:8088`). A **Letzte Nachricht → Mesh** button lets you manually re-forward the most recently received WinTAK message into the Meshtastic mesh — useful for confirming the receive path without relying on auto-relay. The TCP listener now also responds to WinTAK keepalive pings (`t-x-c-t`) with a proper pong (`t-x-c-t-r`) so that long-lived WinTAK connections stay open and messages are reliably delivered.
- **WinTAK → Meshtastic (generic CoT / PLI):** friendly position CoT events (`a-f-G-U-C` plus WinTAK/iTAK marker variants such as `a-f-G-U-C-*`) are encoded as Meshtastic `ATAK_PLUGIN` PLI packets on port `72`, matching the official ATAK plugin format for contact markers. Other WinTAK CoT/marker events now prefer official Meshtastic `TAKPacket detail=7` packets on the same ATAK plugin port, using zlib compression when needed to stay within the mesh payload limit. Incoming Meshtastic `ATAK_PLUGIN` PLI and `detail=7` markers are reconstructed back into CoT and forwarded to TAK. Binary mesh packets are broadcast using the Meshtastic data-port defaults for better Python API compatibility. Only when a generic CoT event still does not fit does the gateway fall back to `ATAK_FORWARDER` on port `257` with zlib-compressed CoT XML and automatic fragment reassembly on receipt; legacy `COTM:` short-text fragments on main channel `0` are kept as the final fallback.
- **Inbound troubleshooting:** set `log_level: DEBUG` in `config.yaml` (or via the GUI) to log inbound WinTAK/TAK packet source, transport, listener port, packet size, normalization status, CoT/chat detection, and a shortened payload snippet whenever a packet cannot be recognized as CoT/GeoChat.

---

## Usage

### Option 1 — Batch Starter (recommended on Windows)

Double-click **`Meshtastic_Gateway_Start.bat`**.  
The script automatically requests Administrator privileges and launches the gateway.

### Option 2 — Run directly with Python

```bash
python main_app.py
```

Das Programm öffnet automatisch ein **GUI-Fenster**, das auf die wichtigsten Punkte reduziert ist:

| Bereich | Beschreibung |
|---|---|
| **WinTAK-Hinweis** | Zeigt direkt an, dass in WinTAK zwingend ein lokaler Server `127.0.0.1:8088` mit **TCP** angelegt werden muss. |
| **Basis-Einstellungen** | Nur die wesentlichen Einstellungen im Fenster: Meshtastic-Port(s), Remote TAK, WinTAK UDP/TCP, Log-Level, Sync und GPS-Fallback. |
| **▶ Start / ■ Stop** | Gateway starten und stoppen ohne Neustart |
| **WinTAK-Nachrichten (TCP Monitor)** | Zeigt in Echtzeit jede über den konfigurierten TCP-Listener empfangene WinTAK-Nachricht samt Verbindungsstatus. |
| **Log-Ausgabe** | Alle Meldungen erscheinen live farbig im Fenster (DEBUG=grau, INFO=weiß, WARNING=gelb, ERROR=rot). |

Bei kleinen Displays ist der mittlere Fensterinhalt weiterhin vertikal scrollbar.

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
- Nodes without a current GPS fix first use their last known GPS position (if previously received) and are marked as *"Listed (Last Known Position)"*.
- If no last known position exists, nodes are placed at configurable fallback coordinates (`park_lat` / `park_lon`) and marked as *"Listed (No GPS Fix)"* with precision source `USER`. **Set `park_lat` / `park_lon` in `config.yaml` to your own location**, otherwise no-fix nodes are skipped.
- Set `send_nodes_without_gps: false` if you only want nodes with confirmed GPS coordinates to appear.
- If you want your gateway node to announce a fixed location at startup, enable `set_gateway_position_on_start: true`.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| **Gateway cannot find the COM port** | Make sure the Meshtastic radio is connected via USB. Check Device Manager for the correct COM port number and update `meshtastic_port` in `config.yaml`. |
| **No connection to the remote TAK Server** | Verify `tak_server_host` and `tak_server_port` in `config.yaml`. Make sure the server is reachable and the port is open (firewall). |
| **WinTAK/ATAK chat reaches other TAK clients but not the gateway** | Enable/check `tak_multicast_groups` in `config.yaml`. Some TAK setups distribute GeoChat/SA over multicast (`224.10.10.1:17012`, `239.2.3.1:6969`) instead of sending directly to the gateway's UDP port. |
| **Nodes visible in WinTAK but missing in ATAK/iTAK** | See [Channel Routing](#important-channel-routing-for-atak--itak) above. |
| **Certificate / connection errors after a Windows update** | Delete the TAK Server connection in WinTAK and re-add it. Certificates may need to be re-imported. |
| **Missing Python dependencies** | Run `pip install meshtastic pypubsub pyserial pyyaml pytak colorlog`. |
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
