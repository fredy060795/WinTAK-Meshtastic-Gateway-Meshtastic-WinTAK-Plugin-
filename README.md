# WinTAK Meshtastic Gateway

A stable bridge between **Meshtastic** mesh radios and the **TAK ecosystem** (WinTAK, ATAK, iTAK).  
The gateway reads position data and text messages from Meshtastic nodes over a serial connection and forwards them as Cursor on Target (CoT) XML — both to a local WinTAK instance (UDP) and optionally to a remote TAK Server (streaming TCP, SSL/TLS with client certificates, or UDP) — bidirectionally, so the server distributes mesh data to all its ATAK/WinTAK/iTAK clients and server chats/markers reach the mesh. It can also accept outgoing WinTAK CoT packets and send them into the Meshtastic mesh.

---

## Features

| Feature | Description |
|---|---|
| **Dual-Streaming** | Sends CoT data simultaneously to local WinTAK (UDP 4242) and a remote TAK Server (TCP/UDP). |
| **Chat Bridging** | Forwards Meshtastic text messages to TAK GeoChat and can relay GeoChat messages from WinTAK back into the mesh. |
| **CoT over Mesh** | Friendly position reports stay on official Meshtastic `ATAK_PLUGIN` PLI packets, while marker/generic CoT events keep their original iTAK-style CoT semantics on the official `ATAK_FORWARDER` transport path for maximum ATAK/iTAK compatibility. |
| **Serial, Bluetooth LE & TCP** | Radios can be attached via USB/COM port, directly via Bluetooth LE (`BLE:<address or name>`, no virtual COM port needed) or over Wi-Fi/Ethernet (`TCP:<host>[:port]`) — mixed in one gateway. |
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
                                  └──TCP/SSL/UDP◀─▶  Remote TAK Server ──▶ ATAK / WinTAK / iTAK clients
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
cryptography  # SSL/TLS TAK servers with .p12 certificates / data packages
colorlog   # optional – enables colored console output
```

Install all at once (the batch starter does this automatically):

```bash
pip install -r requirements.txt
```

---

## Configuration

Edit **`config.yaml`** in the same directory as `main_app.py`:

```yaml
gateway_callsign: MSHT-GW          # Callsign shown in logs
gateway_uid: GW-01                  # Unique gateway ID
meshtastic_port: COM7               # Serial port, BLE:<address|name> or TCP:<host>[:port] (list for several radios)

local_tak_ip: 127.0.0.1             # Local WinTAK IP
local_tak_port: 4242                # Local WinTAK UDP input for positions/chat
local_tak_chat_listen_port: 4242    # UDP input on this gateway for outgoing WinTAK GeoChat + other CoT (LPU5-style)
local_tak_tcp_receiver_enabled: true # Connect as TCP client to WinTAK/ATAK's local server
local_tak_tcp_receiver_host: 127.0.0.1
local_tak_tcp_receiver_port: 8087   # LPU5-style WinTAK/ATAK TCP server target
prefer_local_tcp_over_udp: true     # TCP-first last-hop: send CoT to TCP 8087 before UDP 4242 (set false for legacy UDP-first)
local_tak_tcp_listen_port: 8088     # Optional fallback TCP listener for external/legacy setups
tak_multicast_groups:               # Optional extra TAK multicast groups to listen on and send to
  - 224.10.10.1:17012               # GeoChat multicast used by some TAK clients/setups
  - 239.2.3.1:6969                  # SA/CoT multicast used by common TAK setups
# tak_multicast_interface_ip: 0.0.0.0 # Optional NIC IP to use when joining multicast groups
# local_tak_chat_listen_ip: 0.0.0.0 # Optional bind IP for outgoing WinTAK CoT (default: all local interfaces)
# local_tak_tcp_listen_ip: 0.0.0.0 # Optional bind IP for local TCP chat/CoT input

tak_server_host: tak.example.com    # Hostname, IP or URL (ssl://host:8089, host:8089:ssl)
tak_server_port: 8089               # empty = default for the protocol (TCP 8087, SSL 8089)
tak_server_protocol: SSL            # SSL (TLS + client certificate), TCP (streaming) or UDP
tak_server_client_cert: tak_certs/myserver/gateway.p12   # SSL only (.p12 or .pem)
tak_server_client_cert_password: atakatak
tak_server_ca_cert: tak_certs/myserver/truststore-root.p12
tak_server_ca_cert_password: atakatak
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

### Remote TAK Server

The gateway connects to a TAK Server (official TAK Server, OpenTAKServer, FreeTAKServer, …) the same way ATAK/WinTAK do:

- **Address:** hostname, IPv4/IPv6 or URL (`ssl://tak.example.com:8089`, ATAK form `tak.example.com:8089:ssl`). An empty port uses the protocol default.
- **SSL/TLS (port 8089):** client certificate as `.p12` (from a TAK data package) or PEM, truststore/CA as `.p12` or PEM. The server certificate is verified when a truststore is configured (`tak_server_tls_verify: auto`).
- **Single `.p12` file:** select it in the same file picker as data packages (*Datenpaket (.zip) oder Zertifikat (.p12) hochladen*; Tk GUI: *Import TAK package / .p12…* or *Use .p12 certificate…*). The gateway stores a `.p12` under `tak_certs/<server>/` and detects what it is: without private key it is used as truststore (CA), with private key as client certificate.
- **Request a client certificate (enrollment):** if you only have the truststore plus a username/password (OpenTAKServer / TAK Server certificate enrollment, like ATAK's *Enroll for client certificate*), enter them under *Client-Zertifikat beim TAK-Server anfordern* (Tk GUI: *Request certificate…*). The gateway sends a CSR to `https://<server>:8446/Marti/api/tls/signClient/v2`, verifies the server with the truststore and stores the signed certificate as `tak_certs/<server>/<username>.p12`. The password is not saved.
- **Data package import:** in the browser UI choose *Datenpaket (.zip) oder Zertifikat (.p12) hochladen* (Tk GUI: *Import TAK package / .p12…*) and select the `.zip` you would give to ATAK/WinTAK. Host, port, SSL, certificates and passwords are taken over; the certificates are stored in `tak_certs/` next to `config.yaml`. For packages without a client certificate (enrollment packages) request the certificate with username/password as described above.
- **Bidirectional stream:** the connection is read continuously, keepalive pings are sent and the gateway announces itself with its own SA (`gateway_callsign`, position from `park_lat`/`park_lon` or the local radio). Several servers (e.g. FreeTAKServer) only distribute data from clients that identified themselves this way.
- **Mesh → server:** all node positions, markers and chats from the mesh are streamed to the server, which distributes them to every connected client. CoT produced while the server is unreachable is buffered and sent after the automatic reconnect.
- **Server → mesh:** `tak_server_to_mesh` controls what goes into the (slow) LoRa mesh: `chat_markers` (default), `chat`, `all` (positions throttled to one per UID every 5 minutes) or `off`.
- **WinTAK at the gateway:** `local_tak_to_server` forwards CoT/chats from the local WinTAK to the server, `tak_server_to_local_tak` shows server traffic in the local WinTAK — the gateway then works as a TAK proxy.
- The browser UI shows the live server status (connected, sent/received/buffered counters, last error).

### Bluetooth LE and TCP radios

Besides physical/virtual COM ports, every entry in `meshtastic_port` may point to a radio that is reached directly over Bluetooth LE or the network:

```yaml
meshtastic_port:
  - COM7                       # USB / serial
  - BLE:AA:BB:CC:DD:EE:FF      # Bluetooth LE by address (a bare MAC address works too)
  - BLE:Meshtastic_1a2b        # Bluetooth LE by advertised device name
  - TCP:192.168.1.50           # Wi-Fi/Ethernet node (default port 4403, or TCP:host:port)
```

- `BLE` without a target connects to the first Meshtastic BLE device found.
- BLE address/name matching is case- and separator-insensitive (`aa-bb-…` == `AA:BB:…`).
- In the browser UI use **BLE scannen** (Tk GUI: **Scan BLE**) to list nearby Meshtastic radios; the scan takes about 10 seconds. Selected devices are added as `BLE:<address>` entries.
- BLE needs the `bleak` package (installed automatically with `meshtastic`). Make sure the radio is not connected to another app (e.g. the Meshtastic phone/desktop app) at the same time — a BLE radio accepts only one client. Windows pairing is not required.
- Lost BLE/TCP/USB links are detected and re-established automatically on the next send or sync cycle.
- BLE and TCP entries can be used in the relay settings (`relay_text_from_ports` / `relay_text_to_ports`) exactly like COM ports.

### WinTAK Chat / CoT Relay

- **Meshtastic → WinTAK:** incoming `TEXT_MESSAGE_APP` packets and direct Meshtastic `ATAK_PLUGIN` GeoChat packets (for example from ATAK/iTAK clients over mesh) are converted into TAK GeoChat events and sent to local WinTAK and the optional remote TAK target, preserving the original 1:1 recipient UID/callsign fields when present.
- **WinTAK → Meshtastic (GeoChat):** configure WinTAK to send outgoing GeoChat CoT via UDP to `local_tak_chat_listen_port` (default `4242`) on the gateway host. By default the UDP listener binds to `0.0.0.0`, so WinTAK can target either `127.0.0.1` or the gateway PC's LAN IP. The gateway now forwards normal and 1:1 GeoChat as Meshtastic `ATAK_PLUGIN` chat packets on port `72`.
  Official ATAK/iTAK recipients therefore see the same direct-message destination metadata instead of a downgraded plain text broadcast. The gateway only falls back to plain text when the plugin packet is too large or unsupported by the connected Python API.
- **WinTAK TCP path (LPU5-style):** the gateway now also starts a TCP receiver client that connects to `local_tak_tcp_receiver_host:local_tak_tcp_receiver_port` (defaults `127.0.0.1:8087`) and ingests stream-framed CoT/GeoChat exactly like the working `lpu5-tactical` bridge. Ping/pong keepalives are handled on that client connection before packets are forwarded through the gateway's existing canonical inbound GeoChat/CoT handler. The same active TCP session is now also used in the opposite direction, so mesh-originated CoT is pushed straight back to connected WinTAK/ATAK clients instead of relying only on local UDP delivery. **TCP-first last-hop delivery (`prefer_local_tcp_over_udp: true`, default):** the gateway sends CoT to TCP/8087 first and falls back to UDP/4242, making delivery reliable on systems where UDP/4242 is blocked (Windows WinError 10013). Set `prefer_local_tcp_over_udp: false` to restore the legacy UDP-first order.
- **Required in WinTAK / ATAK:** create a **local server** with `127.0.0.1`, port `8087`, protocol `TCP` (no certificate required for the local path) so the gateway can connect as TCP client. The older fallback listener on `local_tak_tcp_listen_port` (`8088` by default) remains available for direct inbound TCP setups.
- **Additional TAK multicast:** if your TAK setup distributes GeoChat/SA via multicast instead of direct unicast, configure `tak_multicast_groups` (defaults include `224.10.10.1:17012` and `239.2.3.1:6969`) so the gateway also joins those multicast streams and mirrors mesh-originated CoT back to them. This covers common SA multicast on `239.2.3.1:6969` for both inbound and outbound flow. The gateway accepts common WinTAK GeoChat CoT variants including `<chat>` / `<__chat>` payloads with nested numbered message elements, normalizes multiline messages, collapses WinTAK transcript/history exports down to the newest typed message, and splits oversized TAK chat text into multiple mesh-safe messages when needed.
- **WinTAK TCP Monitor (GUI):** the gateway UI shows a dedicated **WinTAK-Nachrichten** panel that displays every GeoChat/text message received over the TCP receiver/listener paths in real time (timestamp, sender, message). The receiver path defaults to `127.0.0.1:8087`; the fallback listener remains configurable on `local_tak_tcp_listen_port` (default `0.0.0.0:8088`). A **Letzte Nachricht → Mesh** button lets you manually re-forward the most recently received WinTAK message into the Meshtastic mesh — useful for confirming the receive path without relying on auto-relay. Both TCP paths respond to WinTAK keepalive pings (`t-x-c-t`) with a proper pong (`t-x-c-t-r`) so that long-lived WinTAK connections stay open, and active TCP peers now also receive mesh-originated CoT back over the same session.
- **WinTAK → Meshtastic (generic CoT / PLI):** only true PLI position events (`a-f-G-U-C`) are encoded as Meshtastic `ATAK_PLUGIN` PLI packets on the legacy plugin port `72`; the gateway deliberately keeps its legacy protobuf payloads off `ATAK_PLUGIN_V2`/`78` until full TAKPacketV2 dictionary compression is embedded. Marker and other generic CoT events are kept separate from PLI, preserve their original `uid` / `type` / `how` / `point` semantics, and now first try the compact legacy `ATAK_PLUGIN detail=7` packet on port `72` before falling back to the official `ATAK_FORWARDER` path on port `257` when the marker payload is too large or the plugin send fails. Incoming Meshtastic `ATAK_PLUGIN` PLI and `detail=7` marker packets are still reconstructed back into CoT and forwarded to TAK; detail=7 packets are no longer allowed to fall through into the PLI path when decode fails, `ATAK_FORWARDER` decode now also tolerates Meshtastic transfer-type prefixes (`0x00`/`'0'`) used by official plugin transports, and the inbound PLI path now reuses a cached TAK marker `type`/`how` for known TAK-originated UIDs instead of blindly downgrading them to `a-f-G-U-C`; when only a TAK-style UID is present but no cached type exists, the gateway now falls back to the generic marker type `a-u-G` instead of reconstructing the packet as Meshtastic PLI. The receiver now also recognizes raw/uncompressed `ATAK_PLUGIN_V2` marker / range-and-bearing / PLI / chat packets, rebuilds `raw_detail` fallback packets back into CoT when official clients send unmapped TAK v2 detail payloads, refreshes its marker-type lookup from the bundled `atak.proto` so newer marker enums/iconset layouts from the official proto can still be forwarded to WinTAK correctly, and preserves `Marker.kind`, `iconset`, `color` / `color_argb`, `parent_uid`, `parent_type`, and `parent_callsign` so mesh-originated markers do not degrade back to generic `a-f-G-U-C` objects on the local WinTAK hop. Binary mesh packets are broadcast using the Meshtastic data-port defaults for better Python API compatibility; legacy `COTM:` short-text fragments on main channel `0` remain only as the final emergency fallback.
- **Marker / generic CoT — FTN fountain-code alignment:** the gateway's `ATAK_FORWARDER` path for marker and generic CoT events is now aligned with the official `meshtastic/ATAK-Plugin` reference implementation. On the **receive** side the gateway recognises and reassembles FTN fountain-code blocks (`FTN` magic, Luby-Transform LT-code, peeling decoder) as sent by real ATAK/iTAK clients; previously these packets failed silently with *"Payload konnte nicht dekodiert werden"*. On the **send** side large `ATAK_FORWARDER` payloads are now encoded with the same FTN fountain codec (replacing the old gateway-internal `COTF` sequential fragment format), so any ATAK/iTAK device on the mesh can reassemble them. Small CoT events that fit in a single Meshtastic packet are still sent as plain zlib-compressed packets, which is the same behaviour as the reference plugin.
- **Inbound troubleshooting:** set `log_level: DEBUG` in `config.yaml` (or via the GUI) to log inbound WinTAK/TAK packet source, transport, listener port, packet size, normalization status, CoT/chat detection, and a shortened payload snippet whenever a packet cannot be recognized as CoT/GeoChat.

---

## Usage

### Option 1 — Batch Starter (recommended on Windows)

Double-click **`Meshtastic_Gateway_Start.bat`** (works from any install folder).
The script

1. requests Administrator privileges (continues without them if declined),
2. finds Python 3.9+ (`py -3` or `python`) and creates a private environment `.venv` next to the script on first start,
3. installs everything from **`requirements.txt`** automatically — again whenever `requirements.txt` changes or a package is missing,
4. starts the gateway; the browser UI opens by itself on port 5013.

Arguments are passed through, e.g. `Meshtastic_Gateway_Start.bat --gui`.

### Option 2 — Run directly with Python

```bash
python main_app.py
```

Das Programm startet standardmäßig eine **HTML-Browseroberfläche** auf Port **5013** und öffnet dort die Hauptoberfläche:

| Bereich | Beschreibung |
|---|---|
| **WinTAK-Hinweis** | Zeigt direkt an, dass in WinTAK zwingend ein lokaler Server `127.0.0.1:8088` mit **TCP** angelegt werden muss. |
| **Basis-Einstellungen** | Meshtastic-Port(s), Remote TAK, WinTAK UDP/TCP, Log-Level, Sync, Relay-Optionen und GPS-Fallback direkt im Browser. |
| **▶ Start / ■ Stop** | Gateway starten und stoppen ohne Neustart – keine Eingabe mehr im Backend-Fenster nötig. |
| **WinTAK-Nachrichten (TCP Monitor)** | Zeigt in Echtzeit jede über den konfigurierten TCP-Listener empfangene WinTAK-Nachricht samt Verbindungsstatus. |
| **Log-Ausgabe** | Alle Meldungen erscheinen live farbig im Browser (DEBUG=grau, INFO=weiß, WARNING=gelb, ERROR=rot). |
| **Zusatzseiten** | Marker-Referenz und separater CoT-Monitor bleiben über Links in der Browseroberfläche erreichbar. |

Die klassische Tk-Desktopoberfläche bleibt mit `--gui` weiterhin verfügbar.

```bash
# Tk-Desktopoberfläche explizit starten
python main_app.py --gui

# Terminal-Modus erzwingen (z. B. auf Server ohne Browser)
python main_app.py --no-gui
```

Ohne `--no-gui` bleibt das Backend-Fenster nur als laufender Dienst offen; die Bedienung erfolgt über die HTML-Oberfläche.

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
| **BLE radio not found** | Run the BLE scan in the UI, make sure Bluetooth is on and the radio is not connected to another app/phone. Use the address shown by the scan as `BLE:<address>`. |
| **No connection to the remote TAK Server** | Check the TAK server status line in the browser UI (it shows the last error). Verify host/port/protocol, that the port is open (firewall), and for SSL the client certificate, its password and the truststore — importing the server's data package sets all of them. |
| **Server shows the gateway, but other clients don't see mesh nodes** | Make sure the gateway's client certificate/user is in the same TAK Server group as the other clients. |
| **WinTAK/ATAK chat reaches other TAK clients but not the gateway** | Enable/check `tak_multicast_groups` in `config.yaml`. Some TAK setups distribute GeoChat/SA over multicast (`224.10.10.1:17012`, `239.2.3.1:6969`) instead of sending directly to the gateway's UDP port. |
| **Nodes visible in WinTAK but missing in ATAK/iTAK** | See [Channel Routing](#important-channel-routing-for-atak--itak) above. |
| **Certificate / connection errors after a Windows update** | Delete the TAK Server connection in WinTAK and re-add it. Certificates may need to be re-imported. |
| **Missing Python dependencies** | Run `pip install meshtastic pypubsub pyserial pyyaml cryptography colorlog` (`bleak` for Bluetooth LE comes with `meshtastic`). |
| **No-fix nodes are missing** | Set `park_lat` and `park_lon` in `config.yaml` to your base or site coordinates. A startup warning is shown when this is misconfigured. |

---

## Project Structure

```
├── main_app.py                    # Gateway application
├── config.yaml                    # Configuration file
├── Meshtastic_Gateway_Start.bat   # Windows launcher (auto-admin, auto-installs requirements)
├── requirements.txt              # Python dependencies
├── tak_server_client.py          # Remote TAK Server connection (TCP/SSL/UDP, certificates, enrollment)
├── tak_certs/                    # TAK certificates (client certificates are git-ignored)
├── Meshtastic_Gateway.spec        # PyInstaller build spec
└── README.md
```

---

## License

This project is provided as-is for use within the TAK community. See the repository for any license details.
