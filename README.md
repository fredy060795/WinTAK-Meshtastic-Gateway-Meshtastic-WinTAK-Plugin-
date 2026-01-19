# WinTAK-Meshtastic-Gateway-Meshtastic-WinTAK-Plugin-
1. Overview

This gateway acts as a high-stability bridge between Meshtastic hardware and the TAK ecosystem (WinTAK, ATAK, iTAK). Specifically optimized for Windows environments, it addresses common Cursor on Target (CoT) ingestion issues and ensures long-term connectivity.

Key Features:

Dual-Streaming: Simultaneous broadcast to local WinTAK (UDP) and a remote TAK Server (TCP/UDP).

Data Sanitization: Automatically fixes non-standard team colors (e.g., "Black" to "Cyan") and sanitizes GPS values to prevent protocol crashes.

Config-Driven: Manage all settings (IPs, Ports, Callsigns) via a simple config.yaml.

Admin-Ready: Includes a batch starter that automatically handles Windows Administrator privileges.

2. Technical Bug Report: The "Channel Routing" Issue
A critical observation has been made regarding data distribution to mobile clients (ATAK/iTAK):

The Symptom: Meshtastic nodes appear perfectly in the local WinTAK instance but are missing on ATAK/iTAK devices connected to the same server.

The Cause: This occurs when the TAK Server has no active data channels configured or subscribed to. In the TAK architecture, the server requires a "Channel" to replicate incoming packets to other subscribers.

The Solution: WinTAK must be connected to a server with active channels (e.g., a "Default" channel). WinTAK then acts as a relay; once it receives the local Meshtastic data via UDP, it pushes it to the server's channel, making it visible to all other connected devices.

Note on GPS Fixes:

Nodes without a valid GPS fix (e.g., indoors) are placed at 0.0, 0.0 (the Atlantic Ocean) by default. This ensures the node appears in the TAK contact list immediately, rather than being ignored until a fix is acquired.

3. Full Source Code & Configuration
Directory: C:\Program Files\WinTAK\Meshttastic Gateway\

File 1: config.yaml

YAML

gateway_callsign: MSHT-GW
gateway_uid: GW-01
meshtastic_port: COM7
# Enter your remote TAK Server IP here
tak_server_host: 123.123.123 
# IMPORTANT: Port 8088 is used for this specific bridge
tak_server_port: 8088
tak_server_protocol: TCP
sync_interval_seconds: 300
File 2: main_app.py

Python

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import datetime
import socket
import time
import logging
import threading
import traceback
from xml.etree.ElementTree import Element, SubElement, tostring

try:
    import yaml
    import colorlog
    import serial.tools.list_ports
    import meshtastic.serial_interface
    from pubsub import pub
except ImportError as e:
    print(f"Missing dependency: {e}. Run: pip install meshtastic pypubsub pyserial colorlog pyyaml")

def get_tak_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')

class TAKMeshtasticGateway:
    def __init__(self, port, cfg):
        self.port = port
        self.cfg = cfg
        self.logger = self.setup_logging()
        
        # Connection Settings
        self.server_ip = cfg.get("tak_server_host", "127.0.0.1")
        self.server_port = int(cfg.get("tak_server_port", 8088))
        self.server_protocol = str(cfg.get("tak_server_protocol", "TCP")).upper()
        
        self.tak_ip = "127.0.0.1" 
        self.tak_port = 4242
        self.sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_remote = None
        self.server_lock = threading.Lock()
        
        try:
            self.interface = meshtastic.serial_interface.SerialInterface(self.port)
            pub.subscribe(self.on_any_packet, "meshtastic.receive")
            threading.Thread(target=self.maintain_server_connection, daemon=True).start()
            self.full_sync()
        except Exception as e:
            self.logger.error(f"Hardware error: {e}")

    def setup_logging(self):
        logger = logging.getLogger('TAK_Gateway')
        handler = colorlog.StreamHandler()
        handler.setFormatter(colorlog.ColoredFormatter('[%(asctime)s] %(log_color)s%(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def maintain_server_connection(self):
        while True:
            if self.server_protocol == "TCP" and self.sock_remote is None:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(10)
                    s.connect((self.server_ip, self.server_port))
                    with self.server_lock: self.sock_remote = s
                    self.logger.info("✅ REMOTE SERVER CONNECTED")
                except: self.sock_remote = None
            time.sleep(15)

    def on_any_packet(self, packet, interface):
        from_id = packet.get('fromId') or packet.get('from')
        if from_id:
            node = self.interface.nodes.get(from_id)
            if node: self.process_node(node, 0, force_update=True)

    def process_node(self, node, index, force_update=False):
        user = node.get('user', {})
        pos = node.get('position', {})
        uid = (user.get('id') or f"ID-{node.get('num'):08x}").replace('!', 'ID-')
        callsign = user.get('longName', user.get('shortName', uid))
        
        lat = pos.get('latitude_i', 0) * 1e-7 or pos.get('latitude', 0)
        lon = pos.get('longitude_i', 0) * 1e-7 or pos.get('longitude', 0)
        is_real = (lat != 0)

        self.send_broadcast(uid, callsign, lat, lon, pos.get('altitude', 0), is_real)

    def send_broadcast(self, uid, callsign, lat, lon, alt, is_real):
        t = get_tak_timestamp()
        event = Element('event', {'how': 'm-g', 'type': 'a-f-G-U-C', 'uid': uid, 'start': t, 'time': t, 'stale': t, 'version': '2.0'})
        SubElement(event, 'point', {'hae': str(alt or 0), 'lat': f"{lat:.6f}", 'lon': f"{lon:.6f}", 'ce': '10', 'le': '10'})
        detail = SubElement(event, 'detail')
        SubElement(detail, 'contact', {'callsign': callsign})
        SubElement(detail, '__group', {'name': 'Cyan', 'role': 'Team Member'})
        
        packet_xml = tostring(event)
        self.sock_udp.sendto(packet_xml, (self.tak_ip, self.tak_port))
        
        if self.sock_remote:
            try: self.sock_remote.sendall(packet_xml + b"\n")
            except: self.sock_remote = None

    def run(self):
        while True:
            time.sleep(300)
            self.full_sync()

    def full_sync(self):
        if self.interface and self.interface.nodes:
            for i, node in enumerate(self.interface.nodes.values()):
                self.process_node(node, i)
4. Execution & Setup
Step 1: Administrator Startup Script (Meshtastic_Gateway_Start.bat)

Save this as a .bat file to ensure the gateway starts with correct permissions and environment.

Code-Snippet

u/echo off
net session >nul 2>&1
if %errorLevel% neq 0 (
    powershell -Command "Start-Process -FilePath '%~0' -Verb RunAs"
    exit /b
)
cd /d "C:\Program Files\WinTAK\Meshttastic Gateway"
.\venv\Scripts\python.exe main_app.py
pause
Step 2: Create Standalone EXE

If you want to distribute this as a single file, use PyInstaller:

Open PowerShell as Admin.

Run: .\venv\Scripts\pyinstaller --onefile --name "Meshtastic_Gateway" main_app.py

Find your .exe in the dist/ folder.


19.01.2026 - Troubleshooting: Connection error to the server under Windows 16000. Solution: Delete the WinTAK server connection and reconnect to the server; apparently, certificates are lost.
