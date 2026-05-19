#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TAK Meshtastic Gateway - vollständige, robuste Version
- Lädt config.yaml (optional)
- Unterstützt TCP und UDP zu entferntem TAK-Server
- Sendet CoT-XML an lokales WinTAK (UDP) und optional an entfernten TAK-Server
- Verbessertes Logging, stabile Wiederverbindung, sichere COM-Port-Auswahl
- Unterstützt mehrere gleichzeitige Eingabe-Streams (COM-Ports 1-6)
"""

import os
import sys
import asyncio
import argparse
import base64
import codecs
import datetime
import hashlib
import http.server
import ipaddress
import inspect
import json
import math
import mimetypes
import pathlib
import queue
import re
import socket
import sqlite3
import time
import logging
import threading
import traceback
import urllib.parse
import uuid
import webbrowser
import zlib
import warnings
from xml.etree.ElementTree import Element, ParseError, SubElement, tostring, fromstring
try:
    import tkinter as tk
    from tkinter import ttk
except ImportError:
    tk = None
    ttk = None

# optionale Abhängigkeiten
try:
    import yaml
except Exception:
    yaml = None

try:
    import colorlog
except Exception:
    colorlog = None

try:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"The 'aiohttp' package is required but not installed\..*",
            category=UserWarning,
        )
        import pytak
except Exception:
    pytak = None

try:
    import serial.tools.list_ports
except Exception:
    serial = None

# meshtastic-Imports
try:
    import meshtastic.serial_interface
    from pubsub import pub
    from meshtastic.protobuf import portnums_pb2
except Exception:
    meshtastic = None
    pub = None
    portnums_pb2 = None

CFG_FILENAME = "config.yaml"
MAX_DETECTED_PORTS_DISPLAY = 6
MIN_PORT_NUMBER = 1
MAX_PORT_NUMBER = 65535
DEFAULT_CHATROOM_NAME = "All Chat Rooms"
DEFAULT_CHAT_LISTEN_PORT = 4242
TCP_LISTENER_DEFAULT_PORT = 8088
TCP_RECEIVER_DEFAULT_PORT = 8087
WINTAK_REQUIRED_HOST = "127.0.0.1"
MAX_WINTAK_MONITOR_LINES = 150
DETECTED_PORTS_SUMMARY_DEFAULT_WRAP = 520
DETECTED_PORTS_SUMMARY_WRAP_PADDING = 32
DETECTED_PORTS_SUMMARY_MIN_WRAP = 240
DEFAULT_TAK_MULTICAST_GROUPS = (
    "224.10.10.1:17012",
    "239.2.3.1:6969",
)
DEFAULT_SOURCE_PROTOCOL = "UNKNOWN"
TCP_SOCKET_TIMEOUT_SECONDS = 1.0
TCP_LISTENER_BACKLOG = 5
TCP_RECV_BUFFER_SIZE = 4096
MAX_TCP_STREAM_BUFFER_BYTES = 262144
TCP_STREAM_BUFFER_TAIL_BYTES = 64
SERVICE_WEB_UI_PORT = 5013
SERVICE_WEB_UI_PRIMARY_FILE = "gateway_ui.html"
SERVICE_WEB_UI_ALLOWED_FILES = {
    "gateway_ui.html",
    "cot_monitor_ui.html",
    "maker.html",
    "cot-client.js",
    "logo.png",
    "Meshgateway logo.png",
    "Icon ATAK Mesh Setup.ico",
}
TCP_RECEIVER_RECONNECT_SECONDS = 5.0
INBOUND_TAK_DEBUG_SNIPPET_CHARS = 240
UTF8_BOM_CHAR = "\ufeff"
RECENT_CHAT_CACHE_TTL_SECONDS = 30
RECENT_CHAT_CACHE_MAX_ENTRIES = 256
RECENT_COT_IDENTITY_CACHE_TTL_SECONDS = 3600
RECENT_COT_IDENTITY_CACHE_MAX_ENTRIES = 512
EMPTY_MESHTASTIC_COT_ERROR = "Leere CoT-Nachricht kann nicht ins Mesh gesendet werden."
PYTAK_REMOTE_QUEUE_TIMEOUT_SECONDS = 2.0
PYTAK_REMOTE_RECONNECT_SECONDS = 10
MESHTASTIC_TEXT_CHUNK_MAX_BYTES = 180
MESHTASTIC_DATA_PAYLOAD_MAX_BYTES = 180
MESHTASTIC_COT_FRAGMENT_PREFIX = "COTM"
MESHTASTIC_COT_FRAGMENT_PAYLOAD_BYTES = 140
MESHTASTIC_COT_FRAGMENT_TTL_SECONDS = 120
MESHTASTIC_FORWARDER_FRAGMENT_MAGIC = b"COTF"
MESHTASTIC_FORWARDER_FRAGMENT_VERSION = 1
MESHTASTIC_FORWARDER_FRAGMENT_MESSAGE_ID_BYTES = 6
MESHTASTIC_FORWARDER_FRAGMENT_HEADER_BYTES = 13
MESHTASTIC_FORWARDER_FRAGMENT_PAYLOAD_BYTES = (
    MESHTASTIC_DATA_PAYLOAD_MAX_BYTES - MESHTASTIC_FORWARDER_FRAGMENT_HEADER_BYTES
)
MESHTASTIC_TRANSFER_TYPE_COT = 0x00
MESHTASTIC_TRANSFER_TYPE_FILE = 0x01
MESHTASTIC_TRANSFER_TYPE_COT_ASCII = 0x30  # '0'
MESHTASTIC_TRANSFER_TYPE_FILE_ASCII = 0x31  # '1'
SERVICE_MONITOR_COT_TYPE_MAP = (
    ("b-m-p-s-m", "hostile"),
    ("u-d-c-e", "hostile"),
    ("u-d-c-c", "hostile"),
    ("u-d-r", "friendly"),
    ("u-d-f", "hostile"),
    ("u-d-p", "hostile"),
    ("a-f-g-e-s-u-m", "meshtastic_node"),
    ("a-f-g-e", "meshtastic_node"),
    ("a-f", "friendly"),
    ("a-h", "hostile"),
    ("a-n", "neutral"),
    ("a-u", "unknown"),
    ("a-p", "pending"),
)
SERVICE_MONITOR_ATAK_TO_CBT_TYPE = {
    "hostile": "cbt_hostile",
    "friendly": "cbt_friendly",
    "neutral": "cbt_neutral",
    "unknown": "cbt_unknown",
}


def _text_widget_is_at_bottom(widget, threshold=0.999):
    """Return True when the text widget scrollbar is already at the bottom."""
    try:
        return float(widget.yview()[1]) >= threshold
    except Exception:
        return True


def detect_reachable_local_ip(preferred_host=None):
    """Best-effort LAN IPv4 detection for URLs users can open in a browser."""
    connect_targets = []
    preferred = str(preferred_host or "").strip()
    if preferred and preferred not in ("0.0.0.0", "127.0.0.1", "localhost"):
        connect_targets.append((preferred, 80))
    connect_targets.extend((("8.8.8.8", 80), ("1.1.1.1", 80)))

    for host, port in connect_targets:
        probe = None
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.connect((host, port))
            detected_ip = str(probe.getsockname()[0]).strip()
            if detected_ip and not detected_ip.startswith(("127.", "169.254.")):
                return detected_ip
        except OSError:
            pass
        finally:
            if probe is not None:
                try:
                    probe.close()
                except OSError:
                    pass

    try:
        for detected_ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            detected_ip = str(detected_ip).strip()
            if detected_ip and not detected_ip.startswith(("127.", "169.254.")):
                return detected_ip
    except OSError:
        pass

    try:
        detected_ip = str(socket.gethostbyname(socket.gethostname())).strip()
        if detected_ip:
            return detected_ip
    except OSError:
        pass

    return "127.0.0.1"


def build_service_web_ui_url(bind_ip, port=SERVICE_WEB_UI_PORT):
    return f"http://{str(bind_ip).strip() or '127.0.0.1'}:{int(port)}/"


class _ServiceWebUIEventStore:
    """Small thread-safe event store that keeps browser monitor state alive."""

    def __init__(self, max_events=512):
        self._max_events = max(1, int(max_events))
        self._events = []
        self._lock = threading.Lock()
        self._sse_queues = []

    def add(self, record):
        with self._lock:
            stored = dict(record)
            stored.setdefault("idx", len(self._events))
            self._events.append(stored)
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events:]
                for idx, item in enumerate(self._events):
                    item["idx"] = idx
            for subscriber in list(self._sse_queues):
                try:
                    subscriber.put_nowait(dict(stored))
                except queue.Full:
                    try:
                        self._sse_queues.remove(subscriber)
                    except ValueError:
                        pass

    def get_all(self):
        with self._lock:
            return [dict(event) for event in self._events]

    def set_correction(self, idx, correction, notes):
        with self._lock:
            if 0 <= idx < len(self._events):
                self._events[idx]["correction"] = correction
                self._events[idx]["notes"] = notes

    def clear(self):
        with self._lock:
            self._events = []

    def subscribe_sse(self):
        subscriber = queue.Queue(maxsize=100)
        with self._lock:
            self._sse_queues.append(subscriber)
        return subscriber

    def unsubscribe_sse(self, subscriber):
        with self._lock:
            try:
                self._sse_queues.remove(subscriber)
            except ValueError:
                pass


def _build_service_web_ui_status(bind_ip, port, cfg=None):
    listen_ip = str(bind_ip).strip() or "127.0.0.1"
    url = build_service_web_ui_url(listen_ip, port)
    cfg = cfg or {}
    return {
        "listen_ip": listen_ip,
        "port": int(port),
        "url": url,
        "primary_path": "/",
        "monitor_path": f"/{SERVICE_WEB_UI_PRIMARY_FILE}",
        "maker_path": "/maker.html",
        "remote_tak": {
            "host": str(cfg.get("tak_server_host", "")).strip(),
            "port": int(cfg.get("tak_server_port", 8088)),
            "protocol": str(cfg.get("tak_server_protocol", "TCP")).upper(),
        },
    }


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def _utc_isoformat_z(value=None):
    timestamp = (value or _utc_now()).replace(microsecond=0).isoformat()
    return timestamp.replace("+00:00", "Z")


def _build_service_web_ui_startup_event(status_payload):
    timestamp = _utc_isoformat_z()
    service_url = status_payload.get("url", "")
    listen_ip = status_payload.get("listen_ip", "127.0.0.1")
    return {
        "parsed": {
            "uid": f"gateway-service-ui-{listen_ip}",
            "cot_type": "service-status",
            "how": "m-g",
            "time": timestamp,
            "start": timestamp,
            "stale": timestamp,
            "lat": None,
            "lon": None,
            "hae": None,
            "ce": None,
            "le": None,
            "callsign": "Gateway HTML UI",
            "uid_droid": None,
            "endpoint": service_url,
            "team": None,
            "role": None,
            "remarks": f"HTML UI verfügbar unter {service_url}",
            "color_argb": None,
            "has_meshtastic": False,
            "mesh_longName": None,
            "mesh_shortName": None,
            "has_archive": False,
            "speed": None,
            "course": None,
            "base_lpu5_type": "gateway",
            "detected_type": "gateway",
            "detection_reason": "Gateway backend serves the browser UI on the detected LAN IP",
            "is_echo_back": False,
        },
        "direction": ">>>",
        "source": "Gateway Service",
        "raw_xml": "",
        "notes": f"Open {service_url} in a browser.",
    }


class _GatewayServiceWebUIServer(http.server.ThreadingHTTPServer):
    logger = None

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            if self.logger is not None and self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    "Service Web UI client disconnected: %s (%s)",
                    client_address,
                    exc,
                )
            return
        super().handle_error(request, client_address)


class _ServiceWebUIRequestHandler(http.server.BaseHTTPRequestHandler):
    asset_dir = pathlib.Path(".")
    event_store = None
    status_payload = {}
    logger = None
    controller = None

    def log_message(self, format, *args):
        return

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        request_path = urllib.parse.unquote(urllib.parse.urlparse(self.path).path or "/")
        if request_path == "/api/service/status":
            self._send_json(self.status_payload)
            return
        if request_path == "/api/browser/state":
            if self.controller is None:
                self.send_error(503, "Browser UI controller unavailable")
                return
            self._send_json(self.controller.get_state())
            return
        if request_path == "/api/cot/monitor/events":
            store = self.event_store
            self._send_json({"events": store.get_all() if store is not None else []})
            return
        if request_path == "/api/cot/monitor/stream":
            self._stream_events()
            return
        if request_path == "/api/cot/monitor/export":
            self._send_json({"events": self.event_store.get_all() if self.event_store is not None else []})
            return
        self._serve_static(request_path)

    def do_POST(self):
        request_path = urllib.parse.unquote(urllib.parse.urlparse(self.path).path or "/")
        if request_path.startswith("/api/browser/"):
            self._handle_browser_post(request_path)
            return
        if request_path == "/api/cot/monitor/clear":
            if self.event_store is not None:
                self.event_store.clear()
            self._send_json({"ok": True})
            return
        if request_path.startswith("/api/cot/monitor/events/") and request_path.endswith("/correction"):
            body = self._read_json_body()
            if body is None:
                return
            try:
                idx = int(request_path.split("/")[5])
            except (IndexError, ValueError):
                self.send_error(400, "Invalid event index")
                return
            if self.event_store is not None:
                self.event_store.set_correction(idx, body.get("correction", ""), body.get("notes", ""))
            self._send_json({"ok": True})
            return
        if request_path == "/api/cot/monitor/export":
            body = self._read_json_body()
            if body is None:
                return
            timestamp = _utc_now().strftime("%Y%m%d_%H%M%S")
            export_path = self.asset_dir / f"cot_monitor_log_{timestamp}.json"
            try:
                export_path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
                self._send_json({"ok": True, "file": str(export_path)})
            except OSError as exc:
                if self.logger is not None:
                    self.logger.warning("Konnte Browser-Export nicht speichern: %s", exc)
                self._send_json({"ok": False, "error": str(exc)}, status=500)
            return
        self.send_error(404)

    def _handle_browser_post(self, request_path):
        if self.controller is None:
            self.send_error(503, "Browser UI controller unavailable")
            return
        body = self._read_json_body()
        if body is None:
            return
        controller = self.controller
        try:
            if request_path == "/api/browser/start":
                response = controller.start_gateway(body)
            elif request_path == "/api/browser/stop":
                response = controller.stop_gateway()
            elif request_path == "/api/browser/manual-sync":
                response = controller.manual_sync()
            elif request_path == "/api/browser/send-text":
                response = controller.send_mesh_text(body.get("message", ""))
            elif request_path == "/api/browser/send-cot":
                response = controller.send_cot(body.get("packet_xml", ""))
            elif request_path == "/api/browser/command":
                response = controller.run_command(body.get("command", ""))
            elif request_path == "/api/browser/refresh-ports":
                response = controller.refresh_detected_ports()
            else:
                self.send_error(404)
                return
        except ValueError as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        except Exception as exc:
            if self.logger is not None:
                self.logger.error("Browser UI request failed: %s", exc)
            self._send_json({"ok": False, "error": str(exc)}, status=500)
            return
        self._send_json(response)

    def _send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        try:
            body_length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(body_length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            self.send_error(400, "Invalid JSON")
            return None

    def _stream_events(self):
        store = self.event_store
        if store is None:
            self.send_error(503, "Web UI event stream unavailable")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        subscriber = store.subscribe_sse()
        try:
            while True:
                try:
                    record = subscriber.get(timeout=15)
                    payload = json.dumps(record, ensure_ascii=False)
                    self.wfile.write(f"event: cot_event\ndata: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            store.unsubscribe_sse(subscriber)

    def _serve_static(self, request_path):
        relative_path = request_path.lstrip("/") or SERVICE_WEB_UI_PRIMARY_FILE
        if relative_path in ("", "index.html"):
            relative_path = SERVICE_WEB_UI_PRIMARY_FILE
        if relative_path == "browser-logo.png":
            relative_path = "logo.png"
        if relative_path not in SERVICE_WEB_UI_ALLOWED_FILES:
            self.send_error(404)
            return
        asset_path = (self.asset_dir / relative_path).resolve()
        try:
            asset_path.relative_to(self.asset_dir.resolve())
        except ValueError:
            self.send_error(403)
            return
        if not asset_path.exists():
            self.send_error(404)
            return
        try:
            body = asset_path.read_bytes()
        except OSError as exc:
            self.send_error(500, str(exc))
            return
        content_type = mimetypes.guess_type(str(asset_path))[0] or "application/octet-stream"
        if asset_path.suffix.lower() == ".js":
            content_type = "application/javascript; charset=utf-8"
        elif asset_path.suffix.lower() == ".html":
            content_type = "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_gateway_service_web_ui(bind_ip, port=SERVICE_WEB_UI_PORT, asset_dir=None, logger=None, cfg=None, controller=None):
    asset_root = pathlib.Path(asset_dir or pathlib.Path(__file__).resolve().parent).resolve()
    status_payload = _build_service_web_ui_status(bind_ip, port, cfg=cfg)
    event_store = _ServiceWebUIEventStore()
    event_store.add(_build_service_web_ui_startup_event(status_payload))
    handler_cls = type("GatewayServiceWebUIHandler", (_ServiceWebUIRequestHandler,), {})
    handler_cls.asset_dir = asset_root
    handler_cls.event_store = event_store
    handler_cls.status_payload = status_payload
    handler_cls.logger = logger
    handler_cls.controller = controller
    server = _GatewayServiceWebUIServer((str(bind_ip), int(port)), handler_cls)
    server.logger = logger
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="gateway-service-web-ui")
    thread.start()
    return {
        "bind_ip": status_payload["listen_ip"],
        "port": int(port),
        "url": status_payload["url"],
        "status": status_payload,
        "event_store": event_store,
        "server": server,
        "thread": thread,
    }

# Fountain code (FTN) constants — matching meshtastic/ATAK-Plugin FountainPacket.java
FOUNTAIN_MAGIC = b"FTN"
FOUNTAIN_BLOCK_SIZE = 214        # FountainPacket.MAX_PAYLOAD_SIZE
FOUNTAIN_DATA_HEADER_SIZE = 11   # MAGIC(3)+xfer_id(3)+seed(2)+K(1)+total_len(2)
FOUNTAIN_ACK_SIZE = 19           # MAGIC(3)+xfer_id(3)+type(1)+recv(2)+need(2)+hash(8)
FOUNTAIN_C = 0.1                 # Robust Soliton parameter c (FountainCodec default)
FOUNTAIN_DELTA = 0.5             # Robust Soliton parameter delta (FountainCodec default)
FOUNTAIN_RECEIVE_TTL_SECONDS = 300
DEFAULT_MESHTASTIC_CHANNEL_INDEX = 0
GEOCHAT_UID_PREFIX = "GeoChat."
MESHTASTIC_PLI_COT_EVENT_TYPE = "a-f-G-U-C"
MESHTASTIC_PLI_COT_EVENT_TYPE_NORMALIZED = MESHTASTIC_PLI_COT_EVENT_TYPE.lower()
MESHTASTIC_DEFAULT_TEAM_ENUM = 1  # White
MESHTASTIC_DEFAULT_ROLE_ENUM = 1  # TeamMember
MESHTASTIC_TEAM_NAME_TO_ENUM = {
    "WHITE": 1,
    "YELLOW": 2,
    "ORANGE": 3,
    "MAGENTA": 4,
    "RED": 5,
    "MAROON": 6,
    "PURPLE": 7,
    "DARKBLUE": 8,
    "BLUE": 9,
    "CYAN": 10,
    "TEAL": 11,
    "GREEN": 12,
    "DARKGREEN": 13,
    "BROWN": 14,
}
MESHTASTIC_ROLE_NAME_TO_ENUM = {
    "TEAMMEMBER": 1,
    "TEAMLEAD": 2,
    "HQ": 3,
    "SNIPER": 4,
    "MEDIC": 5,
    "FORWARDOBSERVER": 6,
    "RTO": 7,
    "K9": 8,
}
MESHTASTIC_TEAM_ENUM_TO_NAME = {value: key.title() for key, value in MESHTASTIC_TEAM_NAME_TO_ENUM.items()}
MESHTASTIC_ROLE_ENUM_TO_NAME = {
    value: (
        "Team Member" if key == "TEAMMEMBER"
        else "Team Lead" if key == "TEAMLEAD"
        else "Forward Observer" if key == "FORWARDOBSERVER"
        else key.title()
    )
    for key, value in MESHTASTIC_ROLE_NAME_TO_ENUM.items()
}
MESHTASTIC_TEAM_NAME_TO_COT_EVENT_TYPE = {
    "BLUE": "a-f-G-U-C",
    "CYAN": "a-f-G-U-C",
    "DARKBLUE": "a-f-G-U-C",
    "RED": "a-h-G-U-C",
    "MAROON": "a-h-G-U-C",
    "GREEN": "a-n-G-U-C",
    "DARKGREEN": "a-n-G-U-C",
    "YELLOW": "a-u-G-U-C",
    "ORANGE": "a-p-G-U-C",
}
MESHTASTIC_TEAM_ENUM_TO_ARGB = {
    1: 0xFFFFFFFF,
    2: 0xFFFFFF00,
    3: 0xFFFF7700,
    4: 0xFFFF00FF,
    5: 0xFFFF0000,
    6: 0xFF7F0000,
    7: 0xFF7F007F,
    8: 0xFF00007F,
    9: 0xFF0000FF,
    10: 0xFF00FFFF,
    11: 0xFF007F7F,
    12: 0xFF00FF00,
    13: 0xFF007F00,
    14: 0xFF777777,
}
MESHTASTIC_COT_HOW_ENUM_TO_NAME = {
    1: "h-e",
    2: "m-g",
    3: "h-g-i-g-o",
    4: "m-r",
    5: "m-f",
    6: "m-p",
    7: "m-s",
}
MESHTASTIC_COT_HOW_ENUM_TO_VALUE = dict(MESHTASTIC_COT_HOW_ENUM_TO_NAME)
MESHTASTIC_COT_TYPE_ENUM_TO_NAME = {
    1: "a-f-G-U-C",
    2: "a-f-G-U-C-I",
    25: "b-t-f",
    32: "a-f-G",
    33: "a-f-G-U",
    37: "b-m-r",
    34: "a-h-G",
    35: "a-u-G",
    36: "a-n-G",
    38: "b-m-p-w",
    39: "b-m-p-s-p-i",
    43: "u-rb-a",
    72: "b-m-p-s-p-loc",
    78: "b-m-p-s-m",
    79: "b-m-p-c",
    113: "b-m-p-w-GOTO",
    114: "b-m-p-c-ip",
    115: "b-m-p-c-cp",
    116: "b-m-p-s-p-op",
    120: "b-i-x-i",
    121: "b-t-f-d",
    122: "b-t-f-r",
}
MESHTASTIC_PROTO_MARKER_KIND_NAME_TO_COT_TYPE = {
    "SPOT": "b-m-p-s-m",
    "WAYPOINT": "b-m-p-w",
    "CHECKPOINT": "b-m-p-c",
    "SELFPOSITION": "b-m-p-s-p-i",
    "GOTOPOINT": "b-m-p-w-GOTO",
    "INITIALPOINT": "b-m-p-c-ip",
    "CONTACTPOINT": "b-m-p-c-cp",
    "OBSERVATIONPOST": "b-m-p-s-p-op",
    "IMAGEMARKER": "b-i-x-i",
}
MESHTASTIC_V2_MARKER_KIND_TO_COT_TYPE = {
    1: "b-m-p-s-m",
    2: "b-m-p-w",
    3: "b-m-p-c",
    4: "b-m-p-s-p-i",
    8: "b-m-p-w-GOTO",
    9: "b-m-p-c-ip",
    10: "b-m-p-c-cp",
    11: "b-m-p-s-p-op",
    12: "b-i-x-i",
}
MESHTASTIC_MARKER_KIND_TO_COT_TYPE = dict(MESHTASTIC_V2_MARKER_KIND_TO_COT_TYPE)
MESHTASTIC_MARKER_KIND_TO_LABEL = {
    1: "Spot",
    2: "Waypoint",
    3: "Checkpoint",
    4: "SelfPosition",
    5: "Symbol2525",
    6: "SpotMap",
    7: "CustomIcon",
    8: "GoToPoint",
    9: "InitialPoint",
    10: "ContactPoint",
    11: "ObservationPost",
    12: "ImageMarker",
}


def _enum_identifier_to_cot_type(identifier):
    normalized = str(identifier or "").strip()
    if not normalized:
        return ""
    if normalized.startswith("CotType_"):
        normalized = normalized[len("CotType_"):]
    parts = [part for part in normalized.split("_") if part]
    return "-".join(parts)


def _load_proto_enum_value_map(proto_path, enum_name, scope=None):
    try:
        with open(proto_path, "r", encoding="utf-8") as proto_file:
            proto_text = proto_file.read()
    except OSError:
        return {}

    if scope:
        scope_match = re.search(
            rf"\b(?:message|enum)\s+{re.escape(scope)}\s*\{{(?P<body>.*?)\n\}}",
            proto_text,
            re.DOTALL,
        )
        if not scope_match:
            return {}
        proto_text = scope_match.group("body")

    enum_match = re.search(
        rf"\benum\s+{re.escape(enum_name)}\s*\{{(?P<body>.*?)\n\s*\}}",
        proto_text,
        re.DOTALL,
    )
    if not enum_match:
        return {}

    values = {}
    for name, raw_value in re.findall(
        r"^\s*([A-Za-z][A-Za-z0-9_]*)\s*=\s*(-?\d+)\s*;",
        enum_match.group("body"),
        re.MULTILINE,
    ):
        try:
            values[int(raw_value)] = name
        except ValueError:
            continue
    return values


def _load_meshtastic_atak_proto_mappings():
    proto_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "atak.proto")
    cot_type_map = dict(MESHTASTIC_COT_TYPE_ENUM_TO_NAME)
    for value, enum_name in _load_proto_enum_value_map(proto_path, "CotType").items():
        cot_type = _enum_identifier_to_cot_type(enum_name)
        if cot_type and cot_type.lower() != "other":
            cot_type_map[value] = cot_type

    marker_kind_map = dict(MESHTASTIC_V2_MARKER_KIND_TO_COT_TYPE)
    for value, enum_name in _load_proto_enum_value_map(proto_path, "Kind", scope="Marker").items():
        normalized_name = re.sub(r"[^A-Z0-9]", "", str(enum_name or "").upper())
        if normalized_name.startswith("KIND"):
            normalized_name = normalized_name[4:]
        cot_type = MESHTASTIC_PROTO_MARKER_KIND_NAME_TO_COT_TYPE.get(normalized_name)
        if cot_type:
            marker_kind_map[value] = cot_type
    return cot_type_map, marker_kind_map


MESHTASTIC_COT_TYPE_ENUM_TO_NAME, MESHTASTIC_V2_MARKER_KIND_TO_COT_TYPE = _load_meshtastic_atak_proto_mappings()
MESHTASTIC_V2_COT_TYPE_ID_TO_VALUE = dict(MESHTASTIC_COT_TYPE_ENUM_TO_NAME)
MESHTASTIC_MARKER_KIND_TO_COT_TYPE = dict(MESHTASTIC_V2_MARKER_KIND_TO_COT_TYPE)
TAK_EVENT_PATTERN = re.compile(r"<event\b[^>]*>.*?</event>", re.DOTALL)
TAK_PING_EVENT_TYPE = "t-x-c-t"
TAK_PONG_EVENT_TYPE = "t-x-c-t-r"
# Compiled patterns for detecting WinTAK/ATAK keepalive ping and pong types in CoT XML.
# Using anchors around the quoted value avoids false positives
# (e.g. the pong type "t-x-c-t-r" also contains the ping type "t-x-c-t").
TAK_PING_TYPE_PATTERN = re.compile(r'\btype=["\']t-x-c-t["\']')
TAK_PONG_TYPE_PATTERN = re.compile(r'\btype=["\']t-x-c-t-r["\']')
MIN_NULL_BYTES_FOR_UTF16 = 2
UTF16_NULL_BYTE_RATIO_THRESHOLD = 4
_WINTAK_CHAT_TRANSCRIPT_LINE_PATTERN = re.compile(
    r"^\((?P<time>\d{1,2}:\d{2}(?::\d{2})?)\)\s+(?P<sender>.+):(?:\s*(?P<message>.*))?$"
)
_WINTAK_CHAT_FIELD_PATTERN = re.compile(
    r"^(?:message|text|note|remarks|body|content)(?P<index>\d+)?$",
    re.IGNORECASE,
)
_TAK_CHAT_MESSAGE_FIELD_NAMES = frozenset({"message", "text", "body", "content", "note", "_chat"})


def _resolve_meshtastic_portnum(primary_name, fallback, *alternate_names):
    """Resolve a Meshtastic PortNum enum name with legacy fallback values.

    This helper is evaluated during module import to populate cached module-level
    constants for outbound ATAK packet routing.

    Args:
        primary_name: Preferred enum attribute name from ``portnums_pb2.PortNum``.
        fallback: Legacy numeric port number used when the enum is unavailable.
        *alternate_names: Additional enum attribute names checked in order.

    Returns:
        int: The first matching enum value, or the provided fallback when
        ``portnums_pb2`` is unavailable or none of the requested enum names
        exist in the installed Meshtastic version.
    """
    if portnums_pb2 is None:
        return fallback
    for name in (primary_name,) + alternate_names:
        try:
            value = getattr(portnums_pb2.PortNum, name, None)
            if value is not None:
                return int(value)
        except (AttributeError, TypeError, ValueError):
            continue
    return fallback


MESHTASTIC_ATAK_PLUGIN_V1_PORTNUM = _resolve_meshtastic_portnum(
    "ATAK_PLUGIN",
    72,
)
MESHTASTIC_ATAK_PLUGIN_V2_PORTNUM = _resolve_meshtastic_portnum(
    "ATAK_PLUGIN_V2",
    78,
)
MESHTASTIC_ATAK_PLUGIN_PORTNUM = MESHTASTIC_ATAK_PLUGIN_V1_PORTNUM
MESHTASTIC_ATAK_FORWARDER_PORTNUM = _resolve_meshtastic_portnum(
    "ATAK_FORWARDER",
    257,
)


def get_tak_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')


def as_bool(value, default=False):
    """Convert common config value types to bool with a fallback default.

    Args:
        value: Input value from config (bool/str/number/other).
        default (bool): Fallback when value cannot be mapped explicitly.

    Returns:
        bool: Parsed boolean value.

    Notes:
        True values: bool True, non-zero numbers (including negative), and strings like
        '1', 'true', 'yes', 'y', 'on' (case-insensitive).
        False values: bool False, zero, and strings like
        '0', 'false', 'no', 'n', 'off' (case-insensitive).
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def normalize_coordinates(lat, lon):
    """Validate and normalize latitude/longitude values.

    Args:
        lat: Latitude value convertible to float.
        lon: Longitude value convertible to float.

    Returns:
        tuple[float, float] | None: Valid (lat, lon) coordinates, or None if
        parsing fails, values are NaN/out of range, or exactly (0, 0).
    """
    try:
        latitude = float(lat)
        longitude = float(lon)
    except (TypeError, ValueError):
        return None

    if math.isnan(latitude) or math.isnan(longitude):
        return None
    if not (-90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0):
        return None
    if latitude == 0.0 and longitude == 0.0:
        return None
    return latitude, longitude


def to_float_or_none(value):
    """Convert value to float if possible, otherwise return None."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_meshtastic_node_ids(node_num):
    """Return common string representations for a Meshtastic node number."""
    return f"!{node_num:08x}", f"ID-{node_num:08x}"


def normalize_meshtastic_uid(raw_uid):
    """Normalize Meshtastic IDs like !4ef117fc to the TAK-friendly ID-4ef117fc form."""
    raw_uid = str(raw_uid)
    if raw_uid.startswith("!") and len(raw_uid) == 9:
        try:
            return format_meshtastic_node_ids(int(raw_uid[1:], 16))[1]
        except ValueError:
            pass
    return raw_uid.replace("!", "ID-", 1)


def resolve_meshtastic_destination_id(raw_uid, default="^all"):
    """Convert TAK-facing chat destinations to Meshtastic sendData destination IDs."""
    normalized = _strip_tak_sender_prefix(str(raw_uid or "").strip())
    if not normalized:
        return default
    if normalized in {"^all", DEFAULT_CHATROOM_NAME}:
        return "^all"
    if normalized.startswith("!") and len(normalized) == 9:
        try:
            int(normalized[1:], 16)
            return normalized
        except ValueError:
            return normalized
    if normalized.startswith("ID-") and len(normalized) == 11:
        try:
            node_num = int(normalized[3:], 16)
        except ValueError:
            return normalized
        return format_meshtastic_node_ids(node_num)[0]
    return normalized


def _xml_local_name(tag):
    """Return an XML tag name without any namespace prefix."""
    if not tag:
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _find_child_by_local_name(parent, name):
    """Find the first direct child with the given local XML tag name."""
    if parent is None:
        return None
    for child in parent:
        if _xml_local_name(child.tag) == name:
            return child
    return None


def _find_descendant_by_local_name(parent, name):
    """Find the first descendant with the given local XML tag name."""
    if parent is None:
        return None
    for child in parent.iter():
        if child is parent:
            continue
        if _xml_local_name(child.tag) == name:
            return child
    return None


def _find_descendant_by_local_names(parent, *names):
    """Find the first descendant whose local XML tag name matches any given name."""
    if parent is None:
        return None
    wanted = {str(name or "").lower() for name in names if str(name or "").strip()}
    if not wanted:
        return None
    for child in parent.iter():
        if child is parent:
            continue
        if _xml_local_name(child.tag).lower() in wanted:
            return child
    return None


def _find_descendant_attribute_value(parent, *names):
    """Return the first non-empty descendant attribute whose name matches any given name."""
    if parent is None:
        return ""
    wanted = {str(name or "").lower() for name in names if str(name or "").strip()}
    if not wanted:
        return ""
    for child in parent.iter():
        for attr_name, attr_value in child.attrib.items():
            if str(attr_name or "").lower() not in wanted:
                continue
            normalized_value = str(attr_value or "").strip()
            if normalized_value:
                return normalized_value
    return ""


def _collect_xml_text(element):
    """Collect stripped text from an XML element and all of its descendants."""
    if element is None:
        return ""
    text_parts = [text.strip() for text in element.itertext() if text and text.strip()]
    return "\n".join(text_parts)


def _get_tak_tcp_listener_endpoint_from_cfg(cfg, strict_port=False):
    """Return configured TCP listener bind IP/port with shared defaults."""
    listen_ip = str(cfg.get("local_tak_tcp_listen_ip", "0.0.0.0")).strip() or "0.0.0.0"
    try:
        listen_port = int(cfg.get("local_tak_tcp_listen_port", TCP_LISTENER_DEFAULT_PORT))
    except (TypeError, ValueError):
        if strict_port:
            raise
        listen_port = TCP_LISTENER_DEFAULT_PORT
    return listen_ip, listen_port


def _get_tak_tcp_receiver_endpoint_from_cfg(cfg, strict_port=False):
    """Return configured TCP receiver target host/port with shared defaults."""
    receiver_host = str(
        cfg.get("local_tak_tcp_receiver_host", cfg.get("local_tak_ip", WINTAK_REQUIRED_HOST))
    ).strip() or str(cfg.get("local_tak_ip", WINTAK_REQUIRED_HOST)).strip() or WINTAK_REQUIRED_HOST
    try:
        receiver_port = int(cfg.get("local_tak_tcp_receiver_port", TCP_RECEIVER_DEFAULT_PORT))
    except (TypeError, ValueError):
        if strict_port:
            raise
        receiver_port = TCP_RECEIVER_DEFAULT_PORT
    return receiver_host, receiver_port


def _strip_tak_sender_prefix(value):
    """Normalize common TAK sender prefixes like BAO.F.ATAK./BAO.F.WinTAK."""
    normalized = "" if value is None else str(value)
    for prefix in ("BAO.F.ATAK.", "BAO.F.WinTAK."):
        if normalized.startswith(prefix):
            return normalized[len(prefix):]
    return normalized


def _looks_like_valid_tak_uid(value):
    uid = str(value or "").strip()
    if not uid:
        return False
    if uid.startswith("<") or uid.startswith("<?xml"):
        return False
    return all(ch >= " " and ch != "\x7f" for ch in uid)


def _extract_latest_wintak_chat_message(text):
    """Collapse WinTAK transcript exports to the newest message body.

    Args:
        text: Raw WinTAK chat text or transcript export. May be None.

    Returns:
        str: The newest extracted message body, the original text for non-transcript
        payloads, or an empty string when only transcript headers are present.
    """
    normalized_text = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized_text:
        return ""

    transcript_blocks = []
    saw_transcript_header = False
    current_block = None
    for raw_line in normalized_text.split("\n"):
        line = raw_line.strip()
        if not line:
            if current_block is not None:
                current_block.append("")
            continue

        match = _WINTAK_CHAT_TRANSCRIPT_LINE_PATTERN.match(line)
        if match:
            saw_transcript_header = True
            inline_message = (match.group("message") or "").strip()
            current_block = [inline_message] if inline_message else []
            transcript_blocks.append(current_block)
            continue

        if current_block is not None:
            current_block.append(line)

    for block_lines in reversed(transcript_blocks):
        message = "\n".join(part for part in block_lines if part).strip()
        if message:
            return message

    if saw_transcript_header:
        return ""

    return normalized_text


def _extract_latest_wintak_chat_attribute_message(element):
    """Find the newest usable chat payload from supported chat-like XML attributes."""
    if element is None:
        return ""

    candidates = []
    for node in element.iter():
        matching_attrs = []
        for attr_name, attr_value in node.attrib.items():
            if attr_value is None:
                continue
            normalized_value = str(attr_value).strip()
            if not normalized_value:
                continue
            match = _WINTAK_CHAT_FIELD_PATTERN.match(attr_name)
            if not match:
                continue
            attr_index = int(match.group("index") or "0")
            matching_attrs.append((attr_index, attr_name.lower(), normalized_value))
        # Preserve the natural numbered order so later messageN fields can win when
        # WinTAK exports several message-like attributes on the same XML element.
        matching_attrs.sort(key=lambda item: (item[0], item[1]))
        candidates.extend(value for _, _, value in matching_attrs)

    for candidate in reversed(candidates):
        latest_message = _extract_latest_wintak_chat_message(candidate)
        if latest_message:
            return latest_message
    return ""


def _is_tak_chat_message_local_name(local_name):
    normalized = str(local_name or "").strip().lower()
    if not normalized:
        return False
    return normalized in _TAK_CHAT_MESSAGE_FIELD_NAMES or bool(_WINTAK_CHAT_FIELD_PATTERN.match(normalized))


def _looks_like_tak_chat_remarks(remarks):
    """Return True when a TAK <remarks> element looks like a GeoChat payload."""
    if remarks is None:
        return False
    for attr_name in ("source", "sourceID", "sourceId", "to"):
        if str(remarks.get(attr_name) or "").strip():
            return True
    return False


def _has_tak_chat_message_fields(element):
    """Return True when detail XML contains message-like nodes/attributes.

    The attribute fallback reuses ``_WINTAK_CHAT_FIELD_PATTERN`` so WinTAK exports
    such as ``message1``/``text2``/``note`` are recognized even without GeoChat
    wrapper elements like ``<chat>`` or ``<__chat>``.
    """
    if element is None:
        return False
    for node in element.iter():
        local_name = _xml_local_name(node.tag).lower()
        if _is_tak_chat_message_local_name(local_name):
            node_text = _collect_xml_text(node).strip()
            if node_text:
                return True
            if any(str(value or "").strip() for value in node.attrib.values()):
                return True
        for attr_name, attr_value in node.attrib.items():
            if not str(attr_value or "").strip():
                continue
            if _WINTAK_CHAT_FIELD_PATTERN.match(attr_name):
                return True
    return False


def _format_network_endpoint(address):
    """Render IPv4/IPv6 socket addresses in a compact log-friendly form."""
    if not address:
        return "unbekannt"
    if isinstance(address, (tuple, list)) and address:
        host = str(address[0])
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        if len(address) >= 2 and address[1] is not None:
            return f"{host}:{address[1]}"
        return host
    return str(address)


def _build_safe_payload_snippet(payload, limit=INBOUND_TAK_DEBUG_SNIPPET_CHARS):
    """Return a single-line, truncated payload preview for DEBUG diagnostics."""
    if isinstance(payload, (bytes, bytearray)):
        text = bytes(payload).decode("utf-8", errors="replace")
    else:
        text = str(payload or "")
    compact = " ".join(text.replace("\x00", " ").replace("\r", "\n").split())
    if len(compact) > limit:
        return compact[:limit] + "…"
    return compact


RAW_PAYLOAD_FULL_HEX_THRESHOLD = 128  # bytes – payloads up to this size get a full hex dump


def _format_raw_meshtastic_payload(payload, full=False):
    """Return a human-readable, inspectable representation of a raw binary payload.

    Args:
        payload: bytes/bytearray or any value that can be coerced to bytes.
        full:    When True every byte is included regardless of size.
                 When False payloads larger than RAW_PAYLOAD_FULL_HEX_THRESHOLD
                 are represented as base64 with a hex-prefix snippet.

    Returns:
        A multi-field string suitable for a single log line:
        ``len=N hex=<hex> b64=<base64>``  (full)
        ``len=N hex_prefix=<first 16 bytes hex> b64=<base64>``  (truncated)
    """
    if payload is None:
        return "len=0 hex= b64="
    if isinstance(payload, (bytes, bytearray)):
        raw = bytes(payload)
    else:
        raw = str(payload).encode("utf-8", errors="replace")
    n = len(raw)
    b64 = base64.b64encode(raw).decode("ascii")
    if full or n <= RAW_PAYLOAD_FULL_HEX_THRESHOLD:
        hex_str = raw.hex()
        return f"len={n} hex={hex_str} b64={b64}"
    # Large payload: show a 16-byte prefix hex and full base64
    hex_prefix = raw[:16].hex()
    return f"len={n} hex_prefix={hex_prefix}… b64={b64}"


def _ensure_bytes(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value or "").encode("utf-8")


def _strip_invalid_xml_chars(text):
    if not text:
        return ""
    return "".join(
        char
        for char in str(text)
        if (
            char in "\t\n\r"
            or 0x20 <= ord(char) <= 0xD7FF
            or 0xE000 <= ord(char) <= 0xFFFD
            or 0x10000 <= ord(char) <= 0x10FFFF
        )
    )


# ─────────────────── Fountain Code (FTN) helpers ──────────────────────────
# Implements the LT-code fountain codec used by meshtastic/ATAK-Plugin for
# large ATAK_FORWARDER payloads.  The algorithm is a strict port of
# FountainCodec.java / FountainPacket.java (meshtastic/ATAK-Plugin).

class _JavaRandom:
    """Mimics java.util.Random (LCG, 48-bit state) for fountain-code compatibility."""
    _MUL = 0x5DEECE66D
    _ADD = 0xB
    _MASK = (1 << 48) - 1

    def __init__(self, seed):
        self.state = (int(seed) ^ self._MUL) & self._MASK

    def _next(self, bits):
        self.state = (self.state * self._MUL + self._ADD) & self._MASK
        return self.state >> (48 - bits)

    def next_int(self, bound=None):
        if bound is None:
            return self._next(32)
        if bound <= 0:
            raise ValueError("bound must be positive")
        if (bound & -bound) == bound:          # power of 2 fast path
            return (bound * self._next(31)) >> 31
        while True:
            bits = self._next(31)
            val = bits % bound
            if bits - val + (bound - 1) >= 0:
                return val

    def next_double(self):
        return ((self._next(26) << 27) + self._next(27)) / float(1 << 53)


def _fountain_robust_soliton_cdf(K, c=FOUNTAIN_C, delta=FOUNTAIN_DELTA):
    """Build Robust Soliton CDF (matches FountainCodec.buildRobustSolitonCDF)."""
    rho = [0.0] * (K + 1)
    tau = [0.0] * (K + 1)
    cdf = [0.0] * (K + 1)
    rho[1] = 1.0 / K
    for d in range(2, K + 1):
        rho[d] = 1.0 / (d * (d - 1))
    S = c * math.log(K / delta) * math.sqrt(K)
    threshold = int(math.floor(K / S))
    for d in range(1, K + 1):
        if d < threshold:
            tau[d] = S / (K * d)
        elif d == threshold:
            tau[d] = S * math.log(S / delta) / K
    Z = sum(rho[d] + tau[d] for d in range(1, K + 1))
    cumulative = 0.0
    for d in range(1, K + 1):
        cumulative += (rho[d] + tau[d]) / Z
        cdf[d] = cumulative
    return cdf


def _fountain_sample_degree(rng, K):
    """Sample degree from Robust Soliton distribution."""
    cdf = _fountain_robust_soliton_cdf(K)
    u = rng.next_double()
    for d in range(1, K + 1):
        if u <= cdf[d]:
            return d
    return K


def _fountain_select_indices(rng, K, degree):
    """Select *degree* source-block indices without replacement (matches Java selectIndices)."""
    degree = min(degree, K)
    selected = set()
    while len(selected) < degree:
        selected.add(rng.next_int(K))
    return sorted(selected)


def _fountain_regenerate_indices(seed, K, transfer_id):
    """Regenerate source-block indices from a received block's seed (decoder side).
    Matches FountainCodec.regenerateIndices(seed, K, transferId)."""
    rng = _JavaRandom(seed)
    block0_seed = (transfer_id * 31337) & 0xFFFF
    is_first_block = (seed == block0_seed)
    # Always consume one degree sample to keep RNG in sync with the encoder
    _fountain_sample_degree(rng, K)
    if is_first_block:
        degree = 1
        return _fountain_select_indices(rng, K, degree)
    # For non-first blocks: re-seed and sample again (matches Java logic)
    rng = _JavaRandom(seed)
    degree = _fountain_sample_degree(rng, K)
    return _fountain_select_indices(rng, K, degree)


def _fountain_decode(blocks, K, total_length, block_size=FOUNTAIN_BLOCK_SIZE):
    """LT-code peeling decoder (matches FountainCodec.decode).

    *blocks* is a list of ``(seed, transfer_id, raw_payload_bytes)`` tuples.
    Returns the original data bytes on success, or ``None`` if decoding failed.
    """
    decoded = [None] * K
    is_decoded = [False] * K
    decoded_count = 0
    working = []
    for seed, transfer_id, payload in blocks:
        indices = set(_fountain_regenerate_indices(seed, K, transfer_id))
        padded = bytearray(block_size)
        src = bytes(payload)
        padded[:len(src)] = src[:block_size]
        working.append([indices, padded])

    progress = True
    while progress and decoded_count < K:
        progress = False
        for i, wb in enumerate(working):
            if wb is None:
                continue
            indices, payload = wb
            remaining = set()
            for idx in indices:
                if is_decoded[idx]:
                    xor_len = min(len(payload), len(decoded[idx]))
                    for j in range(xor_len):
                        payload[j] ^= decoded[idx][j]
                else:
                    remaining.add(idx)
            wb[0] = remaining
            if len(remaining) == 1:
                idx = next(iter(remaining))
                decoded[idx] = bytes(payload)
                is_decoded[idx] = True
                decoded_count += 1
                working[i] = None
                progress = True
            elif not remaining:
                working[i] = None

    if decoded_count < K:
        return None
    result = bytearray()
    for block in decoded:
        result.extend(block or bytes(block_size))
    return bytes(result[:total_length])


def _fountain_parse_data_block(raw):
    """Parse a FTN data-block packet from raw bytes.
    Returns a dict with keys (transfer_id, seed, K, total_len, payload) or None."""
    if not raw or len(raw) < FOUNTAIN_DATA_HEADER_SIZE:
        return None
    raw = bytes(raw)
    if raw[:3] != FOUNTAIN_MAGIC:
        return None
    # ACK packets have the same magic — length distinguishes them
    if len(raw) == FOUNTAIN_ACK_SIZE:
        return None  # ACK, not a data block
    transfer_id = (raw[3] << 16) | (raw[4] << 8) | raw[5]
    seed = (raw[6] << 8) | raw[7]
    K = raw[8]
    total_len = (raw[9] << 8) | raw[10]
    payload = raw[FOUNTAIN_DATA_HEADER_SIZE:]
    if K == 0 or total_len == 0 or not payload:
        return None
    return {
        "transfer_id": transfer_id,
        "seed": seed,
        "K": K,
        "total_len": total_len,
        "payload": payload,
    }


def _fountain_generate_seed(transfer_id, block_index):
    """Deterministic seed for a block (matches FountainCodec.generateSeed)."""
    return (transfer_id * 31337 + block_index * 7919) & 0xFFFF


def _fountain_encode(data, transfer_id):
    """Encode *data* into FTN fountain packets (matches FountainChunkManager.send).

    Returns a list of raw packet bytes (each ready to send as an ATAK_FORWARDER payload).
    The transfer type byte (0x00 = COT) is prepended to *data* before encoding,
    matching FountainChunkManager behaviour.
    """
    # Prepend transfer-type byte (0x00 = COT) — matches FountainChunkManager
    payload_with_type = bytes([MESHTASTIC_TRANSFER_TYPE_COT]) + bytes(data)
    total = len(payload_with_type)
    K = math.ceil(total / FOUNTAIN_BLOCK_SIZE)
    if K == 0:
        return []
    source_blocks = []
    for i in range(K):
        start = i * FOUNTAIN_BLOCK_SIZE
        block = payload_with_type[start:start + FOUNTAIN_BLOCK_SIZE]
        if len(block) < FOUNTAIN_BLOCK_SIZE:
            block = block + b"\x00" * (FOUNTAIN_BLOCK_SIZE - len(block))
        source_blocks.append(bytearray(block))
    # Adaptive overhead: 50% for K≤10, 25% for K≤50, else 15%
    if K <= 10:
        overhead = 0.50
    elif K <= 50:
        overhead = 0.25
    else:
        overhead = 0.15
    num_blocks = math.ceil(K * (1 + overhead))
    packets = []
    for i in range(num_blocks):
        seed = _fountain_generate_seed(transfer_id, i)
        rng = _JavaRandom(seed)
        if i == 0:
            # Block 0: forced degree 1; advance RNG past sampleDegree first
            _fountain_sample_degree(rng, K)
            indices = _fountain_select_indices(rng, K, 1)
        else:
            degree = _fountain_sample_degree(rng, K)
            indices = _fountain_select_indices(rng, K, degree)
        enc = bytearray(FOUNTAIN_BLOCK_SIZE)
        for idx in indices:
            for j in range(FOUNTAIN_BLOCK_SIZE):
                enc[j] ^= source_blocks[idx][j]
        packet = bytes([
            FOUNTAIN_MAGIC[0], FOUNTAIN_MAGIC[1], FOUNTAIN_MAGIC[2],
            (transfer_id >> 16) & 0xFF, (transfer_id >> 8) & 0xFF, transfer_id & 0xFF,
            (seed >> 8) & 0xFF, seed & 0xFF,
            K & 0xFF,
            (total >> 8) & 0xFF, total & 0xFF,
        ]) + bytes(enc)
        packets.append(packet)
    return packets


def _fountain_decode_payload(data_bytes):
    """Strip the transfer-type prefix and zlib-decompress a fountain-decoded payload.

    Returns the CoT XML bytes on success, or ``None`` on failure.
    """
    if not data_bytes:
        return None
    # Strip leading transfer-type byte when present
    if data_bytes[0] in (
        MESHTASTIC_TRANSFER_TYPE_COT,
        MESHTASTIC_TRANSFER_TYPE_COT_ASCII,
        MESHTASTIC_TRANSFER_TYPE_FILE,
        MESHTASTIC_TRANSFER_TYPE_FILE_ASCII,
    ):
        if data_bytes[0] in (MESHTASTIC_TRANSFER_TYPE_FILE, MESHTASTIC_TRANSFER_TYPE_FILE_ASCII):
            return None  # File transfer — not a CoT
        data_bytes = data_bytes[1:]
    if not data_bytes:
        return None
    # Try standard zlib, then raw deflate, then raw XML
    for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
        try:
            return zlib.decompress(data_bytes, wbits)
        except zlib.error:
            continue
    if data_bytes.lstrip().startswith(b"<"):
        return data_bytes
    return None
# ──────────────────────────────────────────────────────────────────────────


def _normalize_tak_xml_payload(packet_xml):
    normalized_bytes = _extract_first_tak_event(packet_xml)
    if not normalized_bytes:
        normalized_bytes = _decode_tak_packet_bytes(packet_xml)
    normalized_text = _strip_invalid_xml_chars(
        _ensure_bytes(normalized_bytes).decode("utf-8", errors="ignore")
    ).strip()
    return normalized_text.encode("utf-8")


def _is_xml_text(text):
    stripped = str(text or "").lstrip(UTF8_BOM_CHAR).strip()
    return stripped.startswith("<") and ">" in stripped


def _should_attempt_utf16_decode(raw_bytes):
    null_bytes = raw_bytes.count(b"\x00")
    return (
        null_bytes >= MIN_NULL_BYTES_FOR_UTF16
        and null_bytes >= len(raw_bytes) // UTF16_NULL_BYTE_RATIO_THRESHOLD
    )


def _detect_tak_stream_encoding(packet_bytes):
    """Best-effort encoding detection for inbound TAK XML packets/streams."""
    # Keep UTF-16 NUL bytes intact here; stripping them can corrupt odd-length payloads.
    packet_bytes = _ensure_bytes(packet_bytes).strip(b" \t\r\n")
    if not packet_bytes:
        return None

    if packet_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if packet_bytes.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    if _should_attempt_utf16_decode(packet_bytes):
        for encoding in ("utf-16-le", "utf-16-be"):
            try:
                decoded_text = packet_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
            if _is_xml_text(decoded_text):
                return encoding
        return None
    if packet_bytes.startswith(b"<") or b"<event" in packet_bytes or b"<?xml" in packet_bytes:
        return "utf-8"
    return None


def _decode_tak_packet_bytes(packet_bytes):
    """Normalize common TAK UDP packet encodings to UTF-8 XML bytes."""
    packet_bytes = _ensure_bytes(packet_bytes)
    # Keep UTF-16 NUL bytes intact here; stripping them can corrupt odd-length payloads.
    packet_bytes = packet_bytes.strip(b" \t\r\n")
    if not packet_bytes:
        return b""

    encoding = _detect_tak_stream_encoding(packet_bytes)
    if encoding is None:
        return packet_bytes
    try:
        return packet_bytes.decode(encoding).encode("utf-8")
    except UnicodeDecodeError:
        return packet_bytes


def _extract_first_tak_event(packet_bytes):
    """Return the first CoT <event> XML found in a decoded TAK packet payload."""
    normalized_bytes = _decode_tak_packet_bytes(packet_bytes)
    normalized_bytes = _ensure_bytes(normalized_bytes).strip(b"\x00 \t\r\n")
    if not normalized_bytes:
        return b""

    try:
        root = fromstring(normalized_bytes)
    except (ParseError, TypeError, ValueError):
        root = None
    if root is not None and _xml_local_name(root.tag) == "event":
        return normalized_bytes

    decoded_text = normalized_bytes.decode("utf-8", errors="ignore")
    match = TAK_EVENT_PATTERN.search(decoded_text)
    if not match:
        return normalized_bytes
    return match.group(0).encode("utf-8")


def _coerce_cot_point_float(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _read_protobuf_varint(buffer, offset):
    """Read a protobuf varint from buffer starting at offset."""
    result = 0
    shift = 0
    while offset < len(buffer):
        byte = buffer[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, offset
        shift += 7
        if shift >= 64:
            break
    raise ValueError("Ungültige Protobuf-Varint-Daten")


def _encode_protobuf_varint(value):
    """Encode an integer as protobuf varint bytes."""
    value = int(value)
    if value < 0:
        # Protobuf int32/int64 fields use two's complement varints for negative values.
        # (Only sint32/sint64 use zig-zag encoding.)
        value += 1 << 64
    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value & 0x7F)
    return bytes(encoded)


def _encode_protobuf_key(field_number, wire_type):
    return _encode_protobuf_varint((int(field_number) << 3) | int(wire_type))


def _encode_protobuf_length_delimited(value):
    value = _ensure_bytes(value)
    return _encode_protobuf_varint(len(value)) + value


def _encode_protobuf_varint_field(field_number, value):
    return _encode_protobuf_key(field_number, 0) + _encode_protobuf_varint(value)


def _encode_protobuf_sfixed32_field(field_number, value):
    return _encode_protobuf_key(field_number, 5) + int(value).to_bytes(4, byteorder="little", signed=True)


def _encode_protobuf_string_field(field_number, value):
    encoded = _ensure_bytes(value)
    if not encoded:
        return b""
    return _encode_protobuf_key(field_number, 2) + _encode_protobuf_length_delimited(encoded)


def _encode_protobuf_message_field(field_number, payload):
    payload = bytes(payload or b"")
    if not payload:
        return b""
    return _encode_protobuf_key(field_number, 2) + _encode_protobuf_length_delimited(payload)


def _skip_protobuf_field(buffer, offset, wire_type):
    """Skip a protobuf field payload and return the next offset."""
    if wire_type == 0:  # varint
        _, offset = _read_protobuf_varint(buffer, offset)
        return offset
    if wire_type == 1:  # fixed64
        return offset + 8
    if wire_type == 2:  # length-delimited
        length, offset = _read_protobuf_varint(buffer, offset)
        return offset + length
    if wire_type == 5:  # fixed32
        return offset + 4
    raise ValueError(f"Nicht unterstützter Protobuf-Wire-Type: {wire_type}")


def _decode_protobuf_string(value):
    if not value:
        return ""
    return bytes(value).decode("utf-8", errors="ignore").strip()


def _decode_protobuf_sfixed32(value):
    value = bytes(value or b"")
    if len(value) != 4:
        raise ValueError("Ungültige Länge für protobuf sfixed32")
    return int.from_bytes(value, byteorder="little", signed=True)


def _decode_protobuf_fixed32(value):
    value = bytes(value or b"")
    if len(value) != 4:
        raise ValueError("Ungültige Länge für protobuf fixed32")
    return int.from_bytes(value, byteorder="little", signed=False)


def _decode_protobuf_sint32(value):
    value = int(value or 0)
    return (value >> 1) ^ -(value & 1)


def _read_protobuf_length_delimited(buffer, offset):
    """Read a protobuf length-delimited field and return its bytes and next offset."""
    length, offset = _read_protobuf_varint(buffer, offset)
    end_offset = offset + length
    if length < 0 or end_offset > len(buffer):
        raise ValueError("Protobuf-Feld überschreitet das Ende des Puffers")
    return buffer[offset:end_offset], end_offset


def _parse_meshtastic_atak_contact(payload):
    """Extract the subset of ATAK contact fields needed for chat forwarding."""
    contact = {}
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type != 2:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        field_bytes, offset = _read_protobuf_length_delimited(payload, offset)
        if field_number == 1:
            contact["callsign"] = _decode_protobuf_string(field_bytes)
        elif field_number == 2:
            contact["device_callsign"] = _decode_protobuf_string(field_bytes)
    return contact


def _parse_meshtastic_atak_chat(payload):
    """Extract the subset of ATAK GeoChat fields needed for TAK chat forwarding."""
    chat = {}
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type == 2:
            field_bytes, offset = _read_protobuf_length_delimited(payload, offset)
            if field_number == 1:
                chat["message"] = _decode_protobuf_string(field_bytes)
            elif field_number == 2:
                chat["to"] = _decode_protobuf_string(field_bytes)
            elif field_number == 3:
                chat["to_callsign"] = _decode_protobuf_string(field_bytes)
            elif field_number == 4:
                chat["receipt_for_uid"] = _decode_protobuf_string(field_bytes)
        elif wire_type == 0:
            value, offset = _read_protobuf_varint(payload, offset)
            if field_number == 5:
                chat["receipt_type"] = value
        else:
            offset = _skip_protobuf_field(payload, offset, wire_type)
    return chat


def _parse_meshtastic_atak_group(payload):
    group = {}
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type != 0:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        value, offset = _read_protobuf_varint(payload, offset)
        if field_number == 1:
            group["role"] = value
        elif field_number == 2:
            group["team"] = value
    return group


def _parse_meshtastic_atak_status(payload):
    status = {}
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type != 0:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        value, offset = _read_protobuf_varint(payload, offset)
        if field_number == 1:
            status["battery"] = value
    return status


def _parse_meshtastic_atak_pli(payload):
    pli = {}
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type == 5:
            field_bytes = payload[offset:offset + 4]
            offset += 4
            if len(field_bytes) != 4:
                raise ValueError("Unvollständiges protobuf sfixed32-Feld im PLI-Payload")
            value = _decode_protobuf_sfixed32(field_bytes)
            if field_number == 1:
                pli["latitude_i"] = value
            elif field_number == 2:
                pli["longitude_i"] = value
            continue
        if wire_type == 0:
            value, offset = _read_protobuf_varint(payload, offset)
            if field_number == 3:
                pli["altitude"] = value
            elif field_number == 4:
                pli["speed"] = value
            elif field_number == 5:
                pli["course"] = value
            continue
        offset = _skip_protobuf_field(payload, offset, wire_type)
    return pli


def _parse_meshtastic_atak_payload(payload):
    """Decode the ATAK plugin fields needed for TAK chat and marker forwarding."""
    decoded = {
        "is_compressed": False,
        "contact": {},
        "group": {},
        "status": {},
        "pli": {},
        "chat": {},
        "detail": b"",
    }
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if field_number == 1 and wire_type == 0:
            value, offset = _read_protobuf_varint(payload, offset)
            decoded["is_compressed"] = bool(value)
            continue
        if wire_type != 2:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        field_bytes, offset = _read_protobuf_length_delimited(payload, offset)
        if field_number == 2:
            decoded["contact"] = _parse_meshtastic_atak_contact(field_bytes)
        elif field_number == 3:
            decoded["group"] = _parse_meshtastic_atak_group(field_bytes)
        elif field_number == 4:
            decoded["status"] = _parse_meshtastic_atak_status(field_bytes)
        elif field_number == 5:
            decoded["pli"] = _parse_meshtastic_atak_pli(field_bytes)
        elif field_number == 6:
            decoded["chat"] = _parse_meshtastic_atak_chat(field_bytes)
        elif field_number == 7:
            decoded["detail"] = bytes(field_bytes)
    return decoded


def _parse_meshtastic_takv2_cot_geopoint(payload):
    point = {}
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type != 0:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        value, offset = _read_protobuf_varint(payload, offset)
        if field_number == 1:
            point["lat_delta_i"] = _decode_protobuf_sint32(value)
        elif field_number == 2:
            point["lon_delta_i"] = _decode_protobuf_sint32(value)
    return point


def _parse_meshtastic_takv2_marker(payload):
    marker = {
        "kind": 0,
        "kind_label": "",
        "color": 0,
        "color_argb": None,
        "has_readiness": False,
        "readiness": False,
        "parent_uid": "",
        "parent_type": "",
        "parent_callsign": "",
        "iconset": "",
    }
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type == 5:
            field_bytes = payload[offset:offset + 4]
            offset += 4
            if len(field_bytes) != 4:
                raise ValueError("Unvollständiges protobuf fixed32-Feld im Marker-Payload")
            if field_number == 3:
                marker["color_argb"] = _decode_protobuf_fixed32(field_bytes)
            continue
        if wire_type == 0:
            value, offset = _read_protobuf_varint(payload, offset)
            if field_number == 1:
                marker["kind"] = value
                marker["kind_label"] = MESHTASTIC_MARKER_KIND_TO_LABEL.get(int(value), "")
            elif field_number == 2:
                marker["color"] = value
            elif field_number == 4:
                marker["has_readiness"] = True
                marker["readiness"] = bool(value)
            continue
        if wire_type != 2:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        field_bytes, offset = _read_protobuf_length_delimited(payload, offset)
        if field_number == 5:
            marker["parent_uid"] = _decode_protobuf_string(field_bytes)
        elif field_number == 6:
            marker["parent_type"] = _decode_protobuf_string(field_bytes)
        elif field_number == 7:
            marker["parent_callsign"] = _decode_protobuf_string(field_bytes)
        elif field_number == 8:
            marker["iconset"] = _decode_protobuf_string(field_bytes)
    return marker


def _parse_meshtastic_takv2_rab(payload):
    rab = {}
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type == 5:
            field_bytes = payload[offset:offset + 4]
            offset += 4
            if len(field_bytes) != 4:
                raise ValueError("Unvollständiges protobuf fixed32-Feld im RangeAndBearing-Payload")
            if field_number == 6:
                rab["stroke_argb"] = _decode_protobuf_fixed32(field_bytes)
            continue
        if wire_type == 0:
            value, offset = _read_protobuf_varint(payload, offset)
            if field_number == 3:
                rab["range_cm"] = value
            elif field_number == 4:
                rab["bearing_cdeg"] = value
            elif field_number == 5:
                rab["stroke_color"] = value
            elif field_number == 7:
                rab["stroke_weight_x10"] = value
            continue
        if wire_type != 2:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        field_bytes, offset = _read_protobuf_length_delimited(payload, offset)
        if field_number == 1:
            rab["anchor"] = _parse_meshtastic_takv2_cot_geopoint(field_bytes)
        elif field_number == 2:
            rab["anchor_uid"] = _decode_protobuf_string(field_bytes)
    return rab


def _parse_meshtastic_takv2_payload(payload):
    decoded = {
        "cot_type_id": 0,
        "how": 0,
        "callsign": "",
        "team": 0,
        "role": 0,
        "latitude_i": None,
        "longitude_i": None,
        "altitude": 0,
        "speed": 0,
        "course": 0,
        "battery": 0,
        "uid": "",
        "device_callsign": "",
        "stale_seconds": 0,
        "cot_type_str": "",
        "remarks": "",
        "payload_variant": None,
        "pli": False,
        "chat": {},
        "raw_detail": b"",
        "marker": {},
        "rab": {},
    }
    offset = 0
    payload = bytes(payload or b"")
    while offset < len(payload):
        key, offset = _read_protobuf_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type == 5:
            field_bytes = payload[offset:offset + 4]
            offset += 4
            if len(field_bytes) != 4:
                raise ValueError("Unvollständiges protobuf sfixed32-Feld im TAKPacketV2-Payload")
            if field_number == 6:
                decoded["latitude_i"] = _decode_protobuf_sfixed32(field_bytes)
            elif field_number == 7:
                decoded["longitude_i"] = _decode_protobuf_sfixed32(field_bytes)
            continue
        if wire_type == 0:
            value, offset = _read_protobuf_varint(payload, offset)
            if field_number == 1:
                decoded["cot_type_id"] = value
            elif field_number == 2:
                decoded["how"] = value
            elif field_number == 4:
                decoded["team"] = value
            elif field_number == 5:
                decoded["role"] = value
            elif field_number == 8:
                decoded["altitude"] = _decode_protobuf_sint32(value)
            elif field_number == 9:
                decoded["speed"] = value
            elif field_number == 10:
                decoded["course"] = value
            elif field_number == 11:
                decoded["battery"] = value
            elif field_number == 16:
                decoded["stale_seconds"] = value
            elif field_number == 30:
                decoded["payload_variant"] = "pli"
                decoded["pli"] = bool(value)
            continue
        if wire_type != 2:
            offset = _skip_protobuf_field(payload, offset, wire_type)
            continue
        field_bytes, offset = _read_protobuf_length_delimited(payload, offset)
        if field_number == 3:
            decoded["callsign"] = _decode_protobuf_string(field_bytes)
        elif field_number == 14:
            decoded["uid"] = _decode_protobuf_string(field_bytes)
        elif field_number == 15:
            decoded["device_callsign"] = _decode_protobuf_string(field_bytes)
        elif field_number == 23:
            decoded["cot_type_str"] = _decode_protobuf_string(field_bytes)
        elif field_number == 24:
            decoded["remarks"] = _decode_protobuf_string(field_bytes)
        elif field_number == 31:
            decoded["payload_variant"] = "chat"
            decoded["chat"] = _parse_meshtastic_atak_chat(field_bytes)
        elif field_number == 33:
            decoded["payload_variant"] = "raw_detail"
            decoded["raw_detail"] = bytes(field_bytes)
        elif field_number == 35:
            decoded["payload_variant"] = "marker"
            decoded["marker"] = _parse_meshtastic_takv2_marker(field_bytes)
        elif field_number == 36:
            decoded["payload_variant"] = "rab"
            decoded["rab"] = _parse_meshtastic_takv2_rab(field_bytes)
    return decoded


def _decode_meshtastic_takv2_wire_payload(payload):
    payload = bytes(payload or b"")
    if len(payload) < 2:
        raise ValueError("ATAK_PLUGIN_V2-Payload ist zu kurz")
    flags = payload[0]
    protobuf_bytes = payload[1:]
    if flags == 0xFF:
        return _parse_meshtastic_takv2_payload(protobuf_bytes)
    dict_id = flags & 0x3F
    raise ValueError(
        f"Komprimierte ATAK_PLUGIN_V2-Payload ohne eingebettetes Wörterbuch nicht unterstützt (dict_id={dict_id})"
    )


def _normalize_meshtastic_enum_key(value):
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _resolve_meshtastic_team_cot_event_type(team_value, default=MESHTASTIC_PLI_COT_EVENT_TYPE):
    normalized_team = _normalize_meshtastic_enum_key(team_value)
    return MESHTASTIC_TEAM_NAME_TO_COT_EVENT_TYPE.get(normalized_team, default)


def _extract_meshtastic_iconset_cot_type(iconset):
    icon_parts = [str(part).strip() for part in str(iconset or "").split("/") if str(part).strip()]
    if not icon_parts:
        return ""

    normalized_parts = [part.upper() for part in icon_parts]
    if "COT_MAPPING_2525B" in normalized_parts:
        mapping_index = normalized_parts.index("COT_MAPPING_2525B")
        for candidate in reversed(icon_parts[mapping_index + 1:]):
            if "-" in candidate and re.fullmatch(r"[A-Za-z0-9-]+", candidate):
                return candidate
    if "COT_MAPPING_SPOTMAP" in normalized_parts:
        mapping_index = normalized_parts.index("COT_MAPPING_SPOTMAP")
        for candidate in icon_parts[mapping_index + 1:]:
            if "-" in candidate and re.fullmatch(r"[A-Za-z0-9-]+", candidate):
                return candidate

    return ""


def _resolve_meshtastic_marker_cot_type(marker, fallback="a-u-G"):
    marker = marker or {}
    try:
        kind_value = int(marker.get("kind") or 0)
    except (TypeError, ValueError):
        kind_value = 0
    mapped_kind_type = MESHTASTIC_V2_MARKER_KIND_TO_COT_TYPE.get(kind_value)
    if mapped_kind_type:
        return mapped_kind_type

    iconset_cot_type = _extract_meshtastic_iconset_cot_type(marker.get("iconset"))
    if iconset_cot_type:
        return iconset_cot_type

    return str(fallback or "a-u-G").strip() or "a-u-G"


def _is_persistable_cot_type(event_type):
    """Return True for marker/drawing CoT types that should carry ``<archive/>``.

    TAK clients keep these spot-map, drawing, and affiliation-style marker
    events on the map when an ``archive`` detail is present. The prefixes match
    LPU5's marker categories: ``b-m`` for spot-map markers, ``u-d`` for
    drawings, and ``a-f/a-h/a-n/a-u/a-p`` for affiliation-based markers.
    """
    normalized = str(event_type or "").strip().lower()
    return normalized.startswith((
        "b-m",
        "u-d",
        "a-f",
        "a-h",
        "a-n",
        "a-u",
        "a-p",
    ))


def _is_live_pli_cot_event(event_type, how="", detail=None):
    normalized_type = str(event_type or "").strip().lower()
    if normalized_type != MESHTASTIC_PLI_COT_EVENT_TYPE_NORMALIZED:
        return False

    normalized_how = str(how or "").strip().lower()
    if normalized_how.startswith("h-"):
        return False

    if detail is not None and _find_child_by_local_name(detail, "archive") is not None:
        return False

    return True


def _classify_cot_event_type(event_type):
    normalized = str(event_type or "").strip().lower()
    if normalized == "b-t-f":
        return "chat"
    if normalized == MESHTASTIC_PLI_COT_EVENT_TYPE_NORMALIZED:
        return "pli"
    if _is_persistable_cot_type(normalized):
        return "marker"
    return "generic"


def _is_meshtastic_pli_event_type(event_type):
    return str(event_type or "").strip().lower() == MESHTASTIC_PLI_COT_EVENT_TYPE_NORMALIZED


def _get_cot_subject_label(metadata):
    if not isinstance(metadata, dict):
        return "TAK-CoT"
    if metadata.get("is_marker"):
        return "Marker-CoT"
    if metadata.get("is_pli"):
        return "PLI-CoT"
    return "Generic-CoT"


def _clamp_battery_percentage(value):
    battery_value = to_float_or_none(value)
    if battery_value is None or math.isnan(battery_value):
        return 0
    return max(0, min(100, int(round(battery_value))))


def _service_monitor_cot_type_to_detected_type(cot_type):
    normalized = str(cot_type or "").strip().lower()
    for prefix, detected_type in SERVICE_MONITOR_COT_TYPE_MAP:
        if normalized.startswith(prefix):
            return detected_type
    return "unknown"


def _build_service_monitor_cot_event_record(
    packet_xml,
    *,
    direction,
    source,
    gateway_uid="",
    local_node_ids=None,
    is_echo_back=False,
):
    raw_xml = _normalize_tak_xml_payload(packet_xml)
    if not raw_xml:
        return None
    try:
        root = fromstring(raw_xml)
    except (ParseError, TypeError, ValueError):
        return None
    if _xml_local_name(root.tag) != "event":
        return None

    point = _find_child_by_local_name(root, "point")
    if point is None:
        return None

    detail = _find_child_by_local_name(root, "detail")
    contact = _find_descendant_by_local_name(detail, "contact") if detail is not None else None
    uid_detail = _find_descendant_by_local_name(detail, "uid") if detail is not None else None
    group = _find_descendant_by_local_name(detail, "__group") if detail is not None else None
    remarks_el = _find_descendant_by_local_name(detail, "remarks") if detail is not None else None
    color_el = _find_descendant_by_local_name(detail, "color") if detail is not None else None
    mesh_el = _find_descendant_by_local_name(detail, "meshtastic") if detail is not None else None
    track = _find_descendant_by_local_name(detail, "track") if detail is not None else None

    cot_type = str(root.get("type") or "").strip()
    how = str(root.get("how") or "").strip() or "m-g"
    uid = str(root.get("uid") or "").strip()
    callsign = str(contact.get("callsign") or "").strip() if contact is not None else ""
    endpoint = str(contact.get("endpoint") or "").strip() if contact is not None else ""
    droid_uid = str(uid_detail.get("Droid") or "").strip() if uid_detail is not None else ""
    team = str(group.get("name") or "").strip() if group is not None else ""
    role = str(group.get("role") or "").strip() if group is not None else ""
    remarks = (remarks_el.text or "").strip() if remarks_el is not None and remarks_el.text else ""
    color_argb = str(color_el.get("argb") or "").strip() if color_el is not None else ""
    mesh_long_name = str(mesh_el.get("longName") or "").strip() if mesh_el is not None else ""
    mesh_short_name = str(mesh_el.get("shortName") or "").strip() if mesh_el is not None else ""
    has_meshtastic = mesh_el is not None or (
        detail is not None and _find_descendant_by_local_name(detail, "__meshtastic") is not None
    )
    has_archive = detail is not None and _find_descendant_by_local_name(detail, "archive") is not None

    base_lpu5_type = _service_monitor_cot_type_to_detected_type(cot_type)
    detected_type = base_lpu5_type
    detection_reason = f"CoT-Typ '{cot_type or '-'}' per Präfix klassifiziert."
    local_node_ids = set(local_node_ids or ())

    if has_meshtastic or base_lpu5_type == "meshtastic_node":
        if uid and gateway_uid and uid == str(gateway_uid).strip():
            detected_type = "gateway"
            detection_reason = "Meshtastic-Detail am lokalen Gateway-CoT erkannt."
        elif uid and uid in local_node_ids:
            detected_type = "node"
            detection_reason = "Meshtastic-Detail am lokalen Knoten-CoT erkannt."
        else:
            detected_type = "meshtastic_node"
            detection_reason = "Meshtastic-Detail im CoT vorhanden."
    elif _is_live_pli_cot_event(cot_type, how, detail):
        if uid and gateway_uid and uid == str(gateway_uid).strip():
            detected_type = "gateway"
            detection_reason = "Live-PLI des lokalen Gateways erkannt."
        elif uid and uid in local_node_ids:
            detected_type = "node"
            detection_reason = "Live-PLI eines lokalen Meshtastic-Knotens erkannt."
        else:
            detected_type = "gps_position"
            detection_reason = "Live-PLI (a-f-G-U-C ohne archive/h-*) erkannt."
    elif base_lpu5_type == "friendly" and how.lower().startswith("h-"):
        detected_type = "tak_maker"
        detection_reason = f"Friendly-CoT mit how='{how}' als manuell gesetzter ATAK/WinTAK-Marker erkannt."
    elif _is_persistable_cot_type(cot_type):
        cbt_type = SERVICE_MONITOR_ATAK_TO_CBT_TYPE.get(base_lpu5_type)
        if cbt_type:
            detected_type = cbt_type
            detection_reason = f"Persistenter ATAK-Marker ({base_lpu5_type}) als {cbt_type} eingeordnet."

    return {
        "parsed": {
            "uid": uid,
            "cot_type": cot_type,
            "how": how,
            "time": str(root.get("time") or "").strip(),
            "start": str(root.get("start") or "").strip(),
            "stale": str(root.get("stale") or "").strip(),
            "lat": _coerce_cot_point_float(point.get("lat")),
            "lon": _coerce_cot_point_float(point.get("lon")),
            "hae": _coerce_cot_point_float(point.get("hae")),
            "ce": _coerce_cot_point_float(point.get("ce")),
            "le": _coerce_cot_point_float(point.get("le")),
            "callsign": callsign,
            "uid_droid": droid_uid,
            "endpoint": endpoint,
            "team": team,
            "role": role,
            "remarks": remarks,
            "color_argb": color_argb or None,
            "has_meshtastic": has_meshtastic,
            "mesh_longName": mesh_long_name or None,
            "mesh_shortName": mesh_short_name or None,
            "has_archive": has_archive,
            "speed": str(track.get("speed") or "").strip() if track is not None else "",
            "course": str(track.get("course") or "").strip() if track is not None else "",
            "base_lpu5_type": base_lpu5_type,
            "detected_type": detected_type,
            "detection_reason": detection_reason,
            "is_echo_back": bool(is_echo_back),
        },
        "direction": direction,
        "source": str(source or "").strip() or "?",
        "raw_xml": _ensure_bytes(raw_xml).decode("utf-8", errors="replace"),
    }


def _is_meshtastic_payload_too_big_error(exc):
    return "payload too big" in str(exc or "").strip().lower()


def load_config():
    """
    Lädt config.yaml aus dem selben Verzeichnis wie dieses Skript (falls vorhanden).
    Gibt ein Dict zurück (oder {} bei Fehlern / fehlender PyYAML).
    """
    cfg = {}
    base = os.path.dirname(os.path.abspath(__file__))
    cfg_path = os.path.join(base, CFG_FILENAME)
    if os.path.exists(cfg_path):
        if yaml is None:
            print("Hinweis: PyYAML nicht installiert, config.yaml wird nicht gelesen.")
            return {}
        try:
            with open(cfg_path, "r", encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}
        except Exception as e:
            print(f"Fehler beim Lesen von {CFG_FILENAME}: {e}")
    return cfg


def save_config(cfg):
    """Saves config.yaml in the script directory (if PyYAML is available)."""
    if yaml is None:
        return
    base = os.path.dirname(os.path.abspath(__file__))
    cfg_path = os.path.join(base, CFG_FILENAME)
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh, sort_keys=False, allow_unicode=True)


def detect_serial_port_devices():
    """Return a list of detected serial port device names."""
    return [entry["device"] for entry in detect_serial_port_details()]


def detect_serial_port_details():
    """Return detected serial ports with human-friendly labels."""
    if serial is None:
        return []
    try:
        details = []
        for port in serial.tools.list_ports.comports():
            device = str(getattr(port, "device", "") or "").strip()
            if not device:
                continue
            info_parts = []
            for value in (
                getattr(port, "description", None),
                getattr(port, "manufacturer", None),
                getattr(port, "product", None),
            ):
                text = str(value or "").strip()
                if not text or text.lower() == "n/a" or text in info_parts:
                    continue
                info_parts.append(text)
            details.append(
                {
                    "device": device,
                    "label": f"{device} - {' | '.join(info_parts)}" if info_parts else device,
                }
            )
        return details
    except Exception:
        return []


def _parse_ports_text(ports_text):
    if not ports_text:
        return []
    raw_ports = [p for p in (part.strip() for part in re.split(r"[,\s;]+", ports_text)) if p]
    seen = set()
    unique_ports = []
    for p in raw_ports:
        upper_p = p.upper()
        if upper_p in seen:
            continue
        seen.add(upper_p)
        unique_ports.append(p)
    return unique_ports


def _config_ports_to_list(value):
    if isinstance(value, list):
        return _parse_ports_text(", ".join(str(item) for item in value))
    if value is None:
        return []
    return _parse_ports_text(str(value))


def _format_ports_for_entry(value):
    return ", ".join(_config_ports_to_list(value))


def _store_ports_in_config(cfg, key, ports):
    if ports:
        cfg[key] = ports[0] if len(ports) == 1 else ports
    else:
        cfg.pop(key, None)


def _parse_int_field_value(raw_value, field_name, min_value=MIN_PORT_NUMBER, max_value=MAX_PORT_NUMBER):
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a whole number.")
    if not (min_value <= value <= max_value):
        raise ValueError(f"{field_name} must be between {min_value} and {max_value}.")
    return value


def _config_bool_value(source, key, default=False):
    if isinstance(source, dict) and key in source:
        return as_bool(source.get(key))
    return as_bool(default)


def _apply_settings_payload_to_cfg(cfg, payload, ports):
    cfg["log_level"] = str(payload.get("log_level", cfg.get("log_level", "INFO"))).strip().upper() or "INFO"
    if cfg["log_level"] not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        cfg["log_level"] = "INFO"
    cfg["meshtastic_port"] = ports[0] if len(ports) == 1 else ports
    cfg["tak_server_host"] = str(payload.get("tak_server_host", cfg.get("tak_server_host", ""))).strip()
    cfg["tak_server_protocol"] = str(
        payload.get("tak_server_protocol", cfg.get("tak_server_protocol", "TCP"))
    ).strip().upper() or "TCP"
    if cfg["tak_server_protocol"] not in ("TCP", "UDP"):
        cfg["tak_server_protocol"] = "TCP"
    cfg["local_tak_ip"] = str(
        payload.get("local_tak_ip", cfg.get("local_tak_ip", WINTAK_REQUIRED_HOST))
    ).strip() or WINTAK_REQUIRED_HOST

    cfg["tak_server_port"] = _parse_int_field_value(
        payload.get("tak_server_port", cfg.get("tak_server_port", 8088)), "Remote Port"
    )
    cfg["local_tak_port"] = _parse_int_field_value(
        payload.get("local_tak_port", cfg.get("local_tak_port", 4242)), "Local TAK Port"
    )
    cfg["local_tak_chat_listen_port"] = _parse_int_field_value(
        payload.get("local_tak_chat_listen_port", cfg.get("local_tak_chat_listen_port", DEFAULT_CHAT_LISTEN_PORT)),
        "Local TAK Chat Listen Port",
    )
    cfg["local_tak_tcp_listen_port"] = _parse_int_field_value(
        payload.get("local_tak_tcp_listen_port", cfg.get("local_tak_tcp_listen_port", TCP_LISTENER_DEFAULT_PORT)),
        "Local TAK TCP Listen Port",
    )
    cfg["sync_interval_seconds"] = _parse_int_field_value(
        payload.get("sync_interval_seconds", cfg.get("sync_interval_seconds", 300)),
        "Sync Interval",
        min_value=1,
        max_value=86400,
    )

    cfg["log_raw_meshtastic_payloads"] = _config_bool_value(
        payload, "log_raw_meshtastic_payloads", cfg.get("log_raw_meshtastic_payloads", False)
    )
    cfg["log_raw_meshtastic_payloads_full"] = bool(
        cfg["log_raw_meshtastic_payloads"]
        and _config_bool_value(
            payload, "log_raw_meshtastic_payloads_full", cfg.get("log_raw_meshtastic_payloads_full", False)
        )
    )
    cfg["relay_text_messages"] = _config_bool_value(
        payload, "relay_text_messages", cfg.get("relay_text_messages", True)
    )
    relay_from_ports = _parse_ports_text(
        payload.get("relay_text_from_ports", _format_ports_for_entry(cfg.get("relay_text_from_ports")))
    )
    relay_to_mode = str(payload.get("relay_text_to_mode", "")).strip().lower()
    relay_to_text = payload.get("relay_text_to_ports", _format_ports_for_entry(cfg.get("relay_text_to_ports")))
    relay_to_ports = [] if relay_to_mode == "all-other-selected-ports" else _parse_ports_text(relay_to_text)
    selected_ports_upper = {port.upper() for port in ports}
    unknown_relay_from = [port for port in relay_from_ports if port.upper() not in selected_ports_upper]
    unknown_relay_to = [port for port in relay_to_ports if port.upper() not in selected_ports_upper]
    if unknown_relay_from:
        raise ValueError(
            "Relay From COM Port(s) contains ports that are not selected above: " + ", ".join(unknown_relay_from)
        )
    if unknown_relay_to:
        raise ValueError(
            "Relay To COM Port(s) contains ports that are not selected above: " + ", ".join(unknown_relay_to)
        )
    _store_ports_in_config(cfg, "relay_text_from_ports", relay_from_ports)
    _store_ports_in_config(cfg, "relay_text_to_ports", relay_to_ports)

    cfg["send_nodes_without_gps"] = _config_bool_value(
        payload, "send_nodes_without_gps", cfg.get("send_nodes_without_gps", True)
    )
    cfg["set_gateway_position_on_start"] = _config_bool_value(
        payload, "set_gateway_position_on_start", cfg.get("set_gateway_position_on_start", False)
    )

    park_lat_text = str(payload.get("park_lat", "" if cfg.get("park_lat") is None else cfg.get("park_lat"))).strip()
    park_lon_text = str(payload.get("park_lon", "" if cfg.get("park_lon") is None else cfg.get("park_lon"))).strip()
    if park_lat_text and park_lon_text:
        try:
            lat_val = float(park_lat_text)
        except ValueError:
            raise ValueError("park_lat must be a valid number.")
        try:
            lon_val = float(park_lon_text)
        except ValueError:
            raise ValueError("park_lon must be a valid number.")
        if lat_val == 0.0 and lon_val == 0.0:
            raise ValueError(
                "park_lat and park_lon cannot both be 0.0 (Null Island / invalid location). "
                "Enter real coordinates or leave both fields empty."
            )
        cfg["park_lat"] = lat_val
        cfg["park_lon"] = lon_val
    elif not park_lat_text and not park_lon_text:
        cfg.pop("park_lat", None)
        cfg.pop("park_lon", None)
    else:
        missing = "park_lon" if park_lat_text else "park_lat"
        raise ValueError(
            "Both coordinates (park_lat and park_lon) must be filled in, or both must stay empty. "
            f"Currently missing: {missing}."
        )


def _build_browser_ui_form_state(cfg):
    relay_to_ports = _config_ports_to_list(cfg.get("relay_text_to_ports"))
    return {
        "meshtastic_port": _format_ports_for_entry(cfg.get("meshtastic_port")),
        "tak_server_host": str(cfg.get("tak_server_host", "")),
        "tak_server_port": str(cfg.get("tak_server_port", 8088)),
        "tak_server_protocol": str(cfg.get("tak_server_protocol", "TCP")).upper(),
        "local_tak_ip": str(cfg.get("local_tak_ip", WINTAK_REQUIRED_HOST)),
        "local_tak_port": str(cfg.get("local_tak_port", 4242)),
        "local_tak_chat_listen_port": str(cfg.get("local_tak_chat_listen_port", DEFAULT_CHAT_LISTEN_PORT)),
        "local_tak_tcp_listen_port": str(cfg.get("local_tak_tcp_listen_port", TCP_LISTENER_DEFAULT_PORT)),
        "log_level": str(cfg.get("log_level", "INFO")).upper(),
        "sync_interval_seconds": str(cfg.get("sync_interval_seconds", 300)),
        "log_raw_meshtastic_payloads": as_bool(cfg.get("log_raw_meshtastic_payloads", False)),
        "log_raw_meshtastic_payloads_full": as_bool(cfg.get("log_raw_meshtastic_payloads_full", False)),
        "relay_text_messages": as_bool(cfg.get("relay_text_messages", True)),
        "relay_text_from_ports": _format_ports_for_entry(cfg.get("relay_text_from_ports")),
        "relay_text_to_ports": ", ".join(relay_to_ports),
        "relay_text_to_mode": "all-other-selected-ports" if not relay_to_ports else "custom",
        "send_nodes_without_gps": as_bool(cfg.get("send_nodes_without_gps", True)),
        "set_gateway_position_on_start": as_bool(cfg.get("set_gateway_position_on_start", False)),
        "park_lat": "" if cfg.get("park_lat") is None else str(cfg.get("park_lat")),
        "park_lon": "" if cfg.get("park_lon") is None else str(cfg.get("park_lon")),
    }


def _normalize_tak_multicast_endpoint(value):
    if isinstance(value, dict):
        group = str(value.get("group") or value.get("host") or value.get("ip") or "").strip()
        port = value.get("port")
    else:
        raw_value = str(value or "").strip()
        if not raw_value:
            return None
        group, separator, port = raw_value.rpartition(":")
        if not separator:
            raise ValueError(
                "tak_multicast_groups-Einträge müssen als 'Multicast-IP:Port' angegeben werden."
            )
        group = group.strip()
        port = port.strip()

    if not group:
        raise ValueError("tak_multicast_groups-Eintrag ohne Multicast-IP.")
    try:
        packed_group = socket.inet_aton(group)
    except OSError as exc:
        raise ValueError(f"Ungültige Multicast-IP '{group}' in tak_multicast_groups.") from exc
    first_octet = packed_group[0]
    if not (224 <= first_octet <= 239):
        raise ValueError(f"IP '{group}' in tak_multicast_groups ist keine IPv4-Multicast-Adresse.")
    try:
        port = int(str(port).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Ungültiger Port '{port}' in tak_multicast_groups.") from exc
    if not (MIN_PORT_NUMBER <= port <= MAX_PORT_NUMBER):
        raise ValueError(
            f"Port '{port}' in tak_multicast_groups muss zwischen {MIN_PORT_NUMBER} und {MAX_PORT_NUMBER} liegen."
        )
    return group, port


def _config_tak_multicast_groups_to_list(value):
    if value is None:
        raw_entries = list(DEFAULT_TAK_MULTICAST_GROUPS)
    elif isinstance(value, list):
        raw_entries = value
    elif isinstance(value, tuple):
        raw_entries = list(value)
    elif isinstance(value, str):
        raw_entries = [entry for entry in re.split(r"[,\n;]+", value) if entry.strip()]
    else:
        raw_entries = [value]

    normalized_entries = []
    seen = set()
    for raw_entry in raw_entries:
        normalized_entry = _normalize_tak_multicast_endpoint(raw_entry)
        if normalized_entry is None or normalized_entry in seen:
            continue
        seen.add(normalized_entry)
        normalized_entries.append(normalized_entry)
    return normalized_entries


class NodeDatabase:
    """SQLite-Datenbank für Meshtastic-Knoten mit GPS-Koordinaten.

    Speichert jeden bekannten Knoten mit seinem Rufzeichen, seinen GPS-Koordinaten,
    dem Zeitstempel des letzten Empfangs und der Positionsquelle.
    GPS-Koordinaten werden automatisch aktualisiert, sobald sich eine neue Position empfangen wird.
    Manuelle Positionen können über set_manual_position() gesetzt werden.
    """

    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS nodes (
            uid              TEXT PRIMARY KEY,
            callsign         TEXT,
            lat              REAL NOT NULL,
            lon              REAL NOT NULL,
            alt              REAL    DEFAULT 0,
            last_seen        TEXT,
            position_source  TEXT    DEFAULT 'GPS'
        )
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._lock, sqlite3.connect(self.db_path) as conn:
            conn.execute(self._CREATE_TABLE)
            conn.commit()

    def upsert_node(self, uid, callsign, lat, lon, alt, source="GPS"):
        """Füge einen Knoten ein oder aktualisiere seine Position."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._lock, sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO nodes (uid, callsign, lat, lon, alt, last_seen, position_source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET
                    callsign=excluded.callsign,
                    lat=excluded.lat,
                    lon=excluded.lon,
                    alt=excluded.alt,
                    last_seen=excluded.last_seen,
                    position_source=excluded.position_source
                """,
                (uid, callsign, lat, lon, alt if alt is not None else 0, now, source),
            )
            conn.commit()

    def get_node(self, uid):
        """Gibt (lat, lon, alt, callsign, last_seen, position_source) zurück, oder None."""
        with self._lock, sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT lat, lon, alt, callsign, last_seen, position_source "
                "FROM nodes WHERE uid=?",
                (uid,),
            ).fetchone()

    def get_all_nodes(self):
        """Gibt alle Knoten sortiert nach Rufzeichen zurück."""
        with self._lock, sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT uid, callsign, lat, lon, alt, last_seen, position_source "
                "FROM nodes ORDER BY callsign",
            ).fetchall()

    def set_manual_position(self, uid, lat, lon, alt=0.0, callsign=None):
        """Setzt die Position eines Knotens manuell (behält vorhandenes Rufzeichen bei)."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._lock, sqlite3.connect(self.db_path) as conn:
            if callsign is None:
                row = conn.execute(
                    "SELECT callsign FROM nodes WHERE uid=?", (uid,)
                ).fetchone()
                callsign = row[0] if row else uid
            conn.execute(
                """
                INSERT INTO nodes (uid, callsign, lat, lon, alt, last_seen, position_source)
                VALUES (?, ?, ?, ?, ?, ?, 'MANUAL')
                ON CONFLICT(uid) DO UPDATE SET
                    lat=excluded.lat,
                    lon=excluded.lon,
                    alt=excluded.alt,
                    last_seen=excluded.last_seen,
                    position_source='MANUAL'
                """,
                (uid, callsign, lat, lon, alt if alt is not None else 0, now),
            )
            conn.commit()


class _GUILogHandler(logging.Handler):
    """Logging-Handler, der Einträge thread-sicher in eine GUI-Callback-Funktion weiterleitet."""

    def __init__(self, callback):
        super().__init__()
        self._callback = callback

    def emit(self, record):
        try:
            msg = self.format(record)
            self._callback(msg, record.levelname)
        except Exception:
            pass


class _ServiceWebUIController:
    """Browser-first controller that mirrors the legacy desktop UI actions."""

    def __init__(self, cfg, service_status=None, event_store=None):
        self.cfg = dict(cfg or {})
        self.service_status = service_status or {}
        self._event_store = event_store
        self._lock = threading.RLock()
        self._gateway = None
        self._gateway_thread = None
        self._log_handler = None
        self._logs = []
        self._wintak_monitor = []
        self._status_text = "⬛ Stopped"
        self._mesh_status = "Gateway not started."
        self._cot_status = "Gateway not started."
        self._active_ports = []
        self._detected_ports = detect_serial_port_details()

    def _append_log(self, msg, level="INFO"):
        entry = {
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "level": level if level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "CMD") else "INFO",
            "message": str(msg),
        }
        with self._lock:
            self._logs.append(entry)
            if len(self._logs) > 400:
                self._logs = self._logs[-400:]

    def _append_wintak_monitor(self, msg, level="INFO"):
        entry = {
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "level": level if level in ("INFO", "WARNING", "ERROR") else "INFO",
            "message": str(msg),
        }
        with self._lock:
            self._wintak_monitor.append(entry)
            if len(self._wintak_monitor) > MAX_WINTAK_MONITOR_LINES:
                self._wintak_monitor = self._wintak_monitor[-MAX_WINTAK_MONITOR_LINES:]

    def get_state(self):
        with self._lock:
            return {
                "ok": True,
                "service": dict(self.service_status),
                "config": _build_browser_ui_form_state(self.cfg),
                "running": self._gateway is not None and not self._gateway.shutdown_flag.is_set(),
                "status_text": self._status_text,
                "mesh_status": self._mesh_status,
                "cot_status": self._cot_status,
                "active_ports": list(self._active_ports),
                "detected_ports": [dict(item) for item in self._detected_ports],
                "logs": list(self._logs),
                "wintak_monitor": list(self._wintak_monitor),
                "dependencies": self._missing_dependencies(),
            }

    def refresh_detected_ports(self):
        with self._lock:
            self._detected_ports = detect_serial_port_details()
            detected_ports = [dict(item) for item in self._detected_ports]
        return {"ok": True, "detected_ports": detected_ports}

    def _missing_dependencies(self):
        missing = []
        if meshtastic is None:
            missing.append("meshtastic")
        if pub is None:
            missing.append("pypubsub")
        if serial is None:
            missing.append("pyserial")
        return missing

    def start_gateway(self, payload):
        ports = _parse_ports_text(payload.get("meshtastic_port", ""))
        if not ports:
            raise ValueError("No port specified.")
        with self._lock:
            if (self._gateway is not None and not self._gateway.shutdown_flag.is_set()) or (
                self._gateway_thread is not None and self._gateway_thread.is_alive()
            ):
                raise ValueError("Gateway is already running.")
            updated_cfg = dict(self.cfg)
        _apply_settings_payload_to_cfg(updated_cfg, payload, ports)
        updated_cfg["enable_service_web_ui"] = False
        save_config(updated_cfg)

        log_level = getattr(logging, updated_cfg.get("log_level", "INFO"), logging.INFO)
        log_handler = _GUILogHandler(self._append_log)
        log_handler.setLevel(log_level)
        log_handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S")
        )
        logger = logging.getLogger("TAK_Meshtastic_Gateway")
        logger.setLevel(log_level)
        logger.addHandler(log_handler)

        with self._lock:
            self.cfg = updated_cfg
            self._log_handler = log_handler
            self._status_text = "🟡 Starting …"
            self._mesh_status = "Gateway starting …"
            self._cot_status = "Gateway starting …"
            self._active_ports = list(ports)
            self._append_log("Browser UI start requested.", "INFO")
            self._gateway_thread = threading.Thread(
                target=self._run_gateway_thread,
                args=(list(ports),),
                daemon=True,
                name="gateway-browser-controller",
            )
            self._gateway_thread.start()
        return {"ok": True}

    def _run_gateway_thread(self, ports):
        try:
            gw = TAKMeshtasticGateway(ports, dict(self.cfg))
            gw.wintak_tcp_chat_callback = self._on_wintak_tcp_chat
            gw.service_ui_event_store = self._event_store
            with self._lock:
                self._gateway = gw
                self._status_text = f"🟢 Running  –  {', '.join(ports)}"
                self._mesh_status = "Ready for manual mesh test sending."
                self._cot_status = "Ready for manual CoT injection."
            gw.run()
        except Exception:
            self._append_log("Gateway thread error:\n" + traceback.format_exc(), "ERROR")
        finally:
            self._on_gateway_stopped()

    def stop_gateway(self):
        with self._lock:
            gw = self._gateway
            if gw is None and not (self._gateway_thread is not None and self._gateway_thread.is_alive()):
                return {"ok": True}
            if gw is not None:
                gw.shutdown_flag.set()
            self._status_text = "🟡 Stopping …"
            self._mesh_status = "Gateway stopping …"
            self._cot_status = "Gateway stopping …"
        return {"ok": True}

    def _on_gateway_stopped(self):
        with self._lock:
            if self._log_handler is not None:
                logging.getLogger("TAK_Meshtastic_Gateway").removeHandler(self._log_handler)
                self._log_handler = None
            self._gateway = None
            self._gateway_thread = None
            self._status_text = "⬛ Stopped"
            self._mesh_status = "Gateway not started."
            self._cot_status = "Gateway not started."

    def manual_sync(self):
        with self._lock:
            gw = self._gateway
        if gw is None:
            raise ValueError("Gateway is not running.")
        threading.Thread(target=gw.full_sync, daemon=True, name="gateway-manual-sync").start()
        self._append_log("Manual full sync started.", "INFO")
        return {"ok": True}

    def send_mesh_text(self, message):
        text = str(message or "").strip()
        if not text:
            raise ValueError("Please enter a test message first.")
        with self._lock:
            gw = self._gateway
        if gw is None:
            raise ValueError("Gateway is not running.")
        with self._lock:
            self._mesh_status = "🟡 Sending test message …"
        threading.Thread(
            target=self._send_mesh_text_worker,
            args=(gw, text),
            daemon=True,
            name="gateway-browser-send-text",
        ).start()
        return {"ok": True}

    def _send_mesh_text_worker(self, gw, message):
        try:
            total_sent = gw._send_text_to_meshtastic(message)
        except Exception as exc:
            self._append_log(f"Manual mesh send failed: {exc}", "ERROR")
            self._append_log("Manual mesh send error details:\n" + traceback.format_exc(), "DEBUG")
            with self._lock:
                self._mesh_status = f"❌ Send failed: {exc}"
            return
        self._append_log(f"Test message sent to the mesh successfully (interfaces: {total_sent}).", "INFO")
        with self._lock:
            self._mesh_status = f"✅ Sent (interfaces: {total_sent})."

    def send_cot(self, packet_xml):
        payload = str(packet_xml or "").strip()
        if not payload:
            raise ValueError("Please paste a CoT <event> XML first.")
        with self._lock:
            gw = self._gateway
        if gw is None:
            raise ValueError("Gateway is not running.")
        with self._lock:
            self._cot_status = "🟡 Sending CoT to mesh …"
        threading.Thread(
            target=self._send_cot_worker,
            args=(gw, payload),
            daemon=True,
            name="gateway-browser-send-cot",
        ).start()
        return {"ok": True}

    def _send_cot_worker(self, gw, packet_xml):
        try:
            send_result = gw.send_cot_to_meshtastic(packet_xml)
        except Exception as exc:
            self._append_log(f"Manual CoT send failed: {exc}", "ERROR")
            self._append_log("Manual CoT send error details:\n" + traceback.format_exc(), "DEBUG")
            with self._lock:
                self._cot_status = f"❌ Send failed: {exc}"
            return
        transport = send_result.get("transport", "UNKNOWN")
        count = send_result.get("count", 0)
        self._append_log(
            f"Manual CoT sent to the mesh successfully (transport: {transport}, packets: {count}).",
            "INFO",
        )
        with self._lock:
            self._cot_status = f"✅ Sent via {transport} ({count} packet(s))."

    def _on_wintak_tcp_chat(self, kind, sender, message, addr=None):
        if kind == "connect":
            ip = addr[0] if addr else "?"
            listen_ip, listen_port = _get_tak_tcp_listener_endpoint_from_cfg(self.cfg)
            monitor_line = f"LINK UP  {ip} -> {listen_ip}:{listen_port}"
            self._append_log(f"WinTAK connected from {ip} to TCP listener {listen_ip}:{listen_port}.", "INFO")
            self._append_wintak_monitor(monitor_line, "INFO")
        elif kind == "disconnect":
            self._append_log("WinTAK TCP connection closed.", "INFO")
            self._append_wintak_monitor("LINK DOWN  WinTAK TCP connection closed.", "WARNING")
        elif kind == "chat":
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            self._append_log(f"[WinTAK {ts}] {sender}: {message}", "INFO")
            self._append_wintak_monitor(f"[{ts}] {sender}: {message}", "INFO")

    def run_command(self, command):
        cmd = str(command or "").strip()
        if not cmd:
            raise ValueError("Command must not be empty.")
        self._append_log(f"> {cmd}", "CMD")
        parts_raw = cmd.split()
        parts = [p.lower() for p in parts_raw]
        action = parts[0]
        if action == "sync":
            return self.manual_sync()
        if action == "log" and len(parts) >= 2:
            new_level = parts[1].upper()
            if new_level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
                raise ValueError("Unknown level. Valid values: debug|info|warning|error")
            with self._lock:
                self.cfg["log_level"] = new_level
                if self._log_handler is not None:
                    self._log_handler.setLevel(getattr(logging, new_level, logging.INFO))
                logging.getLogger("TAK_Meshtastic_Gateway").setLevel(getattr(logging, new_level, logging.INFO))
            save_config(self.cfg)
            self._append_log(f"Log level changed to {new_level}.", "INFO")
            return {"ok": True}
        if action == "clear":
            with self._lock:
                self._logs = []
            return {"ok": True}
        if action == "nodes":
            with self._lock:
                gw = self._gateway
            if gw is None:
                raise ValueError("Gateway is not running.")
            rows = gw.node_db.get_all_nodes()
            if not rows:
                self._append_log("No nodes in the database.", "INFO")
            else:
                self._append_log(
                    f"{'UID':<22} {'Callsign':<20} {'Lat':>10} {'Lon':>11} {'Alt':>6}  {'Source':<10} Last Seen",
                    "INFO",
                )
                for uid, callsign, lat, lon, alt, last_seen, source in rows:
                    self._append_log(
                        f"{uid:<22} {callsign:<20} {lat:>10.5f} {lon:>11.5f} {alt:>6.0f}  {source:<10} {last_seen or '-'}",
                        "INFO",
                    )
            return {"ok": True}
        if action == "setpos" and len(parts_raw) >= 4:
            try:
                lat = float(parts_raw[2])
                lon = float(parts_raw[3])
                alt = float(parts_raw[4]) if len(parts_raw) >= 5 else 0.0
            except ValueError:
                raise ValueError("setpos: invalid coordinates. Syntax: setpos <uid> <lat> <lon> [alt]")
            normalized = normalize_coordinates(lat, lon)
            if normalized is None:
                raise ValueError(f"setpos: invalid coordinates ({lat}, {lon}).")
            with self._lock:
                gw = self._gateway
            if gw is None:
                raise ValueError("Gateway is not running.")
            uid = parts_raw[1]
            lat, lon = normalized
            gw.node_db.set_manual_position(uid, lat, lon, alt)
            gw.last_known_positions[uid] = (lat, lon, alt)
            self._append_log(f"Position for '{uid}' set to ({lat:.6f}, {lon:.6f}, {alt:.0f}m).", "INFO")
            return {"ok": True}
        if action == "help":
            for line in [
                "Available commands:",
                "  sync                         – Start a manual full sync for all nodes",
                "  nodes                        – Show all nodes from the database",
                "  setpos <uid> <lat> <lon> [alt] – Set a node position manually",
                "  log <level>                  – Set the log level (debug/info/warning/error)",
                "  clear                        – Clear the log output",
                "  help                         – Show this help",
            ]:
                self._append_log(line, "INFO")
            return {"ok": True}
        raise ValueError(f"Unknown command: '{cmd}'. Type 'help' for help.")


class GatewayApp:
    """Vollständige Tkinter-GUI für den WinTAK Meshtastic Gateway.

    Ersetzt das reine Terminal-Fenster. Zeigt Log-Ausgaben live im Fenster
    und erlaubt direkte Befehlseingaben während der Gateway läuft.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self._gateway = None
        self._gateway_thread = None
        self._gui_handler = None
        self._scroll_canvas = None
        self._scrollable_body = None
        self._scroll_window_id = None
        self._wintak_monitor_text = None
        self._cot_input_text = None
        self._detected_ports_wraplength = DETECTED_PORTS_SUMMARY_DEFAULT_WRAP

        try:
            self._root = tk.Tk()
        except (tk.TclError, RuntimeError):
            raise

        self._root.title("WinTAK Meshtastic Gateway")
        self._root.geometry("1100x760")
        self._root.minsize(960, 700)
        self._root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._root.configure(bg="#0d0f14")

        self._setup_styles()
        self._build_ui()

    # ─────────────────────────── Styles / Theming ─────────────────────────────

    def _setup_styles(self):
        style = ttk.Style(self._root)
        style.theme_use("clam")

        # ── Farben (WinTAK-inspired dark blue military palette) ──
        BG        = "#0d0f14"   # Window background – near black, slight blue
        PANEL     = "#131c2e"   # Header/panel background – dark navy
        CARD      = "#1a2540"   # Card/frame background – navy
        BORDER    = "#2d4a7a"   # Border color – blue-grey
        FG        = "#e2eaf5"   # Main text – cool white
        FG_SUB    = "#7b9bbf"   # Subtle helper text – steel blue
        ACCENT    = "#2074d4"   # Primary accent – WinTAK blue
        ACCENT_H  = "#4a99f5"   # Hover – lighter blue
        SUCCESS   = "#22c55e"   # Success – green
        WARNING   = "#f59e0b"   # Warning – amber
        DANGER    = "#ef4444"   # Danger – red
        TEXT_BG   = "#080a10"   # Text/log background – darkest

        # ── Allgemein ──
        style.configure(".",
                         background=BG,
                         foreground=FG,
                         font=("Segoe UI", 9),
                         relief="flat",
                         borderwidth=0)

        # ── TFrame ──
        style.configure("TFrame", background=BG)
        style.configure("Card.TFrame", background=CARD)

        # ── TLabel ──
        style.configure("TLabel", background=BG, foreground=FG)
        style.configure("Sub.TLabel", background=CARD, foreground=FG_SUB)
        style.configure("Card.TLabel", background=CARD, foreground=FG)
        style.configure("Hint.TLabel", background=CARD, foreground=WARNING)

        # ── TLabelFrame ──
        style.configure("TLabelframe",
                         background=CARD,
                         foreground=FG_SUB,
                         bordercolor=BORDER,
                         relief="solid",
                         borderwidth=1)
        style.configure("TLabelframe.Label",
                         background=CARD,
                         foreground=ACCENT,
                         font=("Segoe UI", 9, "bold"))

        # ── TEntry ──
        FIELD_BG  = "#111827"   # Input field background – dark blue-grey
        style.configure("TEntry",
                         fieldbackground=FIELD_BG,
                         foreground=FG,
                         insertcolor=FG,
                         selectbackground=ACCENT,
                         selectforeground=FG,
                         bordercolor=BORDER,
                         lightcolor=BORDER,
                         darkcolor=BORDER,
                         relief="solid",
                         borderwidth=1)
        style.map("TEntry",
                  bordercolor=[("focus", ACCENT)],
                  lightcolor=[("focus", ACCENT)],
                  darkcolor=[("focus", ACCENT)])

        # ── TCombobox ──
        style.configure("TCombobox",
                         fieldbackground=FIELD_BG,
                         foreground=FG,
                         selectbackground=ACCENT,
                         selectforeground=FG,
                         bordercolor=BORDER,
                         arrowcolor=FG_SUB)
        style.map("TCombobox",
                  bordercolor=[("focus", ACCENT)],
                  fieldbackground=[("readonly", FIELD_BG)])
        self._root.option_add("*TCombobox*Listbox.background", FIELD_BG)
        self._root.option_add("*TCombobox*Listbox.foreground", FG)
        self._root.option_add("*TCombobox*Listbox.selectBackground", ACCENT)

        # ── TButton (standard) ──
        style.configure("TButton",
                         background="#243352",
                         foreground=FG,
                         bordercolor=BORDER,
                         focuscolor=ACCENT,
                         padding=(10, 5),
                         relief="flat",
                         font=("Segoe UI", 9))
        style.map("TButton",
                  background=[("active", BORDER), ("disabled", "#1a2540")],
                  foreground=[("disabled", FG_SUB)])

        # ── Accent.TButton (primary / start) ──
        style.configure("Accent.TButton",
                         background=ACCENT,
                         foreground="#ffffff",
                         bordercolor=ACCENT,
                         padding=(12, 5),
                         relief="flat",
                         font=("Segoe UI", 9, "bold"))
        style.map("Accent.TButton",
                  background=[("active", ACCENT_H), ("disabled", BORDER)],
                  foreground=[("disabled", FG_SUB)])

        # ── Danger.TButton (stop) ──
        style.configure("Danger.TButton",
                         background="#7f1d1d",
                         foreground="#fca5a5",
                         bordercolor="#991b1b",
                         padding=(12, 5),
                         relief="flat",
                         font=("Segoe UI", 9, "bold"))
        style.map("Danger.TButton",
                  background=[("active", "#991b1b"), ("disabled", BORDER)],
                  foreground=[("disabled", FG_SUB)])

        # ── TCheckbutton ──
        style.configure("TCheckbutton",
                         background=CARD,
                         foreground=FG,
                         indicatorcolor=FIELD_BG,
                         indicatorbackground=FIELD_BG,
                         indicatorforeground="#ffffff",
                         indicatorrelief="flat")
        style.map("TCheckbutton",
                  indicatorcolor=[("selected", ACCENT)],
                  indicatorforeground=[("selected", "#ffffff"), ("!selected", "#ffffff")],
                  background=[("active", CARD)])

        # ── TSeparator ──
        style.configure("TSeparator", background=BORDER)

        # ── TScrollbar ──
        style.configure("TScrollbar",
                         background=CARD,
                         troughcolor=FIELD_BG,
                         arrowcolor=FG_SUB,
                         bordercolor=BORDER,
                         relief="flat")
        style.map("TScrollbar",
                  background=[("active", BORDER)])

        # ── Listbox (nicht-ttk, via option_add) ──
        self._root.option_add("*Listbox.background", FIELD_BG)
        self._root.option_add("*Listbox.foreground", FG)
        self._root.option_add("*Listbox.selectBackground", ACCENT)
        self._root.option_add("*Listbox.selectForeground", "#ffffff")
        self._root.option_add("*Listbox.relief", "flat")
        self._root.option_add("*Listbox.borderWidth", "1")
        self._root.option_add("*Listbox.highlightThickness", "0")

        # Speichere Farben für direkte Nutzung in _build_ui
        self._colors = {
            "bg": BG, "panel": PANEL, "card": CARD, "border": BORDER,
            "fg": FG, "fg_sub": FG_SUB, "accent": ACCENT,
            "success": SUCCESS, "warning": WARNING, "danger": DANGER,
            "text_bg": TEXT_BG, "field_bg": FIELD_BG,
        }

        # Font-Verfügbarkeit einmalig prüfen und cachen
        try:
            import tkinter.font as tkfont
            available_families = set(tkfont.families())
        except Exception:
            available_families = set()
        self._log_font = (
            "Cascadia Code" if "Cascadia Code" in available_families else "Consolas", 9
        )

    # ─────────────────────────── UI-Aufbau ────────────────────────────────────

    def _build_ui(self):
        root = self._root
        C = self._colors

        # WinTAK-style thin accent stripe at very top
        tk.Frame(root, bg=C["accent"], height=3).pack(fill="x")

        header = tk.Frame(root, bg=C["panel"], bd=0, highlightthickness=1,
                          highlightbackground=C["border"], highlightcolor=C["border"])
        header.pack(fill="x", padx=10, pady=(6, 0))

        header_left = tk.Frame(header, bg=C["panel"])
        header_left.pack(fill="both", expand=True, padx=12, pady=10)

        try:
            _logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo.png")
            self._logo_image = tk.PhotoImage(file=_logo_path)
            _logo_h = 54
            _orig_h = self._logo_image.height()
            if _orig_h > _logo_h:
                _subsample = max(1, round(_orig_h / _logo_h))
                self._logo_image = self._logo_image.subsample(_subsample, _subsample)
            tk.Label(header_left, image=self._logo_image, bg=C["panel"], anchor="w").pack(
                side="left", padx=(0, 12)
            )
        except Exception:
            self._logo_image = None

        title_block = tk.Frame(header_left, bg=C["panel"])
        title_block.pack(side="left", fill="both", expand=True)
        tk.Label(
            title_block,
            text="WINTAK  MESHTASTIC  GATEWAY",
            bg=C["panel"],
            fg=C["fg"],
            font=("Segoe UI", 16, "bold"),
            anchor="w",
        ).pack(fill="x")
        tk.Label(
            title_block,
            text="Live gateway operations with direct mesh text and WinTAK CoT send controls.",
            bg=C["panel"],
            fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="w",
        ).pack(fill="x", pady=(2, 6))
        tk.Label(
            title_block,
            text="▶  MESH  ⟷  TAK  CONTROL  SURFACE",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        ).pack(fill="x")
        endpoint_row = tk.Frame(title_block, bg=C["panel"])
        endpoint_row.pack(fill="x", pady=(8, 0), anchor="w")

        endpoint_items = [
            ("WinTAK UDP", f"{self.cfg.get('local_tak_ip', WINTAK_REQUIRED_HOST)}:{self.cfg.get('local_tak_port', 4242)}"),
            (
                "WinTAK TCP Rx",
                f"{self.cfg.get('local_tak_tcp_receiver_host', self.cfg.get('local_tak_ip', WINTAK_REQUIRED_HOST))}:"
                f"{self.cfg.get('local_tak_tcp_receiver_port', TCP_RECEIVER_DEFAULT_PORT)}",
            ),
            ("WinTAK TCP In", f"0.0.0.0:{self.cfg.get('local_tak_tcp_listen_port', TCP_LISTENER_DEFAULT_PORT)}"),
            ("Remote TAK", f"{self.cfg.get('tak_server_protocol', 'TCP')} {self.cfg.get('tak_server_host', '82.165.11.84')}:{self.cfg.get('tak_server_port', 8088)}"),
        ]
        for title, value in endpoint_items:
            card = tk.Frame(
                endpoint_row,
                bg=C["card"],
                bd=0,
                highlightthickness=1,
                highlightbackground=C["border"],
                highlightcolor=C["accent"],
                padx=10,
                pady=5,
            )
            card.pack(side="left", anchor="w", padx=(0, 8))
            tk.Label(card, text=title.upper(), bg=C["card"], fg=C["fg_sub"],
                     font=("Segoe UI", 7, "bold")).pack(anchor="w")
            tk.Label(card, text=value, bg=C["card"], fg=C["fg"],
                     font=("Consolas", 9)).pack(anchor="w", pady=(1, 0))

        content_host = tk.Frame(root, bg=C["bg"])
        content_host.pack(fill="both", expand=True, padx=10, pady=(10, 0))

        self._scroll_canvas = tk.Canvas(content_host, bg=C["bg"], highlightthickness=0, bd=0)
        self._scroll_canvas.pack(side="left", fill="both", expand=True)

        content_scrollbar = ttk.Scrollbar(content_host, orient="vertical", command=self._scroll_canvas.yview)
        content_scrollbar.pack(side="right", fill="y")
        self._scroll_canvas.configure(yscrollcommand=content_scrollbar.set)

        self._scrollable_body = ttk.Frame(self._scroll_canvas)
        self._scroll_window_id = self._scroll_canvas.create_window((0, 0), window=self._scrollable_body, anchor="nw")
        self._scrollable_body.bind("<Configure>", self._on_scrollable_body_configure)
        self._scroll_canvas.bind("<Configure>", self._on_scrollable_canvas_configure)

        body = self._scrollable_body

        main_grid = tk.Frame(body, bg=C["bg"])
        main_grid.pack(fill="both", expand=True)
        main_grid.rowconfigure(0, weight=1)
        main_grid.columnconfigure(0, weight=5)
        main_grid.columnconfigure(1, weight=3)

        left_col = tk.Frame(main_grid, bg=C["bg"])
        left_col.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        right_col = tk.Frame(main_grid, bg=C["bg"])
        right_col.grid(row=0, column=1, sticky="nsew")

        wintak_hint_frame = ttk.LabelFrame(left_col, text=" WINTAK LINK ", padding=(14, 10))
        wintak_hint_frame.pack(fill="x", pady=(0, 8))
        self._wintak_banner_var = tk.StringVar()
        tk.Label(
            wintak_hint_frame,
            textvariable=self._wintak_banner_var,
            bg=C["card"],
            fg=C["warning"],
            font=("Segoe UI", 10, "bold"),
            anchor="w",
            justify="left",
            wraplength=620,
        ).pack(fill="x")
        tk.Label(
            wintak_hint_frame,
            text="This TCP listener is required so WinTAK can reach the gateway locally before traffic is forwarded to the mesh or remote TAK.",
            bg=C["card"],
            fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=620,
        ).pack(fill="x", pady=(6, 0))

        cfg_frame = ttk.LabelFrame(left_col, text=" GATEWAY SETTINGS ", padding=(14, 10))
        cfg_frame.pack(fill="x")

        def cfg_label(text, row, col, **kw):
            lbl = ttk.Label(cfg_frame, text=text, style="Card.TLabel")
            lbl.grid(row=row, column=col, sticky="w", **kw)
            return lbl

        cfg_label("Meshtastic Port(s):", row=0, col=0, pady=(0, 4))
        cfg_port = self.cfg.get("meshtastic_port", "")
        if isinstance(cfg_port, list):
            default_ports = ", ".join(str(p) for p in cfg_port)
        elif cfg_port:
            default_ports = str(cfg_port).strip()
        else:
            detected_init = detect_serial_port_devices()
            default_ports = ", ".join(detected_init) if detected_init else ""
        self._ports_var = tk.StringVar(value=default_ports)
        ttk.Entry(cfg_frame, textvariable=self._ports_var, width=28).grid(
            row=0, column=1, sticky="ew", padx=(6, 12), pady=(0, 4)
        )
        self._ports_var.trace_add("write", self._on_ports_var_changed)

        detected_details = detect_serial_port_details()
        self._detected_port_details = detected_details
        detected = [entry["device"] for entry in detected_details]
        self._detected_ports = detected
        self._detected_ports_var = tk.StringVar(value=self._build_detected_ports_summary(detected_details))
        self._detected_ports_label = ttk.Label(
            cfg_frame,
            textvariable=self._detected_ports_var,
            style="Sub.TLabel",
            justify="left",
            wraplength=DETECTED_PORTS_SUMMARY_DEFAULT_WRAP,
        )
        self._detected_ports_label.grid(row=1, column=0, columnspan=5, sticky="ew", pady=(0, 2))
        cfg_frame.bind("<Configure>", self._on_cfg_frame_configure)
        self._detected_ports_list = tk.Listbox(
            cfg_frame,
            height=min(max(len(detected), 1), MAX_DETECTED_PORTS_DISPLAY),
            selectmode="extended",
            exportselection=False,
            bg=C["field_bg"],
            fg=C["fg"],
            selectbackground=C["accent"],
            selectforeground="#ffffff",
            relief="flat",
        )
        self._detected_ports_list.grid(row=2, column=1, sticky="ew", padx=(6, 4), pady=(0, 6))
        detected_scrollbar = ttk.Scrollbar(cfg_frame, orient="vertical", command=self._detected_ports_list.yview)
        self._detected_ports_list.configure(yscrollcommand=detected_scrollbar.set)
        detected_scrollbar.grid(row=2, column=2, sticky="nsw", padx=(0, 8), pady=(0, 6))
        for entry in detected_details:
            self._detected_ports_list.insert("end", entry["label"])
        ttk.Button(cfg_frame, text="Use selected ports", command=self._apply_selected_ports_from_list).grid(
            row=2, column=3, sticky="ew", pady=(0, 6)
        )
        ttk.Button(cfg_frame, text="Refresh ports", command=self._refresh_detected_ports).grid(
            row=2, column=4, sticky="ew", padx=(8, 0), pady=(0, 6)
        )

        ttk.Separator(cfg_frame, orient="horizontal").grid(row=3, column=0, columnspan=6, sticky="ew", pady=(4, 10))

        cfg_label("Remote TAK Host:", row=4, col=0, pady=(0, 4))
        self._server_host_var = tk.StringVar(value=str(self.cfg.get("tak_server_host", "82.165.11.84")))
        ttk.Entry(cfg_frame, textvariable=self._server_host_var, width=28).grid(
            row=4, column=1, sticky="ew", padx=(6, 12), pady=(0, 4)
        )
        cfg_label("Remote Port:", row=4, col=2, padx=(8, 6), pady=(0, 4))
        self._server_port_var = tk.StringVar(value=str(self.cfg.get("tak_server_port", 8088)))
        ttk.Entry(cfg_frame, textvariable=self._server_port_var, width=10).grid(
            row=4, column=3, sticky="w", pady=(0, 4)
        )

        cfg_label("Remote Protocol:", row=5, col=0, pady=(0, 4))
        server_protocol = str(self.cfg.get("tak_server_protocol", "TCP")).upper()
        if server_protocol not in ("TCP", "UDP"):
            server_protocol = "TCP"
        self._server_protocol_var = tk.StringVar(value=server_protocol)
        ttk.Combobox(
            cfg_frame,
            textvariable=self._server_protocol_var,
            state="readonly",
            values=["TCP", "UDP"],
            width=10,
        ).grid(row=5, column=1, sticky="w", padx=(6, 12), pady=(0, 4))
        cfg_label("WinTAK UDP Target IP:", row=5, col=2, padx=(8, 6), pady=(0, 4))
        self._local_tak_ip_var = tk.StringVar(value=str(self.cfg.get("local_tak_ip", WINTAK_REQUIRED_HOST)))
        ttk.Entry(cfg_frame, textvariable=self._local_tak_ip_var, width=16).grid(
            row=5, column=3, sticky="w", pady=(0, 4)
        )

        cfg_label("WinTAK UDP Port:", row=6, col=0, pady=(0, 4))
        self._local_tak_port_var = tk.StringVar(value=str(self.cfg.get("local_tak_port", 4242)))
        ttk.Entry(cfg_frame, textvariable=self._local_tak_port_var, width=10).grid(
            row=6, column=1, sticky="w", padx=(6, 12), pady=(0, 4)
        )
        cfg_label("WinTAK TCP Port:", row=6, col=2, padx=(8, 6), pady=(0, 4))
        self._local_tak_tcp_listen_port_var = tk.StringVar(
            value=str(self.cfg.get("local_tak_tcp_listen_port", TCP_LISTENER_DEFAULT_PORT))
        )
        ttk.Entry(cfg_frame, textvariable=self._local_tak_tcp_listen_port_var, width=10).grid(
            row=6, column=3, sticky="w", pady=(0, 4)
        )

        cfg_label("Log-Level:", row=7, col=0, pady=(0, 4))
        log_default = str(self.cfg.get("log_level", "INFO")).upper()
        if log_default not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            log_default = "INFO"
        self._log_level_var = tk.StringVar(value=log_default)
        log_combo = ttk.Combobox(
            cfg_frame,
            textvariable=self._log_level_var,
            state="readonly",
            values=["DEBUG", "INFO", "WARNING", "ERROR"],
            width=10,
        )
        log_combo.grid(row=7, column=1, sticky="w", padx=(6, 12), pady=(0, 4))
        log_combo.bind("<<ComboboxSelected>>", self._on_log_level_change)
        cfg_label("Sync Interval (s):", row=7, col=2, padx=(8, 6), pady=(0, 4))
        self._sync_interval_var = tk.StringVar(value=str(self.cfg.get("sync_interval_seconds", 300)))
        ttk.Entry(cfg_frame, textvariable=self._sync_interval_var, width=10).grid(
            row=7, column=3, sticky="w", pady=(0, 4)
        )

        self._log_raw_meshtastic_payloads_var = tk.BooleanVar(
            value=as_bool(self.cfg.get("log_raw_meshtastic_payloads", False))
        )
        ttk.Checkbutton(
            cfg_frame,
            text="Enable RAW Meshtastic output",
            variable=self._log_raw_meshtastic_payloads_var,
            command=self._update_raw_payload_logging_controls,
        ).grid(row=8, column=0, columnspan=2, sticky="w", pady=(0, 4))
        self._log_raw_meshtastic_payloads_full_var = tk.BooleanVar(
            value=as_bool(self.cfg.get("log_raw_meshtastic_payloads_full", False))
        )
        self._raw_payloads_full_check = ttk.Checkbutton(
            cfg_frame,
            text="Log complete RAW payload (full dump)",
            variable=self._log_raw_meshtastic_payloads_full_var,
        )
        self._raw_payloads_full_check.grid(row=9, column=0, columnspan=2, sticky="w", pady=(0, 4))

        self._relay_text_messages_var = tk.BooleanVar(value=as_bool(self.cfg.get("relay_text_messages", True)))
        ttk.Checkbutton(
            cfg_frame,
            text="Enable relay between selected COM ports",
            variable=self._relay_text_messages_var,
        ).grid(row=10, column=0, columnspan=2, sticky="w", pady=(0, 4))
        cfg_label("Relay From COM Port(s):", row=10, col=2, padx=(8, 6), pady=(0, 4))
        self._relay_text_from_ports_var = tk.StringVar(value=_format_ports_for_entry(self.cfg.get("relay_text_from_ports")))
        ttk.Entry(cfg_frame, textvariable=self._relay_text_from_ports_var, width=16).grid(
            row=10, column=3, sticky="ew", pady=(0, 4)
        )

        cfg_label("Relay To COM Port(s):", row=11, col=2, padx=(8, 6), pady=(0, 4))
        self._relay_text_to_ports_var = tk.StringVar(value=_format_ports_for_entry(self.cfg.get("relay_text_to_ports")))
        self._relay_text_to_ports_var.trace_add("write", self._on_relay_to_ports_changed)
        relay_to_frame = ttk.Frame(cfg_frame)
        relay_to_frame.grid(row=11, column=3, columnspan=2, sticky="ew", pady=(0, 4))
        relay_to_frame.columnconfigure(0, weight=1)
        self._relay_text_to_picker_var = tk.StringVar()
        self._relay_text_to_picker = ttk.Combobox(
            relay_to_frame,
            textvariable=self._relay_text_to_picker_var,
            state="readonly",
            width=28,
        )
        self._relay_text_to_picker.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(relay_to_frame, text="Use", command=self._apply_relay_to_picker_selection).grid(
            row=0, column=1, sticky="ew"
        )
        ttk.Entry(relay_to_frame, textvariable=self._relay_text_to_ports_var, width=16).grid(
            row=1, column=0, columnspan=2, sticky="ew", pady=(4, 0)
        )
        ttk.Label(
            cfg_frame,
            text="Leave Relay From blank to relay from any selected port. For Relay To, choose 'All other selected ports' or list custom ports.",
            style="Sub.TLabel",
        ).grid(row=12, column=0, columnspan=5, sticky="w", pady=(0, 4))
        self._refresh_relay_to_picker_options()

        self._send_nodes_without_gps_var = tk.BooleanVar(value=as_bool(self.cfg.get("send_nodes_without_gps", True)))
        ttk.Checkbutton(
            cfg_frame,
            text="Send nodes without GPS fix",
            variable=self._send_nodes_without_gps_var,
            command=self._update_no_gps_hint,
        ).grid(row=13, column=0, columnspan=2, sticky="w", pady=(0, 4))
        cfg_label("Fallback Lat:", row=13, col=2, padx=(8, 6), pady=(0, 4))
        raw_park_lat = self.cfg.get("park_lat")
        park_lat_val = "" if raw_park_lat is None else f"{float(raw_park_lat):.6f}".rstrip("0").rstrip(".")
        self._park_lat_var = tk.StringVar(value=park_lat_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lat_var, width=16).grid(
            row=13, column=3, sticky="w", pady=(0, 4)
        )

        self._wintak_setup_var = tk.StringVar()
        ttk.Label(cfg_frame, textvariable=self._wintak_setup_var, style="Hint.TLabel").grid(
            row=14, column=0, columnspan=2, sticky="w", pady=(0, 4)
        )
        cfg_label("Fallback Lon:", row=14, col=2, padx=(8, 6), pady=(0, 4))
        raw_park_lon = self.cfg.get("park_lon")
        park_lon_val = "" if raw_park_lon is None else f"{float(raw_park_lon):.6f}".rstrip("0").rstrip(".")
        self._park_lon_var = tk.StringVar(value=park_lon_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lon_var, width=16).grid(
            row=14, column=3, sticky="w", pady=(0, 4)
        )
        self._local_tak_tcp_listen_port_var.trace_add("write", self._update_wintak_setup_hint)

        self._local_tak_chat_listen_port_var = tk.StringVar(
            value=str(self.cfg.get("local_tak_chat_listen_port", DEFAULT_CHAT_LISTEN_PORT))
        )
        self._set_gateway_position_var = tk.BooleanVar(
            value=as_bool(self.cfg.get("set_gateway_position_on_start", False))
        )
        self._park_lat_var.trace_add("write", lambda *_: self._update_no_gps_hint())
        self._park_lon_var.trace_add("write", lambda *_: self._update_no_gps_hint())
        self._no_gps_hint_var = tk.StringVar()
        ttk.Label(cfg_frame, textvariable=self._no_gps_hint_var, style="Hint.TLabel").grid(
            row=15, column=0, columnspan=6, sticky="w", pady=(0, 2)
        )
        self._update_raw_payload_logging_controls()
        self._update_no_gps_hint()

        cfg_frame.columnconfigure(1, weight=1)
        cfg_frame.columnconfigure(3, weight=1)
        cfg_frame.columnconfigure(4, weight=1)

        ops_frame = ttk.LabelFrame(right_col, text=" OPERATIONS ", padding=(14, 10))
        ops_frame.pack(fill="x", pady=(0, 8))
        ops_toolbar = tk.Frame(ops_frame, bg=C["card"])
        ops_toolbar.pack(fill="x")

        self._start_btn = ttk.Button(ops_toolbar, text="Start Gateway", command=self._on_start, style="Accent.TButton")
        self._start_btn.pack(side="left", padx=(0, 6))
        self._stop_btn = ttk.Button(
            ops_toolbar,
            text="Stop Gateway",
            command=self._on_stop,
            state="disabled",
            style="Danger.TButton",
        )
        self._stop_btn.pack(side="left", padx=(0, 6))
        ttk.Button(ops_toolbar, text="Manual Sync", command=self._on_manual_sync).pack(side="left")

        self._status_var = tk.StringVar(value="⬛ Stopped")
        tk.Label(
            ops_frame,
            textvariable=self._status_var,
            bg=C["card"],
            fg=C["accent"],
            font=("Segoe UI", 10, "bold"),
            anchor="w",
        ).pack(fill="x", pady=(10, 6))

        tk.Label(
            ops_frame,
            text="Quick command console",
            bg=C["card"],
            fg=C["fg_sub"],
            font=("Segoe UI", 8, "bold"),
            anchor="w",
        ).pack(fill="x")
        self._input_var = tk.StringVar()
        command_row = tk.Frame(ops_frame, bg=C["card"])
        command_row.pack(fill="x", pady=(4, 0))
        command_entry = ttk.Entry(command_row, textvariable=self._input_var)
        command_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        command_entry.bind("<Return>", lambda _event: self._on_send_command())
        ttk.Button(command_row, text="Run", command=self._on_send_command).pack(side="right")

        mesh_send_frame = ttk.LabelFrame(right_col, text=" DIRECT TEXT -> MESH ", padding=(14, 10))
        mesh_send_frame.pack(fill="x", pady=(0, 8))
        tk.Label(
            mesh_send_frame,
            text="Send a plain text message directly to the connected Meshtastic interfaces.",
            bg=C["card"],
            fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=360,
        ).pack(fill="x")
        self._mesh_test_message_var = tk.StringVar()
        mesh_entry = ttk.Entry(mesh_send_frame, textvariable=self._mesh_test_message_var)
        mesh_entry.pack(fill="x", pady=(8, 6))
        mesh_entry.bind("<Return>", lambda _event: self._on_send_mesh_test_message())
        self._send_mesh_test_btn = ttk.Button(
            mesh_send_frame,
            text="Send Text",
            command=self._on_send_mesh_test_message,
            state="disabled",
            style="Accent.TButton",
        )
        self._send_mesh_test_btn.pack(anchor="e")
        self._mesh_test_status_var = tk.StringVar(value="Gateway not started.")
        tk.Label(
            mesh_send_frame,
            textvariable=self._mesh_test_status_var,
            bg=C["card"],
            fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=360,
        ).pack(fill="x", pady=(8, 0))

        cot_frame = ttk.LabelFrame(right_col, text=" WINTAK COT -> MESH ", padding=(14, 10))
        cot_frame.pack(fill="both", expand=False, pady=(0, 8))
        tk.Label(
            cot_frame,
            text="Paste a full WinTAK/ATAK <event> XML packet here to inject it into the mesh path for testing.",
            bg=C["card"],
            fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=360,
        ).pack(fill="x")
        self._cot_input_text = tk.Text(
            cot_frame,
            height=10,
            wrap="word",
            bg=C["text_bg"],
            fg=C["fg"],
            insertbackground=C["fg"],
            selectbackground=C["accent"],
            relief="flat",
            bd=0,
            font=self._log_font,
        )
        self._cot_input_text.pack(fill="both", expand=True, pady=(8, 6))
        cot_scrollbar = ttk.Scrollbar(cot_frame, command=self._cot_input_text.yview)
        cot_scrollbar.pack(side="right", fill="y")
        self._cot_input_text.configure(yscrollcommand=cot_scrollbar.set)
        self._send_cot_btn = ttk.Button(
            cot_frame,
            text="Send CoT",
            command=self._on_send_cot_to_mesh,
            state="disabled",
            style="Accent.TButton",
        )
        self._send_cot_btn.pack(anchor="e")
        self._cot_status_var = tk.StringVar(value="Gateway not started.")
        tk.Label(
            cot_frame,
            textvariable=self._cot_status_var,
            bg=C["card"],
            fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=360,
        ).pack(fill="x", pady=(8, 0))

        monitor_frame = ttk.LabelFrame(right_col, text=" WINTAK MONITOR ", padding=(14, 10))
        monitor_frame.pack(fill="both", expand=True)
        tk.Label(
            monitor_frame,
            text="Recent WinTAK TCP listener activity appears here for quick situational awareness.",
            bg=C["card"],
            fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=360,
        ).pack(fill="x")
        self._wintak_monitor_text = tk.Text(
            monitor_frame,
            height=9,
            wrap="word",
            state="disabled",
            bg=C["text_bg"],
            fg=C["fg"],
            font=self._log_font,
            relief="flat",
            bd=0,
            insertbackground=C["fg"],
            selectbackground=C["accent"],
        )
        self._wintak_monitor_text.pack(fill="both", expand=True, pady=(8, 0))
        self._wintak_monitor_text.tag_configure("INFO", foreground=C["fg"])
        self._wintak_monitor_text.tag_configure("WARNING", foreground=C["warning"])
        self._wintak_monitor_text.tag_configure("ERROR", foreground=C["danger"])

        log_frame = ttk.LabelFrame(left_col, text=" EVENT LOG ", padding=6)
        log_frame.pack(fill="both", expand=True, pady=(8, 0))
        self._log_text = tk.Text(
            log_frame,
            wrap="word",
            state="disabled",
            bg=C["text_bg"],
            fg=C["fg"],
            font=self._log_font,
            relief="flat",
            bd=0,
            insertbackground=C["fg"],
            selectbackground=C["accent"],
        )
        scrollbar = ttk.Scrollbar(log_frame, command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self._log_text.pack(fill="both", expand=True, padx=2, pady=2)
        self._log_text.tag_configure("DEBUG", foreground="#78a69a")
        self._log_text.tag_configure("INFO", foreground=C["fg"])
        self._log_text.tag_configure("WARNING", foreground=C["warning"])
        self._log_text.tag_configure("ERROR", foreground=C["danger"])
        self._log_text.tag_configure("CRITICAL", foreground="#ffb4ad", font=("Consolas", 9, "bold"))
        self._log_text.tag_configure("CMD", foreground=C["accent"])

        # Bottom accent stripe + status bar
        tk.Frame(root, bg=C["border"], height=1).pack(fill="x", side="bottom", padx=10)
        status_bar = tk.Frame(root, bg=C["panel"], height=26)
        status_bar.pack(fill="x", side="bottom", padx=10, pady=(6, 6))
        status_bar.pack_propagate(False)
        self._bottom_wintak_hint_var = tk.StringVar()
        tk.Label(
            status_bar,
            textvariable=self._bottom_wintak_hint_var,
            bg=C["panel"],
            fg=C["fg_sub"],
            font=("Segoe UI", 8),
            anchor="w",
        ).pack(side="left", padx=10, fill="y")
        self._update_wintak_setup_hint()

    def _on_scrollable_body_configure(self, _event=None):
        if self._scroll_canvas is None:
            return
        self._scroll_canvas.configure(scrollregion=self._scroll_canvas.bbox("all"))
        self._sync_scrollable_body_width()

    def _on_cfg_frame_configure(self, event=None):
        if not hasattr(self, "_detected_ports_label"):
            return
        frame_width = getattr(event, "width", 0)
        if not frame_width:
            if self._detected_ports_label.winfo_ismapped():
                frame_width = self._detected_ports_label.winfo_width()
            else:
                frame_width = DETECTED_PORTS_SUMMARY_DEFAULT_WRAP + DETECTED_PORTS_SUMMARY_WRAP_PADDING
        wraplength = max(frame_width - DETECTED_PORTS_SUMMARY_WRAP_PADDING, DETECTED_PORTS_SUMMARY_MIN_WRAP)
        if wraplength == self._detected_ports_wraplength:
            return
        self._detected_ports_wraplength = wraplength
        self._detected_ports_label.configure(wraplength=wraplength)

    def _on_scrollable_canvas_configure(self, event=None):
        canvas_width = getattr(event, "width", None)
        self._sync_scrollable_body_width(canvas_width)

    def _sync_scrollable_body_width(self, canvas_width=None):
        if self._scroll_canvas is None or self._scroll_window_id is None:
            return
        if canvas_width is None:
            canvas_width = self._scroll_canvas.winfo_width()
        if canvas_width > 1:
            self._scroll_canvas.itemconfigure(self._scroll_window_id, width=canvas_width)

    def _build_detected_ports_summary(self, port_details):
        if not port_details:
            return "Detected ports: –"
        return "Detected ports: " + ", ".join(entry["label"] for entry in port_details)

    def _build_relay_to_picker_values(self):
        selected_ports = _parse_ports_text(self._ports_var.get())
        if not selected_ports:
            selected_ports = list(self._detected_ports)
        detail_map = {
            entry["device"].upper(): entry["label"]
            for entry in getattr(self, "_detected_port_details", [])
        }
        picker_values = ["All other selected ports", "Custom list below"]
        self._relay_text_to_picker_map = {
            "All other selected ports": "",
            "Custom list below": None,
        }
        for port in selected_ports:
            display = detail_map.get(port.upper(), port)
            if display in self._relay_text_to_picker_map:
                continue
            self._relay_text_to_picker_map[display] = port
            picker_values.append(display)
        return picker_values

    def _refresh_relay_to_picker_options(self):
        if not hasattr(self, "_relay_text_to_picker"):
            return
        picker_values = self._build_relay_to_picker_values()
        self._relay_text_to_picker.configure(values=picker_values)
        current_choice = self._relay_text_to_picker_var.get().strip()
        if current_choice not in picker_values:
            if self._relay_text_to_ports_var.get().strip():
                self._relay_text_to_picker_var.set("Custom list below")
            else:
                self._relay_text_to_picker_var.set("All other selected ports")

    def _on_ports_var_changed(self, *_):
        self._refresh_relay_to_picker_options()

    def _on_relay_to_ports_changed(self, *_):
        if not hasattr(self, "_relay_text_to_picker_var"):
            return
        relay_to_text = self._relay_text_to_ports_var.get().strip()
        current_choice = self._relay_text_to_picker_var.get().strip()
        if relay_to_text and current_choice == "All other selected ports":
            self._relay_text_to_picker_var.set("Custom list below")
        elif not relay_to_text and current_choice == "Custom list below":
            self._relay_text_to_picker_var.set("All other selected ports")

    def _apply_relay_to_picker_selection(self):
        selected_display = self._relay_text_to_picker_var.get().strip()
        if not selected_display:
            return
        selected_port = self._relay_text_to_picker_map.get(selected_display)
        if selected_port == "":
            self._relay_text_to_ports_var.set("")
            return
        if not selected_port:
            return
        merged = []
        seen = set()
        for port in _parse_ports_text(self._relay_text_to_ports_var.get()) + [selected_port]:
            upper_port = port.upper()
            if upper_port in seen:
                continue
            seen.add(upper_port)
            merged.append(port)
        self._relay_text_to_ports_var.set(", ".join(merged))

    def _apply_selected_ports_from_list(self):
        if not self._detected_ports:
            return
        indices = self._detected_ports_list.curselection()
        if not indices:
            return
        selected = [self._detected_ports[i] for i in indices if 0 <= i < len(self._detected_ports)]
        if selected:
            existing = _parse_ports_text(self._ports_var.get())
            merged = []
            seen = set()
            for port in existing + selected:
                upper_port = port.upper()
                if upper_port in seen:
                    continue
                seen.add(upper_port)
                merged.append(port)
            self._ports_var.set(", ".join(merged))

    def _refresh_detected_ports(self):
        self._detected_port_details = detect_serial_port_details()
        self._detected_ports = [entry["device"] for entry in self._detected_port_details]
        self._detected_ports_var.set(self._build_detected_ports_summary(self._detected_port_details))
        self._detected_ports_list.delete(0, "end")
        for entry in self._detected_port_details:
            self._detected_ports_list.insert("end", entry["label"])
        self._detected_ports_list.configure(
            height=min(max(len(self._detected_ports), 1), MAX_DETECTED_PORTS_DISPLAY)
        )
        self._refresh_relay_to_picker_options()

    def _get_wintak_tcp_port_text(self):
        return self._local_tak_tcp_listen_port_var.get().strip() or str(TCP_LISTENER_DEFAULT_PORT)

    def _update_wintak_setup_hint(self, *_):
        tcp_listener_port = self._get_wintak_tcp_port_text()
        tcp_receiver_host, tcp_receiver_port = _get_tak_tcp_receiver_endpoint_from_cfg(self.cfg)
        banner_text = (
            f"WinTAK server: {tcp_receiver_host} | Port {tcp_receiver_port} | TCP "
            f"(Gateway fallback listener: {tcp_listener_port})"
        )
        self._wintak_banner_var.set(banner_text)
        self._wintak_setup_var.set(
            f"WinTAK: add a local server at {tcp_receiver_host}:{tcp_receiver_port} using TCP. "
            f"Optional fallback listener on the gateway stays at port {tcp_listener_port}."
        )
        self._bottom_wintak_hint_var.set(
            f"WinTAK TCP: {tcp_receiver_host} | Port {tcp_receiver_port} | Fallback listen {tcp_listener_port}"
        )

    def _update_no_gps_hint(self):
        send_without_gps = bool(self._send_nodes_without_gps_var.get())
        has_park = bool(self._park_lat_var.get().strip()) and bool(self._park_lon_var.get().strip())
        if send_without_gps and not has_park:
            self._no_gps_hint_var.set(
                "Note: set park_lat and park_lon for nodes without GPS, or they will be skipped."
            )
        else:
            self._no_gps_hint_var.set("")

    def _update_raw_payload_logging_controls(self):
        enabled = bool(self._log_raw_meshtastic_payloads_var.get())
        if not enabled:
            self._log_raw_meshtastic_payloads_full_var.set(False)
        self._raw_payloads_full_check.configure(state="normal" if enabled else "disabled")

    def _parse_int_field(self, raw_value, field_name, min_value=MIN_PORT_NUMBER, max_value=MAX_PORT_NUMBER):
        return _parse_int_field_value(raw_value, field_name, min_value=min_value, max_value=max_value)

    def _apply_form_to_cfg(self, ports):
        _apply_settings_payload_to_cfg(
            self.cfg,
            {
                "log_level": self._log_level_var.get(),
                "tak_server_host": self._server_host_var.get(),
                "tak_server_protocol": self._server_protocol_var.get(),
                "local_tak_ip": self._local_tak_ip_var.get(),
                "tak_server_port": self._server_port_var.get(),
                "local_tak_port": self._local_tak_port_var.get(),
                "local_tak_chat_listen_port": self._local_tak_chat_listen_port_var.get(),
                "local_tak_tcp_listen_port": self._local_tak_tcp_listen_port_var.get(),
                "sync_interval_seconds": self._sync_interval_var.get(),
                "log_raw_meshtastic_payloads": bool(self._log_raw_meshtastic_payloads_var.get()),
                "log_raw_meshtastic_payloads_full": bool(self._log_raw_meshtastic_payloads_full_var.get()),
                "relay_text_messages": bool(self._relay_text_messages_var.get()),
                "relay_text_from_ports": self._relay_text_from_ports_var.get(),
                "relay_text_to_mode": (
                    "all-other-selected-ports"
                    if self._relay_text_to_picker_var.get().strip() == "All other selected ports"
                    else "custom"
                ),
                "relay_text_to_ports": self._relay_text_to_ports_var.get(),
                "send_nodes_without_gps": bool(self._send_nodes_without_gps_var.get()),
                "set_gateway_position_on_start": bool(self._set_gateway_position_var.get()),
                "park_lat": self._park_lat_var.get(),
                "park_lon": self._park_lon_var.get(),
            },
            ports,
        )

    # ─────────────────────────── Gateway-Steuerung ────────────────────────────

    def _on_start(self):
        ports_text = self._ports_var.get().strip()
        ports = _parse_ports_text(ports_text)
        if not ports:
            self._append_log("No port specified.", "WARNING")
            return

        try:
            self._apply_form_to_cfg(ports)
            save_config(self.cfg)
        except Exception as e:
            self._append_log(f"Invalid settings: {e}", "WARNING")
            return

        self._start_btn.configure(state="disabled")
        self._stop_btn.configure(state="normal")
        self._status_var.set("🟡 Starting …")

        # GUI-Handler anlegen und VOR Gateway-Erstellung am Logger registrieren,
        # damit TAKMeshtasticGateway.setup_logging() keinen StreamHandler mehr ergänzt.
        log_level = getattr(logging, self.cfg.get("log_level", "INFO"), logging.INFO)
        self._gui_handler = _GUILogHandler(self._queue_log)
        self._gui_handler.setLevel(log_level)
        self._gui_handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S")
        )
        logger = logging.getLogger("TAK_Meshtastic_Gateway")
        logger.addHandler(self._gui_handler)

        self._gateway_thread = threading.Thread(
            target=self._run_gateway, args=(ports,), daemon=True
        )
        self._gateway_thread.start()
        self._mesh_test_status_var.set("Gateway starting …")
        self._cot_status_var.set("Gateway starting …")

    def _run_gateway(self, ports):
        try:
            gw = TAKMeshtasticGateway(ports, self.cfg)
            self._gateway = gw
            # Log WinTAK TCP listener events in the GUI.
            gw.wintak_tcp_chat_callback = self._on_wintak_tcp_chat
            self._root.after(0, lambda: self._status_var.set(f"🟢 Running  –  {', '.join(ports)}"))
            self._root.after(0, lambda: self._send_mesh_test_btn.configure(state="normal"))
            self._root.after(0, lambda: self._send_cot_btn.configure(state="normal"))
            self._root.after(0, lambda: self._mesh_test_status_var.set("Ready for manual mesh test sending."))
            self._root.after(0, lambda: self._cot_status_var.set("Ready for manual CoT injection."))
            gw.run()
        except Exception:
            err = traceback.format_exc()
            self._queue_log(f"Gateway thread error:\n{err}", "ERROR")
        finally:
            self._root.after(0, self._on_gateway_stopped)

    def _on_stop(self):
        gw = self._gateway
        if gw:
            gw.shutdown_flag.set()
        self._stop_btn.configure(state="disabled")
        self._status_var.set("🟡 Stopping …")
        self._send_mesh_test_btn.configure(state="disabled")
        self._send_cot_btn.configure(state="disabled")
        self._mesh_test_status_var.set("Gateway stopping …")
        self._cot_status_var.set("Gateway stopping …")

    def _on_gateway_stopped(self):
        if self._gui_handler:
            logging.getLogger("TAK_Meshtastic_Gateway").removeHandler(self._gui_handler)
            self._gui_handler = None
        self._gateway = None
        self._start_btn.configure(state="normal")
        self._stop_btn.configure(state="disabled")
        self._status_var.set("⬛ Stopped")
        self._send_mesh_test_btn.configure(state="disabled")
        self._send_cot_btn.configure(state="disabled")
        self._mesh_test_status_var.set("Gateway not started.")
        self._cot_status_var.set("Gateway not started.")

    def _on_manual_sync(self):
        gw = self._gateway
        if gw:
            threading.Thread(target=gw.full_sync, daemon=True).start()
            self._append_log("Manual full sync started.", "INFO")
        else:
            self._append_log("Gateway is not running.", "WARNING")

    def _on_send_mesh_test_message(self):
        message = self._mesh_test_message_var.get().strip()
        if not message:
            self._mesh_test_status_var.set("⚠ Please enter a test message first.")
            self._append_log("Manual mesh test send canceled: empty message.", "WARNING")
            return

        gw = self._gateway
        if not gw:
            self._mesh_test_status_var.set("⚠ Gateway is not running.")
            self._append_log("Manual mesh test send is not possible: gateway is not running.", "WARNING")
            return

        self._mesh_test_status_var.set("🟡 Sending test message …")
        threading.Thread(
            target=self._send_mesh_test_message_worker,
            args=(gw, message),
            daemon=True,
        ).start()

    def _send_mesh_test_message_worker(self, gw, message):
        try:
            total_sent = gw._send_text_to_meshtastic(message)
        except Exception as exc:
            error_message = f"❌ Send failed: {exc}"
            self._queue_log(f"Manual mesh send failed: {exc}", "ERROR")
            self._queue_log("Manual mesh send error details:\n" + traceback.format_exc(), "DEBUG")
            self._root.after(
                0,
                lambda: self._mesh_test_status_var.set(error_message),
            )
            return

        self._queue_log(f"Test message sent to the mesh successfully (interfaces: {total_sent}).", "INFO")
        self._root.after(0, lambda: self._mesh_test_status_var.set(f"✅ Sent (interfaces: {total_sent})."))
        self._root.after(0, lambda: self._mesh_test_message_var.set(""))

    def _on_send_cot_to_mesh(self):
        packet_xml = self._cot_input_text.get("1.0", "end").strip()
        if not packet_xml:
            self._cot_status_var.set("⚠ Please paste a CoT <event> XML first.")
            self._append_log("Manual CoT send canceled: empty payload.", "WARNING")
            return

        gw = self._gateway
        if not gw:
            self._cot_status_var.set("⚠ Gateway is not running.")
            self._append_log("Manual CoT send is not possible: gateway is not running.", "WARNING")
            return

        self._cot_status_var.set("🟡 Sending CoT to mesh …")
        threading.Thread(
            target=self._send_cot_to_mesh_worker,
            args=(gw, packet_xml),
            daemon=True,
        ).start()

    def _send_cot_to_mesh_worker(self, gw, packet_xml):
        try:
            send_result = gw.send_cot_to_meshtastic(packet_xml)
        except Exception as exc:
            error_message = f"❌ Send failed: {exc}"
            self._queue_log(f"Manual CoT send failed: {exc}", "ERROR")
            self._queue_log("Manual CoT send error details:\n" + traceback.format_exc(), "DEBUG")
            self._root.after(0, lambda: self._cot_status_var.set(error_message))
            return

        transport = send_result.get("transport", "UNKNOWN")
        count = send_result.get("count", 0)
        self._queue_log(
            f"Manual CoT sent to the mesh successfully (transport: {transport}, packets: {count}).",
            "INFO",
        )
        self._root.after(0, lambda: self._cot_status_var.set(f"✅ Sent via {transport} ({count} packet(s))."))
        self._root.after(0, lambda: self._cot_input_text.delete("1.0", "end"))

    # ─────────────────────────── WinTAK TCP Events ───────────────────────────

    def _get_wintak_tcp_listener_endpoint(self):
        return _get_tak_tcp_listener_endpoint_from_cfg(self.cfg)

    def _on_wintak_tcp_chat(self, kind, sender, message, addr=None):
        """Thread-safe callback invoked by the gateway TCP listener for WinTAK events."""
        self._root.after(0, self._update_wintak_monitor, kind, sender, message, addr)

    def _update_wintak_monitor(self, kind, sender, message, addr):
        """Write WinTAK TCP listener events into the GUI log."""
        if kind == "connect":
            ip = addr[0] if addr else "?"
            listen_ip, listen_port = self._get_wintak_tcp_listener_endpoint()
            monitor_line = f"LINK UP  {ip} -> {listen_ip}:{listen_port}"
            self._append_log(f"WinTAK connected from {ip} to TCP listener {listen_ip}:{listen_port}.", "INFO")
            self._append_wintak_monitor(monitor_line, "INFO")
        elif kind == "disconnect":
            self._append_log("WinTAK TCP connection closed.", "INFO")
            self._append_wintak_monitor("LINK DOWN  WinTAK TCP connection closed.", "WARNING")
        elif kind == "chat":
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            self._append_log(f"[WinTAK {ts}] {sender}: {message}", "INFO")
            self._append_wintak_monitor(f"[{ts}] {sender}: {message}", "INFO")

    def _append_wintak_monitor(self, msg, level="INFO"):
        self._wintak_monitor_text.configure(state="normal")
        self._wintak_monitor_text.insert(
            "end",
            msg + "\n",
            level if level in ("INFO", "WARNING", "ERROR") else "INFO",
        )
        current_lines = int(self._wintak_monitor_text.index("end-1c").split(".")[0])
        if current_lines > MAX_WINTAK_MONITOR_LINES:
            # Keep only the newest monitor rows once the text widget grows past the cap.
            excess_lines = current_lines - MAX_WINTAK_MONITOR_LINES
            self._wintak_monitor_text.delete("1.0", f"{excess_lines + 1}.0")
        self._wintak_monitor_text.see("end")
        self._wintak_monitor_text.configure(state="disabled")

    def _on_log_level_change(self, _event=None):
        level_str = self._log_level_var.get()
        level = getattr(logging, level_str, logging.INFO)
        logging.getLogger("TAK_Meshtastic_Gateway").setLevel(level)
        if self._gui_handler:
            self._gui_handler.setLevel(level)
        self._append_log(f"Log level changed to {level_str}.", "INFO")

    # ─────────────────────────── Command input ───────────────────────────────

    def _on_send_command(self):
        cmd = self._input_var.get().strip()
        if not cmd:
            return
        self._input_var.set("")
        self._append_log(f"> {cmd}", "CMD")
        self._handle_command(cmd)

    def _handle_command(self, cmd):
        parts_raw = cmd.split()
        if not parts_raw:
            return
        parts = [p.lower() for p in parts_raw]
        action = parts[0]
        if action == "sync":
            self._on_manual_sync()
        elif action == "log" and len(parts) >= 2:
            new_level = parts[1].upper()
            if new_level in ("DEBUG", "INFO", "WARNING", "ERROR"):
                self._log_level_var.set(new_level)
                self._on_log_level_change()
            else:
                self._append_log(f"Unknown level: '{parts[1]}'. Valid values: debug|info|warning|error", "WARNING")
        elif action == "clear":
            self._log_text.configure(state="normal")
            self._log_text.delete("1.0", "end")
            self._log_text.configure(state="disabled")
        elif action == "nodes":
            gw = self._gateway
            if not gw:
                self._append_log("Gateway is not running.", "WARNING")
                return
            rows = gw.node_db.get_all_nodes()
            if not rows:
                self._append_log("No nodes in the database.", "INFO")
            else:
                self._append_log(
                    f"{'UID':<22} {'Callsign':<20} {'Lat':>10} {'Lon':>11} {'Alt':>6}  {'Source':<10} Last Seen",
                    "INFO",
                )
                for uid, callsign, lat, lon, alt, last_seen, source in rows:
                    self._append_log(
                        f"{uid:<22} {callsign:<20} {lat:>10.5f} {lon:>11.5f} {alt:>6.0f}  {source:<10} {last_seen or '-'}",
                        "INFO",
                    )
        elif action == "setpos" and len(parts_raw) >= 4:
            uid = parts_raw[1]
            try:
                lat = float(parts_raw[2])
                lon = float(parts_raw[3])
                alt = float(parts_raw[4]) if len(parts_raw) >= 5 else 0.0
            except ValueError:
                self._append_log("setpos: invalid coordinates. Syntax: setpos <uid> <lat> <lon> [alt]", "WARNING")
                return
            normalized = normalize_coordinates(lat, lon)
            if normalized is None:
                self._append_log(f"setpos: invalid coordinates ({lat}, {lon}).", "WARNING")
                return
            lat, lon = normalized
            gw = self._gateway
            if not gw:
                self._append_log("Gateway is not running.", "WARNING")
                return
            gw.node_db.set_manual_position(uid, lat, lon, alt)
            gw.last_known_positions[uid] = (lat, lon, alt)
            self._append_log(f"Position for '{uid}' set to ({lat:.6f}, {lon:.6f}, {alt:.0f}m).", "INFO")
        elif action == "setpos":
            self._append_log("Syntax: setpos <uid> <lat> <lon> [alt]", "WARNING")
        elif action == "help":
            for line in [
                "Available commands:",
                "  sync                         – Start a manual full sync for all nodes",
                "  nodes                        – Show all nodes from the database",
                "  setpos <uid> <lat> <lon> [alt] – Set a node position manually",
                "  log <level>                  – Set the log level (debug/info/warning/error)",
                "  clear                        – Clear the log output",
                "  help                         – Show this help",
            ]:
                self._append_log(line, "INFO")
        else:
            self._append_log(f"Unknown command: '{cmd}'. Type 'help' for help.", "WARNING")

    # ─────────────────────────── Logging in GUI ───────────────────────────────

    def _queue_log(self, msg, level="INFO"):
        """Thread-safe call: enqueue a message for the GUI thread."""
        self._root.after(0, self._append_log, msg, level)

    def _append_log(self, msg, level="INFO"):
        should_autoscroll = _text_widget_is_at_bottom(self._log_text)
        self._log_text.configure(state="normal")
        tag = level if level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "CMD") else "INFO"
        self._log_text.insert("end", msg + "\n", tag)
        if should_autoscroll:
            self._log_text.see("end")
        self._log_text.configure(state="disabled")

    # ─────────────────────────── Shutdown ─────────────────────────────────────

    def _on_close(self):
        gw = self._gateway
        if gw:
            gw.shutdown_flag.set()
        self._root.destroy()

    def run(self):
        self._root.mainloop()


class TAKMeshtasticGateway:
    # Class constants
    SOCKET_TIMEOUT = 5.0  # Socket timeout in seconds

    def __init__(self, ports, cfg=None):
        self.ports = ports if isinstance(ports, list) else [ports]
        self.cfg = cfg or {}
        self.gateway_callsign = str(self.cfg.get("gateway_callsign", "MSHT-GW")).strip() or "MSHT-GW"
        self.gateway_uid = str(self.cfg.get("gateway_uid", "GW-01")).strip() or "GW-01"
        self.server_ip = self.cfg.get("tak_server_host", "82.165.11.84")
        # Validate port numbers
        try:
            server_port = int(self.cfg.get("tak_server_port", 8088))
            if not (1 <= server_port <= 65535):
                raise ValueError(f"Invalid server port: {server_port}")
            self.server_port = server_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid tak_server_port in config: {e}")
        
        self.server_protocol = str(self.cfg.get("tak_server_protocol", "TCP")).upper()

        # lokaler WinTAK / TAK-Client (Standard)
        self.tak_ip = self.cfg.get("local_tak_ip", WINTAK_REQUIRED_HOST)
        try:
            tak_port = int(self.cfg.get("local_tak_port", 4242))
            if not (1 <= tak_port <= 65535):
                raise ValueError(f"Invalid local TAK port: {tak_port}")
            self.tak_port = tak_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid local_tak_port in config: {e}")

        # Match the LPU5 TAK bridge pattern by defaulting the inbound chat
        # listener to the same standard TAK UDP port used for local delivery.
        default_chat_listen_port = self.tak_port
        try:
            chat_listen_port = int(
                self.cfg.get(
                    "local_tak_chat_listen_port",
                    default_chat_listen_port,
                )
            )
            if not (1 <= chat_listen_port <= 65535):
                raise ValueError(f"Invalid local TAK chat listen port: {chat_listen_port}")
            self.chat_listen_port = chat_listen_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid local_tak_chat_listen_port in config: {e}")
        self.chat_listen_ip = str(self.cfg.get("local_tak_chat_listen_ip", "0.0.0.0")).strip() or "0.0.0.0"
        self.tak_multicast_interface_ip = (
            str(self.cfg.get("tak_multicast_interface_ip", "0.0.0.0")).strip() or "0.0.0.0"
        )
        try:
            socket.inet_aton(self.tak_multicast_interface_ip)
        except OSError as exc:
            raise ValueError(
                f"Invalid tak_multicast_interface_ip in config: {self.tak_multicast_interface_ip}"
            ) from exc
        try:
            self.tak_multicast_groups = _config_tak_multicast_groups_to_list(
                self.cfg.get("tak_multicast_groups")
            )
        except ValueError as exc:
            raise ValueError(f"Invalid tak_multicast_groups in config: {exc}") from exc
        try:
            tcp_chat_listen_ip, tcp_chat_listen_port = _get_tak_tcp_listener_endpoint_from_cfg(
                self.cfg,
                strict_port=True,
            )
            if not (1 <= tcp_chat_listen_port <= 65535):
                raise ValueError(f"Invalid local TAK TCP listen port: {tcp_chat_listen_port}")
            self.tcp_chat_listen_port = tcp_chat_listen_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid local_tak_tcp_listen_port in config: {e}")
        self.tcp_chat_listen_ip = tcp_chat_listen_ip
        self.tak_tcp_receiver_enabled = as_bool(self.cfg.get("local_tak_tcp_receiver_enabled", True))
        try:
            tcp_chat_receiver_host, tcp_chat_receiver_port = _get_tak_tcp_receiver_endpoint_from_cfg(
                self.cfg,
                strict_port=True,
            )
            if not (1 <= tcp_chat_receiver_port <= 65535):
                raise ValueError(f"Invalid local TAK TCP receiver port: {tcp_chat_receiver_port}")
            self.tcp_chat_receiver_host = tcp_chat_receiver_host
            self.tcp_chat_receiver_port = tcp_chat_receiver_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid local_tak_tcp_receiver_port in config: {e}")

        self.sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_udp.settimeout(self.SOCKET_TIMEOUT)  # Add timeout to prevent hanging
        self.sock_chat_listeners = []
        self.local_tak_tcp_peers = []
        self.local_tak_tcp_peer_lock = threading.RLock()

        # Callback invoked by the TCP listener for connection events and received chat messages.
        # Signature: callback(kind, sender, message, addr)
        # kind: "connect" | "disconnect" | "chat"
        self.wintak_tcp_chat_callback = None

        # Local WinTAK TCP push socket: the active TCP-CLIENT connection established by
        # receive_tak_chat_tcp() to WinTAK's TAK-server port (default 8087).  Because the
        # TAK protocol is bidirectional on a single TCP stream we reuse this connection to
        # also *push* CoT events (especially markers) to WinTAK in addition to UDP.  This
        # makes marker delivery more reliable when WinTAK is configured as a TAK server
        # (localhost:8087) instead of relying solely on UDP 4242.
        self._local_tak_tcp_push_conn = None
        self._local_tak_tcp_push_peer = None
        self._local_tak_tcp_push_lock = threading.Lock()

        # When True (default) the gateway sends CoT to the local TCP receiver
        # connection (127.0.0.1:8087) before falling back to UDP/4242.  Set to
        # False only if you specifically need legacy UDP-first behaviour.
        self.prefer_local_tcp_over_udp = as_bool(
            self.cfg.get("prefer_local_tcp_over_udp", True)
        )

        # Remote server socket(s)
        self.sock_remote = None  # für TCP: persistent socket; für UDP: socket used for sendto
        self.remote_pytak_loop = None
        self.remote_pytak_queue = None
        self.remote_pytak_ready = threading.Event()
        self.remote_cot_url = self._build_remote_cot_url()

        # interne State
        self.logger = self.setup_logging()
        self.service_web_ui = None
        self.service_web_ui_port = SERVICE_WEB_UI_PORT
        self.service_web_ui_bind_ip = detect_reachable_local_ip(self.server_ip)
        if as_bool(self.cfg.get("enable_service_web_ui", True)):
            try:
                self.service_web_ui = start_gateway_service_web_ui(
                    self.service_web_ui_bind_ip,
                    port=self.service_web_ui_port,
                    asset_dir=pathlib.Path(__file__).resolve().parent,
                    logger=self.logger,
                    cfg=self.cfg,
                )
                self.logger.info("=" * 72)
                self.logger.info("HTML-BROWSER-UI AKTIV: %s", self.service_web_ui["url"])
                self.logger.info(
                    "Backend-Dienst läuft weiter. Öffne diese URL im Browser für die Hauptoberfläche."
                )
                self.logger.info(
                    "Falls die Seite nicht erreichbar ist, prüfe Firewall, IP-Adresse und Port %s.",
                    self.service_web_ui_port,
                )
                self.logger.info("=" * 72)
            except OSError as exc:
                self.logger.warning(
                    "HTML-Browser-UI konnte auf %s:%s nicht gestartet werden: %s",
                    self.service_web_ui_bind_ip,
                    self.service_web_ui_port,
                    exc,
                )
        self.interfaces = []
        self.interface_lock = threading.RLock()
        self._meshtastic_pubsub_registered = False
        self.server_lock = threading.Lock()
        self.shutdown_flag = threading.Event()  # For graceful shutdown
        self.last_known_positions = {}
        self.local_node_numbers = set()
        self.local_node_ids = set()
        self.chat_cache_lock = threading.Lock()
        self.recent_tak_chat_ids = {}
        self.recent_meshtastic_chat_ids = {}
        self.recent_meshtastic_outbound_texts = {}
        self.recent_cot_ids = {}
        self.recent_cot_identity_by_uid = {}
        self.partial_meshtastic_cot_messages = {}
        self.partial_meshtastic_forwarder_messages = {}
        self.partial_fountain_transfers = {}   # FTN fountain-code receive state
        self.service_ui_event_store = None
        self.relay_text_messages = as_bool(self.cfg.get("relay_text_messages", True))
        self.relay_text_from_ports = {
            port.upper() for port in _config_ports_to_list(self.cfg.get("relay_text_from_ports"))
        }
        self.relay_text_to_ports = {
            port.upper() for port in _config_ports_to_list(self.cfg.get("relay_text_to_ports"))
        }

        # Knoten-Datenbank (SQLite) – persistente GPS-Koordinaten über Neustarts hinweg
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nodes.db")
        self.node_db = NodeDatabase(db_path)
        self.logger.info(f"Knoten-Datenbank geöffnet: {db_path}")
        # Letzte bekannte Positionen aus der Datenbank vorausfüllen
        for _uid, _cs, _lat, _lon, _alt, _ts, _src in self.node_db.get_all_nodes():
            self.last_known_positions[_uid] = (_lat, _lon, _alt if _alt is not None else 0)
        if self.last_known_positions:
            self.logger.info(
                f"{len(self.last_known_positions)} Knoten-Position(en) aus Datenbank geladen."
            )

        # Park coordinates wenn kein GPS-Fix (optional)
        self.park_lat = to_float_or_none(self.cfg.get("park_lat"))
        self.park_lon = to_float_or_none(self.cfg.get("park_lon"))
        # NOTE: (0,0) is treated as "not configured" (same as GPS fix logic) to prevent
        # accidentally placing nodes on Null Island when the user leaves the default 0.0 values.
        if (
            self.park_lat is not None
            and self.park_lon is not None
            and not math.isnan(self.park_lat)
            and not math.isnan(self.park_lon)
            and -90.0 <= self.park_lat <= 90.0
            and -180.0 <= self.park_lon <= 180.0
            and not (self.park_lat == 0.0 and self.park_lon == 0.0)
        ):
            self.park_coords = (self.park_lat, self.park_lon)
        else:
            self.park_coords = None
        self.send_nodes_without_gps = as_bool(self.cfg.get("send_nodes_without_gps", True))
        self.set_gateway_position_on_start = as_bool(self.cfg.get("set_gateway_position_on_start", False))

        # Sync interval
        self.sync_interval_seconds = int(self.cfg.get("sync_interval_seconds", 300))

        # Raw Meshtastic payload logging (off by default, turn on in config.yaml for diagnosis)
        self.log_raw_meshtastic_payloads = as_bool(
            self.cfg.get("log_raw_meshtastic_payloads", False)
        )
        # When true, include the complete payload (hex + base64) regardless of size.
        # When false (default), only the first 16 bytes hex prefix is shown for large packets.
        self.log_raw_meshtastic_payloads_full = as_bool(
            self.cfg.get("log_raw_meshtastic_payloads_full", False)
        )

        # Warn when no-fix nodes would be placed at an invalid/unconfigured position
        if self.send_nodes_without_gps and self.park_coords is None:
            self.logger.warning(
                "ACHTUNG: send_nodes_without_gps=true, aber park_lat/park_lon sind nicht gesetzt. "
                "Nodes ohne GPS-Fix werden übersprungen. "
                "Bitte park_lat und park_lon in config.yaml auf einen sinnvollen Standort setzen."
            )

        # Start
        try:
            if meshtastic is None:
                raise RuntimeError("meshtastic-Paket nicht installiert / importierbar.")
            connected_interfaces = self._connect_meshtastic_interfaces()
            if not connected_interfaces:
                self.logger.warning(
                    "Beim Start konnte noch keine Meshtastic-COM-Verbindung aufgebaut werden. "
                    "Weitere Verbindungsversuche erfolgen automatisch."
                )
            # Start maintenance thread
            threading.Thread(target=self.maintain_server, daemon=True).start()
            self._register_local_nodes()
            for listen_port in self._iter_tak_listener_ports():
                threading.Thread(
                    target=self.listen_for_tak_chat,
                    args=(listen_port,),
                    daemon=True,
                ).start()
            for multicast_group, multicast_port in self.tak_multicast_groups:
                threading.Thread(
                    target=self.listen_for_tak_multicast,
                    args=(multicast_group, multicast_port),
                    daemon=True,
                ).start()
            threading.Thread(
                target=self.listen_for_tak_chat_tcp,
                args=(self.tcp_chat_listen_port,),
                daemon=True,
            ).start()
            if self.tak_tcp_receiver_enabled:
                threading.Thread(
                    target=self.receive_tak_chat_tcp,
                    daemon=True,
                ).start()
            self.apply_gateway_fixed_position()
            self.logger.info("Gateway gestartet. Führe initiale Vollsynchronisation aus.")
            self.full_sync()
        except Exception as e:
            self.logger.error(f"Fehler beim Initialisieren: {e}")
            self.logger.debug(traceback.format_exc())

    def setup_logging(self):
        # Logger mit colorlog wenn verfügbar, sonst Standardlogging
        log_level_str = str(self.cfg.get("log_level", "INFO")).upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        logger = logging.getLogger("TAK_Meshtastic_Gateway")
        logger.setLevel(log_level)
        if not logger.handlers:
            if colorlog is not None:
                handler = colorlog.StreamHandler()
                handler.setFormatter(colorlog.ColoredFormatter('[%(asctime)s] %(log_color)s%(levelname)s: %(message)s', datefmt="%H:%M:%S"))
            else:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt="%H:%M:%S"))
            logger.addHandler(handler)
        return logger

    def _remember_recent_chat(self, cache, key):
        if not key:
            return
        now = time.time()
        with self.chat_cache_lock:
            expired = [item for item, ts in cache.items() if now - ts > RECENT_CHAT_CACHE_TTL_SECONDS]
            for item in expired:
                cache.pop(item, None)
            cache[key] = now
            while len(cache) > RECENT_CHAT_CACHE_MAX_ENTRIES:
                oldest = min(cache, key=cache.get)
                cache.pop(oldest, None)

    def _was_seen_recently(self, cache, key):
        if not key:
            return False
        now = time.time()
        with self.chat_cache_lock:
            ts = cache.get(key)
            if ts is None:
                return False
            if now - ts > RECENT_CHAT_CACHE_TTL_SECONDS:
                cache.pop(key, None)
                return False
            return True

    def _iter_recent_cot_identity_cache_keys(self, *, uid="", callsign="", droid_uid=""):
        seen = set()
        candidates = (
            ("uid", str(uid or "").strip()),
            ("droid", str(droid_uid or "").strip()),
            ("callsign", str(callsign or "").strip().casefold()),
        )
        for prefix, value in candidates:
            if not value:
                continue
            cache_key = f"{prefix}:{value}"
            if cache_key in seen:
                continue
            seen.add(cache_key)
            yield cache_key

    def _remember_recent_cot_identity(self, metadata, *, source_label=None):
        if not isinstance(metadata, dict):
            return
        uid = str(metadata.get("uid") or "").strip()
        event_type = str(metadata.get("type") or "").strip()
        cot_class = str(metadata.get("cot_class") or "").strip() or _classify_cot_event_type(event_type)
        if not uid or not event_type:
            return
        if cot_class == "pli":
            return
        now = time.time()
        entry = {
            "uid": uid,
            "type": event_type,
            "how": str(metadata.get("how") or "").strip() or "m-g",
            "cot_class": cot_class,
            "updated_at": now,
            "source_label": source_label,
        }
        cache_keys = tuple(
            self._iter_recent_cot_identity_cache_keys(
                uid=uid,
                callsign=metadata.get("callsign"),
                droid_uid=metadata.get("droid_uid"),
            )
        )
        if not cache_keys:
            return
        with self.chat_cache_lock:
            expired = [
                item
                for item, cached in self.recent_cot_identity_by_uid.items()
                if now - float(cached.get("updated_at") or 0) > RECENT_COT_IDENTITY_CACHE_TTL_SECONDS
            ]
            for item in expired:
                self.recent_cot_identity_by_uid.pop(item, None)
            for cache_key in cache_keys:
                self.recent_cot_identity_by_uid[cache_key] = dict(entry)
            while len(self.recent_cot_identity_by_uid) > RECENT_COT_IDENTITY_CACHE_MAX_ENTRIES:
                oldest = min(
                    self.recent_cot_identity_by_uid,
                    key=lambda key: self.recent_cot_identity_by_uid[key].get("updated_at", 0),
                )
                self.recent_cot_identity_by_uid.pop(oldest, None)

    def _get_recent_cot_identity(self, uid, *, callsign="", droid_uid=""):
        lookup_keys = tuple(
            self._iter_recent_cot_identity_cache_keys(
                uid=uid,
                callsign=callsign,
                droid_uid=droid_uid,
            )
        )
        if not lookup_keys:
            return None
        now = time.time()
        with self.chat_cache_lock:
            for lookup_key in lookup_keys:
                entry = self.recent_cot_identity_by_uid.get(lookup_key)
                if entry is None:
                    continue
                if now - float(entry.get("updated_at") or 0) > RECENT_COT_IDENTITY_CACHE_TTL_SECONDS:
                    self.recent_cot_identity_by_uid.pop(lookup_key, None)
                    continue
                return dict(entry)
            return None

    def _record_service_monitor_event(self, packet_xml, direction, source, *, is_echo_back=False):
        event_store = getattr(self, "service_ui_event_store", None)
        if event_store is None:
            return
        try:
            record = _build_service_monitor_cot_event_record(
                packet_xml,
                direction=direction,
                source=source,
                gateway_uid=getattr(self, "gateway_uid", ""),
                local_node_ids=getattr(self, "local_node_ids", set()),
                is_echo_back=is_echo_back,
            )
        except Exception:
            self.logger.debug("CoT-Monitor-Event konnte nicht erstellt werden:\n" + traceback.format_exc())
            return
        if record is not None:
            event_store.add(record)

    def _cleanup_expired_meshtastic_cot_fragments(self):
        now = time.time()
        with self.chat_cache_lock:
            expired_keys = []
            for key, entry in self.partial_meshtastic_cot_messages.items():
                updated_at = entry.get("updated_at")
                created_at = entry.get("created_at")
                last_seen_at = updated_at if updated_at is not None else created_at
                if last_seen_at is None or now - last_seen_at > MESHTASTIC_COT_FRAGMENT_TTL_SECONDS:
                    expired_keys.append(key)
            for key in expired_keys:
                self.partial_meshtastic_cot_messages.pop(key, None)

    def _cleanup_expired_meshtastic_forwarder_fragments(self):
        now = time.time()
        with self.chat_cache_lock:
            expired_keys = []
            for key, entry in self.partial_meshtastic_forwarder_messages.items():
                updated_at = entry.get("updated_at")
                created_at = entry.get("created_at")
                last_seen_at = updated_at if updated_at is not None else created_at
                if last_seen_at is None or now - last_seen_at > MESHTASTIC_COT_FRAGMENT_TTL_SECONDS:
                    expired_keys.append(key)
            for key in expired_keys:
                self.partial_meshtastic_forwarder_messages.pop(key, None)
            # Also expire stale FTN fountain transfers
            ftn_expired = [
                k for k, v in self.partial_fountain_transfers.items()
                if now - v.get("updated_at", v.get("created_at", now)) > FOUNTAIN_RECEIVE_TTL_SECONDS
            ]
            for key in ftn_expired:
                self.partial_fountain_transfers.pop(key, None)

    def _extract_cot_event_metadata(self, packet_xml):
        try:
            root = fromstring(packet_xml)
        except (ParseError, TypeError, ValueError):
            return None
        if _xml_local_name(root.tag) != "event":
            return None
        point = _find_child_by_local_name(root, "point")
        detail = _find_child_by_local_name(root, "detail")
        if point is None:
            return None
        lat = _coerce_cot_point_float(point.get("lat"))
        lon = _coerce_cot_point_float(point.get("lon"))
        if lat is None or lon is None:
            return None
        contact = _find_descendant_by_local_name(detail, "contact") if detail is not None else None
        uid_detail = _find_descendant_by_local_name(detail, "uid") if detail is not None else None
        event_uid = (root.get("uid") or "").strip()
        event_type = (root.get("type") or "").strip()
        event_how = (root.get("how") or "").strip() or "m-g"
        if not event_uid or not event_type:
            return None
        if _is_live_pli_cot_event(event_type, event_how, detail):
            cot_class = "pli"
        elif _is_meshtastic_pli_event_type(event_type) and _is_persistable_cot_type(event_type):
            cot_class = "marker"
        else:
            cot_class = _classify_cot_event_type(event_type)
        return {
            "uid": event_uid,
            "type": event_type,
            "how": event_how,
            "cot_class": cot_class,
            "is_marker": cot_class == "marker",
            "is_pli": cot_class == "pli",
            "start": (root.get("start") or "").strip(),
            "time": (root.get("time") or "").strip(),
            "stale": (root.get("stale") or "").strip(),
            "callsign": str(contact.get("callsign") or "").strip() if contact is not None else "",
            "droid_uid": str(uid_detail.get("Droid") or "").strip() if uid_detail is not None else "",
            "lat": lat,
            "lon": lon,
            "hae": _coerce_cot_point_float(point.get("hae")),
            "ce": _coerce_cot_point_float(point.get("ce")),
            "le": _coerce_cot_point_float(point.get("le")),
            "has_meshtastic_marker": (
                detail is not None
                and (
                    _find_descendant_by_local_name(detail, "__meshtastic") is not None
                    or _find_descendant_by_local_name(detail, "meshtastic") is not None
                )
            ),
        }

    def _apply_meshtastic_bridge_detail(
        self,
        detail,
        uid,
        callsign,
        *,
        geopointsrc="GPS",
        battery=None,
        speed=0.0,
        course=0.0,
        endpoint=None,
    ):
        contact = _find_child_by_local_name(detail, "contact")
        if contact is None:
            contact = SubElement(detail, "contact")
        if callsign and not str(contact.get("callsign") or "").strip():
            contact.set("callsign", callsign)
        if endpoint and not str(contact.get("endpoint") or "").strip():
            contact.set("endpoint", endpoint)

        uid_detail = _find_child_by_local_name(detail, "uid")
        if uid_detail is None and callsign:
            uid_detail = SubElement(detail, "uid")
        if uid_detail is not None and callsign and not str(uid_detail.get("Droid") or "").strip():
            uid_detail.set("Droid", callsign)

        group = _find_child_by_local_name(detail, "__group")
        if group is None:
            group = SubElement(detail, "__group")
        if not str(group.get("name") or "").strip():
            group.set("name", "Cyan")
        if not str(group.get("role") or "").strip():
            group.set("role", "Team Member")

        mesh_detail = _find_child_by_local_name(detail, "meshtastic")
        if mesh_detail is None:
            mesh_detail = SubElement(detail, "meshtastic")
        if callsign and not str(mesh_detail.get("longName") or "").strip():
            mesh_detail.set("longName", callsign)
        short_name = str(mesh_detail.get("shortName") or "").strip()
        if not short_name:
            short_name = (callsign or uid or "")[:2]
            if short_name:
                mesh_detail.set("shortName", short_name)

        if battery is not None:
            status = _find_child_by_local_name(detail, "status")
            if status is None:
                status = SubElement(detail, "status")
            if not str(status.get("battery") or "").strip():
                status.set("battery", str(_clamp_battery_percentage(battery)))

        track = _find_child_by_local_name(detail, "track")
        if track is None:
            track = SubElement(detail, "track")
        if not str(track.get("speed") or "").strip():
            track.set("speed", str(speed))
        if not str(track.get("course") or "").strip():
            track.set("course", str(course))

        precisionlocation = _find_child_by_local_name(detail, "precisionlocation")
        if precisionlocation is None:
            precisionlocation = SubElement(detail, "precisionlocation")
        if geopointsrc and not str(precisionlocation.get("geopointsrc") or "").strip():
            precisionlocation.set("geopointsrc", geopointsrc)

    def _normalize_generic_cot_event(
        self,
        packet_xml,
        add_meshtastic_marker=False,
        meshtastic_live_contact=False,
    ):
        packet_xml = _normalize_tak_xml_payload(packet_xml)
        if not packet_xml:
            return None, None, "CoT-Payload ist leer"
        try:
            root = fromstring(packet_xml)
        except (ParseError, TypeError, ValueError) as exc:
            return None, None, f"XML-Parsing fehlgeschlagen: {exc}"
        if _xml_local_name(root.tag) != "event":
            return None, None, "Root-Element ist kein <event>"

        uid = (root.get("uid") or "").strip()
        event_type = (root.get("type") or "").strip()
        event_how = (root.get("how") or "").strip()
        if not uid:
            return None, None, "CoT-UID fehlt"
        if not event_type:
            return None, None, "CoT-Typ fehlt"
        if not event_how:
            event_how = "m-g"
            root.set("how", event_how)

        point = _find_child_by_local_name(root, "point")
        if point is None:
            return None, None, "CoT-point fehlt"
        lat = _coerce_cot_point_float(point.get("lat"))
        lon = _coerce_cot_point_float(point.get("lon"))
        if lat is None or lon is None:
            return None, None, "CoT-point enthält ungültige lat/lon-Werte"
        if point.get("hae") is None:
            point.set("hae", "0.0")
        if point.get("ce") is None:
            point.set("ce", "9999999.0")
        if point.get("le") is None:
            point.set("le", "9999999.0")

        now = get_tak_timestamp()
        stale_default = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
        ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        if not str(root.get("time") or "").strip():
            root.set("time", now)
        if not str(root.get("start") or "").strip():
            root.set("start", root.get("time") or now)
        if not str(root.get("stale") or "").strip():
            root.set("stale", stale_default)

        detail = _find_child_by_local_name(root, "detail")
        if detail is None:
            detail = SubElement(root, "detail")
        if add_meshtastic_marker and _find_child_by_local_name(detail, "__meshtastic") is None:
            SubElement(detail, "__meshtastic")
        is_live_pli = _is_live_pli_cot_event(event_type, event_how, detail)
        if meshtastic_live_contact or is_live_pli:
            contact = _find_child_by_local_name(detail, "contact")
            link = _find_child_by_local_name(detail, "link")
            callsign = ""
            if contact is not None:
                callsign = str(contact.get("callsign") or "").strip()
            if not callsign and link is not None:
                callsign = str(link.get("uid") or "").strip()
            if not callsign:
                callsign = uid
            self._apply_meshtastic_bridge_detail(detail, uid, callsign)
            archive = _find_child_by_local_name(detail, "archive")
            if archive is not None:
                detail.remove(archive)
        elif _is_persistable_cot_type(event_type) and _find_child_by_local_name(detail, "archive") is None:
            SubElement(detail, "archive")

        normalized_packet = tostring(root, encoding="utf-8")
        metadata = self._extract_cot_event_metadata(normalized_packet)
        if metadata is None:
            return None, None, "CoT-Struktur nach Normalisierung ungültig"
        return normalized_packet, metadata, None

    def _extract_meshtastic_pli_candidate(self, packet_xml):
        try:
            root = fromstring(packet_xml)
        except (ParseError, TypeError, ValueError):
            return None
        if _xml_local_name(root.tag) != "event":
            return None

        event_type = (root.get("type") or "").strip()
        detail = _find_child_by_local_name(root, "detail")
        if not _is_live_pli_cot_event(event_type, root.get("how"), detail):
            return None

        point = _find_child_by_local_name(root, "point")
        if point is None:
            return None
        lat = to_float_or_none(point.get("lat"))
        lon = to_float_or_none(point.get("lon"))
        normalized_coords = normalize_coordinates(lat, lon)
        if normalized_coords is None:
            return None
        lat, lon = normalized_coords

        contact = _find_child_by_local_name(detail, "contact")
        link = _find_child_by_local_name(detail, "link")
        group = _find_child_by_local_name(detail, "__group")
        status = _find_child_by_local_name(detail, "status")
        track = _find_child_by_local_name(detail, "track")

        uid = (root.get("uid") or "").strip()
        callsign = ""
        if contact is not None:
            callsign = str(contact.get("callsign") or "").strip()
        if not callsign:
            callsign = uid or self.gateway_callsign

        device_callsign = ""
        if link is not None:
            device_callsign = str(link.get("uid") or "").strip()
        if not device_callsign:
            device_callsign = uid or callsign or self.gateway_uid

        team_name = ""
        role_name = ""
        if group is not None:
            team_name = str(group.get("name") or "").strip()
            role_name = str(group.get("role") or "").strip()

        battery = 0
        if status is not None:
            battery = _clamp_battery_percentage(status.get("battery"))

        speed = 0
        course = 0
        if track is not None:
            speed_value = to_float_or_none(track.get("speed"))
            if speed_value is not None and not math.isnan(speed_value):
                speed = max(0, int(round(speed_value)))
            course_value = to_float_or_none(track.get("course"))
            if course_value is not None and not math.isnan(course_value):
                course = int(round(course_value)) % 360

        altitude = to_float_or_none(point.get("hae"))
        if altitude is None or math.isnan(altitude):
            altitude = 0.0

        return {
            "uid": uid,
            "callsign": callsign,
            "device_callsign": device_callsign,
            "team": team_name,
            "role": role_name,
            "battery": battery,
            "speed": speed,
            "course": course,
            "altitude": int(round(altitude)),
            "latitude_i": int(round(lat / 1e-7)),
            "longitude_i": int(round(lon / 1e-7)),
        }

    def _build_meshtastic_contact_payload(self, pli_candidate):
        return b"".join(
            (
                _encode_protobuf_string_field(1, pli_candidate.get("callsign") or ""),
                _encode_protobuf_string_field(2, pli_candidate.get("device_callsign") or ""),
            )
        )

    def _build_meshtastic_group_payload(self, pli_candidate):
        role_enum = MESHTASTIC_ROLE_NAME_TO_ENUM.get(
            _normalize_meshtastic_enum_key(pli_candidate.get("role")),
            MESHTASTIC_DEFAULT_ROLE_ENUM,
        )
        team_enum = MESHTASTIC_TEAM_NAME_TO_ENUM.get(
            _normalize_meshtastic_enum_key(pli_candidate.get("team")),
            MESHTASTIC_DEFAULT_TEAM_ENUM,
        )
        return b"".join(
            (
                _encode_protobuf_varint_field(1, role_enum),
                _encode_protobuf_varint_field(2, team_enum),
            )
        )

    def _build_meshtastic_status_payload(self, pli_candidate):
        return _encode_protobuf_varint_field(
            1,
            _clamp_battery_percentage(pli_candidate.get("battery", 0)),
        )

    def _build_meshtastic_pli_payload(self, pli_candidate):
        return b"".join(
            (
                _encode_protobuf_sfixed32_field(1, pli_candidate["latitude_i"]),
                _encode_protobuf_sfixed32_field(2, pli_candidate["longitude_i"]),
                _encode_protobuf_varint_field(3, int(pli_candidate.get("altitude", 0) or 0)),
                _encode_protobuf_varint_field(4, max(0, int(pli_candidate.get("speed", 0) or 0))),
                _encode_protobuf_varint_field(5, max(0, int(pli_candidate.get("course", 0) or 0))),
            )
        )

    def _extract_meshtastic_atak_detail_candidate(self, packet_xml):
        try:
            root = fromstring(packet_xml)
        except (ParseError, TypeError, ValueError):
            return None
        if _xml_local_name(root.tag) != "event":
            return None

        event_type = (root.get("type") or "").strip()
        detail = _find_child_by_local_name(root, "detail")
        if _is_live_pli_cot_event(event_type, root.get("how"), detail):
            return None

        metadata = self._extract_cot_event_metadata(packet_xml)
        if metadata is None:
            return None

        contact = _find_child_by_local_name(detail, "contact")
        link = _find_child_by_local_name(detail, "link")
        group = _find_child_by_local_name(detail, "__group")
        status = _find_child_by_local_name(detail, "status")

        uid = (root.get("uid") or "").strip()
        callsign = ""
        if contact is not None:
            callsign = str(contact.get("callsign") or "").strip()
        if not callsign:
            callsign = uid or self.gateway_callsign

        device_callsign = ""
        if link is not None:
            device_callsign = str(link.get("uid") or "").strip()
        if not device_callsign:
            device_callsign = uid or callsign or self.gateway_uid

        team_name = ""
        role_name = ""
        if group is not None:
            team_name = str(group.get("name") or "").strip()
            role_name = str(group.get("role") or "").strip()

        battery = 0
        if status is not None:
            battery = _clamp_battery_percentage(status.get("battery"))

        full_cot_xml = _ensure_bytes(packet_xml).strip()
        if not full_cot_xml:
            return None

        return {
            "uid": uid,
            "callsign": callsign,
            "device_callsign": device_callsign,
            "team": team_name,
            "role": role_name,
            "battery": battery,
            "cot_class": metadata.get("cot_class") or "generic",
            "detail": full_cot_xml,
        }

    def _prepare_meshtastic_pli_packet(self, packet_xml):
        pli_candidate = self._extract_meshtastic_pli_candidate(packet_xml)
        if pli_candidate is None:
            return None
        payload = b"".join(
            (
                _encode_protobuf_message_field(2, self._build_meshtastic_contact_payload(pli_candidate)),
                _encode_protobuf_message_field(3, self._build_meshtastic_group_payload(pli_candidate)),
                _encode_protobuf_message_field(4, self._build_meshtastic_status_payload(pli_candidate)),
                _encode_protobuf_message_field(5, self._build_meshtastic_pli_payload(pli_candidate)),
            )
        )
        if not payload:
            return None
        return {
            "uid": pli_candidate["uid"],
            "callsign": pli_candidate["callsign"],
            "payload": payload,
        }

    def _prepare_meshtastic_detail_packet(self, packet_xml):
        detail_candidate = self._extract_meshtastic_atak_detail_candidate(packet_xml)
        if detail_candidate is None:
            return None

        cot_class = detail_candidate.get("cot_class") or "generic"
        if cot_class in {"marker", "generic"}:
            # Marker/generic CoT packets prefer the smallest detail=7 variants first so
            # uid/type/how/point/detail semantics stay on the ATAK plugin path longer
            # before the gateway has to fall back to ATAK_FORWARDER fragmentation.
            payload_variants = (
                {"compressed": True, "contact": False, "group": False, "status": False},
                {"compressed": False, "contact": False, "group": False, "status": False},
                {"compressed": True, "contact": True, "group": False, "status": False},
                {"compressed": True, "contact": True, "group": True, "status": True},
            )
        else:
            payload_variants = (
                {"compressed": False, "contact": True, "group": True, "status": True},
                {"compressed": True, "contact": True, "group": True, "status": True},
                {"compressed": True, "contact": True, "group": False, "status": False},
                {"compressed": True, "contact": False, "group": False, "status": False},
            )
        for variant in payload_variants:
            processed_payload = detail_candidate["detail"]
            if variant["compressed"]:
                processed_payload = zlib.compress(processed_payload)
            payload_parts = []
            if variant["compressed"]:
                payload_parts.append(_encode_protobuf_varint_field(1, 1))
            if variant["contact"]:
                payload_parts.append(
                    _encode_protobuf_message_field(2, self._build_meshtastic_contact_payload(detail_candidate))
                )
            if variant["group"]:
                payload_parts.append(
                    _encode_protobuf_message_field(3, self._build_meshtastic_group_payload(detail_candidate))
                )
            if variant["status"]:
                payload_parts.append(
                    _encode_protobuf_message_field(4, self._build_meshtastic_status_payload(detail_candidate))
                )
            payload_parts.append(_encode_protobuf_message_field(7, processed_payload))
            payload = b"".join(payload_parts)
            if payload and len(payload) <= MESHTASTIC_DATA_PAYLOAD_MAX_BYTES:
                return {
                    "uid": detail_candidate["uid"],
                    "callsign": detail_candidate["callsign"],
                    "payload": payload,
                    "is_compressed": variant["compressed"],
                }
        return None

    def _build_cot_dedupe_key(self, packet_xml):
        normalized_packet = _normalize_tak_xml_payload(packet_xml)
        packet_bytes = _ensure_bytes(normalized_packet or packet_xml).strip()
        if not packet_bytes:
            return None
        return hashlib.sha256(packet_bytes).hexdigest()

    def _build_pong_xml(self):
        """Build a minimal CoT t-x-c-t-r ping-ack reply for WinTAK/ATAK keepalive pings."""
        now = datetime.datetime.now(datetime.timezone.utc)
        stale = now + datetime.timedelta(seconds=30)
        def _fmt_ts(dt):
            return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
        uid = self.gateway_uid or "GW-01"
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<event version="2.0" uid="{uid}" type="{TAK_PONG_EVENT_TYPE}" how="m-g"'
            f' time="{_fmt_ts(now)}" start="{_fmt_ts(now)}" stale="{_fmt_ts(stale)}">'
            '<point lat="0.0" lon="0.0" hae="0.0" ce="9999999.0" le="9999999.0"/>'
            '<detail/></event>'
        )

    def _register_local_nodes(self):
        for iface in self._get_interfaces_snapshot():
            try:
                my_info = getattr(iface, "myInfo", None)
                node_num = getattr(my_info, "my_node_num", None)
                if node_num is None:
                    continue
                node_num = int(node_num)
                self.local_node_numbers.add(node_num)
                self.local_node_ids.update(format_meshtastic_node_ids(node_num))
            except Exception:
                self.logger.debug("Lokale Node-ID konnte nicht registriert werden:\n" + traceback.format_exc())

    def _find_node_for_packet(self, interface, from_id):
        if not from_id:
            return None
        other_interfaces = [i for i in self._get_interfaces_snapshot() if i is not interface]
        for iface in ([interface] + other_interfaces):
            if hasattr(iface, 'nodes') and iface.nodes:
                node = iface.nodes.get(from_id)
                if node:
                    return node
        return None

    def _is_text_message_packet(self, packet):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return False
        portnum = decoded.get("portnum")
        if portnum is None:
            return False
        if str(portnum).upper() == "TEXT_MESSAGE_APP":
            return True
        if portnums_pb2 is not None:
            try:
                return int(portnum) == int(portnums_pb2.PortNum.TEXT_MESSAGE_APP)
            except (TypeError, ValueError):
                return False
        return False

    def _is_atak_plugin_packet(self, packet):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return False
        portnum = decoded.get("portnum")
        if portnum is None:
            return False
        portnum_name = str(portnum).upper()
        if portnum_name in {"ATAK_PLUGIN", "ATAK_PLUGIN_V2"}:
            return True
        if portnums_pb2 is not None:
            try:
                portnum_values = {int(portnums_pb2.PortNum.ATAK_PLUGIN)}
                atak_plugin_v2 = getattr(portnums_pb2.PortNum, "ATAK_PLUGIN_V2", None)
                if atak_plugin_v2 is not None:
                    portnum_values.add(int(atak_plugin_v2))
                return int(portnum) in portnum_values
            except (AttributeError, TypeError, ValueError):
                return False
        return False

    def _get_atak_plugin_packet_version(self, packet):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return None
        portnum = decoded.get("portnum")
        if portnum is None:
            return None
        portnum_name = str(portnum).upper()
        if portnum_name == "ATAK_PLUGIN_V2":
            return 2
        if portnum_name == "ATAK_PLUGIN":
            return 1
        try:
            portnum_value = int(portnum)
        except (TypeError, ValueError):
            return None
        if portnum_value == MESHTASTIC_ATAK_PLUGIN_V2_PORTNUM:
            return 2
        if portnum_value == MESHTASTIC_ATAK_PLUGIN_V1_PORTNUM:
            return 1
        return None

    def _resolve_meshtastic_v2_argb(self, team_value, fallback_argb=0):
        try:
            team_value = int(team_value)
        except (TypeError, ValueError):
            team_value = 0
        palette_argb = MESHTASTIC_TEAM_ENUM_TO_ARGB.get(team_value)
        if palette_argb is not None:
            return palette_argb
        try:
            return int(fallback_argb or 0) & 0xFFFFFFFF
        except (TypeError, ValueError):
            return 0

    def _derive_meshtastic_marker_event_type(self, base_type, marker):
        event_type = str(base_type or "").strip()
        marker = marker if isinstance(marker, dict) else {}
        derived_marker_type = _resolve_meshtastic_marker_cot_type(marker, fallback="")
        if derived_marker_type and (
            not event_type
            or event_type == MESHTASTIC_PLI_COT_EVENT_TYPE
            or event_type.endswith("-U-C")
            or event_type in {"a-f-G", "a-h-G", "a-u-G", "a-n-G"}
        ):
            event_type = derived_marker_type
        if not event_type:
            event_type = str(marker.get("parent_type") or "").strip()
        if not event_type:
            event_type = _resolve_meshtastic_marker_cot_type(marker, fallback="a-u-G")
        return event_type

    def _resolve_meshtastic_v2_event_type(self, tak_packet):
        cot_type_id = int(tak_packet.get("cot_type_id") or 0)
        event_type = MESHTASTIC_COT_TYPE_ENUM_TO_NAME.get(cot_type_id)
        fallback_type = str(tak_packet.get("cot_type_str") or "").strip()
        payload_variant = tak_packet.get("payload_variant")
        if payload_variant == "marker":
            marker = tak_packet.get("marker") or {}
            return self._derive_meshtastic_marker_event_type(
                event_type or fallback_type,
                marker,
            )
        if event_type:
            return event_type
        if fallback_type:
            return fallback_type
        if payload_variant == "rab":
            return "u-rb-a"
        if payload_variant == "pli":
            team_name = MESHTASTIC_TEAM_ENUM_TO_NAME.get(
                int(tak_packet.get("team") or MESHTASTIC_DEFAULT_TEAM_ENUM),
                MESHTASTIC_TEAM_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_TEAM_ENUM, "White"),
            )
            return _resolve_meshtastic_team_cot_event_type(team_name, default=MESHTASTIC_PLI_COT_EVENT_TYPE)
        if payload_variant == "chat":
            chat = tak_packet.get("chat") or {}
            receipt_type = int(chat.get("receipt_type") or 0)
            if receipt_type == 1:
                return "b-t-f-d"
            if receipt_type == 2:
                return "b-t-f-r"
            return "b-t-f"
        return ""

    def _resolve_meshtastic_v2_event_uid(self, tak_packet, packet, node=None):
        user = node.get("user", {}) if node else {}
        fallback_sender_uid = normalize_meshtastic_uid(
            packet.get("fromId") or packet.get("from") or "MESH-UNKNOWN"
        )
        raw_uid = str(
            tak_packet.get("uid")
            or tak_packet.get("device_callsign")
            or user.get("id")
            or packet.get("fromId")
            or packet.get("from")
            or "MESH-UNKNOWN"
        ).strip()
        resolved_uid = _strip_tak_sender_prefix(normalize_meshtastic_uid(raw_uid))
        if not _looks_like_valid_tak_uid(resolved_uid):
            return fallback_sender_uid
        return resolved_uid

    def _build_meshtastic_takv2_pli_cot_xml(self, tak_packet, packet, node=None):
        lat_i = tak_packet.get("latitude_i")
        lon_i = tak_packet.get("longitude_i")
        if lat_i is None or lon_i is None:
            return None
        normalized_coords = normalize_coordinates(lat_i * 1e-7, lon_i * 1e-7)
        if normalized_coords is None:
            return None
        lat, lon = normalized_coords

        user = node.get("user", {}) if node else {}
        raw_v2_uid = str(
            tak_packet.get("uid")
            or tak_packet.get("device_callsign")
            or user.get("id")
            or packet.get("fromId")
            or packet.get("from")
            or "MESH-UNKNOWN"
        ).strip()
        fallback_sender_uid = normalize_meshtastic_uid(
            packet.get("fromId") or packet.get("from") or "MESH-UNKNOWN"
        )
        normalized_sender_uid = _strip_tak_sender_prefix(normalize_meshtastic_uid(raw_v2_uid))
        sender_uid = self._resolve_meshtastic_v2_event_uid(tak_packet, packet, node=node)
        callsign = str(
            tak_packet.get("callsign")
            or user.get("longName")
            or user.get("shortName")
            or sender_uid
        ).strip() or sender_uid
        team_enum = int(tak_packet.get("team") or MESHTASTIC_DEFAULT_TEAM_ENUM)
        team_name = MESHTASTIC_TEAM_ENUM_TO_NAME.get(
            team_enum,
            MESHTASTIC_TEAM_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_TEAM_ENUM, "White"),
        )
        role_name = MESHTASTIC_ROLE_ENUM_TO_NAME.get(
            int(tak_packet.get("role") or MESHTASTIC_DEFAULT_ROLE_ENUM),
            MESHTASTIC_ROLE_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_ROLE_ENUM, "Team Member"),
        )
        pli_event_type = self._resolve_meshtastic_v2_event_type(tak_packet) or MESHTASTIC_PLI_COT_EVENT_TYPE
        event_type = pli_event_type
        event_how = MESHTASTIC_COT_HOW_ENUM_TO_NAME.get(int(tak_packet.get("how") or 0), "m-g")
        cot_class = "pli"
        cached_identity = self._get_recent_cot_identity(
            sender_uid,
            callsign=callsign,
            droid_uid=tak_packet.get("device_callsign"),
        )
        if (
            cached_identity
            and cached_identity.get("cot_class") in {"marker", "generic"}
            and str(cached_identity.get("type") or "").strip()
        ):
            event_type = str(cached_identity["type"]).strip()
            event_how = str(cached_identity.get("how") or "").strip() or event_how
            cot_class = cached_identity.get("cot_class") or _classify_cot_event_type(event_type)
            self.logger.debug(
                "ATAK_PLUGIN_V2-PLI nutzt zwischengespeicherten TAK-Typ statt PLI-Default: "
                f"uid={sender_uid} cached_type={event_type} cached_how={event_how} "
                f"cached_class={cot_class} mesh_default_type={pli_event_type}"
            )
        elif normalized_sender_uid != fallback_sender_uid:
            event_type = "a-u-G"
            cot_class = "marker"
            self.logger.debug(
                "ATAK_PLUGIN_V2-PLI enthält TAK-fähige UID ohne zwischengespeicherten Marker-Typ; "
                f"nutze generischen Marker-Fallback statt PLI-Default: "
                f"uid={sender_uid} normalized_uid={normalized_sender_uid} "
                f"inferred_type={event_type} mesh_default_type={pli_event_type}"
            )

        timestamp = get_tak_timestamp()
        stale_seconds = max(int(tak_packet.get("stale_seconds") or 0), 45)
        stale = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=stale_seconds)
        ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        event = Element("event", {
            "version": "2.0",
            "uid": sender_uid,
            "type": event_type,
            "how": event_how,
            "start": timestamp,
            "time": timestamp,
            "stale": stale,
        })
        SubElement(event, "point", {
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "hae": str(int(tak_packet.get("altitude") or 0)),
            "ce": "9999999.0",
            "le": "9999999.0",
        })
        detail = SubElement(event, "detail")
        SubElement(detail, "contact", {"callsign": callsign})
        SubElement(detail, "link", {
            "uid": sender_uid,
            "type": event_type,
            "relation": "p-p",
        })
        SubElement(detail, "__group", {
            "name": team_name,
            "role": role_name,
        })
        battery = _clamp_battery_percentage(tak_packet.get("battery", 0))
        if battery > 0:
            SubElement(detail, "status", {"battery": str(battery)})
        speed = max(0, int(tak_packet.get("speed") or 0))
        course = max(0, int(tak_packet.get("course") or 0)) % 36000
        if speed > 0 or course > 0:
            SubElement(detail, "track", {
                "speed": str(speed / 100.0),
                "course": str(course / 100.0),
            })
        SubElement(detail, "precisionlocation", {"geopointsrc": "GPS", "altsrc": "GPS"})
        if tak_packet.get("device_callsign"):
            SubElement(detail, "uid", {"Droid": str(tak_packet["device_callsign"])})
        if tak_packet.get("remarks"):
            SubElement(detail, "remarks").text = str(tak_packet["remarks"])
        self.logger.debug(
            "ATAK_PLUGIN_V2 aus dem Mesh als CoT rekonstruiert: "
            f"uid={sender_uid} reconstructed_type={event_type} how={event_how} "
            f"classification={cot_class} mesh_default_type={pli_event_type} "
            f"type_changed={'ja' if event_type != pli_event_type else 'nein'} "
            f"lat={lat:.6f} lon={lon:.6f}"
        )
        return tostring(event, encoding="utf-8")

    def _build_meshtastic_takv2_marker_cot_xml(self, tak_packet, packet, node=None):
        lat_i = tak_packet.get("latitude_i")
        lon_i = tak_packet.get("longitude_i")
        if lat_i is None or lon_i is None:
            return None
        normalized_coords = normalize_coordinates(lat_i * 1e-7, lon_i * 1e-7)
        if normalized_coords is None:
            return None
        lat, lon = normalized_coords
        event_type = self._resolve_meshtastic_v2_event_type(tak_packet)
        if not event_type:
            return None

        sender_uid = self._resolve_meshtastic_v2_event_uid(tak_packet, packet, node=node)
        event_how = MESHTASTIC_COT_HOW_ENUM_TO_NAME.get(int(tak_packet.get("how") or 0), "m-g")
        marker = tak_packet.get("marker") or {}
        user = node.get("user", {}) if node else {}
        callsign = str(
            tak_packet.get("callsign")
            or marker.get("parent_callsign")
            or user.get("longName")
            or user.get("shortName")
            or sender_uid
        ).strip() or sender_uid
        team_name = MESHTASTIC_TEAM_ENUM_TO_NAME.get(
            int(tak_packet.get("team") or MESHTASTIC_DEFAULT_TEAM_ENUM),
            MESHTASTIC_TEAM_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_TEAM_ENUM, "White"),
        )
        role_name = MESHTASTIC_ROLE_ENUM_TO_NAME.get(
            int(tak_packet.get("role") or MESHTASTIC_DEFAULT_ROLE_ENUM),
            MESHTASTIC_ROLE_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_ROLE_ENUM, "Team Member"),
        )
        color_team_name = MESHTASTIC_TEAM_ENUM_TO_NAME.get(int(marker.get("color") or 0), "")
        marker_kind = int(marker.get("kind") or 0)
        marker_kind_label = str(marker.get("kind_label") or f"Kind_{marker_kind}")
        iconset = str(marker.get("iconset") or "").strip()

        timestamp = get_tak_timestamp()
        stale_seconds = max(int(tak_packet.get("stale_seconds") or 0), 45)
        stale = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=stale_seconds)
        ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        event = Element("event", {
            "version": "2.0",
            "uid": sender_uid,
            "type": event_type,
            "how": event_how,
            "start": timestamp,
            "time": timestamp,
            "stale": stale,
        })
        SubElement(event, "point", {
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "hae": str(int(tak_packet.get("altitude") or 0)),
            "ce": "9999999.0",
            "le": "9999999.0",
        })
        detail = SubElement(event, "detail")
        SubElement(detail, "contact", {"callsign": callsign})
        parent_uid = str(marker.get("parent_uid") or "").strip()
        parent_type = str(marker.get("parent_type") or "").strip() or event_type
        if parent_uid:
            link_attrs = {
                "uid": parent_uid,
                "relation": "p-p",
                "type": parent_type,
            }
            if marker.get("parent_callsign"):
                link_attrs["parent_callsign"] = str(marker["parent_callsign"])
            SubElement(detail, "link", link_attrs)
        SubElement(detail, "__group", {
            "name": color_team_name or team_name,
            "role": role_name,
        })
        color_argb = self._resolve_meshtastic_v2_argb(
            marker.get("color"),
            marker.get("color_argb"),
        )
        if color_argb:
            SubElement(detail, "color", {"argb": str(_decode_protobuf_sfixed32(color_argb.to_bytes(4, "little")))})
        if iconset:
            SubElement(detail, "usericon", {"iconsetpath": iconset})
        status_attrs = {}
        if marker.get("has_readiness"):
            status_attrs["readiness"] = "true" if marker.get("readiness") else "false"
        battery = _clamp_battery_percentage(tak_packet.get("battery", 0))
        if battery > 0:
            status_attrs["battery"] = str(battery)
        if status_attrs:
            SubElement(detail, "status", status_attrs)
        marker_meta_attrs = {
            "kind": marker_kind_label,
            "kindId": str(marker_kind),
            "derivedType": event_type,
        }
        if iconset:
            marker_meta_attrs["iconset"] = iconset
        if parent_uid:
            marker_meta_attrs["parentUid"] = parent_uid
        if parent_type:
            marker_meta_attrs["parentType"] = parent_type
        if marker.get("parent_callsign"):
            marker_meta_attrs["parentCallsign"] = str(marker.get("parent_callsign"))
        SubElement(detail, "meshtastic_marker", marker_meta_attrs)
        SubElement(detail, "archive")
        if tak_packet.get("remarks"):
            SubElement(detail, "remarks").text = str(tak_packet["remarks"])
        self.logger.debug(
            "ATAK_PLUGIN_V2 Marker aus dem Mesh dekodiert: "
            f"uid={sender_uid} marker_kind={marker_kind_label} marker_kind_id={marker_kind} "
            f"iconset={iconset or '-'} color_team={color_team_name or '-'} color_argb={color_argb} "
            f"parent_uid={parent_uid or '-'} parent_type={parent_type or '-'} "
            f"parent_callsign={marker.get('parent_callsign') or '-'} "
            f"cot_type_input={MESHTASTIC_COT_TYPE_ENUM_TO_NAME.get(int(tak_packet.get('cot_type_id') or 0)) or str(tak_packet.get('cot_type_str') or '-').strip() or '-'} "
            f"derived_type={event_type}"
        )
        return tostring(event, encoding="utf-8")

    def _append_meshtastic_takv2_detail_fragment(self, detail, raw_detail):
        raw_detail_text = _strip_invalid_xml_chars(
            _ensure_bytes(raw_detail).decode("utf-8", errors="ignore")
        ).strip()
        if not raw_detail_text:
            return False
        try:
            raw_detail_root = fromstring(raw_detail_text)
        except (ParseError, TypeError, ValueError):
            try:
                raw_detail_root = fromstring(f"<detail>{raw_detail_text}</detail>")
            except (ParseError, TypeError, ValueError):
                return False
        if _xml_local_name(raw_detail_root.tag) == "detail":
            for attr_name, attr_value in raw_detail_root.attrib.items():
                if attr_name not in detail.attrib:
                    detail.set(attr_name, attr_value)
            for child in list(raw_detail_root):
                detail.append(child)
        else:
            detail.append(raw_detail_root)
        return True

    def _build_meshtastic_takv2_raw_detail_cot_xml(self, tak_packet, packet, node=None):
        lat_i = tak_packet.get("latitude_i")
        lon_i = tak_packet.get("longitude_i")
        if lat_i is None or lon_i is None:
            return None
        normalized_coords = normalize_coordinates(lat_i * 1e-7, lon_i * 1e-7)
        if normalized_coords is None:
            return None
        lat, lon = normalized_coords

        event_type = self._resolve_meshtastic_v2_event_type(tak_packet)
        if not event_type:
            return None

        sender_uid = self._resolve_meshtastic_v2_event_uid(tak_packet, packet, node=node)
        event_how = MESHTASTIC_COT_HOW_ENUM_TO_NAME.get(int(tak_packet.get("how") or 0), "m-g")
        user = node.get("user", {}) if node else {}
        callsign = str(
            tak_packet.get("callsign")
            or user.get("longName")
            or user.get("shortName")
            or sender_uid
        ).strip() or sender_uid
        team_name = MESHTASTIC_TEAM_ENUM_TO_NAME.get(
            int(tak_packet.get("team") or MESHTASTIC_DEFAULT_TEAM_ENUM),
            MESHTASTIC_TEAM_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_TEAM_ENUM, "White"),
        )
        role_name = MESHTASTIC_ROLE_ENUM_TO_NAME.get(
            int(tak_packet.get("role") or MESHTASTIC_DEFAULT_ROLE_ENUM),
            MESHTASTIC_ROLE_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_ROLE_ENUM, "Team Member"),
        )

        timestamp = get_tak_timestamp()
        stale_seconds = max(int(tak_packet.get("stale_seconds") or 0), 45)
        stale = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=stale_seconds)
        ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        event = Element("event", {
            "version": "2.0",
            "uid": sender_uid,
            "type": event_type,
            "how": event_how,
            "start": timestamp,
            "time": timestamp,
            "stale": stale,
        })
        SubElement(event, "point", {
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "hae": str(int(tak_packet.get("altitude") or 0)),
            "ce": "9999999.0",
            "le": "9999999.0",
        })
        detail = SubElement(event, "detail")
        raw_detail = bytes(tak_packet.get("raw_detail") or b"").strip()
        if raw_detail and not self._append_meshtastic_takv2_detail_fragment(detail, raw_detail):
            self.logger.debug(
                "ATAK_PLUGIN_V2 raw_detail konnte nicht als XML-Detail geparst werden: "
                f"{_build_safe_payload_snippet(raw_detail)}"
            )

        contact = _find_child_by_local_name(detail, "contact")
        if contact is None:
            contact = SubElement(detail, "contact")
        if callsign and not str(contact.get("callsign") or "").strip():
            contact.set("callsign", callsign)

        group = _find_child_by_local_name(detail, "__group")
        if group is None:
            group = SubElement(detail, "__group")
        if not str(group.get("name") or "").strip():
            group.set("name", team_name)
        if not str(group.get("role") or "").strip():
            group.set("role", role_name)

        if tak_packet.get("device_callsign"):
            uid_detail = _find_child_by_local_name(detail, "uid")
            if uid_detail is None:
                uid_detail = SubElement(detail, "uid")
            if not str(uid_detail.get("Droid") or "").strip():
                uid_detail.set("Droid", str(tak_packet.get("device_callsign")))

        battery = _clamp_battery_percentage(tak_packet.get("battery", 0))
        if battery > 0:
            status = _find_child_by_local_name(detail, "status")
            if status is None:
                status = SubElement(detail, "status")
            if not str(status.get("battery") or "").strip():
                status.set("battery", str(battery))

        if _is_persistable_cot_type(event_type) and _find_child_by_local_name(detail, "archive") is None:
            SubElement(detail, "archive")
        if tak_packet.get("remarks") and _find_child_by_local_name(detail, "remarks") is None:
            SubElement(detail, "remarks").text = str(tak_packet["remarks"])
        self.logger.debug(
            "ATAK_PLUGIN_V2 raw_detail aus dem Mesh dekodiert: "
            f"uid={sender_uid} type={event_type} how={event_how} "
            f"callsign={callsign} raw_detail_bytes={len(raw_detail)}"
        )
        return tostring(event, encoding="utf-8")

    def _build_meshtastic_takv2_rab_cot_xml(self, tak_packet, packet, node=None):
        lat_i = tak_packet.get("latitude_i")
        lon_i = tak_packet.get("longitude_i")
        if lat_i is None or lon_i is None:
            return None
        normalized_coords = normalize_coordinates(lat_i * 1e-7, lon_i * 1e-7)
        if normalized_coords is None:
            return None
        lat, lon = normalized_coords
        sender_uid = self._resolve_meshtastic_v2_event_uid(tak_packet, packet, node=node)
        callsign = str(tak_packet.get("callsign") or sender_uid).strip() or sender_uid
        rab = tak_packet.get("rab") or {}

        timestamp = get_tak_timestamp()
        stale_seconds = max(int(tak_packet.get("stale_seconds") or 0), 45)
        stale = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=stale_seconds)
        ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        event = Element("event", {
            "version": "2.0",
            "uid": sender_uid,
            "type": self._resolve_meshtastic_v2_event_type(tak_packet) or "u-rb-a",
            "how": MESHTASTIC_COT_HOW_ENUM_TO_NAME.get(int(tak_packet.get("how") or 0), "m-g"),
            "start": timestamp,
            "time": timestamp,
            "stale": stale,
        })
        SubElement(event, "point", {
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "hae": str(int(tak_packet.get("altitude") or 0)),
            "ce": "9999999.0",
            "le": "9999999.0",
        })
        detail = SubElement(event, "detail")
        SubElement(detail, "contact", {"callsign": callsign})
        anchor = rab.get("anchor") or {}
        anchor_lat_i = int(lat_i) + int(anchor.get("lat_delta_i") or 0)
        anchor_lon_i = int(lon_i) + int(anchor.get("lon_delta_i") or 0)
        if anchor_lat_i or anchor_lon_i or rab.get("anchor_uid"):
            link_attrs = {
                "relation": "p-p",
                "type": "b-m-p-w",
                "point": f"{anchor_lat_i / 1e7},{anchor_lon_i / 1e7}",
            }
            if rab.get("anchor_uid"):
                link_attrs["uid"] = str(rab["anchor_uid"])
            SubElement(detail, "link", link_attrs)
        if int(rab.get("range_cm") or 0) > 0:
            SubElement(detail, "range", {"value": str(int(rab["range_cm"]) / 100.0)})
        if int(rab.get("bearing_cdeg") or 0) > 0:
            SubElement(detail, "bearing", {"value": str(int(rab["bearing_cdeg"]) / 100.0)})
        stroke_argb = self._resolve_meshtastic_v2_argb(
            rab.get("stroke_color"),
            rab.get("stroke_argb"),
        )
        if stroke_argb:
            SubElement(
                detail,
                "strokeColor",
                {"value": str(_decode_protobuf_sfixed32(stroke_argb.to_bytes(4, "little")))},
            )
        if int(rab.get("stroke_weight_x10") or 0) > 0:
            SubElement(detail, "strokeWeight", {"value": str(int(rab["stroke_weight_x10"]) / 10.0)})
        SubElement(detail, "archive")
        if tak_packet.get("remarks"):
            SubElement(detail, "remarks").text = str(tak_packet["remarks"])
        return tostring(event, encoding="utf-8")

    def _build_meshtastic_takv2_cot_xml(self, packet, node=None):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return None
        payload = decoded.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return None
        try:
            tak_packet = _decode_meshtastic_takv2_wire_payload(payload)
        except Exception:
            self.logger.debug("ATAK_PLUGIN_V2-Payload konnte nicht dekodiert werden:\n" + traceback.format_exc())
            return None
        payload_variant = tak_packet.get("payload_variant")
        if payload_variant == "marker":
            return self._build_meshtastic_takv2_marker_cot_xml(tak_packet, packet, node=node)
        if payload_variant == "rab":
            return self._build_meshtastic_takv2_rab_cot_xml(tak_packet, packet, node=node)
        if payload_variant == "pli":
            return self._build_meshtastic_takv2_pli_cot_xml(tak_packet, packet, node=node)
        if payload_variant == "raw_detail":
            return self._build_meshtastic_takv2_raw_detail_cot_xml(tak_packet, packet, node=node)
        return None

    def _is_atak_forwarder_packet(self, packet):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return False
        portnum = decoded.get("portnum")
        if portnum is None:
            return False
        if str(portnum).upper() == "ATAK_FORWARDER":
            return True
        try:
            return int(portnum) == MESHTASTIC_ATAK_FORWARDER_PORTNUM
        except (TypeError, ValueError):
            return False

    def _extract_meshtastic_chat_payload(self, packet, node=None):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return None
        if self._is_text_message_packet(packet):
            text = decoded.get("text")
            if not text:
                payload = decoded.get("payload")
                if isinstance(payload, (bytes, bytearray)):
                    text = payload.decode("utf-8", errors="ignore")
            if text is None:
                return None
            text = str(text).strip()
            if not text:
                return None
            return {
                "message": text,
                "callsign": None,
                "sender_uid": None,
                "chatroom": DEFAULT_CHATROOM_NAME,
                "recipient_uid": DEFAULT_CHATROOM_NAME,
                "recipient_callsign": DEFAULT_CHATROOM_NAME,
            }

        if not self._is_atak_plugin_packet(packet):
            return None

        payload = decoded.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return None
        plugin_version = self._get_atak_plugin_packet_version(packet)
        if plugin_version == 2:
            try:
                tak_packet = _decode_meshtastic_takv2_wire_payload(payload)
            except Exception:
                self.logger.debug("ATAK_PLUGIN_V2-Chat konnte nicht dekodiert werden:\n" + traceback.format_exc())
                return None
            if tak_packet.get("payload_variant") != "chat":
                return None
            chat = tak_packet.get("chat") or {}
            message = str(chat.get("message") or "").strip()
            if not message or int(chat.get("receipt_type") or 0) != 0:
                return None
            user = node.get('user', {}) if node else {}
            sender_uid = self._resolve_meshtastic_v2_event_uid(tak_packet, packet, node=node)
            callsign = str(
                tak_packet.get("callsign")
                or user.get("longName")
                or user.get("shortName")
                or sender_uid
            ).strip() or sender_uid
            chatroom = (
                chat.get("to_callsign")
                or chat.get("to")
                or DEFAULT_CHATROOM_NAME
            )
            return {
                "message": message,
                "callsign": callsign,
                "sender_uid": sender_uid,
                "chatroom": chatroom,
                "recipient_uid": chat.get("to") or chatroom,
                "recipient_callsign": chat.get("to_callsign") or chatroom,
            }
        try:
            atak_payload = _parse_meshtastic_atak_payload(payload)
        except Exception:
            self.logger.debug("ATAK-Plugin-Payload konnte nicht dekodiert werden:\n" + traceback.format_exc())
            return None

        chat = atak_payload.get("chat") or {}
        message = str(chat.get("message") or "").strip()
        if not message:
            return None
        if int(chat.get("receipt_type") or 0) != 0:
            return None

        user = node.get('user', {}) if node else {}
        from_identifier = packet.get('fromId') or packet.get('from')
        raw_uid = user.get('id')
        if not raw_uid:
            raw_uid = str(from_identifier) if from_identifier else "MESH-UNKNOWN"
        sender_uid = normalize_meshtastic_uid(raw_uid)
        contact = atak_payload.get("contact") or {}
        callsign = (
            contact.get("callsign")
            or user.get("longName")
            or user.get("shortName")
            or sender_uid
        )
        chatroom = (
            chat.get("to_callsign")
            or chat.get("to")
            or DEFAULT_CHATROOM_NAME
        )
        return {
            "message": message,
            "callsign": callsign,
            "sender_uid": sender_uid,
            "chatroom": chatroom,
            "recipient_uid": chat.get("to") or chatroom,
            "recipient_callsign": chat.get("to_callsign") or chatroom,
        }

    def _build_meshtastic_pli_cot_xml(self, packet, node=None):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return None
        if not self._is_atak_plugin_packet(packet):
            return None
        if self._get_atak_plugin_packet_version(packet) == 2:
            return self._build_meshtastic_takv2_cot_xml(packet, node=node)
        payload = decoded.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return None
        try:
            atak_payload = _parse_meshtastic_atak_payload(payload)
        except Exception:
            self.logger.debug("ATAK-Plugin-PLI konnte nicht dekodiert werden:\n" + traceback.format_exc())
            return None

        pli = atak_payload.get("pli") or {}
        lat_i = pli.get("latitude_i")
        lon_i = pli.get("longitude_i")
        if lat_i is None or lon_i is None:
            return None
        normalized_coords = normalize_coordinates(lat_i * 1e-7, lon_i * 1e-7)
        if normalized_coords is None:
            return None
        lat, lon = normalized_coords

        user = node.get("user", {}) if node else {}
        contact = atak_payload.get("contact") or {}
        group = atak_payload.get("group") or {}
        status = atak_payload.get("status") or {}

        raw_sender_uid = str(
            contact.get("device_callsign")
            or user.get("id")
            or packet.get("fromId")
            or packet.get("from")
            or "MESH-UNKNOWN"
        ).strip()
        fallback_sender_uid = normalize_meshtastic_uid(
            packet.get("fromId") or packet.get("from") or "MESH-UNKNOWN"
        )
        normalized_sender_uid = _strip_tak_sender_prefix(normalize_meshtastic_uid(raw_sender_uid))
        sender_uid = normalized_sender_uid
        if not _looks_like_valid_tak_uid(sender_uid):
            self.logger.debug(
                "ATAK_PLUGIN-PLI UID wirkt ungültig/binär, nutze Fallback auf Mesh-Absender: "
                f"raw={_build_safe_payload_snippet(raw_sender_uid)} fallback={fallback_sender_uid}"
            )
            sender_uid = fallback_sender_uid
        callsign = str(
            contact.get("callsign")
            or user.get("longName")
            or user.get("shortName")
            or sender_uid
        ).strip() or sender_uid

        team_enum = int(group.get("team") or MESHTASTIC_DEFAULT_TEAM_ENUM)
        team_name = MESHTASTIC_TEAM_ENUM_TO_NAME.get(
            team_enum,
            MESHTASTIC_TEAM_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_TEAM_ENUM, "White"),
        )
        role_name = MESHTASTIC_ROLE_ENUM_TO_NAME.get(
            int(group.get("role") or MESHTASTIC_DEFAULT_ROLE_ENUM),
            MESHTASTIC_ROLE_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_ROLE_ENUM, "Team Member"),
        )
        pli_event_type = _resolve_meshtastic_team_cot_event_type(
            team_name,
            default=MESHTASTIC_PLI_COT_EVENT_TYPE,
        )
        event_type = pli_event_type
        event_how = "m-g"
        cot_class = "pli"
        cached_identity = self._get_recent_cot_identity(
            sender_uid,
            callsign=callsign,
            droid_uid=raw_sender_uid,
        )
        if (
            cached_identity
            and cached_identity.get("cot_class") in {"marker", "generic"}
            and str(cached_identity.get("type") or "").strip()
        ):
            event_type = str(cached_identity["type"]).strip()
            event_how = str(cached_identity.get("how") or "").strip() or "m-g"
            cot_class = cached_identity.get("cot_class") or _classify_cot_event_type(event_type)
            self.logger.debug(
                "ATAK_PLUGIN-PLI nutzt zwischengespeicherten TAK-Typ statt PLI-Default: "
                f"uid={sender_uid} cached_type={event_type} cached_how={event_how} "
                f"cached_class={cot_class} mesh_default_type={pli_event_type}"
            )
        elif normalized_sender_uid != fallback_sender_uid:
            event_type = "a-u-G"
            cot_class = "marker"
            self.logger.debug(
                "ATAK_PLUGIN-PLI enthält TAK-fähige UID ohne zwischengespeicherten Marker-Typ; "
                f"nutze generischen Marker-Fallback statt PLI-Default: "
                f"uid={sender_uid} normalized_uid={normalized_sender_uid} "
                f"inferred_type={event_type} mesh_default_type={pli_event_type}"
            )
        battery = _clamp_battery_percentage(status.get("battery", 0))
        altitude = max(0, int(pli.get("altitude") or 0))
        speed = max(0, int(pli.get("speed") or 0))
        course = max(0, int(pli.get("course") or 0)) % 360

        timestamp = get_tak_timestamp()
        stale = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
        ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        event = Element("event", {
            "version": "2.0",
            "uid": sender_uid,
            "type": event_type,
            "how": event_how,
            "start": timestamp,
            "time": timestamp,
            "stale": stale,
        })
        SubElement(event, "point", {
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "hae": str(altitude),
            "ce": "9999999.0",
            "le": "9999999.0",
        })
        detail = SubElement(event, "detail")
        SubElement(detail, "contact", {"callsign": callsign})
        SubElement(detail, "link", {
            "uid": sender_uid,
            "relation": "p-p",
            "type": event_type,
        })
        SubElement(detail, "__group", {"name": team_name, "role": role_name})
        SubElement(detail, "status", {"battery": str(battery)})
        SubElement(detail, "track", {"speed": str(speed), "course": str(course)})
        SubElement(detail, "precisionlocation", {"geopointsrc": "GPS"})
        self.logger.debug(
            "ATAK_PLUGIN aus dem Mesh als CoT rekonstruiert: "
            f"uid={sender_uid} reconstructed_type={event_type} how={event_how} "
            f"classification={cot_class} mesh_default_type={pli_event_type} "
            f"type_changed={'ja' if event_type != pli_event_type else 'nein'} "
            f"lat={lat:.6f} lon={lon:.6f}"
        )
        return tostring(event, encoding="utf-8")

    def _resolve_chat_position(self, uid, node=None):
        alt = 0.0
        if node:
            pos = node.get("position", {}) or {}
            lat_i, lon_i = pos.get("latitude_i"), pos.get("longitude_i")
            lat_f, lon_f = pos.get("latitude"), pos.get("longitude")
            alt = pos.get("altitude", 0) or 0
            if lat_i is not None and lon_i is not None:
                normalized = normalize_coordinates(lat_i * 1e-7, lon_i * 1e-7)
                if normalized:
                    return normalized[0], normalized[1], alt
            if lat_f is not None and lon_f is not None:
                normalized = normalize_coordinates(lat_f, lon_f)
                if normalized:
                    return normalized[0], normalized[1], alt
        last_known = self.last_known_positions.get(uid)
        if last_known:
            return last_known
        db_row = self.node_db.get_node(uid)
        if db_row:
            return db_row[0], db_row[1], db_row[2] if db_row[2] is not None else 0
        if self.park_coords is not None:
            return self.park_coords[0], self.park_coords[1], 0
        return 0.0, 0.0, 0.0

    def _build_remote_cot_url(self):
        protocol = str(self.server_protocol or "TCP").strip().lower()
        host = str(self.server_ip or "").strip()
        if not protocol or not host:
            return None
        raw_host = host[1:-1] if host.startswith("[") and host.endswith("]") else host
        try:
            parsed_host = ipaddress.ip_address(raw_host)
            if isinstance(parsed_host, ipaddress.IPv6Address):
                host = f"[{raw_host}]"
        except ValueError:
            host = raw_host
        return f"{protocol}://{host}:{self.server_port}"

    def _should_use_pytak_remote(self):
        return pytak is not None and self.server_protocol in {"TCP", "UDP"} and self.remote_cot_url is not None

    def _reset_remote_pytak_state(self):
        with self.server_lock:
            self.remote_pytak_loop = None
            self.remote_pytak_queue = None
            self.remote_pytak_ready.clear()

    async def _wait_for_remote_pytak_shutdown(self):
        while not self.shutdown_flag.is_set():
            await asyncio.sleep(0.25)

    async def _run_remote_pytak_client(self):
        if not self.remote_cot_url:
            raise ValueError("Remote-TAK-URL konnte nicht aus Host/Port/Protokoll erstellt werden.")

        config = {
            "COT_URL": self.remote_cot_url,
            "PYTAK_NO_HELLO": "1",
        }
        if self.logger.getEffectiveLevel() <= logging.DEBUG:
            config["DEBUG"] = "1"

        clitool = pytak.CLITool(config)
        await clitool.setup()
        with self.server_lock:
            self.remote_pytak_loop = asyncio.get_running_loop()
            self.remote_pytak_queue = clitool.tx_queue
            self.remote_pytak_ready.set()
        self.logger.info(f"PyTAK-Remote aktiv: {self.remote_cot_url}")

        run_task = asyncio.create_task(clitool.run())
        stop_task = asyncio.create_task(self._wait_for_remote_pytak_shutdown())
        try:
            done, pending = await asyncio.wait(
                {run_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                error = task.exception()
                if error is not None and task is run_task:
                    raise error
        finally:
            run_task.cancel()
            stop_task.cancel()
            await asyncio.gather(run_task, stop_task, return_exceptions=True)
            self._reset_remote_pytak_state()

    def _send_remote_packet_via_pytak(self, packet_xml, label):
        with self.server_lock:
            loop = self.remote_pytak_loop
            queue = self.remote_pytak_queue
            is_ready = self.remote_pytak_ready.is_set()
        if not is_ready or loop is None or queue is None:
            self.logger.debug("PyTAK-Remote ist noch nicht bereit, Remote-CoT wird derzeit übersprungen.")
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(queue.put(packet_xml), loop)
            future.result(timeout=PYTAK_REMOTE_QUEUE_TIMEOUT_SECONDS)
            self.logger.info(f"Remote-PyTAK in Queue gestellt: {self.remote_cot_url} ({label})")
            return True
        except Exception as exc:
            self.logger.warning(f"Fehler beim Einreihen an Remote-PyTAK ({type(exc).__name__}): {exc}")
            self._reset_remote_pytak_state()
            return False

    def _register_local_tak_tcp_peer(self, conn, endpoint):
        if conn is None:
            return None
        peer = {
            "conn": conn,
            "endpoint": str(endpoint or "unknown"),
            "lock": threading.Lock(),
        }
        with self.local_tak_tcp_peer_lock:
            self.local_tak_tcp_peers.append(peer)
        return peer

    def _unregister_local_tak_tcp_peer(self, peer):
        if peer is None:
            return
        with self.local_tak_tcp_peer_lock:
            self.local_tak_tcp_peers = [entry for entry in self.local_tak_tcp_peers if entry is not peer]

    def _send_local_tak_tcp_payload(self, peer, payload, *, log_errors=True, append_newline=True):
        if peer is None:
            return False
        payload_bytes = _ensure_bytes(payload).strip()
        if not payload_bytes:
            return False
        try:
            with peer["lock"]:
                peer["conn"].sendall(payload_bytes + (b"\n" if append_newline else b""))
            return True
        except OSError as exc:
            endpoint = peer.get("endpoint") or "unknown"
            if log_errors:
                self.logger.debug(f"Lokales TAK-TCP-Senden fehlgeschlagen ({endpoint}): {exc}")
            self._unregister_local_tak_tcp_peer(peer)
            try:
                peer["conn"].close()
            except Exception:
                pass
            return False

    def _send_packet_to_local_tak_tcp(self, packet_xml, label, *, skip_peer=None):
        packet_bytes = _ensure_bytes(packet_xml).strip()
        if not packet_bytes:
            return 0
        with self.local_tak_tcp_peer_lock:
            peers = list(self.local_tak_tcp_peers)
        delivered = 0
        for peer in peers:
            if peer is skip_peer:
                continue
            if self._send_local_tak_tcp_payload(peer, packet_bytes):
                delivered += 1
        if delivered:
            self.logger.debug(f"Lokales TCP gesendet an {delivered} WinTAK-Verbindung(en) ({label})")
        return delivered

    def _send_packet_to_tak_multicast(self, packet_xml, label):
        packet_bytes = _ensure_bytes(packet_xml).strip()
        if not packet_bytes:
            return 0
        delivered = 0
        for multicast_group, multicast_port in self.tak_multicast_groups:
            try:
                self.sock_udp.sendto(packet_bytes, (multicast_group, multicast_port))
                delivered += 1
                self.logger.debug(
                    f"Lokales UDP-Multicast gesendet an {multicast_group}:{multicast_port} ({label})"
                )
            except Exception as exc:
                self.logger.warning(
                    f"Fehler beim Senden an lokales TAK-Multicast {multicast_group}:{multicast_port}: {exc}"
                )
        return delivered

    def _send_packet_to_local_tak_tcp_push(self, packet_xml, label):
        """Push CoT to WinTAK via the active TCP receiver connection (127.0.0.1:8087).

        This reuses the bidirectional TAK TCP connection established by
        ``receive_tak_chat_tcp`` to deliver CoT events to WinTAK.  The push is
        reliable even when the UDP listener on port 4242 is blocked or unavailable
        (e.g. Windows WinError 10013).
        """
        with self._local_tak_tcp_push_lock:
            tcp_push_conn = self._local_tak_tcp_push_conn
            tcp_push_peer = self._local_tak_tcp_push_peer
        if tcp_push_conn is not None and tcp_push_peer is not None:
            if self._send_local_tak_tcp_payload(tcp_push_peer, packet_xml, log_errors=False):
                self.logger.debug(
                    f"[LastHop-TCP] Lokal an WinTAK gesendet via TCP: "
                    f"{self.tcp_chat_receiver_host}:{self.tcp_chat_receiver_port} "
                    f"label={label} bytes={len(packet_xml)}"
                )
                return 1
            self.logger.debug(
                "[LastHop-TCP] Lokaler TCP-Push fehlgeschlagen; Socket wird zurückgesetzt."
            )
            with self._local_tak_tcp_push_lock:
                if self._local_tak_tcp_push_conn is tcp_push_conn:
                    self._local_tak_tcp_push_conn = None
                    self._local_tak_tcp_push_peer = None
        return 0

    def _send_packet_to_tak(self, packet_xml, label):
        cot_dedupe_key = self._build_cot_dedupe_key(packet_xml)
        if cot_dedupe_key:
            self._remember_recent_chat(self.recent_cot_ids, cot_dedupe_key)

        if self.prefer_local_tcp_over_udp:
            # ── TCP-first local delivery (primary path) ───────────────────────
            # The active TCP connection to WinTAK/LPU5 (127.0.0.1:8087) is used
            # first because UDP/4242 may be blocked on some Windows systems
            # (WinError 10013).  Registered TCP listener peers are included as
            # part of the TCP delivery pass before multicast and UDP fallback.
            pushed = self._send_packet_to_local_tak_tcp_push(packet_xml, label)
            with self._local_tak_tcp_push_lock:
                tcp_push_peer = self._local_tak_tcp_push_peer
            self._send_packet_to_local_tak_tcp(packet_xml, label, skip_peer=tcp_push_peer if pushed else None)
            self._send_packet_to_tak_multicast(packet_xml, label)
            # ── Local UDP send (fallback path) ────────────────────────────────
            try:
                self.sock_udp.sendto(packet_xml, (self.tak_ip, self.tak_port))
                self.logger.debug(
                    f"[LastHop-UDP] Lokal an WinTAK gesendet (Fallback): {self.tak_ip}:{self.tak_port} "
                    f"label={label} bytes={len(packet_xml)}"
                )
            except Exception as e:
                self.logger.warning(f"Fehler beim Senden an lokales TAK (UDP-Fallback): {e}")
        else:
            # ── UDP-first local delivery (legacy) ─────────────────────────────
            try:
                self.sock_udp.sendto(packet_xml, (self.tak_ip, self.tak_port))
                self.logger.debug(
                    f"[LastHop-UDP] Lokal an WinTAK gesendet: {self.tak_ip}:{self.tak_port} "
                    f"label={label} bytes={len(packet_xml)}"
                )
            except Exception as e:
                self.logger.warning(f"Fehler beim Senden an lokales TAK (UDP): {e}")
            self._send_packet_to_tak_multicast(packet_xml, label)
            self._send_packet_to_local_tak_tcp(packet_xml, label)
            self._send_packet_to_local_tak_tcp_push(packet_xml, label)

        if self._should_use_pytak_remote():
            # PyTAK replaces the legacy remote socket path when available so packets
            # are not sent twice to the same remote TAK endpoint unless PyTAK is
            # currently unavailable, in which case we fall back to the legacy socket.
            if self._send_remote_packet_via_pytak(packet_xml, label):
                return
            self.logger.debug("PyTAK-Remote nicht verfügbar, verwende Remote-Socket-Fallback.")

        if self.server_protocol == "UDP":
            with self.server_lock:
                sock = self.sock_remote
                if sock:
                    try:
                        sock.sendto(packet_xml, (self.server_ip, self.server_port))
                        self.logger.info(f"Remote-UDP gesendet an {self.server_ip}:{self.server_port} ({label})")
                    except (socket.timeout, socket.error, OSError) as e:
                        self.logger.warning(f"Fehler beim Senden an Remote-UDP-Server ({type(e).__name__}): {e}")
        else:
            with self.server_lock:
                s = self.sock_remote
                if s:
                    try:
                        s.sendall(packet_xml + b"\n")
                        self.logger.info(f"Remote-TCP gesendet an {self.server_ip}:{self.server_port} ({label})")
                    except (socket.timeout, socket.error, OSError) as e:
                        self.logger.warning(f"Fehler beim Senden an Remote-TCP-Server ({type(e).__name__}), Socket wird zurückgesetzt: {e}")
                        try:
                            s.close()
                        except Exception:
                            pass
                        self.sock_remote = None

    def _build_chat_cot_xml(
        self,
        sender_uid,
        callsign,
        message,
        lat,
        lon,
        alt=0.0,
        chatroom=DEFAULT_CHATROOM_NAME,
        recipient_uid=None,
        recipient_callsign=None,
    ):
        t = get_tak_timestamp()
        stale = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        destination_uid = str(recipient_uid or chatroom or DEFAULT_CHATROOM_NAME).strip() or DEFAULT_CHATROOM_NAME
        destination_callsign = (
            str(recipient_callsign or chatroom or recipient_uid or DEFAULT_CHATROOM_NAME).strip()
            or DEFAULT_CHATROOM_NAME
        )
        message_hash = hashlib.sha256(_ensure_bytes(message)).hexdigest()[:16]
        message_timestamp_ms = time.time_ns() // 1_000_000
        message_id = f"{sender_uid}-{message_hash}-{message_timestamp_ms}"
        event = Element('event', {
            'version': '2.0',
            'uid': f"GeoChat.{sender_uid}.{message_id}",
            'type': 'b-t-f',
            'how': 'h-g-i-g-o',
            'start': t,
            'time': t,
            'stale': stale,
        })
        SubElement(event, 'point', {
            'lat': f"{lat:.6f}",
            'lon': f"{lon:.6f}",
            'hae': str(alt or 0),
            'ce': '9999999.0',
            'le': '9999999.0',
        })
        detail = SubElement(event, 'detail')
        chat = SubElement(detail, '__chat', {
            'parent': 'RootContactGroup',
            'groupOwner': 'false',
            'messageId': message_id,
            'chatroom': destination_callsign,
            'id': destination_uid,
            'senderCallsign': callsign,
        })
        SubElement(chat, 'chatgrp', {
            'uid0': sender_uid,
            'uid1': destination_uid,
            'id': destination_uid,
        })
        SubElement(detail, 'link', {
            'uid': sender_uid,
            'relation': 'p-p',
            'type': MESHTASTIC_PLI_COT_EVENT_TYPE,
        })
        server_destination_host = str(getattr(self, "chat_listen_ip", "0.0.0.0") or "0.0.0.0").strip() or "0.0.0.0"
        if server_destination_host in {"*", "::"}:
            server_destination_host = "0.0.0.0"
        SubElement(detail, '__serverdestination', {
            'destination': f"{server_destination_host}:{self.chat_listen_port}:tcp:{sender_uid}",
        })
        remarks = SubElement(detail, 'remarks', {
            'source': f"BAO.F.ATAK.{sender_uid}",
            'to': destination_uid,
            'time': t,
        })
        remarks.text = message
        return tostring(event, encoding='utf-8')

    def _get_interface_label(self, interface):
        if interface is None:
            return "unbekannt"
        for attr in ("devPath", "_port", "port", "portName"):
            value = getattr(interface, attr, None)
            if value:
                return str(value)
        return "unbekannt"

    def _get_interface_label_key(self, interface):
        return self._normalize_interface_label_key(self._get_interface_label(interface))

    def _normalize_interface_label_key(self, label):
        return str(label or "").strip().upper()

    def _format_interface_labels(self, labels):
        normalized = []
        seen = set()
        for label in labels or []:
            text = str(label or "").strip()
            key = text.upper()
            if not text or key in seen:
                continue
            seen.add(key)
            normalized.append(text)
        return normalized

    def _get_interface_label_keys(self, labels):
        return {
            self._normalize_interface_label_key(label)
            for label in labels or []
            if self._normalize_interface_label_key(label)
        }

    def _merge_interfaces_by_label(self, interfaces, additional_interfaces):
        merged = list(interfaces or [])
        seen = {self._get_interface_label_key(iface) for iface in merged if iface is not None}
        for iface in additional_interfaces or []:
            if iface is None:
                continue
            label_key = self._get_interface_label_key(iface)
            if label_key in seen:
                continue
            seen.add(label_key)
            merged.append(iface)
        return merged

    def _get_interfaces_by_labels(self, labels, interfaces=None):
        label_keys = self._get_interface_label_keys(labels)
        if not label_keys:
            return []
        matched = []
        seen = set()
        for iface in interfaces or self._get_interfaces_snapshot():
            if iface is None:
                continue
            label_key = self._get_interface_label_key(iface)
            if not label_key or label_key not in label_keys or label_key in seen:
                continue
            seen.add(label_key)
            matched.append(iface)
        return matched

    def _get_interfaces_snapshot(self):
        with self.interface_lock:
            return list(self.interfaces)

    def _get_missing_configured_ports(self, interfaces=None):
        configured_ports = {str(port).strip().upper() for port in self.ports if str(port).strip()}
        connected_ports = set()
        for iface in interfaces or []:
            label = self._get_interface_label(iface).strip().upper()
            if label:
                connected_ports.add(label)
        return sorted(configured_ports - connected_ports)

    def _ensure_meshtastic_subscription(self):
        if self._meshtastic_pubsub_registered:
            return
        if pub is None:
            self.logger.warning("pypubsub nicht gefunden: Live-Empfangs-Callbacks möglicherweise nicht aktiv.")
            return
        pub.subscribe(self.on_any_packet, "meshtastic.receive")
        self._meshtastic_pubsub_registered = True

    def _close_meshtastic_interface(self, iface):
        try:
            iface.close()
        except Exception:
            pass

    def _connect_meshtastic_interfaces(self, force_reconnect=False):
        if meshtastic is None:
            return self._get_interfaces_snapshot()

        ports_to_connect = [str(port).strip() for port in self.ports if str(port).strip()]
        stale_interfaces = []
        with self.interface_lock:
            if force_reconnect:
                stale_interfaces = list(self.interfaces)
                self.interfaces = []
            connected_ports = {self._get_interface_label(iface).strip().upper() for iface in self.interfaces}

        for iface in stale_interfaces:
            self._close_meshtastic_interface(iface)

        newly_connected = []
        for port in ports_to_connect:
            if port.upper() in connected_ports:
                continue
            try:
                self.logger.info(f"Versuche Verbindung zum Meshtastic-Hardware-Interface an {port} ...")
                iface = meshtastic.serial_interface.SerialInterface(port)
                with self.interface_lock:
                    self.interfaces.append(iface)
                connected_ports.add(port.upper())
                newly_connected.append(iface)
                self.logger.info(f"Interface an {port} erfolgreich verbunden.")
            except Exception as exc:
                self.logger.warning(f"Meshtastic-Interface an {port} konnte nicht verbunden werden: {exc}")
                self.logger.debug("Fehler beim Verbinden des Meshtastic-Interfaces:\n" + traceback.format_exc())

        if newly_connected:
            self._ensure_meshtastic_subscription()
            self._register_local_nodes()

        return self._get_interfaces_snapshot()

    def _ensure_meshtastic_interfaces(self, reconnect=False, raise_on_empty=False, reason=None):
        interfaces = self._get_interfaces_snapshot()
        missing_ports = self._get_missing_configured_ports(interfaces)
        if reconnect or not interfaces or missing_ports:
            if reconnect:
                self.logger.warning(
                    "Meshtastic-COM-Verbindung für ausgehende Nachrichten wird neu aufgebaut."
                    + (f" Grund: {reason}" if reason else "")
                )
            elif missing_ports:
                self.logger.info(
                    "Verbinde fehlende Meshtastic-COM-Ports neu: " + ", ".join(missing_ports)
                )
            interfaces = self._connect_meshtastic_interfaces(force_reconnect=reconnect)
        if raise_on_empty and not interfaces:
            raise RuntimeError("Keine verbundenen Meshtastic-Interfaces verfügbar.")
        return interfaces

    def _build_meshtastic_send_kwargs(self, iface):
        try:
            parameters = inspect.signature(iface.sendText).parameters
            supports_var_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
        except (TypeError, ValueError):
            parameters = {}
            supports_var_kwargs = True

        def supports(name):
            return supports_var_kwargs or name in parameters

        kwargs = {}
        if supports("destinationId"):
            kwargs["destinationId"] = "^all"
        elif supports("destination_id"):
            kwargs["destination_id"] = "^all"

        if supports("wantAck"):
            kwargs["wantAck"] = False
        elif supports("want_ack"):
            kwargs["want_ack"] = False

        if supports("channelIndex"):
            kwargs["channelIndex"] = DEFAULT_MESHTASTIC_CHANNEL_INDEX
        elif supports("channel_index"):
            kwargs["channel_index"] = DEFAULT_MESHTASTIC_CHANNEL_INDEX
        elif supports("channel"):
            kwargs["channel"] = DEFAULT_MESHTASTIC_CHANNEL_INDEX

        return kwargs

    def _build_meshtastic_send_data_kwargs(self, iface, portnum, destination_id="^all"):
        send_data = getattr(iface, "sendData", None)
        if send_data is None:
            raise AttributeError("Meshtastic-Interface unterstützt sendData nicht.")
        try:
            parameters = inspect.signature(send_data).parameters
            supports_var_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
        except (TypeError, ValueError):
            parameters = {}
            supports_var_kwargs = True

        def supports(name):
            return supports_var_kwargs or name in parameters

        kwargs = {}
        resolved_destination_id = resolve_meshtastic_destination_id(destination_id)
        if supports("destinationId"):
            kwargs["destinationId"] = resolved_destination_id
        elif supports("destination_id"):
            kwargs["destination_id"] = resolved_destination_id

        if supports("wantAck"):
            kwargs["wantAck"] = False
        elif supports("want_ack"):
            kwargs["want_ack"] = False

        if supports("channelIndex"):
            kwargs["channelIndex"] = DEFAULT_MESHTASTIC_CHANNEL_INDEX
        elif supports("channel_index"):
            kwargs["channel_index"] = DEFAULT_MESHTASTIC_CHANNEL_INDEX
        elif supports("channel"):
            kwargs["channel"] = DEFAULT_MESHTASTIC_CHANNEL_INDEX

        if supports("portNum"):
            kwargs["portNum"] = portnum
        elif supports("portnum"):
            kwargs["portnum"] = portnum
        elif supports("port_num"):
            kwargs["port_num"] = portnum

        return kwargs

    def _send_text_to_interfaces(self, message, interfaces, allow_reconnect=True):
        interfaces = [iface for iface in (interfaces or []) if iface is not None]
        if not interfaces:
            if allow_reconnect:
                interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
            else:
                raise RuntimeError("Keine verbundenen Meshtastic-Interfaces verfügbar.")
        sent_interfaces = []
        last_error = None
        failed_labels = []
        for iface in interfaces:
            try:
                # Broadcast Chat/CoT always on the primary Meshtastic channel 0.
                kwargs = self._build_meshtastic_send_kwargs(iface)
                iface.sendText(message, **kwargs)
                sent_interfaces.append(iface)
            except Exception as exc:
                last_error = exc
                failed_labels.append(self._get_interface_label(iface))
                self.logger.warning(
                    f"Fehler beim Senden einer Meshtastic-Nachricht auf {self._get_interface_label(iface)}: {exc}"
                )
        normalized_failed_labels = self._format_interface_labels(failed_labels)
        if normalized_failed_labels and allow_reconnect:
            retry_interfaces = self._ensure_meshtastic_interfaces(reconnect=True, reason=str(last_error))
            retry_targets = self._get_interfaces_by_labels(normalized_failed_labels, retry_interfaces)
            retry_target_labels = {
                self._get_interface_label_key(iface) for iface in retry_targets if iface is not None
            }
            missing_retry_labels = [
                label
                for label in normalized_failed_labels
                if self._normalize_interface_label_key(label) not in retry_target_labels
            ]
            if missing_retry_labels:
                failure = RuntimeError(
                    f"Meshtastic-Nachricht konnte nach COM-Neuverbindung nicht an folgende Ports gesendet werden: "
                    f"{', '.join(missing_retry_labels)}"
                )
                raise failure from last_error
            if retry_targets:
                try:
                    retried_interfaces = self._send_text_to_interfaces(
                        message,
                        retry_targets,
                        allow_reconnect=False,
                    )
                    return self._merge_interfaces_by_label(sent_interfaces, retried_interfaces)
                except Exception as retry_exc:
                    self.logger.warning(
                        "Meshtastic-Senden ist auch nach COM-Neuverbindung fehlgeschlagen: "
                        f"erst '{last_error}', dann '{retry_exc}'"
                    )
                    raise retry_exc from last_error
        if normalized_failed_labels:
            failure = RuntimeError(
                f"Meshtastic-Nachricht konnte nicht an alle verbundenen Ports gesendet werden: "
                f"{', '.join(normalized_failed_labels)}"
            )
            raise failure from last_error
        return sent_interfaces

    def _send_data_to_interfaces(
        self,
        payload,
        interfaces,
        portnum,
        allow_reconnect=True,
        destination_id="^all",
    ):
        interfaces = [iface for iface in (interfaces or []) if iface is not None]
        if not interfaces:
            if allow_reconnect:
                interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
            else:
                raise RuntimeError("Keine verbundenen Meshtastic-Interfaces verfügbar.")
        sent_interfaces = []
        last_error = None
        failed_labels = []
        for iface in interfaces:
            try:
                kwargs = self._build_meshtastic_send_data_kwargs(
                    iface,
                    portnum,
                    destination_id=destination_id,
                )
                try:
                    iface.sendData(payload, **kwargs)
                except Exception as exc:
                    compatibility_kwargs = dict(kwargs)
                    removed_destination = compatibility_kwargs.pop("destinationId", None)
                    if removed_destination is None:
                        removed_destination = compatibility_kwargs.pop("destination_id", None)
                    if removed_destination is None:
                        raise
                    self.logger.debug(
                        "Meshtastic-sendData-Broadcast wird ohne destinationId erneut versucht: "
                        f"iface={self._get_interface_label(iface)} port={portnum} previous_destination={removed_destination}"
                    )
                    iface.sendData(payload, **compatibility_kwargs)
                sent_interfaces.append(iface)
            except Exception as exc:
                last_error = exc
                failed_labels.append(self._get_interface_label(iface))
                self.logger.warning(
                    f"Fehler beim Senden eines Meshtastic-Datenpakets auf {self._get_interface_label(iface)}: {exc}"
                )
        should_retry = (
            allow_reconnect
            and last_error is not None
            and failed_labels
            and not _is_meshtastic_payload_too_big_error(last_error)
        )
        normalized_failed_labels = self._format_interface_labels(failed_labels)
        if should_retry:
            retry_interfaces = self._ensure_meshtastic_interfaces(reconnect=True, reason=str(last_error))
            retry_targets = self._get_interfaces_by_labels(normalized_failed_labels, retry_interfaces)
            retry_target_labels = {
                self._get_interface_label_key(iface) for iface in retry_targets if iface is not None
            }
            missing_retry_labels = [
                label
                for label in normalized_failed_labels
                if self._normalize_interface_label_key(label) not in retry_target_labels
            ]
            if missing_retry_labels:
                failure = RuntimeError(
                    f"Meshtastic-Datenpaket konnte nach COM-Neuverbindung nicht an folgende Ports gesendet werden: "
                    f"{', '.join(missing_retry_labels)}"
                )
                raise failure from last_error
            if retry_targets:
                try:
                    retried_interfaces = self._send_data_to_interfaces(
                        payload,
                        retry_targets,
                        portnum,
                        allow_reconnect=False,
                        destination_id=destination_id,
                    )
                    return self._merge_interfaces_by_label(sent_interfaces, retried_interfaces)
                except Exception as retry_exc:
                    self.logger.warning(
                        "Meshtastic-Datensenden ist auch nach COM-Neuverbindung fehlgeschlagen: "
                        f"erst '{last_error}', dann '{retry_exc}'"
                    )
                    raise retry_exc from last_error
        if normalized_failed_labels:
            failure = RuntimeError(
                f"Meshtastic-Datenpaket konnte nicht an alle verbundenen Ports gesendet werden: "
                f"{', '.join(normalized_failed_labels)}"
            )
            raise failure from last_error
        return sent_interfaces

    def _get_relay_targets(self, source_interface, interfaces=None):
        source_label = self._get_interface_label(source_interface).upper()
        if self.relay_text_from_ports and source_label not in self.relay_text_from_ports:
            return []
        relay_targets = []
        for iface in (interfaces or self._ensure_meshtastic_interfaces()):
            target_label = self._get_interface_label(iface).upper()
            if iface is source_interface or (source_label != "UNBEKANNT" and target_label == source_label):
                continue
            if self.relay_text_to_ports and target_label not in self.relay_text_to_ports:
                continue
            relay_targets.append(iface)
        return relay_targets

    def _prepare_meshtastic_text_chunks(self, message):
        raw_message = str(message or "").replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.strip() for line in raw_message.split("\n") if line.strip()]
        normalized = " / ".join(lines) if lines else raw_message.strip()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized:
            return []

        chunks = []
        current = ""
        for word in normalized.split(" "):
            candidate = f"{current} {word}".strip()
            if current and len(candidate.encode("utf-8")) > MESHTASTIC_TEXT_CHUNK_MAX_BYTES:
                chunks.append(current)
                current = ""
                candidate = word
            while len(candidate.encode("utf-8")) > MESHTASTIC_TEXT_CHUNK_MAX_BYTES:
                split_at = len(candidate)
                while split_at > 1 and len(candidate[:split_at].encode("utf-8")) > MESHTASTIC_TEXT_CHUNK_MAX_BYTES:
                    split_at -= 1
                chunks.append(candidate[:split_at])
                candidate = candidate[split_at:].lstrip()
            current = candidate
        if current:
            chunks.append(current)
        return chunks

    def _prepare_meshtastic_cot_chunks(self, packet_xml):
        packet_bytes = _ensure_bytes(packet_xml).strip()
        if not packet_bytes:
            return []
        encoded_packet = base64.urlsafe_b64encode(packet_bytes).decode("ascii")
        payload_chunks = [
            encoded_packet[i:i + MESHTASTIC_COT_FRAGMENT_PAYLOAD_BYTES]
            for i in range(0, len(encoded_packet), MESHTASTIC_COT_FRAGMENT_PAYLOAD_BYTES)
        ]
        if not payload_chunks:
            return []
        message_id = uuid.uuid4().hex[:12]
        total_chunks = len(payload_chunks)
        return [
            f"{MESHTASTIC_COT_FRAGMENT_PREFIX}:{message_id}:{index}:{total_chunks}:{payload}"
            for index, payload in enumerate(payload_chunks, start=1)
        ]

    def _parse_meshtastic_cot_chunk(self, message):
        if not message:
            return None
        prefix = f"{MESHTASTIC_COT_FRAGMENT_PREFIX}:"
        if not str(message).startswith(prefix):
            return None
        parts = str(message).split(":", 4)
        if len(parts) != 5:
            return None
        _, message_id, part_index, total_parts, payload = parts
        try:
            part_index = int(part_index)
            total_parts = int(total_parts)
        except (TypeError, ValueError):
            return None
        has_required_fields = bool(message_id and payload)
        has_valid_part_numbers = part_index >= 1 and total_parts >= 1 and part_index <= total_parts
        if not has_required_fields or not has_valid_part_numbers:
            return None
        return {
            "message_id": message_id,
            "part_index": part_index,
            "total_parts": total_parts,
            "payload": payload,
        }

    def _prepare_meshtastic_forwarder_payload(self, packet_xml):
        packet_bytes = _ensure_bytes(packet_xml).strip()
        if not packet_bytes:
            return b""
        return zlib.compress(packet_bytes)

    def _prepare_meshtastic_forwarder_packets(self, payload):
        """Return a list of raw ATAK_FORWARDER payloads for *payload* bytes.

        Single-packet path: if the zlib payload fits in one Meshtastic packet it
        is returned as-is (identical to the reference behaviour for small CoT).

        Multi-packet path: large payloads are encoded using the FTN fountain-code
        protocol (meshtastic/ATAK-Plugin reference), replacing the old COTF
        sequential fragment format.  FTN packets are understood by real ATAK/iTAK
        clients, COTF is not.
        """
        payload_bytes = _ensure_bytes(payload).strip()
        if not payload_bytes:
            return []
        # Small enough for a single direct packet.  Prepend the CoT transfer-type
        # byte (0x00) so the packet format matches the reference meshtastic/ATAK-Plugin
        # behaviour and FTN fountain packets: ATAK/iTAK clients strip this byte before
        # decompressing.  The gateway receive side already handles both prefixed and
        # unprefixed payloads via decode_candidates fallback.
        prefixed = bytes([MESHTASTIC_TRANSFER_TYPE_COT]) + payload_bytes
        if len(prefixed) <= MESHTASTIC_DATA_PAYLOAD_MAX_BYTES:
            return [prefixed]
        # Large payload — use FTN fountain code (reference behaviour)
        transfer_id = (uuid.uuid4().int >> 104) & 0xFFFFFF  # 24-bit random ID
        self.logger.debug(
            f"ATAK_FORWARDER-Payload zu groß für Einzelpaket ({len(payload_bytes)} Byte), "
            f"verwende FTN-Fountain-Code (transfer_id=0x{transfer_id:06x})"
        )
        packets = _fountain_encode(payload_bytes, transfer_id)
        if not packets:
            raise ValueError("FTN-Encoder hat keine Pakete erzeugt.")
        return packets

    def _parse_meshtastic_forwarder_fragment(self, payload):
        payload_bytes = bytes(payload or b"")
        if len(payload_bytes) < MESHTASTIC_FORWARDER_FRAGMENT_HEADER_BYTES:
            return None
        if not payload_bytes.startswith(MESHTASTIC_FORWARDER_FRAGMENT_MAGIC):
            return None

        version_offset = len(MESHTASTIC_FORWARDER_FRAGMENT_MAGIC)
        version = payload_bytes[version_offset]
        if version != MESHTASTIC_FORWARDER_FRAGMENT_VERSION:
            return None

        message_id_start = version_offset + 1
        message_id_end = message_id_start + MESHTASTIC_FORWARDER_FRAGMENT_MESSAGE_ID_BYTES
        message_id = payload_bytes[message_id_start:message_id_end]
        part_index = payload_bytes[message_id_end]
        total_parts = payload_bytes[message_id_end + 1]
        chunk_payload = payload_bytes[MESHTASTIC_FORWARDER_FRAGMENT_HEADER_BYTES:]

        has_required_fields = bool(message_id and chunk_payload)
        has_valid_part_numbers = total_parts >= 2 and 1 <= part_index <= total_parts
        if not has_required_fields or not has_valid_part_numbers:
            return None

        return {
            "message_id": message_id.hex(),
            "part_index": part_index,
            "total_parts": total_parts,
            "payload": chunk_payload,
        }

    def _decode_meshtastic_forwarder_payload(self, payload):
        payload_bytes = _ensure_bytes(payload).strip()
        if not payload_bytes:
            return None

        decode_candidates = [payload_bytes]
        if payload_bytes and payload_bytes[0] in (
            MESHTASTIC_TRANSFER_TYPE_COT,
            MESHTASTIC_TRANSFER_TYPE_FILE,
            MESHTASTIC_TRANSFER_TYPE_COT_ASCII,
            MESHTASTIC_TRANSFER_TYPE_FILE_ASCII,
        ):
            transfer_type = payload_bytes[0]
            stripped_payload = payload_bytes[1:].strip()
            if stripped_payload:
                if transfer_type in (MESHTASTIC_TRANSFER_TYPE_COT, MESHTASTIC_TRANSFER_TYPE_COT_ASCII):
                    self.logger.debug(
                        f"ATAK_FORWARDER-TransferType erkannt (CoT): 0x{transfer_type:02x}, "
                        f"payload_bytes={len(stripped_payload)}"
                    )
                else:
                    self.logger.debug(
                        f"ATAK_FORWARDER-TransferType erkannt (Datei): 0x{transfer_type:02x}, "
                        "Payload wird nicht als CoT weiterverarbeitet."
                    )
                decode_candidates.append(stripped_payload)

        for candidate in decode_candidates:
            for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
                try:
                    return zlib.decompress(candidate, wbits)
                except zlib.error:
                    continue
            if candidate.lstrip().startswith(b"<"):
                return candidate
        return None

    def _forward_meshtastic_cot_xml_to_tak(
        self,
        packet_xml,
        from_id,
        source_label="Mesh-CoT",
        meshtastic_live_contact=False,
    ):
        # Extract uid/type/how from the RAW received packet BEFORE normalization so we can
        # confirm that the normalization step does not silently change marker semantics.
        pre_norm_metadata = self._extract_cot_event_metadata(packet_xml) if packet_xml else None

        normalized_packet, metadata, error = self._normalize_generic_cot_event(
            packet_xml,
            add_meshtastic_marker=True,
            meshtastic_live_contact=meshtastic_live_contact,
        )
        if normalized_packet is None or metadata is None:
            self.logger.warning(f"{source_label} verworfen: {error or 'ungültiges CoT-Event'}")
            self.logger.debug(f"{source_label} Rohpayload: {_build_safe_payload_snippet(packet_xml)}")
            return True
        pre_uid = pre_norm_metadata.get("uid") if pre_norm_metadata else None
        pre_type = pre_norm_metadata.get("type") if pre_norm_metadata else None
        pre_how = pre_norm_metadata.get("how") if pre_norm_metadata else None
        uid_unchanged = pre_uid == metadata["uid"] if pre_uid is not None else True
        type_unchanged = pre_type == metadata["type"] if pre_type is not None else True
        how_unchanged = pre_how == metadata["how"] if pre_how is not None else True
        self.logger.debug(
            f"[Mesh->WinTAK] Rekonstruiert — source={source_label} from={from_id} "
            f"uid={metadata['uid']} reconstructed_type={pre_type or metadata['type']} "
            f"forward_type={metadata['type']} how={metadata['how']} "
            f"classification={metadata.get('cot_class') or 'generic'} "
            f"type_changed={'ja' if not type_unchanged else 'nein'} "
            f"uid_changed={'ja' if not uid_unchanged else 'nein'} "
            f"how_changed={'ja' if not how_unchanged else 'nein'} "
            f"lat={metadata['lat']:.6f} lon={metadata['lon']:.6f}"
        )
        if metadata.get("is_marker"):
            # ── LPU5-aligned marker last-hop debug ────────────────────────────
            # Confirm that uid/type/how are unchanged after normalization so that
            # TAK-originated marker semantics are forwarded unmodified to WinTAK.
            self.logger.debug(
                f"[Marker-LastHop] Rekonstruiert aus Mesh — source={source_label} from={from_id} "
                f"uid={metadata['uid']} (unverändert={uid_unchanged}) "
                f"type={metadata['type']} (unverändert={type_unchanged}) "
                f"how={metadata['how']} (unverändert={how_unchanged}) "
                f"lat={metadata['lat']:.6f} lon={metadata['lon']:.6f}"
            )
            self.logger.debug(
                f"[Marker-LastHop] CoT-XML an WinTAK (letzte Meile): "
                f"transport=UDP({self.tak_ip}:{self.tak_port})+TCP({self.tcp_chat_receiver_host}:{self.tcp_chat_receiver_port}) "
                f"xml={_build_safe_payload_snippet(normalized_packet)}"
            )
        self.logger.debug(
            f"{source_label} rekonstruiert: uid={metadata['uid']} type={metadata['type']} "
            f"how={metadata['how']} lat={metadata['lat']:.6f} lon={metadata['lon']:.6f} "
            f"payload={_build_safe_payload_snippet(normalized_packet)}"
        )
        self._remember_recent_cot_identity(metadata, source_label=source_label)
        cot_dedupe_key = self._build_cot_dedupe_key(normalized_packet)
        is_echo_back = bool(cot_dedupe_key and self._was_seen_recently(self.recent_cot_ids, cot_dedupe_key))
        self._record_service_monitor_event(
            normalized_packet,
            ">>>",
            f"Mesh {source_label} {from_id}".strip(),
            is_echo_back=is_echo_back,
        )
        if is_echo_back:
            return True

        self._send_packet_to_tak(normalized_packet, metadata["uid"] or f"{source_label} {from_id}")
        self.logger.info(
            f"CoT aus dem Mesh nach TAK weitergeleitet: {metadata['uid'] or f'ohne UID von {from_id}'}"
        )
        return True

    def _handle_meshtastic_forwarder_packet(self, packet, from_id):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return False
        payload = decoded.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return False
        payload = bytes(payload)
        self.logger.debug(
            f"ATAK_FORWARDER-Paket aus dem Mesh empfangen: {len(payload)} Byte von {from_id}"
        )
        # ── Priority 1: FTN fountain-code packet (meshtastic/ATAK-Plugin reference format)
        if payload[:3] == FOUNTAIN_MAGIC:
            return self._handle_fountain_data_block(payload, from_id)
        # ── Priority 2: Gateway-native COTF sequential fragment
        fragment = self._parse_meshtastic_forwarder_fragment(payload)
        if fragment is not None:
            return self._handle_meshtastic_forwarder_fragment(fragment, from_id)
        # ── Priority 3: Direct zlib-compressed (or raw) CoT XML single packet
        packet_xml = self._decode_meshtastic_forwarder_payload(payload)
        if not packet_xml:
            self.logger.warning(
                "ATAK_FORWARDER-Payload aus dem Mesh konnte nicht dekodiert werden. "
                f"first_bytes=0x{payload[:4].hex()} len={len(payload)}"
            )
            return True
        return self._forward_meshtastic_cot_xml_to_tak(packet_xml, from_id, source_label="ATAK_FORWARDER")

    def _handle_fountain_data_block(self, raw_bytes, from_id):
        """Receive and reassemble an FTN fountain-code data block.

        This implements the receiver side of the fountain codec used by the
        official meshtastic/ATAK-Plugin (FountainChunkManager + FountainCodec).
        Blocks are accumulated per (from_id, transfer_id); once enough blocks
        are received the payload is decoded with the LT peeling decoder.
        """
        block = _fountain_parse_data_block(raw_bytes)
        if block is None:
            self.logger.debug(
                f"FTN-Paket ignoriert (kein gültiger Data-Block): len={len(raw_bytes)} from={from_id}"
            )
            return True

        transfer_id = block["transfer_id"]
        K = block["K"]
        total_len = block["total_len"]
        self.logger.debug(
            f"FTN-Block empfangen: transfer_id=0x{transfer_id:06x} seed=0x{block['seed']:04x} "
            f"K={K} total_len={total_len} payload_bytes={len(block['payload'])} from={from_id}"
        )
        self._cleanup_expired_meshtastic_forwarder_fragments()

        cache_key = f"ftn:{from_id}:{transfer_id}"
        now = time.time()
        with self.chat_cache_lock:
            entry = self.partial_fountain_transfers.get(cache_key)
            if entry is None or entry.get("K") != K or entry.get("total_len") != total_len:
                entry = {
                    "created_at": now,
                    "updated_at": now,
                    "K": K,
                    "total_len": total_len,
                    "transfer_id": transfer_id,
                    "blocks": [],
                }
                self.partial_fountain_transfers[cache_key] = entry
            else:
                entry["updated_at"] = now

            entry["blocks"].append((block["seed"], transfer_id, block["payload"]))
            num_received = len(entry["blocks"])

            # The peeling decoder needs at least K blocks; try decoding as soon as K are available
            if num_received < K:
                self.logger.debug(
                    f"FTN-Reassembly: {num_received}/{K} Blöcke empfangen, warte auf mehr "
                    f"(transfer_id=0x{transfer_id:06x} from={from_id})"
                )
                return True

            # Try to decode — may succeed with exactly K blocks or need a few more
            blocks_snapshot = list(entry["blocks"])

        self.logger.debug(
            f"FTN-Decoder gestartet: transfer_id=0x{transfer_id:06x} "
            f"blocks={len(blocks_snapshot)} K={K} total_len={total_len} from={from_id}"
        )
        try:
            decoded_payload = _fountain_decode(blocks_snapshot, K, total_len)
        except Exception as exc:
            self.logger.warning(
                f"FTN-Decoder Fehler: transfer_id=0x{transfer_id:06x} {exc}"
            )
            decoded_payload = None

        if decoded_payload is None:
            # Not enough blocks yet (or decoding failed) — keep accumulating
            self.logger.debug(
                f"FTN-Decoder: noch nicht genug Blöcke für vollständige Rekonstruktion "
                f"(transfer_id=0x{transfer_id:06x} blocks={len(blocks_snapshot)} K={K})"
            )
            return True

        # Decoding succeeded — remove state and process the payload
        with self.chat_cache_lock:
            self.partial_fountain_transfers.pop(cache_key, None)

        decoded_payload_with_padding = _fountain_decode(
            blocks_snapshot,
            K,
            K * FOUNTAIN_BLOCK_SIZE,
        )
        decode_candidates = []
        if decoded_payload_with_padding:
            candidate_lengths = [total_len]
            for extra_bytes in (1, 2, 4, 8):
                candidate_length = total_len + extra_bytes
                if candidate_length <= len(decoded_payload_with_padding):
                    candidate_lengths.append(candidate_length)
            candidate_lengths.append(len(decoded_payload_with_padding))
            seen_lengths = set()
            for candidate_length in candidate_lengths:
                if candidate_length <= 0 or candidate_length in seen_lengths:
                    continue
                seen_lengths.add(candidate_length)
                decode_candidates.append(decoded_payload_with_padding[:candidate_length])
        if not decode_candidates:
            decode_candidates.append(decoded_payload)

        self.logger.debug(
            f"FTN-Reassembly vollständig: transfer_id=0x{transfer_id:06x} "
            f"decoded_bytes={len(decoded_payload)} expected_total_len={total_len} "
            f"candidate_lengths={[len(candidate) for candidate in decode_candidates]} from={from_id}"
        )
        packet_xml = None
        successful_candidate_len = None
        for candidate in decode_candidates:
            packet_xml = self._decode_meshtastic_forwarder_payload(candidate)
            if packet_xml:
                successful_candidate_len = len(candidate)
                break
        if packet_xml:
            self.logger.debug(
                f"FTN-Payload erfolgreich dekodiert: transfer_id=0x{transfer_id:06x} "
                f"candidate_len={successful_candidate_len} xml_bytes={len(packet_xml)} from={from_id}"
            )
        if not packet_xml:
            self.logger.warning(
                f"FTN-Payload konnte nicht als CoT XML dekodiert werden: "
                f"transfer_id=0x{transfer_id:06x} first_bytes=0x{decoded_payload[:4].hex()} "
                f"expected_total_len={total_len} candidate_lengths={[len(candidate) for candidate in decode_candidates]}"
            )
            return True
        return self._forward_meshtastic_cot_xml_to_tak(
            packet_xml, from_id, source_label="ATAK_FORWARDER-FTN"
        )

    def _handle_meshtastic_forwarder_fragment(self, fragment, from_id):
        self.logger.debug(
            f"ATAK_FORWARDER-Fragment empfangen: {fragment['part_index']}/{fragment['total_parts']} "
            f"message_id={fragment['message_id']} from={from_id} payload_bytes={len(fragment['payload'])}"
        )
        self._cleanup_expired_meshtastic_forwarder_fragments()
        cache_key = f"{from_id}:{fragment['message_id']}"
        now = time.time()
        with self.chat_cache_lock:
            entry = self.partial_meshtastic_forwarder_messages.get(cache_key)
            if entry is None or entry.get("total_parts") != fragment["total_parts"]:
                entry = {
                    "created_at": now,
                    "updated_at": now,
                    "total_parts": fragment["total_parts"],
                    "parts": {},
                }
                self.partial_meshtastic_forwarder_messages[cache_key] = entry
            else:
                entry["updated_at"] = now

            entry["parts"][fragment["part_index"]] = fragment["payload"]
            if len(entry["parts"]) < entry["total_parts"]:
                return True
            ordered_payload = b"".join(
                entry["parts"][index]
                for index in range(1, entry["total_parts"] + 1)
                if index in entry["parts"]
            )
            self.partial_meshtastic_forwarder_messages.pop(cache_key, None)

        self.logger.debug(
            f"ATAK_FORWARDER-Fragmentfolge vollständig: message_id={fragment['message_id']} "
            f"gesamt_bytes={len(ordered_payload)}"
        )
        packet_xml = self._decode_meshtastic_forwarder_payload(ordered_payload)
        if not packet_xml:
            self.logger.warning("ATAK_FORWARDER-Fragmentfolge aus dem Mesh konnte nicht dekodiert werden.")
            return True
        return self._forward_meshtastic_cot_xml_to_tak(
            packet_xml,
            from_id,
            source_label="ATAK_FORWARDER-Fragmentfolge",
        )

    def _forward_cot_to_meshtastic(self, packet_xml):
        interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
        normalized_packet, metadata, error = self._normalize_generic_cot_event(packet_xml)
        if normalized_packet is None or metadata is None:
            raise ValueError(error or "Ungültiges CoT-Event")
        self._remember_recent_cot_identity(metadata, source_label="TAK->Mesh")
        cot_class = metadata.get("cot_class") or "generic"
        if cot_class == "marker":
            self.logger.debug(
                f"Eingehender Marker erkannt: uid={metadata['uid']} type={metadata['type']} "
                f"how={metadata['how']} lat={metadata['lat']:.6f} lon={metadata['lon']:.6f}"
            )
        elif cot_class == "generic":
            self.logger.debug(
                f"Eingehender generischer CoT erkannt: uid={metadata['uid']} type={metadata['type']} "
                f"how={metadata['how']} lat={metadata['lat']:.6f} lon={metadata['lon']:.6f}"
            )
        self.logger.debug(
            f"Generic-CoT für Mesh vorbereitet: uid={metadata['uid']} type={metadata['type']} "
            f"how={metadata['how']} lat={metadata['lat']:.6f} lon={metadata['lon']:.6f} "
            f"payload={_build_safe_payload_snippet(normalized_packet)}"
        )

        pli_packet = self._prepare_meshtastic_pli_packet(normalized_packet)
        if pli_packet is not None:
            try:
                self.logger.debug(
                    f"PLI-CoT wird als ATAK_PLUGIN-PLI gesendet: uid={pli_packet['uid']} "
                    f"callsign={pli_packet['callsign']} payload_bytes={len(pli_packet['payload'])}"
                )
                self._send_data_to_interfaces(
                    pli_packet["payload"],
                    interfaces,
                    MESHTASTIC_ATAK_PLUGIN_PORTNUM,
                )
                return {"transport": "ATAK_PLUGIN", "count": 1}
            except Exception as exc:
                self.logger.warning(
                    "ATAK_PLUGIN-PLI-Senden fehlgeschlagen, versuche ATAK_FORWARDER-Fallback: "
                    f"{exc}"
                )

        # Marker CoT events are sent directly via ATAK_FORWARDER (port 257) so that
        # real ATAK/iTAK clients can decode them.  The ATAK_PLUGIN detail=7 path below
        # uses a gateway-internal protobuf field that ATAK devices do not process,
        # which caused WinTAK→ATAK markers to disappear silently.  Generic (non-marker,
        # non-PLI) CoT still tries the compact detail=7 path first because those events
        # are usually gateway-to-gateway and the receiver understands the format.
        if cot_class == "marker":
            self.logger.debug(
                f"{_get_cot_subject_label(metadata)} Typ {metadata['type']} "
                "ist ein Marker – verwende direkt ATAK_FORWARDER für maximale ATAK-Kompatibilität "
                "(ATAK_PLUGIN-detail=7 wird von echten ATAK/iTAK-Clients ignoriert)."
            )
        else:
            if pli_packet is None:
                self.logger.debug(
                    f"{_get_cot_subject_label(metadata)} Typ {metadata['type']} "
                    "nutzt aus Kompatibilitätsgründen direkt ATAK_FORWARDER."
                )
            detail_packet = self._prepare_meshtastic_detail_packet(normalized_packet)
            if detail_packet is not None:
                try:
                    self.logger.debug(
                        f"{_get_cot_subject_label(metadata)} wird als ATAK_PLUGIN-detail=7 gesendet: "
                        f"uid={detail_packet['uid']} callsign={detail_packet['callsign']} "
                        f"compressed={detail_packet['is_compressed']} payload_bytes={len(detail_packet['payload'])}"
                    )
                    self._send_data_to_interfaces(
                        detail_packet["payload"],
                        interfaces,
                        MESHTASTIC_ATAK_PLUGIN_PORTNUM,
                    )
                    return {"transport": "ATAK_PLUGIN_DETAIL", "count": 1}
                except Exception as exc:
                    self.logger.warning(
                        "ATAK_PLUGIN-detail=7-Senden fehlgeschlagen, versuche ATAK_FORWARDER-Fallback: "
                        f"{exc}"
                    )

        forwarder_payload = self._prepare_meshtastic_forwarder_payload(normalized_packet)
        if not forwarder_payload:
            raise ValueError(EMPTY_MESHTASTIC_COT_ERROR)
        try:
            transport_subject = _get_cot_subject_label(metadata)
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    f"{transport_subject} via ATAK_FORWARDER komprimiert: "
                    f"xml_bytes={len(_ensure_bytes(normalized_packet))} compressed_bytes={len(forwarder_payload)} "
                    f"serialized={_build_safe_payload_snippet(forwarder_payload)}"
                )
            forwarder_packets = self._prepare_meshtastic_forwarder_packets(forwarder_payload)
            is_ftn = len(forwarder_packets) > 1 and forwarder_packets[0][:3] == FOUNTAIN_MAGIC
            self.logger.debug(
                f"{transport_subject} via ATAK_FORWARDER verpackt: paketanzahl={len(forwarder_packets)} "
                f"modus={'direkt' if len(forwarder_packets) == 1 else ('FTN-Fountain' if is_ftn else 'fragmentiert')}"
            )
            for forwarder_packet in forwarder_packets:
                self._send_data_to_interfaces(
                    forwarder_packet,
                    interfaces,
                    MESHTASTIC_ATAK_FORWARDER_PORTNUM,
                )
                interfaces = self._get_interfaces_snapshot()
            if len(forwarder_packets) == 1:
                transport = "ATAK_FORWARDER"
            elif is_ftn:
                transport = "ATAK_FORWARDER_FTN"
            else:
                transport = "ATAK_FORWARDER_FRAGMENTS"
            return {"transport": transport, "count": len(forwarder_packets)}
        except Exception as exc:
            self.logger.warning(
                "ATAK_FORWARDER-Senden fehlgeschlagen, versuche Legacy-COTM-Kurztext-Fallback: "
                f"{exc}"
            )

        try:
            cot_chunks = self._prepare_meshtastic_cot_chunks(normalized_packet)
            if not cot_chunks:
                raise ValueError(EMPTY_MESHTASTIC_COT_ERROR)
            self.logger.debug(
                f"Generic-CoT als Legacy-COTM-Kurztext fragmentiert: paketanzahl={len(cot_chunks)}"
            )
            for chunk in cot_chunks:
                self._send_text_to_interfaces(chunk, interfaces)
                interfaces = self._get_interfaces_snapshot()
                self._remember_recent_chat(self.recent_meshtastic_outbound_texts, chunk)
            return {"transport": "LEGACY_COTM", "count": len(cot_chunks)}
        except Exception as exc:
            self.logger.warning(
                "Legacy-COTM-Kurztext-Senden als letzter Fallback fehlgeschlagen: "
                f"{exc}"
            )
            raise

    def send_cot_to_meshtastic(self, packet_xml):
        """Send a full TAK/CoT ``<event>`` XML payload into the Meshtastic pipeline.

        Returns the same result dictionary as ``_forward_cot_to_meshtastic()``, including
        the selected transport and emitted packet count.
        """
        self._record_service_monitor_event(packet_xml, "<<<", "Manual CoT -> Mesh")
        return self._forward_cot_to_meshtastic(packet_xml)

    def _handle_meshtastic_legacy_cot_text(self, message, from_id):
        return self._handle_meshtastic_cot_chunk(message, from_id)

    def _handle_meshtastic_cot_chunk(self, message, from_id):
        chunk = self._parse_meshtastic_cot_chunk(message)
        if chunk is None:
            return False

        self.logger.debug(
            f"Legacy-COTM-Fragment empfangen: {chunk['part_index']}/{chunk['total_parts']} "
            f"message_id={chunk['message_id']} from={from_id}"
        )
        self._cleanup_expired_meshtastic_cot_fragments()
        cache_key = f"{from_id}:{chunk['message_id']}"
        now = time.time()
        with self.chat_cache_lock:
            entry = self.partial_meshtastic_cot_messages.get(cache_key)
            if entry is None or entry.get("total_parts") != chunk["total_parts"]:
                entry = {
                    "created_at": now,
                    "updated_at": now,
                    "total_parts": chunk["total_parts"],
                    "parts": {},
                }
                self.partial_meshtastic_cot_messages[cache_key] = entry
            entry["updated_at"] = now
            entry["parts"][chunk["part_index"]] = chunk["payload"]
            if len(entry["parts"]) < entry["total_parts"]:
                return True
            expected_part_indices = range(1, entry["total_parts"] + 1)
            if any(index not in entry["parts"] for index in expected_part_indices):
                return True
            encoded_packet = "".join(entry["parts"][index] for index in range(1, entry["total_parts"] + 1))
            self.partial_meshtastic_cot_messages.pop(cache_key, None)

        if not encoded_packet:
            self.logger.warning("Unvollständige CoT-Fragmentserie aus dem Mesh verworfen.")
            return True
        self.logger.debug(
            f"Legacy-COTM-Fragmentfolge vollständig: message_id={chunk['message_id']} "
            f"encoded_chars={len(encoded_packet)}"
        )

        try:
            packet_xml = base64.urlsafe_b64decode(encoded_packet.encode("ascii"))
        except Exception as exc:
            self.logger.warning(f"Mesh-CoT konnte nicht dekodiert werden: {exc}")
            self.logger.debug("Fehler beim Dekodieren einer Mesh-CoT-Nachricht:\n" + traceback.format_exc())
            return True

        return self._forward_meshtastic_cot_xml_to_tak(packet_xml, from_id)

    def send_chat_to_tak(
        self,
        sender_uid,
        callsign,
        message,
        lat,
        lon,
        alt=0.0,
        chatroom=DEFAULT_CHATROOM_NAME,
        recipient_uid=None,
        recipient_callsign=None,
    ):
        try:
            packet_xml = self._build_chat_cot_xml(
                sender_uid,
                callsign,
                message,
                lat,
                lon,
                alt,
                chatroom=chatroom,
                recipient_uid=recipient_uid,
                recipient_callsign=recipient_callsign,
            )
            self._send_packet_to_tak(packet_xml, f"Chat {callsign}")
        except Exception:
            self.logger.error("Fehler in send_chat_to_tak:\n" + traceback.format_exc())

    def process_meshtastic_text_message(self, packet, node=None, source_interface=None):
        chat_payload = self._extract_meshtastic_chat_payload(packet, node=node)
        if not chat_payload:
            return False
        message = chat_payload["message"]

        from_id = packet.get('fromId') or packet.get('from')
        from_num = packet.get('from')
        if (from_id in self.local_node_ids or from_num in self.local_node_numbers) and self._was_seen_recently(
            self.recent_meshtastic_outbound_texts, message
        ):
            self.logger.debug("Echo einer gerade vom Gateway gesendeten Chatnachricht ignoriert.")
            return True

        message_id = packet.get('id')
        message_hash = hashlib.sha256(message.encode("utf-8", errors="ignore")).hexdigest()
        dedupe_key = f"mesh:{message_id}" if message_id is not None else f"mesh:{from_id}:{message_hash}"
        if self._was_seen_recently(self.recent_meshtastic_chat_ids, dedupe_key):
            return True
        self._remember_recent_chat(self.recent_meshtastic_chat_ids, dedupe_key)

        available_interfaces = self._ensure_meshtastic_interfaces()
        if self.relay_text_messages and len(available_interfaces) > 1:
            relay_targets = self._get_relay_targets(source_interface, interfaces=available_interfaces)
            if relay_targets:
                sent_interfaces = self._send_text_to_interfaces(message, relay_targets)
                if sent_interfaces:
                    self._remember_recent_chat(self.recent_meshtastic_outbound_texts, message)
                    source_label = self._get_interface_label(source_interface)
                    target_labels = ", ".join(self._get_interface_label(iface) for iface in sent_interfaces)
                    self.logger.info(
                        f"Relay aktiv: Mesh-Text von {source_label} an {target_labels} weitergeleitet."
                    )

        user = node.get('user', {}) if node else {}
        raw_uid = user.get('id') or (str(from_id) if from_id else "MESH-UNKNOWN")
        sender_uid = chat_payload.get("sender_uid") or normalize_meshtastic_uid(raw_uid)
        callsign = chat_payload.get("callsign") or user.get('longName') or user.get('shortName') or sender_uid
        chatroom = chat_payload.get("chatroom") or DEFAULT_CHATROOM_NAME
        recipient_uid = chat_payload.get("recipient_uid") or chatroom
        recipient_callsign = chat_payload.get("recipient_callsign") or chatroom
        lat, lon, alt = self._resolve_chat_position(sender_uid, node=node)
        self.send_chat_to_tak(
            sender_uid,
            callsign,
            message,
            lat,
            lon,
            alt,
            chatroom=chatroom,
            recipient_uid=recipient_uid,
            recipient_callsign=recipient_callsign,
        )
        self.logger.info(f"Meshtastic-Chat nach TAK weitergeleitet: {callsign}: {message}")
        return True

    def process_meshtastic_pli_message(self, packet, node=None):
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return False
        payload = decoded.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return False
        plugin_version = self._get_atak_plugin_packet_version(packet)
        if plugin_version == 2:
            packet_xml = self._build_meshtastic_takv2_cot_xml(packet, node=node)
            if not packet_xml:
                return False
            from_id = packet.get("fromId") or packet.get("from") or "MESH-UNKNOWN"
            self.logger.debug(
                "ATAK_PLUGIN_V2 aus dem Mesh erkannt und als CoT rekonstruiert: "
                f"from={from_id} payload={_build_safe_payload_snippet(packet_xml)}"
            )
            return self._forward_meshtastic_cot_xml_to_tak(
                packet_xml,
                from_id,
                source_label="ATAK_PLUGIN_V2",
            )
        try:
            atak_payload = _parse_meshtastic_atak_payload(payload)
        except Exception:
            self.logger.debug("ATAK-Plugin-Payload konnte nicht dekodiert werden:\n" + traceback.format_exc())
            return False

        detail_payload = bytes(atak_payload.get("detail") or b"").strip()
        if detail_payload:
            detail_data = detail_payload
            if atak_payload.get("is_compressed"):
                detail_data = self._decode_meshtastic_forwarder_payload(detail_payload)
            normalized_packet = _normalize_tak_xml_payload(detail_data)
            if normalized_packet:
                from_id = packet.get("fromId") or packet.get("from") or "MESH-UNKNOWN"
                metadata = self._extract_cot_event_metadata(normalized_packet)
                if metadata and metadata.get("is_marker"):
                    self.logger.debug(
                        "ATAK_PLUGIN-detail=7 Marker-CoT aus dem Mesh erkannt: "
                        f"from={from_id} uid={metadata['uid']} type={metadata['type']} "
                        f"how={metadata['how']} compressed={atak_payload.get('is_compressed')}"
                    )
                self.logger.debug(
                    "ATAK_PLUGIN-detail=7 aus dem Mesh erkannt und als CoT rekonstruiert: "
                    f"from={from_id} compressed={atak_payload.get('is_compressed')} "
                    f"payload={_build_safe_payload_snippet(normalized_packet)}"
                )
                return self._forward_meshtastic_cot_xml_to_tak(
                    normalized_packet,
                    from_id,
                    source_label="ATAK_PLUGIN-detail=7",
                )
            else:
                self.logger.warning(
                    "ATAK_PLUGIN-detail=7 konnte nach Decode/Reassembly nicht als valides CoT rekonstruiert werden: "
                    f"compressed={atak_payload.get('is_compressed')} payload={_build_safe_payload_snippet(detail_data)}"
                )
            self.logger.debug(
                "ATAK_PLUGIN-Paket enthält detail=7 und wird NICHT als PLI fehlinterpretiert."
            )
            return True

        packet_xml = self._build_meshtastic_pli_cot_xml(packet, node=node)
        if not packet_xml:
            return False
        from_id = packet.get("fromId") or packet.get("from") or "MESH-UNKNOWN"
        self.logger.debug(
            "ATAK_PLUGIN-PLI aus dem Mesh erkannt und als CoT rekonstruiert: "
            f"from={from_id} payload={_build_safe_payload_snippet(packet_xml)}"
        )
        return self._forward_meshtastic_cot_xml_to_tak(
            packet_xml,
            from_id,
            source_label="ATAK_PLUGIN-PLI",
        )

    def _extract_tak_chat_payload(self, packet_xml):
        try:
            root = fromstring(packet_xml)
        except Exception:
            return None

        if _xml_local_name(root.tag) != "event":
            return None

        detail = _find_child_by_local_name(root, "detail")
        if detail is None:
            return None

        chat = _find_descendant_by_local_name(detail, "__chat")
        if chat is None:
            chat = _find_descendant_by_local_name(detail, "chat")
        remarks = _find_descendant_by_local_name(detail, "remarks")
        chat_note = _find_descendant_by_local_name(detail, "_chat")
        chatgrp = _find_descendant_by_local_name(detail, "chatgrp")
        source_id_node = _find_descendant_by_local_names(detail, "sourceid", "sourceId", "sourceID")
        destination_node = _find_descendant_by_local_names(detail, "to", "destination", "dest")
        source_id_attr = _find_descendant_attribute_value(detail, "sourceid", "sourceId", "sourceID")
        destination_attr = _find_descendant_attribute_value(detail, "to", "destination", "dest")
        chatroom_attr = _find_descendant_attribute_value(detail, "chatroom", "chatRoom", "name")
        event_uid = root.get("uid") or ""
        event_type = str(root.get("type") or "").lower()
        has_chat_identity = event_uid.startswith(GEOCHAT_UID_PREFIX) or event_type.startswith("b-t-f")
        has_chat_elements = any(element is not None for element in (chat, chat_note, chatgrp))
        has_chat_remarks = _looks_like_tak_chat_remarks(remarks)
        has_chat_message_fields = _has_tak_chat_message_fields(detail)
        has_chat_routing_hints = any(
            value
            for value in (
                source_id_node is not None,
                destination_node is not None,
                source_id_attr,
                destination_attr,
                chatroom_attr,
            )
        )
        generic_message_nodes = []
        for element in detail.iter():
            if element is detail:
                continue
            if _is_tak_chat_message_local_name(_xml_local_name(element.tag)):
                generic_message_nodes.append(element)

        message = ""
        if remarks is not None and remarks.text:
            message = _extract_latest_wintak_chat_message(remarks.text)
        note = _find_descendant_by_local_name(detail, "note")
        if not message:
            for element in (note, chat_note, chat, remarks, chatgrp, *reversed(generic_message_nodes)):
                if element is None:
                    continue
                candidate_text = _extract_latest_wintak_chat_message(_collect_xml_text(element))
                if candidate_text:
                    message = candidate_text
                    break
                message = _extract_latest_wintak_chat_attribute_message(element)
                if message:
                    break
        if not message:
            message = _extract_latest_wintak_chat_attribute_message(detail)
        if not message:
            return None
        if not (
            has_chat_identity
            or has_chat_elements
            or has_chat_remarks
            or has_chat_message_fields
            or has_chat_routing_hints
        ):
            return None

        link = _find_descendant_by_local_name(detail, "link")
        contact = _find_descendant_by_local_name(detail, "contact")
        uid_parts = [part for part in event_uid.split(".") if part]
        sender_uid = ""
        if link is not None and link.get("uid"):
            sender_uid = link.get("uid")
        if not sender_uid and remarks is not None:
            sender_uid = remarks.get("sourceID") or remarks.get("source") or sender_uid
        if not sender_uid and source_id_node is not None:
            sender_uid = (
                source_id_node.get("uid")
                or source_id_node.get("id")
                or _collect_xml_text(source_id_node)
                or sender_uid
            )
        if not sender_uid and source_id_attr:
            sender_uid = source_id_attr
        if not sender_uid and event_uid.startswith(GEOCHAT_UID_PREFIX) and len(uid_parts) >= 2:
            sender_uid = uid_parts[1]
        if not sender_uid and chatgrp is not None:
            sender_uid = chatgrp.get("uid0") or chatgrp.get("uid")
        if not sender_uid:
            sender_uid = event_uid
        sender_uid = _strip_tak_sender_prefix(sender_uid)

        sender_callsign = chat.get("senderCallsign") if chat is not None else None
        if not sender_callsign and contact is not None:
            sender_callsign = contact.get("callsign")
        if not sender_callsign and chat is not None:
            sender_callsign = chat.get("sender") or chat.get("callsign")
        if not sender_callsign and chatgrp is not None:
            sender_callsign = chatgrp.get("uid0") or chatgrp.get("name")
        if not sender_callsign and remarks is not None:
            sender_callsign = remarks.get("source")
        if not sender_callsign:
            sender_callsign = _find_descendant_attribute_value(
                detail,
                "senderCallsign",
                "sender",
                "callsign",
                "source",
            )
        sender_callsign = _strip_tak_sender_prefix(sender_callsign)
        if not sender_callsign:
            sender_callsign = "UNKNOWN-SENDER"

        recipient_uid = ""
        recipient_callsign = DEFAULT_CHATROOM_NAME
        if chat is not None:
            recipient_uid = chat.get("id") or chat.get("uid") or recipient_uid
            recipient_callsign = (
                chat.get("chatroom")
                or chat.get("chatRoom")
                or chat.get("name")
                or recipient_callsign
            )
        if chatgrp is not None:
            recipient_uid = recipient_uid or chatgrp.get("uid1") or chatgrp.get("id")
            if recipient_callsign == DEFAULT_CHATROOM_NAME:
                recipient_callsign = chatgrp.get("name") or chatgrp.get("id") or recipient_callsign
        if recipient_callsign == DEFAULT_CHATROOM_NAME and len(uid_parts) >= 3:
            recipient_callsign = uid_parts[2] or recipient_callsign
        if remarks is not None:
            remarks_to = remarks.get("to")
            if _looks_like_valid_tak_uid(remarks_to):
                recipient_uid = remarks_to or recipient_uid
        if not recipient_uid and destination_node is not None:
            recipient_uid = (
                destination_node.get("uid")
                or destination_node.get("id")
                or _collect_xml_text(destination_node)
                or recipient_uid
            )
        if not recipient_uid and destination_attr:
            recipient_uid = destination_attr
        if not recipient_uid:
            recipient_uid = recipient_callsign or DEFAULT_CHATROOM_NAME
        if recipient_callsign == DEFAULT_CHATROOM_NAME and chatroom_attr:
            recipient_callsign = chatroom_attr
        if recipient_callsign == DEFAULT_CHATROOM_NAME and recipient_uid:
            recipient_callsign = recipient_uid
        chatroom = recipient_callsign or recipient_uid or DEFAULT_CHATROOM_NAME

        return {
            "event_uid": root.get("uid"),
            "sender_uid": sender_uid,
            "sender_callsign": sender_callsign,
            "chatroom": chatroom,
            "recipient_uid": recipient_uid,
            "recipient_callsign": recipient_callsign,
            "message": message,
        }

    def _send_text_to_meshtastic(self, message, prepared_chunks=None):
        interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
        chunks = prepared_chunks if prepared_chunks is not None else self._prepare_meshtastic_text_chunks(message)
        if not chunks:
            raise ValueError("Leere TAK-Chatnachricht kann nicht ins Mesh gesendet werden.")
        total_sent = 0
        for chunk in chunks:
            sent_interfaces = self._send_text_to_interfaces(chunk, interfaces)
            total_sent += len(sent_interfaces)
            interfaces = self._get_interfaces_snapshot()
        if total_sent <= 0:
            raise RuntimeError("TAK-Chat konnte an kein Meshtastic-Interface gesendet werden.")
        return total_sent

    def _build_meshtastic_geochat_payload(self, chat_payload):
        message = str(chat_payload.get("message") or "").strip()
        if not message:
            raise ValueError("Leere TAK-Chatnachricht kann nicht als ATAK GeoChat kodiert werden.")

        sender_uid = str(chat_payload.get("sender_uid") or self.gateway_uid or "UNKNOWN-SENDER").strip() or "UNKNOWN-SENDER"
        sender_callsign = (
            str(chat_payload.get("sender_callsign") or self.gateway_callsign or sender_uid).strip() or sender_uid
        )
        recipient_uid = (
            str(chat_payload.get("recipient_uid") or chat_payload.get("chatroom") or DEFAULT_CHATROOM_NAME).strip()
            or DEFAULT_CHATROOM_NAME
        )
        recipient_callsign = (
            str(chat_payload.get("recipient_callsign") or chat_payload.get("chatroom") or recipient_uid).strip()
            or recipient_uid
        )

        contact_payload = self._build_meshtastic_contact_payload(
            {
                "callsign": sender_callsign,
                "device_callsign": sender_uid,
            }
        )
        geochat_payload = b"".join(
            (
                _encode_protobuf_string_field(1, message),
                _encode_protobuf_string_field(2, recipient_uid),
                _encode_protobuf_string_field(3, recipient_callsign),
            )
        )
        payload = b"".join(
            (
                _encode_protobuf_message_field(2, contact_payload),
                _encode_protobuf_message_field(6, geochat_payload),
            )
        )
        if not payload:
            raise ValueError("ATAK GeoChat-Payload konnte nicht erzeugt werden.")
        return payload

    def _resolve_meshtastic_chat_destination_id(self, chat_payload):
        recipient_uid = chat_payload.get("recipient_uid")
        chatroom = chat_payload.get("chatroom")
        recipient_callsign = chat_payload.get("recipient_callsign")
        return resolve_meshtastic_destination_id(
            recipient_uid or chatroom or recipient_callsign or DEFAULT_CHATROOM_NAME
        )

    def _send_tak_chat_to_meshtastic(self, chat_payload):
        payload = self._build_meshtastic_geochat_payload(chat_payload)
        interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
        destination_id = self._resolve_meshtastic_chat_destination_id(chat_payload)
        if any(getattr(iface, "sendData", None) is None for iface in interfaces):
            sent_chunks = self._prepare_meshtastic_text_chunks(chat_payload.get("message"))
            total_sent = self._send_text_to_meshtastic(chat_payload.get("message"), prepared_chunks=sent_chunks)
            return {
                "transport": "TEXT_MESSAGE_APP",
                "count": total_sent,
                "chunks": len(sent_chunks),
            }
        try:
            sent_interfaces = self._send_data_to_interfaces(
                payload,
                interfaces,
                MESHTASTIC_ATAK_PLUGIN_PORTNUM,
                destination_id=destination_id,
            )
            return {
                "transport": "ATAK_PLUGIN_CHAT",
                "count": len(sent_interfaces),
                "chunks": 1,
            }
        except Exception as exc:
            self.logger.warning(
                "ATAK_PLUGIN-Chat-Senden fehlgeschlagen, verwende TEXT_MESSAGE_APP-Fallback: "
                f"{exc}"
            )
            sent_chunks = self._prepare_meshtastic_text_chunks(chat_payload.get("message"))
            total_sent = self._send_text_to_meshtastic(chat_payload.get("message"), prepared_chunks=sent_chunks)
            return {
                "transport": "TEXT_MESSAGE_APP",
                "count": total_sent,
                "chunks": len(sent_chunks),
            }

    def _normalize_inbound_tak_packet(self, packet_xml):
        return _normalize_tak_xml_payload(packet_xml)

    def _extract_tak_events_from_stream_buffer(self, buffer_text):
        events = []
        last_end = 0
        for match in TAK_EVENT_PATTERN.finditer(buffer_text):
            events.append(match.group(0))
            last_end = match.end()
        remaining_buffer = buffer_text[last_end:]
        if len(remaining_buffer) > MAX_TCP_STREAM_BUFFER_BYTES:
            event_start = remaining_buffer.rfind("<event")
            if event_start >= 0:
                remaining_buffer = remaining_buffer[event_start:]
            else:
                partial_tag_start = remaining_buffer.rfind("<")
                if partial_tag_start >= 0:
                    remaining_buffer = remaining_buffer[partial_tag_start:]
                else:
                    remaining_buffer = remaining_buffer[-TCP_STREAM_BUFFER_TAIL_BYTES:]
        return events, remaining_buffer

    def _iter_tak_listener_ports(self):
        seen = set()
        for port in (self.chat_listen_port, self.tak_port):
            if port in seen:
                continue
            seen.add(port)
            yield port

    def _create_chat_listener_socket(self, listen_port):
        requested_ip = self.chat_listen_ip
        bind_attempts = []
        if requested_ip in ("0.0.0.0", "", "*"):
            bind_attempts.append((socket.AF_INET, "0.0.0.0", False))
            bind_attempts.append((socket.AF_INET6, "::", True))
        else:
            try:
                addr_info = socket.getaddrinfo(
                    requested_ip,
                    listen_port,
                    socket.AF_UNSPEC,
                    socket.SOCK_DGRAM,
                    0,
                    socket.AI_PASSIVE,
                )
            except socket.gaierror as exc:
                raise OSError(f"Ungültige Chat-Listener-IP '{requested_ip}': {exc}") from exc

            seen = set()
            for family, _, _, _, sockaddr in addr_info:
                host = sockaddr[0]
                if (family, host) in seen:
                    continue
                seen.add((family, host))
                should_enable_dual_stack = family == socket.AF_INET6 and host == "::"
                bind_attempts.append((family, host, should_enable_dual_stack))

        last_error = None
        for family, bind_ip, want_dual_stack in bind_attempts:
            try:
                sock = socket.socket(family, socket.SOCK_DGRAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if family == socket.AF_INET6:
                    try:
                        sock.setsockopt(
                            socket.IPPROTO_IPV6,
                            socket.IPV6_V6ONLY,
                            0 if want_dual_stack else 1,
                        )
                    except (AttributeError, OSError):
                        pass
                sock.settimeout(1.0)
                sock.bind((bind_ip, listen_port))
                return sock, bind_ip
            except OSError as exc:
                logger = getattr(self, "logger", None)
                if logger is not None:
                    logger.debug(
                        f"TAK-CoT-Listener Bind fehlgeschlagen auf {bind_ip}:{listen_port} "
                        f"(family={family}): {exc}"
                    )
                last_error = exc
        if last_error is not None:
            raise last_error
        raise OSError("Kein Chat-Listener-Socket konnte erstellt werden.")

    def _create_tak_multicast_listener_socket(self, multicast_group, listen_port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        sock.settimeout(1.0)
        bind_targets = [(multicast_group, listen_port)]
        if self.tak_multicast_interface_ip not in ("0.0.0.0", "", "*"):
            bind_targets.append((self.tak_multicast_interface_ip, listen_port))
        last_error = None
        for bind_target in bind_targets:
            try:
                sock.bind(bind_target)
                break
            except OSError as exc:
                last_error = exc
        else:
            raise OSError(
                f"Multicast-Bind fehlgeschlagen für {multicast_group}:{listen_port}"
            ) from last_error
        membership_request = socket.inet_aton(multicast_group) + socket.inet_aton(
            self.tak_multicast_interface_ip
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)
        return sock

    def _create_chat_tcp_listener_socket(self, listen_port):
        requested_ip = self.tcp_chat_listen_ip
        bind_attempts = []
        if requested_ip in ("0.0.0.0", "", "*"):
            bind_attempts.append((socket.AF_INET, "0.0.0.0", False))
            bind_attempts.append((socket.AF_INET6, "::", True))
        else:
            try:
                addr_info = socket.getaddrinfo(
                    requested_ip,
                    listen_port,
                    socket.AF_UNSPEC,
                    socket.SOCK_STREAM,
                    0,
                    socket.AI_PASSIVE,
                )
            except socket.gaierror as exc:
                raise OSError(f"Ungültige TCP-Listener-IP '{requested_ip}': {exc}") from exc

            seen = set()
            for family, _, _, _, sockaddr in addr_info:
                host = sockaddr[0]
                if (family, host) in seen:
                    continue
                seen.add((family, host))
                should_enable_dual_stack = family == socket.AF_INET6 and host == "::"
                bind_attempts.append((family, host, should_enable_dual_stack))

        last_error = None
        for family, bind_ip, want_dual_stack in bind_attempts:
            try:
                sock = socket.socket(family, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if family == socket.AF_INET6:
                    try:
                        sock.setsockopt(
                            socket.IPPROTO_IPV6,
                            socket.IPV6_V6ONLY,
                            0 if want_dual_stack else 1,
                        )
                    except (AttributeError, OSError):
                        pass
                sock.settimeout(TCP_SOCKET_TIMEOUT_SECONDS)
                sock.bind((bind_ip, listen_port))
                sock.listen(TCP_LISTENER_BACKLOG)
                return sock, bind_ip
            except OSError as exc:
                logger = getattr(self, "logger", None)
                if logger is not None:
                    logger.debug(
                        f"TAK-TCP-Listener Bind fehlgeschlagen auf {bind_ip}:{listen_port} "
                        f"(family={family}): {exc}"
                    )
                last_error = exc
        if last_error is not None:
            raise last_error
        raise OSError("Kein TCP-Listener-Socket konnte erstellt werden.")

    def _log_inbound_tak_diagnostics(
        self,
        source_addr=None,
        source_protocol=None,
        listener_port=None,
        packet_size=None,
        was_normalized=None,
        is_cot_event=None,
        is_chat_payload=None,
        payload_snippet=None,
        discard_reason=None,
    ):
        if not self.logger.isEnabledFor(logging.DEBUG):
            return

        def _format_flag(value):
            if value is True:
                return "ja"
            if value is False:
                return "nein"
            return "unbekannt"

        message = (
            "TAK-Inbound-Diagnose: "
            f"proto={str(source_protocol or DEFAULT_SOURCE_PROTOCOL).upper()} "
            f"quelle={_format_network_endpoint(source_addr)} "
            f"listener_port={listener_port if listener_port is not None else '-'} "
            f"groesse={packet_size if packet_size is not None else '-'} "
            f"normalisiert={_format_flag(was_normalized)} "
            f"cot_event={_format_flag(is_cot_event)} "
            f"chat_payload={_format_flag(is_chat_payload)}"
        )
        if discard_reason:
            message += f" grund={discard_reason}"
        if payload_snippet:
            message += f" snippet={payload_snippet!r}"
        self.logger.debug(message)

    def handle_inbound_tak_packet(
        self,
        packet_xml,
        source_addr=None,
        source_protocol=None,
        listener_port=None,
        packet_size=None,
        was_normalized=None,
    ):
        metadata = self._extract_cot_event_metadata(packet_xml)

        chat_payload = self._extract_tak_chat_payload(packet_xml)
        self._log_inbound_tak_diagnostics(
            source_addr=source_addr,
            source_protocol=source_protocol,
            listener_port=listener_port,
            packet_size=packet_size if packet_size is not None else len(_ensure_bytes(packet_xml)),
            was_normalized=was_normalized,
            is_cot_event=metadata is not None,
            is_chat_payload=bool(chat_payload),
        )

        if chat_payload:
            sender_uid = chat_payload["sender_uid"] or self.gateway_uid
            chat_is_echo = sender_uid in self.local_node_ids
            dedupe_key = chat_payload["event_uid"] or f"tak:{sender_uid}:{chat_payload['message']}"
            if not chat_is_echo:
                chat_is_echo = self._was_seen_recently(self.recent_tak_chat_ids, dedupe_key)
            self._record_service_monitor_event(
                packet_xml,
                "<<<",
                f"TAK {str(source_protocol or DEFAULT_SOURCE_PROTOCOL).upper()} {_format_network_endpoint(source_addr)}".strip(),
                is_echo_back=chat_is_echo,
            )
            if sender_uid in self.local_node_ids:
                self.logger.debug("TAK-Chat vom lokalen Meshtastic-Knoten ignoriert, um Echos zu vermeiden.")
                return

            if self._was_seen_recently(self.recent_tak_chat_ids, dedupe_key):
                self.logger.debug("TAK-Chat wegen Duplikat-Schutz ignoriert.")
                return

            # Notify the UI so it can display the received WinTAK message.
            cb = self.wintak_tcp_chat_callback
            if cb:
                try:
                    cb("chat", chat_payload["sender_callsign"], chat_payload["message"], source_addr)
                except Exception:
                    pass

            try:
                send_result = self._send_tak_chat_to_meshtastic(chat_payload)
            except Exception as exc:
                self.logger.warning(f"TAK-Chat konnte nicht ins Mesh gesendet werden: {exc}")
                self.logger.debug("Fehler beim Senden von TAK-Chat ins Mesh:\n" + traceback.format_exc())
                return
            self._remember_recent_chat(self.recent_tak_chat_ids, dedupe_key)
            self._remember_recent_chat(self.recent_meshtastic_outbound_texts, chat_payload["message"])
            src = chat_payload["sender_callsign"]
            transport = send_result.get("transport")
            total_sent = int(send_result.get("count") or 0)
            sent_chunk_count = int(send_result.get("chunks") or 1)
            destination_label = chat_payload.get("recipient_callsign") or chat_payload.get("recipient_uid")
            if transport == "ATAK_PLUGIN_CHAT":
                self.logger.info(
                    f"TAK-Chat als ATAK GeoChat ins Mesh über {total_sent} Interface(s) gesendet: "
                    f"{src} -> {destination_label}: {chat_payload['message']}"
                )
            elif sent_chunk_count > 1:
                self.logger.info(
                    f"TAK-Chat wurde in {sent_chunk_count} Mesh-Nachrichten über {total_sent} Interface(s) gesendet: "
                    f"{src}: {chat_payload['message']}"
                )
            else:
                self.logger.info(
                    f"TAK-Chat ins Mesh über {total_sent} Interface(s) gesendet: {src}: {chat_payload['message']}"
                )
            return

        cot_dedupe_key = None
        monitor_source = (
            f"TAK {str(source_protocol or DEFAULT_SOURCE_PROTOCOL).upper()} "
            f"{_format_network_endpoint(source_addr)}"
        ).strip()
        if metadata is not None:
            cot_dedupe_key = self._build_cot_dedupe_key(packet_xml)
            duplicate_cot = bool(cot_dedupe_key and self._was_seen_recently(self.recent_cot_ids, cot_dedupe_key))
            is_echo_back = duplicate_cot
            if not is_echo_back and metadata.get("has_meshtastic_marker"):
                is_echo_back = True
            self._record_service_monitor_event(
                packet_xml,
                "<<<",
                monitor_source,
                is_echo_back=is_echo_back,
            )
            if duplicate_cot:
                self.logger.debug("TAK-CoT wegen Duplikat-Schutz ignoriert.")
                return
            if metadata.get("has_meshtastic_marker"):
                self.logger.debug(
                    f"TAK-CoT mit __meshtastic-Marker nicht erneut ins Mesh gesendet: {metadata['uid']}"
                )
                return

        if metadata is None:
            self._log_inbound_tak_diagnostics(
                source_addr=source_addr,
                source_protocol=source_protocol,
                listener_port=listener_port,
                packet_size=packet_size if packet_size is not None else len(_ensure_bytes(packet_xml)),
                was_normalized=was_normalized,
                is_cot_event=False,
                is_chat_payload=False,
                payload_snippet=_build_safe_payload_snippet(packet_xml),
                discard_reason="nicht als CoT oder Chat erkannt",
            )
            return

        try:
            send_result = self._forward_cot_to_meshtastic(packet_xml)
        except Exception as exc:
            self.logger.warning(f"TAK-CoT konnte nicht ins Mesh gesendet werden: {exc}")
            self.logger.debug("Fehler beim Senden von TAK-CoT ins Mesh:\n" + traceback.format_exc())
            return
        if cot_dedupe_key:
            self._remember_recent_chat(self.recent_cot_ids, cot_dedupe_key)
        transport_label = "ATAK_FORWARDER-Paket"
        transport = send_result.get("transport")
        transport_count = int(send_result.get("count") or 0)
        cot_subject = _get_cot_subject_label(metadata)
        if transport == "ATAK_PLUGIN":
            transport_label = "ATAK_PLUGIN-PLI"
        elif transport == "ATAK_PLUGIN_DETAIL":
            transport_label = "ATAK_PLUGIN-detail=7"
        elif transport == "ATAK_FORWARDER_FRAGMENTS":
            transport_label = (
                f"{transport_count} ATAK_FORWARDER-Fragment{'e' if transport_count != 1 else ''}"
            )
        elif transport == "ATAK_FORWARDER_FTN":
            transport_label = (
                f"{transport_count} ATAK_FORWARDER-FTN-Block{'s' if transport_count != 1 else ''}"
            )
        elif transport == "LEGACY_COTM":
            transport_label = (
                f"{transport_count} Legacy-Fragment{'e' if transport_count != 1 else ''}"
            )
        self.logger.info(
            f"{cot_subject} ins Mesh gesendet: {metadata['uid'] or 'ohne UID'} ({transport_label})"
        )

    def handle_tak_chat_message(self, packet_xml, source_addr=None, source_protocol=None):
        """Backward-compatible alias for existing callers of the TAK listener packet handler."""
        self.handle_inbound_tak_packet(packet_xml, source_addr=source_addr, source_protocol=source_protocol)

    def listen_for_tak_chat(self, listen_port=None):
        if listen_port is None:
            listen_port = self.chat_listen_port
        try:
            sock, bind_ip = self._create_chat_listener_socket(listen_port)
            self.sock_chat_listeners.append(sock)
            if listen_port == self.chat_listen_port:
                listener_hint = "konfigurierter Chat/CoT-Eingang"
            elif listen_port == self.tak_port:
                listener_hint = "Fallback auf dem normalen Local-TAK-Port"
            else:
                listener_hint = "zusätzlicher TAK-CoT-Eingang"
            self.logger.info(
                f"TAK-CoT-Listener aktiv auf {bind_ip}:{listen_port} "
                f"({listener_hint})."
            )
        except Exception as e:
            self.logger.warning(f"TAK-CoT-Listener auf Port {listen_port} konnte nicht gestartet werden: {e}")
            return

        while not self.shutdown_flag.is_set():
            try:
                packet_xml, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception:
                self.logger.debug("Fehler beim Empfangen von TAK-Chat:\n" + traceback.format_exc())
                continue
            raw_packet_bytes = _ensure_bytes(packet_xml)
            packet_size = len(raw_packet_bytes)
            normalized_packet = self._normalize_inbound_tak_packet(packet_xml)
            was_normalized = bool(normalized_packet) and _ensure_bytes(normalized_packet) != raw_packet_bytes
            if not normalized_packet:
                self._log_inbound_tak_diagnostics(
                    source_addr=addr,
                    source_protocol="UDP",
                    listener_port=listen_port,
                    packet_size=packet_size,
                    was_normalized=False,
                    is_cot_event=False,
                    is_chat_payload=False,
                    payload_snippet=_build_safe_payload_snippet(packet_xml),
                    discard_reason="Paket konnte nicht zu einem TAK-Event normalisiert werden",
                )
                continue
            self.handle_inbound_tak_packet(
                normalized_packet,
                source_addr=addr,
                source_protocol="UDP",
                listener_port=listen_port,
                packet_size=packet_size,
                was_normalized=was_normalized,
            )

    def listen_for_tak_multicast(self, multicast_group, listen_port):
        try:
            sock = self._create_tak_multicast_listener_socket(multicast_group, listen_port)
            self.sock_chat_listeners.append(sock)
            self.logger.info(
                f"TAK-Multicast-Listener aktiv auf {multicast_group}:{listen_port} "
                f"(Interface {self.tak_multicast_interface_ip})."
            )
        except Exception as e:
            self.logger.warning(
                f"TAK-Multicast-Listener auf {multicast_group}:{listen_port} konnte nicht gestartet werden: {e}"
            )
            return

        while not self.shutdown_flag.is_set():
            try:
                packet_xml, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception:
                self.logger.debug(
                    "Fehler beim Empfangen von TAK-Multicast:\n" + traceback.format_exc()
                )
                continue
            raw_packet_bytes = _ensure_bytes(packet_xml)
            packet_size = len(raw_packet_bytes)
            normalized_packet = self._normalize_inbound_tak_packet(packet_xml)
            was_normalized = bool(normalized_packet) and _ensure_bytes(normalized_packet) != raw_packet_bytes
            if not normalized_packet:
                self._log_inbound_tak_diagnostics(
                    source_addr=addr,
                    source_protocol="UDP-MULTICAST",
                    listener_port=listen_port,
                    packet_size=packet_size,
                    was_normalized=False,
                    is_cot_event=False,
                    is_chat_payload=False,
                    payload_snippet=_build_safe_payload_snippet(packet_xml),
                    discard_reason=(
                        f"Multicast-Paket von {multicast_group}:{listen_port} "
                        "konnte nicht zu einem TAK-Event normalisiert werden"
                    ),
                )
                continue
            self.handle_inbound_tak_packet(
                normalized_packet,
                source_addr=addr,
                source_protocol="UDP-MULTICAST",
                listener_port=listen_port,
                packet_size=packet_size,
                was_normalized=was_normalized,
            )

    def _consume_tak_tcp_stream(
        self,
        conn,
        addr,
        *,
        source_protocol="TCP",
        listener_port=None,
        ping_label="TAK-TCP",
        send_peer=None,
    ):
        buffer_text = ""
        probe_buffer = b""
        decoder = None
        conn.settimeout(TCP_SOCKET_TIMEOUT_SECONDS)
        try:
            while not self.shutdown_flag.is_set():
                try:
                    data = conn.recv(TCP_RECV_BUFFER_SIZE)
                    if not data:
                        break
                    if decoder is None:
                        probe_buffer += data
                        detected_encoding = _detect_tak_stream_encoding(probe_buffer)
                        if detected_encoding is None:
                            if len(probe_buffer) > MAX_TCP_STREAM_BUFFER_BYTES:
                                probe_buffer = probe_buffer[-TCP_STREAM_BUFFER_TAIL_BYTES:]
                            continue
                        decoder = codecs.getincrementaldecoder(detected_encoding)(errors="ignore")
                        data = probe_buffer
                        probe_buffer = b""
                    buffer_text += decoder.decode(data)
                    events, buffer_text = self._extract_tak_events_from_stream_buffer(buffer_text)
                    for packet_xml in events:
                        # Respond to WinTAK/ATAK keepalive pings with a pong so the
                        # connection stays alive long enough to receive chat messages.
                        if TAK_PING_TYPE_PATTERN.search(packet_xml) and not TAK_PONG_TYPE_PATTERN.search(packet_xml):
                            try:
                                if send_peer is not None:
                                    if not self._send_local_tak_tcp_payload(
                                        send_peer,
                                        self._build_pong_xml(),
                                        log_errors=False,
                                    ):
                                        return
                                else:
                                    conn.sendall(self._build_pong_xml().encode("utf-8") + b"\n")
                                self.logger.debug(
                                    f"{ping_label}-Ping von {_format_network_endpoint(addr)} "
                                    "beantwortet (Pong gesendet)."
                                )
                            except OSError:
                                return
                            continue
                        raw_packet_bytes = _ensure_bytes(packet_xml)
                        normalized_packet = self._normalize_inbound_tak_packet(packet_xml)
                        packet_size = len(raw_packet_bytes)
                        was_normalized = bool(normalized_packet) and _ensure_bytes(normalized_packet) != raw_packet_bytes
                        if not normalized_packet:
                            self._log_inbound_tak_diagnostics(
                                source_addr=addr,
                                source_protocol=source_protocol,
                                listener_port=listener_port,
                                packet_size=packet_size,
                                was_normalized=False,
                                is_cot_event=False,
                                is_chat_payload=False,
                                payload_snippet=_build_safe_payload_snippet(packet_xml),
                                discard_reason="TCP-Event konnte nicht zu einem TAK-Event normalisiert werden",
                            )
                            continue
                        self.handle_inbound_tak_packet(
                            normalized_packet,
                            source_addr=addr,
                            source_protocol=source_protocol,
                            listener_port=listener_port,
                            packet_size=packet_size,
                            was_normalized=was_normalized,
                        )
                except socket.timeout:
                    continue
            if decoder is not None:
                buffer_text += decoder.decode(b"", final=True)
            elif probe_buffer:
                probe_buffer_bytes = _ensure_bytes(probe_buffer)
                normalized_packet = self._normalize_inbound_tak_packet(probe_buffer)
                if normalized_packet:
                    self.handle_inbound_tak_packet(
                        normalized_packet,
                        source_addr=addr,
                        source_protocol=source_protocol,
                        listener_port=listener_port,
                        packet_size=len(probe_buffer_bytes),
                        was_normalized=_ensure_bytes(normalized_packet) != probe_buffer_bytes,
                    )
                else:
                    self._log_inbound_tak_diagnostics(
                        source_addr=addr,
                        source_protocol=source_protocol,
                        listener_port=listener_port,
                        packet_size=len(probe_buffer_bytes),
                        was_normalized=False,
                        is_cot_event=False,
                        is_chat_payload=False,
                        payload_snippet=_build_safe_payload_snippet(probe_buffer),
                        discard_reason="TCP-Probe konnte nicht zu einem TAK-Event normalisiert werden",
                    )
            events, buffer_text = self._extract_tak_events_from_stream_buffer(buffer_text)
            for packet_xml in events:
                raw_packet_bytes = _ensure_bytes(packet_xml)
                normalized_packet = self._normalize_inbound_tak_packet(packet_xml)
                packet_size = len(raw_packet_bytes)
                was_normalized = bool(normalized_packet) and _ensure_bytes(normalized_packet) != raw_packet_bytes
                if not normalized_packet:
                    self._log_inbound_tak_diagnostics(
                        source_addr=addr,
                        source_protocol=source_protocol,
                        listener_port=listener_port,
                        packet_size=packet_size,
                        was_normalized=False,
                        is_cot_event=False,
                        is_chat_payload=False,
                        payload_snippet=_build_safe_payload_snippet(packet_xml),
                        discard_reason="TCP-Stream-Event konnte nicht zu einem TAK-Event normalisiert werden",
                    )
                    continue
                self.handle_inbound_tak_packet(
                    normalized_packet,
                    source_addr=addr,
                    source_protocol=source_protocol,
                    listener_port=listener_port,
                    packet_size=packet_size,
                    was_normalized=was_normalized,
                )
        except (OSError, UnicodeError, ValueError):
            self.logger.debug("Fehler beim Lesen einer TAK-TCP-Verbindung:\n" + traceback.format_exc())

    def _handle_tak_tcp_client(self, conn, addr):
        peer = self._register_local_tak_tcp_peer(conn, _format_network_endpoint(addr))
        cb = self.wintak_tcp_chat_callback
        if cb:
            try:
                cb("connect", None, None, addr)
            except Exception:
                pass
        try:
            self._consume_tak_tcp_stream(
                conn,
                addr,
                source_protocol="TCP",
                listener_port=self.tcp_chat_listen_port,
                ping_label="TAK-TCP-Listener",
                send_peer=peer,
            )
        finally:
            self._unregister_local_tak_tcp_peer(peer)
            if cb:
                try:
                    cb("disconnect", None, None, addr)
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass

    def receive_tak_chat_tcp(self):
        target_addr = (self.tcp_chat_receiver_host, self.tcp_chat_receiver_port)
        while not self.shutdown_flag.is_set():
            conn = None
            peer = None
            cb = self.wintak_tcp_chat_callback
            try:
                conn = socket.create_connection(target_addr, timeout=TCP_SOCKET_TIMEOUT_SECONDS)
                conn.settimeout(TCP_SOCKET_TIMEOUT_SECONDS)
                peer = self._register_local_tak_tcp_peer(
                    conn,
                    f"{self.tcp_chat_receiver_host}:{self.tcp_chat_receiver_port}",
                )
                self.logger.info(
                    f"TAK-TCP-Receiver verbunden mit {self.tcp_chat_receiver_host}:{self.tcp_chat_receiver_port}."
                )
                # Expose this socket for bidirectional CoT push to WinTAK (marker delivery).
                with self._local_tak_tcp_push_lock:
                    self._local_tak_tcp_push_conn = conn
                    self._local_tak_tcp_push_peer = peer
                if cb:
                    try:
                        cb("connect", None, None, target_addr)
                    except Exception:
                        pass
                self._consume_tak_tcp_stream(
                    conn,
                    target_addr,
                    source_protocol="TCP-CLIENT",
                    listener_port=self.tcp_chat_receiver_port,
                    ping_label="TAK-TCP-Receiver",
                    send_peer=peer,
                )
            except OSError as exc:
                self.logger.debug(
                    "TAK-TCP-Receiver-Verbindung fehlgeschlagen oder beendet: "
                    f"{self.tcp_chat_receiver_host}:{self.tcp_chat_receiver_port} ({exc})"
                )
            finally:
                # Clear the push socket reference when the connection drops so that
                # _send_packet_to_tak() does not attempt to write to a closed socket.
                self._unregister_local_tak_tcp_peer(peer)
                with self._local_tak_tcp_push_lock:
                    if self._local_tak_tcp_push_conn is conn:
                        self._local_tak_tcp_push_conn = None
                        self._local_tak_tcp_push_peer = None
                if cb:
                    try:
                        cb("disconnect", None, None, target_addr)
                    except Exception:
                        pass
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
            if not self.shutdown_flag.is_set():
                self.shutdown_flag.wait(TCP_RECEIVER_RECONNECT_SECONDS)

    def listen_for_tak_chat_tcp(self, listen_port=None):
        if listen_port is None:
            listen_port = self.tcp_chat_listen_port
        try:
            sock, bind_ip = self._create_chat_tcp_listener_socket(listen_port)
            self.sock_chat_listeners.append(sock)
            self.logger.info(
                f"TAK-TCP-Listener aktiv auf {bind_ip}:{listen_port} "
                "(WinTAK/admin_map TCP-Kompatibilität)."
            )
        except Exception as e:
            self.logger.warning(f"TAK-TCP-Listener auf Port {listen_port} konnte nicht gestartet werden: {e}")
            return

        while not self.shutdown_flag.is_set():
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception:
                self.logger.debug("Fehler beim Annehmen einer TAK-TCP-Verbindung:\n" + traceback.format_exc())
                continue
            self.logger.debug(
                f"TAK-TCP-Verbindung angenommen: quelle={_format_network_endpoint(addr)} "
                f"listener_port={listen_port}"
            )
            threading.Thread(
                target=self._handle_tak_tcp_client,
                args=(conn, addr),
                daemon=True,
            ).start()

    def maintain_server(self):
        """
        Stellt Verbindung zum entfernten TAK-Server her (bei TCP persistent),
        oder bereitet UDP Socket vor (bei UDP).
        Führt Wiederverbindungen durch.
        """
        self.logger.info(f"Server-Mode: {self.server_protocol} -> {self.server_ip}:{self.server_port}")
        if self._should_use_pytak_remote():
            self.logger.info(f"PyTAK-Transport aktiviert für Remote-TAK: {self.remote_cot_url}")
            while not self.shutdown_flag.is_set():
                try:
                    asyncio.run(self._run_remote_pytak_client())
                except Exception as e:
                    self.logger.warning(f"PyTAK-Remote-Verbindung fehlgeschlagen: {e}")
                finally:
                    self._reset_remote_pytak_state()
                if not self.shutdown_flag.is_set():
                    self.shutdown_flag.wait(PYTAK_REMOTE_RECONNECT_SECONDS)
            return
        if pytak is None:
            self.logger.info("PyTAK nicht verfügbar, verwende klassischen Remote-Socket-Transport.")
        if self.server_protocol == "UDP":
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(self.SOCKET_TIMEOUT)
                with self.server_lock:
                    self.sock_remote = sock
                self.logger.info("Remote-UDP-Socket bereit.")
            except Exception as e:
                self.logger.error(f"Fehler beim Erstellen des Remote-UDP-Sockets: {e}")
            # Keep monitoring for shutdown
            while not self.shutdown_flag.is_set():
                time.sleep(10)
            return

        # TCP: persistent Verbindung aufbauen und bei Fehlern zurücksetzen
        while not self.shutdown_flag.is_set():
            if self.sock_remote is None:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(10)
                    s.connect((self.server_ip, self.server_port))
                    s.settimeout(None)
                    with self.server_lock:
                        self.sock_remote = s
                    self.logger.info("✅ REMOTE SERVER (TCP) CONNECTED")
                except Exception as e:
                    with self.server_lock:
                        self.sock_remote = None
                    self.logger.warning(f"Verbindung zum Remote-Take-Server fehlgeschlagen: {e}")
            time.sleep(10)
        
        # Cleanup on shutdown
        self._cleanup_server_socket()

    def on_any_packet(self, packet, interface):
        """
        Callback für empfangene Packets von meshtastic.
        Der 'interface'-Parameter identifiziert, von welchem COM-Port das Paket stammt.
        Fallback: sucht in allen verbundenen Interfaces nach dem Node.
        """
        try:
            self.logger.debug(f"RAW Paket empfangen: {packet}")
            from_id = packet.get('fromId') or packet.get('from')
            node = self._find_node_for_packet(interface, from_id)

            # ── Optional raw payload inspection log ──────────────────────────
            if self.log_raw_meshtastic_payloads:
                self._log_raw_meshtastic_packet(packet, from_id)

            if self._is_atak_forwarder_packet(packet):
                self._handle_meshtastic_forwarder_packet(packet, from_id or "MESH-UNKNOWN")
            if self._is_text_message_packet(packet):
                decoded = packet.get("decoded") or {}
                text_payload = decoded.get("text")
                if not text_payload:
                    payload = decoded.get("payload")
                    if isinstance(payload, (bytes, bytearray)):
                        text_payload = payload.decode("utf-8", errors="ignore")
                handled_legacy_cot = self._handle_meshtastic_legacy_cot_text(
                    text_payload,
                    from_id or "MESH-UNKNOWN",
                )
                if not handled_legacy_cot:
                    self.process_meshtastic_text_message(packet, node=node, source_interface=interface)
            elif self._is_atak_plugin_packet(packet):
                handled_chat = self.process_meshtastic_text_message(
                    packet,
                    node=node,
                    source_interface=interface,
                )
                if not handled_chat:
                    self.process_meshtastic_pli_message(packet, node=node)
            if from_id and node:
                # Force update für Live-Events
                self.process_node(node, 0, force_update=True)
        except Exception:
            self.logger.debug("Fehler im on_any_packet:\n" + traceback.format_exc())

    # ── Raw-payload inspection helper ────────────────────────────────────────

    # Port names that are interesting for raw-payload diagnostics.
    _RAW_LOG_PORTNUMS = frozenset({
        "ATAK_PLUGIN", "ATAK_PLUGIN_V2", "ATAK_FORWARDER",
        "TEXT_MESSAGE_APP", "POSITION_APP", "NODEINFO_APP",
    })

    def _log_raw_meshtastic_packet(self, packet, from_id):
        """Emit a structured raw-payload log entry for inbound Meshtastic packets.

        Enabled via ``log_raw_meshtastic_payloads: true`` in config.yaml.
        Set ``log_raw_meshtastic_payloads_full: true`` to include the complete
        payload hex/base64 for large packets instead of only the first 16 bytes.
        """
        decoded = packet.get("decoded") or {}
        if not isinstance(decoded, dict):
            return
        portnum = str(decoded.get("portnum", "UNKNOWN")).upper()
        payload = decoded.get("payload")

        # Only log packet types that are relevant for TAK reconstruction diagnosis.
        # If the portnum is numeric, also check against the interesting set by value.
        portnum_matches = portnum in self._RAW_LOG_PORTNUMS
        if not portnum_matches:
            try:
                pn_int = int(portnum)
                interesting_ints = set()
                if portnums_pb2 is not None:
                    for name in self._RAW_LOG_PORTNUMS:
                        v = getattr(portnums_pb2.PortNum, name, None)
                        if v is not None:
                            interesting_ints.add(int(v))
                portnum_matches = pn_int in interesting_ints
            except (ValueError, TypeError):
                pass

        if not portnum_matches:
            return

        full = self.log_raw_meshtastic_payloads_full
        if isinstance(payload, (bytes, bytearray)):
            raw_info = _format_raw_meshtastic_payload(payload, full=full)
        else:
            # No binary payload — include the text field if present (TEXT_MESSAGE_APP)
            text_field = decoded.get("text") or ""
            if text_field:
                text_bytes = str(text_field).encode("utf-8", errors="replace")
                raw_info = _format_raw_meshtastic_payload(text_bytes, full=full)
            else:
                raw_info = "len=0 hex= b64="

        self.logger.info(
            f"[RAW-MESH] portnum={portnum} from={from_id or 'UNKNOWN'} {raw_info}"
        )

    def full_sync(self):
        """
        Schickt eine Sync über alle bekannten Nodes aller verbundenen Interfaces.
        """
        interfaces = self._ensure_meshtastic_interfaces()
        if not interfaces:
            self.logger.warning("Keine Meshtastic-Interfaces für Vollsynchronisation verbunden.")
            return
        for iface in interfaces:
            try:
                nodes = []
                if hasattr(iface, 'nodes') and iface.nodes:
                    nodes = sorted(iface.nodes.values(), key=lambda x: x.get('user', {}).get('longName', ''))
                for i, node in enumerate(nodes):
                    self.process_node(node, i)
            except Exception:
                self.logger.error("Fehler während full_sync:\n" + traceback.format_exc())

    def _invoke_position_setter(self, target, lat, lon, alt):
        """Try common meshtastic APIs to set/publish a fixed position."""
        if target is None:
            return False
        # Meshtastic Python APIs differ between versions; try known method/signature variants.
        for method_name in ("setFixedPosition", "set_fixed_position", "setPosition", "set_position"):
            method = getattr(target, method_name, None)
            if not callable(method):
                continue
            method_failed = False
            call_variants = (
                ((), {"lat": lat, "lon": lon, "alt": alt}),
                ((), {"latitude": lat, "longitude": lon, "altitude": alt}),
                ((lat, lon, alt), {}),
                ((lat, lon), {}),
            )
            for args, kwargs in call_variants:
                try:
                    method(*args, **kwargs)
                    self.logger.debug(f"Gateway-Positionssetter erfolgreich über {method_name}")
                    return True
                except TypeError:
                    self.logger.debug(f"Gateway-Positionssetter Signatur passt nicht für {method_name}, nächster Versuch.")
                    continue
                except Exception:
                    self.logger.warning(
                        f"Fehler beim Setzen der Gateway-Position über {method_name}:\n{traceback.format_exc()}"
                    )
                    method_failed = True
                    break
            if method_failed:
                continue
        return False

    def apply_gateway_fixed_position(self):
        """
        Optional: publish/set a fixed position for the local gateway node at startup.
        Uses park_lat/park_lon as source coordinates.
        """
        if not self.set_gateway_position_on_start:
            return
        if self.park_coords is None:
            self.logger.warning(
                "set_gateway_position_on_start=true, aber park_lat/park_lon sind ungültig. "
                "Gateway-Position wurde nicht gesetzt."
            )
            return

        lat, lon = self.park_coords
        updated_ports = []
        for iface in self._get_interfaces_snapshot():
            try:
                local_node = getattr(iface, "localNode", None)
                # localNode first, then interface as fallback for older/newer API variants.
                # Altitude defaults to 0 because this gateway only stores fixed fallback coordinates.
                local_node_updated = self._invoke_position_setter(local_node, lat, lon, 0)
                iface_updated = False
                if not local_node_updated:
                    iface_updated = self._invoke_position_setter(iface, lat, lon, 0)
                if local_node_updated or iface_updated:
                    updated_ports.append(getattr(iface, "devPath", "unknown"))
            except Exception:
                self.logger.debug("Fehler beim Anwenden der Gateway-Fixed-Position:\n" + traceback.format_exc())

        if updated_ports:
            self.logger.info(
                f"Gateway-Fixed-Position gesetzt/gesendet auf Port(s): {', '.join(updated_ports)}"
            )
        else:
            self.logger.warning(
                "set_gateway_position_on_start=true, aber API zum Setzen der Position "
                "wurde in der installierten Meshtastic-Version nicht gefunden."
            )

    def process_node(self, node, index, force_update=False):
        """
        Extrahiert Daten aus node und sendet CoT-Event
        """
        try:
            user = node.get('user', {})
            pos = node.get('position', {})

            raw_uid = user.get('id') or f"!{node.get('num'):08x}"
            uid = normalize_meshtastic_uid(raw_uid)
            callsign = user.get('longName') or user.get('shortName') or uid

            lat_i, lon_i = pos.get('latitude_i'), pos.get('longitude_i')
            lat_f, lon_f = pos.get('latitude'), pos.get('longitude')
            alt = pos.get('altitude', 0) or 0

            final_lat, final_lon, is_real = 0.0, 0.0, False
            position_source = "USER"

            # Priorität: integer-Telemetrie (1e-7) -> float -> last known -> fallback park
            # NOTE: Coordinates at exactly (0,0) are treated as no GPS fix and use fallback
            # OR logic is intentional: accepts lat=0 OR lon=0 (equator/prime meridian) but rejects (0,0)
            if lat_i is not None and lon_i is not None:
                normalized = normalize_coordinates(lat_i * 1e-7, lon_i * 1e-7)
                if normalized:
                    final_lat, final_lon = normalized
                    is_real = True
            if (not is_real) and lat_f is not None and lon_f is not None:
                normalized = normalize_coordinates(lat_f, lon_f)
                if normalized:
                    final_lat, final_lon = normalized
                    is_real = True

            if is_real:
                position_source = "GPS"
                self.last_known_positions[uid] = (final_lat, final_lon, alt)
                # GPS-Position in Datenbank persistieren (aktualisiert bei Änderung)
                self.node_db.upsert_node(uid, callsign, final_lat, final_lon, alt, "GPS")
            else:
                if not self.send_nodes_without_gps:
                    self.logger.debug(f"Überspringe Node ohne gültigen GPS-Fix: {callsign}")
                    return
                last_known = self.last_known_positions.get(uid)
                if not last_known:
                    # Datenbank als Fallback prüfen (z.B. nach Neustart oder manuelle Eingabe)
                    db_row = self.node_db.get_node(uid)
                    if db_row:
                        last_known = (db_row[0], db_row[1], db_row[2] if db_row[2] is not None else 0)
                        self.last_known_positions[uid] = last_known
                if last_known:
                    final_lat, final_lon, cached_alt = last_known
                    alt = alt or cached_alt
                    position_source = "LAST_KNOWN"
                else:
                    if self.park_coords is None:
                        self.logger.debug(f"Überspringe Node ohne gültigen GPS-Fix (kein park_lat/park_lon): {callsign}")
                        return
                    final_lat = self.park_coords[0] - (index * 0.001)
                    final_lon = self.park_coords[1]
                    position_source = "USER"

            if is_real and force_update:
                self.logger.info(f"LIVE: Position-Update empfangen von {callsign}")
            elif position_source == "LAST_KNOWN" and force_update:
                self.logger.info(f"LIVE: Nutze letzte bekannte Position für {callsign}")

            self.send_broadcast(uid, callsign, final_lat, final_lon, alt, is_real, position_source)
        except Exception:
            self.logger.error("Fehler in process_node:\n" + traceback.format_exc())

    def send_broadcast(self, uid, callsign, lat, lon, alt, is_real, position_source="GPS"):
        """
        Baut das CoT-XML und sendet es lokal per UDP an WinTAK und optional an entfernten TAK-Server (TCP/UDP).
        """
        try:
            t = get_tak_timestamp()
            stale = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

            event = Element('event', {
                'how': 'm-g',
                'type': MESHTASTIC_PLI_COT_EVENT_TYPE,
                'uid': uid,
                'start': t,
                'time': t,
                'stale': stale,
                'version': '2.0'
            })
            SubElement(event, 'point', {
                'hae': str(alt or 0),
                'lat': f"{lat:.6f}",
                'lon': f"{lon:.6f}",
                'ce': '10',
                'le': '10'
            })
            detail = SubElement(event, 'detail')
            if position_source == "GPS":
                geopointsrc = "GPS"
            elif position_source == "LAST_KNOWN":
                geopointsrc = "ESTIMATED"
            else:
                geopointsrc = "USER"
            self._apply_meshtastic_bridge_detail(
                detail,
                uid,
                callsign,
                geopointsrc=geopointsrc,
                speed=0.0,
                course=0.0,
                endpoint=f"{self.tak_ip}:{self.tak_port}:udp",
            )
            if position_source == "LAST_KNOWN":
                SubElement(detail, 'remarks').text = "Listed (Last Known Position)"
            elif not is_real:
                SubElement(detail, 'remarks').text = "Listed (No GPS Fix)"

            # ElementTree.tostring mit UTF-8 ergibt bytes
            packet_xml = tostring(event, encoding='utf-8')
            self._record_service_monitor_event(
                packet_xml,
                ">>>",
                f"Node sync ({position_source})",
            )
            self._send_packet_to_tak(packet_xml, callsign)
        except Exception:
            self.logger.error("Fehler in send_broadcast:\n" + traceback.format_exc())

    def _cleanup_server_socket(self):
        """
        Cleanup remote server socket
        """
        self._reset_remote_pytak_state()
        with self.server_lock:
            if self.sock_remote:
                try:
                    self.sock_remote.close()
                except Exception:
                    pass
                self.sock_remote = None

    def cleanup(self):
        """
        Cleanup all resources
        """
        self.logger.info("Cleaning up resources...")
        self.shutdown_flag.set()
        
        # Close UDP socket
        try:
            self.sock_udp.close()
        except Exception:
            pass

        try:
            for sock in self.sock_chat_listeners:
                sock.close()
        except Exception:
            pass
        self.sock_chat_listeners = []

        with self.local_tak_tcp_peer_lock:
            tcp_peers = list(self.local_tak_tcp_peers)
            self.local_tak_tcp_peers = []
        for peer in tcp_peers:
            try:
                peer["conn"].close()
            except Exception:
                pass

        try:
            if self.service_web_ui is not None:
                self.service_web_ui["server"].shutdown()
                self.service_web_ui["server"].server_close()
        except Exception:
            pass
        
        # Close remote socket
        self._cleanup_server_socket()
        
        # Close all meshtastic interfaces
        with self.interface_lock:
            interfaces_to_close = list(self.interfaces)
            self.interfaces = []
        for iface in interfaces_to_close:
            self._close_meshtastic_interface(iface)

    def run(self):
        """
        Hauptschleife: periodische Vollsyncs
        """
        try:
            while not self.shutdown_flag.is_set():
                self.full_sync()
                # Use wait instead of sleep to allow interruption
                self.shutdown_flag.wait(self.sync_interval_seconds)
        except KeyboardInterrupt:
            self.logger.info("Beende auf Benutzereingabe.")
        except Exception:
            self.logger.error("Fehler in run:\n" + traceback.format_exc())
        finally:
            self.cleanup()


def _select_port_interactively(ports, already_selected):
    """
    Interaktive Portauswahl aus einer Liste, bereits gewählte Ports werden ausgeschlossen.
    """
    if serial is None:
        print("pyserial nicht verfügbar; benutze Standard COM7.")
        return "COM7"

    # Filter out already selected ports
    available = [p for p in ports if p.device not in already_selected]
    if not available:
        print("Keine weiteren freien Ports verfügbar. Verwende COM7 als Standard.")
        return "COM7"

    print("Verfügbare serielle Ports:")
    for i, p in enumerate(available):
        print(f"  [{i}] {p.device} - {getattr(p, 'description', '')}")

    while True:
        val = input("\nPort auswählen (Index, Enter = 0): ").strip()
        if val == "":
            idx = 0
            break
        try:
            idx = int(val)
            if 0 <= idx < len(available):
                break
        except Exception:
            pass
        print("Ungültige Auswahl. Bitte Index-Zahl eingeben.")
    return available[idx].device


def choose_serial_ports(cfg, all_ports_mode=False):
    """
    Auswahl von einem oder mehreren COM-Ports (Eingabe-Streams 1-6). Unterstützt:
    - Automatische Auswahl über cfg["meshtastic_port"] (String oder Liste) falls vorhanden
    - all_ports_mode=True: alle verfügbaren seriellen Ports automatisch verwenden (kein interaktiver Dialog)
    - Interaktive Abfrage der Stream-Anzahl (1-6) und Port-Auswahl je Stream
    Returns: Liste von Gerätenamen (z.B. ['COM3', 'COM7'])
    """
    # Alle verfügbaren Ports ermitteln
    all_ports = []
    if serial is not None:
        try:
            all_ports = list(serial.tools.list_ports.comports())
        except Exception:
            pass

    # --all-ports Modus: alle erkannten seriellen Ports automatisch verwenden
    if all_ports_mode:
        if all_ports:
            detected = [p.device for p in all_ports]
            print(f"Alle verfügbaren seriellen Ports werden automatisch verwendet: {', '.join(detected)}")
            return detected
        else:
            print("Keine seriellen Ports gefunden. Verwende Standard COM7.")
            return ["COM7"]

    # Konfigurierte Ports aus config.yaml lesen (String oder Liste)
    cfg_port = cfg.get("meshtastic_port")
    cfg_ports = []
    if isinstance(cfg_port, list):
        cfg_ports = [str(p) for p in cfg_port]
    elif cfg_port:
        cfg_ports = [str(cfg_port)]

    # Anzahl der gewünschten Eingabe-Streams abfragen
    default_count = min(len(cfg_ports), 6) if cfg_ports else 1
    print(f"\nWie viele Eingabe-Streams (COM-Ports) sollen verwendet werden? (1-6, Enter = {default_count}):")
    while True:
        val = input("Anzahl Streams: ").strip()
        if val == "":
            num_streams = default_count
            break
        try:
            num_streams = int(val)
            if 1 <= num_streams <= 6:
                break
        except Exception:
            pass
        print("Bitte eine Zahl zwischen 1 und 6 eingeben.")

    selected_ports = []
    for stream_idx in range(num_streams):
        print(f"\n--- Eingabe-Stream {stream_idx + 1} ---")
        # Vorkonfigurierten Port verwenden, falls vorhanden
        if stream_idx < len(cfg_ports):
            cfg_p = cfg_ports[stream_idx]
            if cfg_p in selected_ports:
                print(f"Port {cfg_p} bereits ausgewählt. Weiter zur manuellen Auswahl...")
                port = _select_port_interactively(all_ports, selected_ports)
                selected_ports.append(port)
                continue
            if not all_ports:
                print(f"Konfigurierter Port {cfg_p} wird verwendet (keine Portprüfung möglich).")
                selected_ports.append(cfg_p)
                continue
            for p in all_ports:
                if p.device == cfg_p:
                    print(f"Konfigurierter Port {cfg_p} gefunden und wird verwendet.")
                    selected_ports.append(cfg_p)
                    break
            else:
                print(f"Konfigurierter Port {cfg_p} nicht gefunden. Weiter zur manuellen Auswahl...")
                port = _select_port_interactively(all_ports, selected_ports)
                selected_ports.append(port)
        else:
            if not all_ports:
                print("Keine seriellen Ports gefunden. Drücke Enter um mit Standard COM7 fortzufahren.")
                input()
                selected_ports.append("COM7")
            else:
                port = _select_port_interactively(all_ports, selected_ports)
                selected_ports.append(port)

    return selected_ports


def resolve_app_start_mode(args, has_tk):
    if getattr(args, "no_gui", False):
        return "terminal"
    if getattr(args, "gui", False):
        return "tk" if has_tk else "terminal"
    return "browser"


def _run_browser_mode(cfg, _args):
    bind_ip = detect_reachable_local_ip(cfg.get("tak_server_host"))
    service = start_gateway_service_web_ui(
        bind_ip,
        port=SERVICE_WEB_UI_PORT,
        asset_dir=pathlib.Path(__file__).resolve().parent,
        cfg=cfg,
    )
    controller = _ServiceWebUIController(
        cfg,
        service_status=service["status"],
        event_store=service["event_store"],
    )
    service["server"].RequestHandlerClass.controller = controller
    url = service["url"]
    print("=" * 72)
    print(f"HTML-Browser-UI aktiv: {url}")
    print("Konfiguriere und starte das Gateway jetzt im Browser.")
    print("Zum Beenden im Backend-Fenster Strg+C drücken.")
    print("=" * 72)
    try:
        webbrowser.open(url, new=2)
    except Exception:
        pass
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        controller.stop_gateway()
    finally:
        service["server"].shutdown()
        service["server"].server_close()


def _run_terminal_mode(cfg, args):
    """Terminal-Fallback: Klassische Konsoleninteraktion ohne GUI."""
    # Fehlende Abhängigkeiten anzeigen
    missing = []
    if meshtastic is None:
        missing.append("meshtastic")
    if pub is None:
        missing.append("pypubsub")
    if serial is None:
        missing.append("pyserial")
    if missing:
        print("WARNUNG: Folgende Python-Pakete fehlen oder konnten nicht importiert werden:")
        for m in missing:
            print(" - " + m)
        print("Bitte installiere sie (z.B. pip install meshtastic pypubsub pyserial colorlog pyyaml pytak).")
        print("Fortfahren? (Enter=ja, Ctrl-C zum Abbrechen)")
        input()

    p_devs = choose_serial_ports(cfg, all_ports_mode=args.all_ports)
    print(f"Verwende Port(s): {', '.join(p_devs)}")
    gw = TAKMeshtasticGateway(p_devs, cfg)
    gw.run()


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="TAK Meshtastic Gateway")
        parser.add_argument(
            "--all-ports", action="store_true",
            help="Alle verfügbaren seriellen USB-Ports automatisch verwenden (kein interaktiver Dialog)"
        )
        parser.add_argument(
            "--gui", action="store_true",
            help="Tk-Desktopoberfläche explizit aktivieren (Standard ist die Browser-UI auf Port 5013)"
        )
        parser.add_argument(
            "--no-gui", action="store_true",
            help="GUI deaktivieren und im Terminal-Modus starten"
        )
        args = parser.parse_args()

        cfg = load_config()

        start_mode = resolve_app_start_mode(args, has_tk=tk is not None)
        if start_mode == "tk":
            try:
                app = GatewayApp(cfg)
                app.run()
            except (tk.TclError, RuntimeError):
                # Kein Display / Tkinter-Fehler → Terminal-Fallback
                _run_terminal_mode(cfg, args)
        elif start_mode == "browser":
            _run_browser_mode(cfg, args)
        else:
            _run_terminal_mode(cfg, args)

    except Exception:
        print("Unerwarteter Fehler beim Starten:")
        traceback.print_exc()
        print("Drücke Enter zum Beenden...")
        input()
        sys.exit(1)
