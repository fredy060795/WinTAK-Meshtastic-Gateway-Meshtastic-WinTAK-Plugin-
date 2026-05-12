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
import argparse
import base64
import codecs
import datetime
import hashlib
import inspect
import math
import re
import socket
import sqlite3
import time
import logging
import threading
import traceback
import uuid
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
DEFAULT_CHAT_LISTEN_PORT = 4243
TCP_LISTENER_DEFAULT_PORT = 8087
DEFAULT_SOURCE_PROTOCOL = "UNKNOWN"
TCP_SOCKET_TIMEOUT_SECONDS = 1.0
TCP_LISTENER_BACKLOG = 5
TCP_RECV_BUFFER_SIZE = 4096
MAX_TCP_STREAM_BUFFER_BYTES = 262144
TCP_STREAM_BUFFER_TAIL_BYTES = 64
RECENT_CHAT_CACHE_TTL_SECONDS = 30
RECENT_CHAT_CACHE_MAX_ENTRIES = 256
MESHTASTIC_TEXT_CHUNK_MAX_BYTES = 180
MESHTASTIC_COT_FRAGMENT_PREFIX = "COTM"
MESHTASTIC_COT_FRAGMENT_PAYLOAD_BYTES = 140
MESHTASTIC_COT_FRAGMENT_TTL_SECONDS = 120
DEFAULT_MESHTASTIC_CHANNEL_INDEX = 0
GEOCHAT_UID_PREFIX = "GeoChat."
TAK_EVENT_PATTERN = re.compile(r"<event\b[^>]*>.*?</event>", re.DOTALL)
MIN_NULL_BYTES_FOR_UTF16 = 2
UTF16_NULL_BYTE_RATIO_THRESHOLD = 4
_WINTAK_CHAT_TRANSCRIPT_LINE_PATTERN = re.compile(
    r"^\((?P<time>\d{1,2}:\d{2}(?::\d{2})?)\)\s+(?P<sender>.+):(?:\s*(?P<message>.*))?$"
)
_WINTAK_CHAT_FIELD_PATTERN = re.compile(r"^(?:message|text|note|remarks)(?P<index>\d+)?$", re.IGNORECASE)


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


def _collect_xml_text(element):
    """Collect stripped text from an XML element and all of its descendants."""
    if element is None:
        return ""
    text_parts = [text.strip() for text in element.itertext() if text and text.strip()]
    return "\n".join(text_parts)


def _strip_tak_sender_prefix(value):
    """Normalize common TAK sender prefixes like BAO.F.ATAK./BAO.F.WinTAK."""
    normalized = "" if value is None else str(value)
    for prefix in ("BAO.F.ATAK.", "BAO.F.WinTAK."):
        if normalized.startswith(prefix):
            return normalized[len(prefix):]
    return normalized


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


def _looks_like_tak_chat_remarks(remarks):
    """Return True when a TAK <remarks> element looks like a GeoChat payload."""
    if remarks is None:
        return False
    for attr_name in ("source", "sourceID", "sourceId", "to"):
        if str(remarks.get(attr_name) or "").strip():
            return True
    return False


def _ensure_bytes(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value or "").encode("utf-8")


def _decode_tak_packet_bytes(packet_bytes):
    """Normalize common TAK UDP packet encodings to UTF-8 XML bytes."""
    packet_bytes = _ensure_bytes(packet_bytes)
    packet_bytes = packet_bytes.strip(b"\x00 \t\r\n")
    if not packet_bytes:
        return b""

    def _is_xml_text(text):
        stripped = str(text or "").strip()
        return stripped.startswith("<") and ">" in stripped

    def _should_attempt_utf16_decode(raw_bytes):
        null_bytes = raw_bytes.count(b"\x00")
        return (
            null_bytes >= MIN_NULL_BYTES_FOR_UTF16
            and null_bytes >= len(raw_bytes) // UTF16_NULL_BYTE_RATIO_THRESHOLD
        )

    if packet_bytes.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return packet_bytes.decode("utf-16").encode("utf-8")
        except UnicodeDecodeError:
            return packet_bytes

    if _should_attempt_utf16_decode(packet_bytes):
        for encoding in ("utf-16-le", "utf-16-be"):
            try:
                decoded_text = packet_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
            if _is_xml_text(decoded_text):
                return decoded_text.encode("utf-8")
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


def _parse_meshtastic_atak_payload(payload):
    """Decode the chat-related subset of Meshtastic ATAK plugin payloads."""
    decoded = {
        "is_compressed": False,
        "contact": {},
        "chat": {},
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
        elif field_number == 6:
            decoded["chat"] = _parse_meshtastic_atak_chat(field_bytes)
    return decoded


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
    if serial is None:
        return []
    try:
        return [p.device for p in serial.tools.list_ports.comports()]
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

        try:
            self._root = tk.Tk()
        except (tk.TclError, RuntimeError):
            raise

        self._root.title("WinTAK Meshtastic Gateway")
        self._root.geometry("980x700")
        self._root.minsize(700, 480)
        self._root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._root.configure(bg="#1f2937")

        self._setup_styles()
        self._build_ui()

    # ─────────────────────────── Styles / Theming ─────────────────────────────

    def _setup_styles(self):
        style = ttk.Style(self._root)
        style.theme_use("clam")

        # ── Farben ──
        BG        = "#1f2937"   # Dunkelblau-Grau (Fenster-Hintergrund)
        PANEL     = "#111827"   # Noch dunklerer Hintergrund für Panels
        CARD      = "#374151"   # Karten-/Rahmen-Hintergrund
        BORDER    = "#4b5563"   # Rahmenfarbe
        FG        = "#f9fafb"   # Heller Text
        FG_SUB    = "#9ca3af"   # Dezenter Hilfstext
        ACCENT    = "#3b82f6"   # Blau (Akzent)
        ACCENT_H  = "#2563eb"   # Blau Hover
        SUCCESS   = "#22c55e"   # Grün
        WARNING   = "#f59e0b"   # Amber
        DANGER    = "#ef4444"   # Rot

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
        style.configure("TEntry",
                         fieldbackground="#1f2937",
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
                         fieldbackground="#1f2937",
                         foreground=FG,
                         selectbackground=ACCENT,
                         selectforeground=FG,
                         bordercolor=BORDER,
                         arrowcolor=FG_SUB)
        style.map("TCombobox",
                  bordercolor=[("focus", ACCENT)],
                  fieldbackground=[("readonly", "#1f2937")])
        self._root.option_add("*TCombobox*Listbox.background", "#1f2937")
        self._root.option_add("*TCombobox*Listbox.foreground", FG)
        self._root.option_add("*TCombobox*Listbox.selectBackground", ACCENT)

        # ── TButton (Primär) ──
        style.configure("TButton",
                         background="#374151",
                         foreground=FG,
                         bordercolor=BORDER,
                         focuscolor=ACCENT,
                         padding=(10, 5),
                         relief="flat",
                         font=("Segoe UI", 9))
        style.map("TButton",
                  background=[("active", BORDER), ("disabled", "#1f2937")],
                  foreground=[("disabled", FG_SUB)])

        # ── Accent.TButton ──
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

        # ── Danger.TButton ──
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
                         indicatorcolor="#1f2937",
                         indicatorbackground="#1f2937",
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
                         troughcolor="#1f2937",
                         arrowcolor=FG_SUB,
                         bordercolor=BORDER,
                         relief="flat")
        style.map("TScrollbar",
                  background=[("active", BORDER)])

        # ── Listbox (nicht-ttk, via option_add) ──
        self._root.option_add("*Listbox.background", "#1f2937")
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

        # ── Header-Banner ──
        header = tk.Frame(root, bg=C["panel"], height=56)
        header.pack(fill="x")
        header.pack_propagate(False)

        # Logo
        try:
            _logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo.png")
            self._logo_image = tk.PhotoImage(file=_logo_path)
            _logo_h = 40
            _orig_h = self._logo_image.height()
            _orig_w = self._logo_image.width()
            if _orig_h > _logo_h:
                _subsample = max(1, round(_orig_h / _logo_h))
                self._logo_image = self._logo_image.subsample(_subsample, _subsample)
            tk.Label(
                header,
                image=self._logo_image,
                bg=C["panel"],
                anchor="w",
            ).pack(side="left", padx=(8, 0), pady=6)
        except Exception:
            self._logo_image = None

        tk.Label(
            header,
            text="WinTAK Meshtastic Gateway",
            bg=C["panel"], fg=C["fg"],
            font=("Segoe UI", 14, "bold"),
            anchor="w",
        ).pack(side="left", padx=12, pady=0, fill="y")
        tk.Label(
            header,
            text="Meshtastic → TAK Bridge",
            bg=C["panel"], fg=C["fg_sub"],
            font=("Segoe UI", 9),
            anchor="e",
        ).pack(side="right", padx=12, pady=0)

        ttk.Separator(root, orient="horizontal").pack(fill="x")

        # ── Einstellungen ──
        cfg_frame = ttk.LabelFrame(root, text=" ⚙  Einstellungen ", padding=(12, 8))
        cfg_frame.pack(fill="x", padx=10, pady=(10, 4))

        # Hilfsfunktion für einheitliche Labels in cfg_frame
        def cfg_label(text, row, col, **kw):
            lbl = ttk.Label(cfg_frame, text=text, style="Card.TLabel")
            lbl.grid(row=row, column=col, sticky="w", **kw)
            return lbl

        # ── Zeile 0: Ports + Log-Level ──
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
            row=0, column=1, sticky="ew", padx=(6, 12), pady=(0, 4))

        cfg_label("Log-Level:", row=0, col=2, padx=(8, 6), pady=(0, 4))
        log_default = str(self.cfg.get("log_level", "INFO")).upper()
        if log_default not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            log_default = "INFO"
        self._log_level_var = tk.StringVar(value=log_default)
        log_combo = ttk.Combobox(
            cfg_frame, textvariable=self._log_level_var,
            state="readonly", values=["DEBUG", "INFO", "WARNING", "ERROR"], width=10
        )
        log_combo.grid(row=0, column=3, sticky="w", padx=(0, 12), pady=(0, 4))
        log_combo.bind("<<ComboboxSelected>>", self._on_log_level_change)

        # ── Zeile 1: Erkannte Ports-Liste ──
        detected = detect_serial_port_devices()
        self._detected_ports = detected
        self._detected_ports_var = tk.StringVar(
            value=f"Erkannte Ports: {', '.join(detected) if detected else '–'}"
        )
        ttk.Label(cfg_frame, textvariable=self._detected_ports_var, style="Sub.TLabel").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(0, 2))
        self._detected_ports_list = tk.Listbox(
            cfg_frame,
            height=min(max(len(detected), 1), MAX_DETECTED_PORTS_DISPLAY),
            selectmode="extended",
            exportselection=False,
        )
        self._detected_ports_list.grid(row=2, column=1, sticky="ew", padx=(6, 4), pady=(0, 6))
        detected_scrollbar = ttk.Scrollbar(cfg_frame, orient="vertical",
                                            command=self._detected_ports_list.yview)
        self._detected_ports_list.configure(yscrollcommand=detected_scrollbar.set)
        detected_scrollbar.grid(row=2, column=2, sticky="nsw", padx=(0, 8), pady=(0, 6))
        for port in detected:
            self._detected_ports_list.insert("end", port)
        ttk.Button(
            cfg_frame, text="↩ Auswahl übernehmen",
            command=self._apply_selected_ports_from_list
        ).grid(row=2, column=3, sticky="w", pady=(0, 6))
        ttk.Button(
            cfg_frame, text="🔄 Ports aktualisieren",
            command=self._refresh_detected_ports
        ).grid(row=2, column=4, sticky="w", padx=(8, 0), pady=(0, 6))

        ttk.Separator(cfg_frame, orient="horizontal").grid(
            row=3, column=0, columnspan=6, sticky="ew", pady=(2, 8))

        # ── Zeile 4: Remote TAK ──
        cfg_label("Remote TAK Host:", row=4, col=0, pady=(0, 4))
        self._server_host_var = tk.StringVar(value=str(self.cfg.get("tak_server_host", "82.165.11.84")))
        ttk.Entry(cfg_frame, textvariable=self._server_host_var, width=28).grid(
            row=4, column=1, sticky="ew", padx=(6, 12), pady=(0, 4))
        cfg_label("Remote Port:", row=4, col=2, padx=(8, 6), pady=(0, 4))
        self._server_port_var = tk.StringVar(value=str(self.cfg.get("tak_server_port", 8087)))
        ttk.Entry(cfg_frame, textvariable=self._server_port_var, width=10).grid(
            row=4, column=3, sticky="w", pady=(0, 4))

        # ── Zeile 5: Protokoll + Local TAK ──
        cfg_label("Remote Protokoll:", row=5, col=0, pady=(0, 4))
        server_protocol = str(self.cfg.get("tak_server_protocol", "TCP")).upper()
        if server_protocol not in ("TCP", "UDP"):
            server_protocol = "TCP"
        self._server_protocol_var = tk.StringVar(value=server_protocol)
        ttk.Combobox(
            cfg_frame, textvariable=self._server_protocol_var,
            state="readonly", values=["TCP", "UDP"], width=10
        ).grid(row=5, column=1, sticky="w", padx=(6, 12), pady=(0, 4))
        cfg_label("Local TAK IP:", row=5, col=2, padx=(8, 6), pady=(0, 4))
        self._local_tak_ip_var = tk.StringVar(value=str(self.cfg.get("local_tak_ip", "127.0.0.1")))
        ttk.Entry(cfg_frame, textvariable=self._local_tak_ip_var, width=16).grid(
            row=5, column=3, sticky="w", pady=(0, 4))

        # ── Zeile 6: Local Port + Sync-Intervall ──
        cfg_label("Local TAK Port:", row=6, col=0, pady=(0, 4))
        self._local_tak_port_var = tk.StringVar(value=str(self.cfg.get("local_tak_port", 4242)))
        ttk.Entry(cfg_frame, textvariable=self._local_tak_port_var, width=10).grid(
            row=6, column=1, sticky="w", padx=(6, 12), pady=(0, 4))
        cfg_label("Sync-Intervall (s):", row=6, col=2, padx=(8, 6), pady=(0, 4))
        self._sync_interval_var = tk.StringVar(value=str(self.cfg.get("sync_interval_seconds", 300)))
        ttk.Entry(cfg_frame, textvariable=self._sync_interval_var, width=10).grid(
            row=6, column=3, sticky="w", pady=(0, 4))

        cfg_label("Chat Listen Port:", row=7, col=0, pady=(0, 4))
        self._local_tak_chat_listen_port_var = tk.StringVar(
            value=str(self.cfg.get("local_tak_chat_listen_port", DEFAULT_CHAT_LISTEN_PORT))
        )
        ttk.Entry(cfg_frame, textvariable=self._local_tak_chat_listen_port_var, width=10).grid(
            row=7, column=1, sticky="w", padx=(6, 12), pady=(0, 4))
        cfg_label("WinTAK TCP Port:", row=7, col=2, padx=(8, 6), pady=(0, 4))
        self._local_tak_tcp_listen_port_var = tk.StringVar(
            value=str(self.cfg.get("local_tak_tcp_listen_port", TCP_LISTENER_DEFAULT_PORT))
        )
        ttk.Entry(cfg_frame, textvariable=self._local_tak_tcp_listen_port_var, width=10).grid(
            row=7, column=3, sticky="w", pady=(0, 4))
        self._relay_text_messages_var = tk.BooleanVar(
            value=as_bool(self.cfg.get("relay_text_messages", True))
        )
        ttk.Checkbutton(
            cfg_frame,
            text="Mesh-Text zwischen ausgewählten COM-Ports weiterleiten",
            variable=self._relay_text_messages_var,
        ).grid(row=8, column=0, columnspan=6, sticky="w", pady=(0, 4))

        cfg_label("Relay von COM:", row=9, col=0, pady=(0, 4))
        self._relay_text_from_ports_var = tk.StringVar(
            value=_format_ports_for_entry(self.cfg.get("relay_text_from_ports"))
        )
        ttk.Entry(cfg_frame, textvariable=self._relay_text_from_ports_var, width=28).grid(
            row=9, column=1, sticky="ew", padx=(6, 12), pady=(0, 4))
        cfg_label("Relay nach COM:", row=9, col=2, padx=(8, 6), pady=(0, 4))
        self._relay_text_to_ports_var = tk.StringVar(
            value=_format_ports_for_entry(self.cfg.get("relay_text_to_ports"))
        )
        ttk.Entry(cfg_frame, textvariable=self._relay_text_to_ports_var, width=16).grid(
            row=9, column=3, sticky="w", pady=(0, 4))

        ttk.Separator(cfg_frame, orient="horizontal").grid(
            row=10, column=0, columnspan=6, sticky="ew", pady=(2, 8))

        # ── GPS-Optionen ──
        self._send_nodes_without_gps_var = tk.BooleanVar(
            value=as_bool(self.cfg.get("send_nodes_without_gps", True))
        )
        self._set_gateway_position_var = tk.BooleanVar(
            value=as_bool(self.cfg.get("set_gateway_position_on_start", False))
        )
        ttk.Checkbutton(
            cfg_frame,
            text="Nodes ohne GPS-Fix senden",
            variable=self._send_nodes_without_gps_var,
            command=self._update_no_gps_hint,
        ).grid(row=11, column=0, columnspan=2, sticky="w", pady=(0, 4))
        ttk.Checkbutton(
            cfg_frame,
            text="Gateway-Position beim Start setzen",
            variable=self._set_gateway_position_var,
        ).grid(row=11, column=2, columnspan=3, sticky="w", padx=(8, 0), pady=(0, 4))

        # ── Zeile 13: park_lat / park_lon ──
        cfg_label("Fallback Lat (park_lat):", row=12, col=0, pady=(0, 4))
        raw_park_lat = self.cfg.get("park_lat")
        park_lat_val = "" if raw_park_lat is None else f"{float(raw_park_lat):.6f}".rstrip("0").rstrip(".")
        self._park_lat_var = tk.StringVar(value=park_lat_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lat_var, width=16).grid(
            row=12, column=1, sticky="w", padx=(6, 12), pady=(0, 4))
        cfg_label("Fallback Lon (park_lon):", row=12, col=2, padx=(8, 6), pady=(0, 4))
        raw_park_lon = self.cfg.get("park_lon")
        park_lon_val = "" if raw_park_lon is None else f"{float(raw_park_lon):.6f}".rstrip("0").rstrip(".")
        self._park_lon_var = tk.StringVar(value=park_lon_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lon_var, width=16).grid(
            row=12, column=3, sticky="w", pady=(0, 4))
        self._park_lat_var.trace_add("write", lambda *_: self._update_no_gps_hint())
        self._park_lon_var.trace_add("write", lambda *_: self._update_no_gps_hint())

        # ── Zeile 14: Hinweis ──
        self._no_gps_hint_var = tk.StringVar()
        ttk.Label(cfg_frame, textvariable=self._no_gps_hint_var, style="Hint.TLabel").grid(
            row=13, column=0, columnspan=6, sticky="w", pady=(0, 2))
        self._update_no_gps_hint()

        cfg_frame.columnconfigure(1, weight=1)
        cfg_frame.columnconfigure(3, weight=1)

        # ── Toolbar / Steuerleiste ──
        toolbar = tk.Frame(root, bg=C["panel"], pady=6)
        toolbar.pack(fill="x", padx=0)

        self._start_btn = ttk.Button(
            toolbar, text="▶  Start", command=self._on_start, style="Accent.TButton", width=10)
        self._start_btn.pack(side="left", padx=(10, 4))
        self._stop_btn = ttk.Button(
            toolbar, text="⏹  Stop", command=self._on_stop, state="disabled",
            style="Danger.TButton", width=10)
        self._stop_btn.pack(side="left", padx=(0, 4))
        ttk.Button(
            toolbar, text="🔄  Sync", command=self._on_manual_sync, width=10
        ).pack(side="left", padx=(0, 4))

        self._status_var = tk.StringVar(value="⬛  Gestoppt")
        ttk.Label(
            toolbar, textvariable=self._status_var,
            font=("Segoe UI", 9, "bold"),
        ).pack(side="left", padx=(16, 0))

        # ── Manuelles Mesh-Test-Senden ──
        mesh_test_frame = ttk.LabelFrame(root, text=" 🧪  Mesh-Testnachricht ", padding=6)
        mesh_test_frame.pack(fill="x", padx=10, pady=(6, 0))

        self._mesh_test_message_var = tk.StringVar()
        mesh_test_entry = ttk.Entry(mesh_test_frame, textvariable=self._mesh_test_message_var)
        mesh_test_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        mesh_test_entry.bind("<Return>", lambda _e: self._on_send_mesh_test_message())

        self._send_mesh_test_btn = ttk.Button(
            mesh_test_frame,
            text="Ins Mesh senden",
            command=self._on_send_mesh_test_message,
            state="disabled",
            style="Accent.TButton",
        )
        self._send_mesh_test_btn.pack(side="left")

        self._mesh_test_status_var = tk.StringVar(value="Gateway nicht gestartet.")
        ttk.Label(
            mesh_test_frame,
            textvariable=self._mesh_test_status_var,
            style="Sub.TLabel",
        ).pack(side="left", padx=(10, 0))

        # ── Eingabe / Befehlszeile ──
        input_frame = ttk.LabelFrame(root, text=" ⌨  Befehlseingabe ", padding=6)
        input_frame.pack(fill="x", padx=10, pady=(6, 0))

        self._input_var = tk.StringVar()
        input_entry = ttk.Entry(input_frame, textvariable=self._input_var)
        input_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        input_entry.bind("<Return>", lambda _e: self._on_send_command())
        ttk.Button(input_frame, text="Senden", command=self._on_send_command,
                   style="Accent.TButton").pack(side="left")

        # ── Log-Ausgabebereich ──
        log_frame = ttk.LabelFrame(root, text=" 📋  Log-Ausgabe ", padding=4)
        log_frame.pack(fill="both", expand=True, padx=10, pady=(6, 0))

        self._log_text = tk.Text(
            log_frame, wrap="word", state="disabled",
            bg="#0d1117", fg="#c9d1d9",
            font=self._log_font,
            relief="flat", bd=0,
            insertbackground="#c9d1d9",
            selectbackground="#264f78",
        )
        scrollbar = ttk.Scrollbar(log_frame, command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self._log_text.pack(fill="both", expand=True, padx=2, pady=2)

        # Farb-Tags je Log-Level
        self._log_text.tag_configure("DEBUG",    foreground="#6e7681")
        self._log_text.tag_configure("INFO",     foreground="#c9d1d9")
        self._log_text.tag_configure("WARNING",  foreground="#d29922")
        self._log_text.tag_configure("ERROR",    foreground="#f85149")
        self._log_text.tag_configure("CRITICAL", foreground="#ff7b72", font=("Consolas", 9, "bold"))
        self._log_text.tag_configure("CMD",      foreground="#79c0ff")

        # ── Statusleiste unten ──
        status_bar = tk.Frame(root, bg=C["panel"], height=22)
        status_bar.pack(fill="x", side="bottom")
        status_bar.pack_propagate(False)
        tk.Label(
            status_bar,
            text="Befehle: sync  |  log debug|info|warning|error  |  clear  |  help",
            bg=C["panel"], fg=C["fg_sub"],
            font=("Segoe UI", 8),
            anchor="w",
        ).pack(side="left", padx=10, fill="y")

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
        self._detected_ports = detect_serial_port_devices()
        self._detected_ports_var.set(
            f"Erkannte Ports: {', '.join(self._detected_ports) if self._detected_ports else '–'}"
        )
        self._detected_ports_list.delete(0, "end")
        for port in self._detected_ports:
            self._detected_ports_list.insert("end", port)
        self._detected_ports_list.configure(
            height=min(max(len(self._detected_ports), 1), MAX_DETECTED_PORTS_DISPLAY)
        )

    def _update_no_gps_hint(self):
        send_without_gps = bool(self._send_nodes_without_gps_var.get())
        has_park = bool(self._park_lat_var.get().strip()) and bool(self._park_lon_var.get().strip())
        if send_without_gps and not has_park:
            self._no_gps_hint_var.set(
                "Hinweis: für Nodes ohne GPS bitte park_lat und park_lon setzen, sonst werden sie übersprungen."
            )
        else:
            self._no_gps_hint_var.set("")

    def _parse_int_field(self, raw_value, field_name, min_value=MIN_PORT_NUMBER, max_value=MAX_PORT_NUMBER):
        try:
            value = int(str(raw_value).strip())
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} muss eine ganze Zahl sein.")
        if not (min_value <= value <= max_value):
            raise ValueError(f"{field_name} muss zwischen {min_value} und {max_value} liegen.")
        return value

    def _apply_form_to_cfg(self, ports):
        self.cfg["log_level"] = self._log_level_var.get()
        self.cfg["meshtastic_port"] = ports[0] if len(ports) == 1 else ports
        self.cfg["tak_server_host"] = self._server_host_var.get().strip()
        self.cfg["tak_server_protocol"] = self._server_protocol_var.get().strip().upper() or "TCP"
        self.cfg["local_tak_ip"] = self._local_tak_ip_var.get().strip() or "127.0.0.1"

        self.cfg["tak_server_port"] = self._parse_int_field(self._server_port_var.get(), "Remote Port")
        self.cfg["local_tak_port"] = self._parse_int_field(self._local_tak_port_var.get(), "Local TAK Port")
        self.cfg["local_tak_chat_listen_port"] = self._parse_int_field(
            self._local_tak_chat_listen_port_var.get(), "Local TAK Chat Listen Port"
        )
        self.cfg["local_tak_tcp_listen_port"] = self._parse_int_field(
            self._local_tak_tcp_listen_port_var.get(), "Local TAK TCP Listen Port"
        )
        if self.cfg["local_tak_chat_listen_port"] == self.cfg["local_tak_port"]:
            raise ValueError(
                f"Local TAK Chat Listen Port ({self.cfg['local_tak_chat_listen_port']}) "
                f"must differ from Local TAK Port ({self.cfg['local_tak_port']})."
            )
        self.cfg["sync_interval_seconds"] = self._parse_int_field(
            self._sync_interval_var.get(), "Sync-Intervall", min_value=1, max_value=86400
        )
        self.cfg["relay_text_messages"] = bool(self._relay_text_messages_var.get())
        relay_from_ports = _parse_ports_text(self._relay_text_from_ports_var.get())
        relay_to_ports = _parse_ports_text(self._relay_text_to_ports_var.get())
        selected_ports_upper = {port.upper() for port in ports}
        unknown_relay_from = [port for port in relay_from_ports if port.upper() not in selected_ports_upper]
        unknown_relay_to = [port for port in relay_to_ports if port.upper() not in selected_ports_upper]
        if unknown_relay_from:
            raise ValueError(
                "Relay von COM enthält nicht ausgewählte Ports: " + ", ".join(unknown_relay_from)
            )
        if unknown_relay_to:
            raise ValueError(
                "Relay nach COM enthält nicht ausgewählte Ports: " + ", ".join(unknown_relay_to)
            )
        _store_ports_in_config(self.cfg, "relay_text_from_ports", relay_from_ports)
        _store_ports_in_config(self.cfg, "relay_text_to_ports", relay_to_ports)

        self.cfg["send_nodes_without_gps"] = bool(self._send_nodes_without_gps_var.get())
        self.cfg["set_gateway_position_on_start"] = bool(self._set_gateway_position_var.get())

        park_lat_text = self._park_lat_var.get().strip()
        park_lon_text = self._park_lon_var.get().strip()
        if park_lat_text and park_lon_text:
            try:
                lat_val = float(park_lat_text)
            except ValueError:
                raise ValueError("park_lat muss eine gültige Zahl sein.")
            try:
                lon_val = float(park_lon_text)
            except ValueError:
                raise ValueError("park_lon muss eine gültige Zahl sein.")
            if lat_val == 0.0 and lon_val == 0.0:
                raise ValueError(
                    "park_lat und park_lon dürfen nicht beide 0.0 sein (Null Island / kein gültiger Standort). "
                    "Bitte echte Koordinaten eintragen oder beide Felder leer lassen."
                )
            self.cfg["park_lat"] = lat_val
            self.cfg["park_lon"] = lon_val
        elif not park_lat_text and not park_lon_text:
            self.cfg.pop("park_lat", None)
            self.cfg.pop("park_lon", None)
        else:
            missing = "park_lon" if park_lat_text else "park_lat"
            raise ValueError(
                "Beide Koordinaten (park_lat und park_lon) müssen gesetzt sein oder beide leer bleiben. "
                f"Aktuell fehlt: {missing}."
            )

    # ─────────────────────────── Gateway-Steuerung ────────────────────────────

    def _on_start(self):
        ports_text = self._ports_var.get().strip()
        ports = _parse_ports_text(ports_text)
        if not ports:
            self._append_log("Kein Port angegeben.", "WARNING")
            return

        try:
            self._apply_form_to_cfg(ports)
            save_config(self.cfg)
        except Exception as e:
            self._append_log(f"Ungültige Einstellungen: {e}", "WARNING")
            return

        self._start_btn.configure(state="disabled")
        self._stop_btn.configure(state="normal")
        self._status_var.set("🟡 Startet …")

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
        self._mesh_test_status_var.set("Gateway startet …")

    def _run_gateway(self, ports):
        try:
            gw = TAKMeshtasticGateway(ports, self.cfg)
            self._gateway = gw
            self._root.after(0, lambda: self._status_var.set(f"🟢 Läuft  –  {', '.join(ports)}"))
            self._root.after(0, lambda: self._send_mesh_test_btn.configure(state="normal"))
            self._root.after(0, lambda: self._mesh_test_status_var.set("Bereit für manuelles Mesh-Test-Senden."))
            gw.run()
        except Exception:
            err = traceback.format_exc()
            self._queue_log(f"Gateway-Thread Fehler:\n{err}", "ERROR")
        finally:
            self._root.after(0, self._on_gateway_stopped)

    def _on_stop(self):
        gw = self._gateway
        if gw:
            gw.shutdown_flag.set()
        self._stop_btn.configure(state="disabled")
        self._status_var.set("🟡 Stoppt …")
        self._send_mesh_test_btn.configure(state="disabled")
        self._mesh_test_status_var.set("Gateway stoppt …")

    def _on_gateway_stopped(self):
        if self._gui_handler:
            logging.getLogger("TAK_Meshtastic_Gateway").removeHandler(self._gui_handler)
            self._gui_handler = None
        self._gateway = None
        self._start_btn.configure(state="normal")
        self._stop_btn.configure(state="disabled")
        self._status_var.set("⬛ Gestoppt")
        self._send_mesh_test_btn.configure(state="disabled")
        self._mesh_test_status_var.set("Gateway nicht gestartet.")

    def _on_manual_sync(self):
        gw = self._gateway
        if gw:
            threading.Thread(target=gw.full_sync, daemon=True).start()
            self._append_log("Manuelle Vollsynchronisation ausgelöst.", "INFO")
        else:
            self._append_log("Gateway ist nicht gestartet.", "WARNING")

    def _on_send_mesh_test_message(self):
        message = self._mesh_test_message_var.get().strip()
        if not message:
            self._mesh_test_status_var.set("⚠ Bitte zuerst eine Testnachricht eingeben.")
            self._append_log("Manuelles Mesh-Test-Senden abgebrochen: leere Nachricht.", "WARNING")
            return

        gw = self._gateway
        if not gw:
            self._mesh_test_status_var.set("⚠ Gateway ist nicht gestartet.")
            self._append_log("Manuelles Mesh-Test-Senden nicht möglich: Gateway ist nicht gestartet.", "WARNING")
            return

        self._mesh_test_status_var.set("🟡 Testnachricht wird gesendet …")
        threading.Thread(
            target=self._send_mesh_test_message_worker,
            args=(gw, message),
            daemon=True,
        ).start()

    def _send_mesh_test_message_worker(self, gw, message):
        try:
            total_sent = gw._send_text_to_meshtastic(message)
        except Exception as exc:
            self._queue_log(f"Manuelles Senden ins Mesh fehlgeschlagen: {exc}", "ERROR")
            self._queue_log("Fehlerdetails manuelles Senden ins Mesh:\n" + traceback.format_exc(), "DEBUG")
            self._root.after(
                0,
                lambda: self._mesh_test_status_var.set(f"❌ Senden fehlgeschlagen: {exc}"),
            )
            return

        self._queue_log(f"Testnachricht erfolgreich ins Mesh gesendet (Interfaces: {total_sent}).", "INFO")
        self._root.after(0, lambda: self._mesh_test_status_var.set(f"✅ Gesendet (Interfaces: {total_sent})."))
        self._root.after(0, lambda: self._mesh_test_message_var.set(""))

    def _on_log_level_change(self, _event=None):
        level_str = self._log_level_var.get()
        level = getattr(logging, level_str, logging.INFO)
        logging.getLogger("TAK_Meshtastic_Gateway").setLevel(level)
        if self._gui_handler:
            self._gui_handler.setLevel(level)
        self._append_log(f"Log-Level geändert auf {level_str}.", "INFO")

    # ─────────────────────────── Befehlseingabe ───────────────────────────────

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
                self._append_log(f"Unbekannter Level: '{parts[1]}'. Gültig: debug|info|warning|error", "WARNING")
        elif action == "clear":
            self._log_text.configure(state="normal")
            self._log_text.delete("1.0", "end")
            self._log_text.configure(state="disabled")
        elif action == "nodes":
            gw = self._gateway
            if not gw:
                self._append_log("Gateway ist nicht gestartet.", "WARNING")
                return
            rows = gw.node_db.get_all_nodes()
            if not rows:
                self._append_log("Keine Knoten in der Datenbank.", "INFO")
            else:
                self._append_log(
                    f"{'UID':<22} {'Rufzeichen':<20} {'Lat':>10} {'Lon':>11} {'Alt':>6}  {'Quelle':<10} Zuletzt gesehen",
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
                self._append_log("setpos: ungültige Koordinaten. Syntax: setpos <uid> <lat> <lon> [alt]", "WARNING")
                return
            normalized = normalize_coordinates(lat, lon)
            if normalized is None:
                self._append_log(f"setpos: ungültige Koordinaten ({lat}, {lon}).", "WARNING")
                return
            lat, lon = normalized
            gw = self._gateway
            if not gw:
                self._append_log("Gateway ist nicht gestartet.", "WARNING")
                return
            gw.node_db.set_manual_position(uid, lat, lon, alt)
            gw.last_known_positions[uid] = (lat, lon, alt)
            self._append_log(f"Position für '{uid}' auf ({lat:.6f}, {lon:.6f}, {alt:.0f}m) gesetzt.", "INFO")
        elif action == "setpos":
            self._append_log("Syntax: setpos <uid> <lat> <lon> [alt]", "WARNING")
        elif action == "help":
            for line in [
                "Verfügbare Befehle:",
                "  sync                         – Manuelle Vollsynchronisation aller Nodes",
                "  nodes                        – Alle Knoten aus der Datenbank anzeigen",
                "  setpos <uid> <lat> <lon> [alt] – Position eines Knotens manuell setzen",
                "  log <level>                  – Log-Level setzen (debug/info/warning/error)",
                "  clear                        – Log-Ausgabe leeren",
                "  help                         – Diese Hilfe anzeigen",
            ]:
                self._append_log(line, "INFO")
        else:
            self._append_log(f"Unbekannter Befehl: '{cmd}'. Tippe 'help' für Hilfe.", "WARNING")

    # ─────────────────────────── Logging in GUI ───────────────────────────────

    def _queue_log(self, msg, level="INFO"):
        """Thread-sicherer Aufruf: Nachricht in den GUI-Thread einreihen."""
        self._root.after(0, self._append_log, msg, level)

    def _append_log(self, msg, level="INFO"):
        self._log_text.configure(state="normal")
        tag = level if level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "CMD") else "INFO"
        self._log_text.insert("end", msg + "\n", tag)
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
            server_port = int(self.cfg.get("tak_server_port", 8087))
            if not (1 <= server_port <= 65535):
                raise ValueError(f"Invalid server port: {server_port}")
            self.server_port = server_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid tak_server_port in config: {e}")
        
        self.server_protocol = str(self.cfg.get("tak_server_protocol", "TCP")).upper()

        # lokaler WinTAK / TAK-Client (Standard)
        self.tak_ip = self.cfg.get("local_tak_ip", "127.0.0.1")
        try:
            tak_port = int(self.cfg.get("local_tak_port", 4242))
            if not (1 <= tak_port <= 65535):
                raise ValueError(f"Invalid local TAK port: {tak_port}")
            self.tak_port = tak_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid local_tak_port in config: {e}")

        default_chat_listen_port = self.tak_port + 1 if self.tak_port < MAX_PORT_NUMBER else DEFAULT_CHAT_LISTEN_PORT
        try:
            chat_listen_port = int(
                self.cfg.get(
                    "local_tak_chat_listen_port",
                    default_chat_listen_port,
                )
            )
            if not (1 <= chat_listen_port <= 65535):
                raise ValueError(f"Invalid local TAK chat listen port: {chat_listen_port}")
            if chat_listen_port == self.tak_port:
                raise ValueError("local_tak_chat_listen_port must differ from local_tak_port")
            self.chat_listen_port = chat_listen_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid local_tak_chat_listen_port in config: {e}")
        self.chat_listen_ip = str(self.cfg.get("local_tak_chat_listen_ip", "0.0.0.0")).strip() or "0.0.0.0"
        try:
            tcp_chat_listen_port = int(
                self.cfg.get("local_tak_tcp_listen_port", TCP_LISTENER_DEFAULT_PORT)
            )
            if not (1 <= tcp_chat_listen_port <= 65535):
                raise ValueError(f"Invalid local TAK TCP listen port: {tcp_chat_listen_port}")
            self.tcp_chat_listen_port = tcp_chat_listen_port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid local_tak_tcp_listen_port in config: {e}")
        self.tcp_chat_listen_ip = str(self.cfg.get("local_tak_tcp_listen_ip", "127.0.0.1")).strip() or "127.0.0.1"
        
        self.sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_udp.settimeout(self.SOCKET_TIMEOUT)  # Add timeout to prevent hanging
        self.sock_chat_listeners = []

        # Remote server socket(s)
        self.sock_remote = None  # für TCP: persistent socket; für UDP: socket used for sendto

        # interne State
        self.logger = self.setup_logging()
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
        self.partial_meshtastic_cot_messages = {}
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
            threading.Thread(
                target=self.listen_for_tak_chat_tcp,
                args=(self.tcp_chat_listen_port,),
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

    def _extract_cot_event_metadata(self, packet_xml):
        try:
            root = fromstring(packet_xml)
        except (ParseError, TypeError, ValueError):
            return None
        if _xml_local_name(root.tag) != "event":
            return None
        return {
            "uid": (root.get("uid") or "").strip(),
        }

    def _build_cot_dedupe_key(self, packet_xml):
        packet_bytes = _ensure_bytes(packet_xml).strip()
        if not packet_bytes:
            return None
        metadata = self._extract_cot_event_metadata(packet_bytes)
        if metadata and metadata["uid"]:
            return metadata["uid"]
        return hashlib.sha256(packet_bytes).hexdigest()

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
            }

        if not self._is_atak_plugin_packet(packet):
            return None

        payload = decoded.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return None
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
        }

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

    def _send_packet_to_tak(self, packet_xml, label):
        cot_dedupe_key = self._build_cot_dedupe_key(packet_xml)
        if cot_dedupe_key:
            self._remember_recent_chat(self.recent_cot_ids, cot_dedupe_key)
        try:
            self.sock_udp.sendto(packet_xml, (self.tak_ip, self.tak_port))
            self.logger.debug(f"Lokales UDP gesendet an {self.tak_ip}:{self.tak_port} ({label})")
        except Exception as e:
            self.logger.warning(f"Fehler beim Senden an lokales TAK (UDP): {e}")

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

    def _build_chat_cot_xml(self, sender_uid, callsign, message, lat, lon, alt=0.0, chatroom=DEFAULT_CHATROOM_NAME):
        t = get_tak_timestamp()
        stale = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        destination_id = chatroom or DEFAULT_CHATROOM_NAME
        event = Element('event', {
            'version': '2.0',
            'uid': f"GeoChat.{sender_uid}.{uuid.uuid4()}",
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
            'chatroom': destination_id,
            'id': destination_id,
            'senderCallsign': callsign,
        })
        SubElement(chat, 'chatgrp', {
            'uid0': sender_uid,
            'uid1': destination_id,
            'id': destination_id,
        })
        SubElement(detail, 'link', {
            'uid': sender_uid,
            'relation': 'p-p',
            'type': 'a-f-G-U-C',
        })
        remarks = SubElement(detail, 'remarks', {
            'source': f"BAO.F.ATAK.{sender_uid}",
            'to': destination_id,
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

    def _get_interfaces_snapshot(self):
        with self.interface_lock:
            return list(self.interfaces)

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
        if reconnect or not interfaces:
            if reconnect:
                self.logger.warning(
                    "Meshtastic-COM-Verbindung für ausgehende Nachrichten wird neu aufgebaut."
                    + (f" Grund: {reason}" if reason else "")
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

    def _send_text_to_interfaces(self, message, interfaces, allow_reconnect=True):
        interfaces = [iface for iface in (interfaces or []) if iface is not None]
        if not interfaces:
            if allow_reconnect:
                interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
            else:
                raise RuntimeError("Keine verbundenen Meshtastic-Interfaces verfügbar.")
        sent_interfaces = []
        last_error = None
        for iface in interfaces:
            try:
                # Broadcast Chat/CoT always on the primary Meshtastic channel 0.
                kwargs = self._build_meshtastic_send_kwargs(iface)
                iface.sendText(message, **kwargs)
                sent_interfaces.append(iface)
            except Exception as exc:
                last_error = exc
                self.logger.warning(
                    f"Fehler beim Senden einer Meshtastic-Nachricht auf {self._get_interface_label(iface)}: {exc}"
                )
        if not sent_interfaces and last_error is not None and allow_reconnect:
            retry_interfaces = self._ensure_meshtastic_interfaces(reconnect=True, reason=str(last_error))
            if retry_interfaces:
                try:
                    return self._send_text_to_interfaces(message, retry_interfaces, allow_reconnect=False)
                except Exception as retry_exc:
                    self.logger.warning(
                        "Meshtastic-Senden ist auch nach COM-Neuverbindung fehlgeschlagen: "
                        f"erst '{last_error}', dann '{retry_exc}'"
                    )
                    raise retry_exc from last_error
        if not sent_interfaces and last_error is not None:
            raise last_error
        return sent_interfaces

    def _get_relay_targets(self, source_interface):
        source_label = self._get_interface_label(source_interface).upper()
        if self.relay_text_from_ports and source_label not in self.relay_text_from_ports:
            return []
        relay_targets = []
        for iface in self._get_interfaces_snapshot():
            if iface is source_interface:
                continue
            target_label = self._get_interface_label(iface).upper()
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

    def _forward_cot_to_meshtastic(self, packet_xml):
        interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
        cot_chunks = self._prepare_meshtastic_cot_chunks(packet_xml)
        if not cot_chunks:
            raise ValueError("Leere CoT-Nachricht kann nicht ins Mesh gesendet werden.")
        for chunk in cot_chunks:
            self._send_text_to_interfaces(chunk, interfaces)
            interfaces = self._get_interfaces_snapshot()
            self._remember_recent_chat(self.recent_meshtastic_outbound_texts, chunk)
        return cot_chunks

    def _handle_meshtastic_cot_chunk(self, message, from_id):
        chunk = self._parse_meshtastic_cot_chunk(message)
        if chunk is None:
            return False

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

        try:
            packet_xml = base64.urlsafe_b64decode(encoded_packet.encode("ascii"))
        except Exception as exc:
            self.logger.warning(f"Mesh-CoT konnte nicht dekodiert werden: {exc}")
            self.logger.debug("Fehler beim Dekodieren einer Mesh-CoT-Nachricht:\n" + traceback.format_exc())
            return True

        cot_dedupe_key = self._build_cot_dedupe_key(packet_xml)
        if cot_dedupe_key and self._was_seen_recently(self.recent_cot_ids, cot_dedupe_key):
            return True

        metadata = self._extract_cot_event_metadata(packet_xml)
        if metadata is None:
            self.logger.warning("Mesh-CoT verworfen: empfangene Nachricht ist kein gültiges CoT-Event.")
            return True

        self._send_packet_to_tak(packet_xml, metadata["uid"] or f"Mesh-CoT {from_id}")
        self.logger.info(
            f"CoT aus dem Mesh nach TAK weitergeleitet: {metadata['uid'] or f'ohne UID von {from_id}'}"
        )
        return True

    def send_chat_to_tak(self, sender_uid, callsign, message, lat, lon, alt=0.0, chatroom=DEFAULT_CHATROOM_NAME):
        try:
            packet_xml = self._build_chat_cot_xml(sender_uid, callsign, message, lat, lon, alt, chatroom=chatroom)
            self._send_packet_to_tak(packet_xml, f"Chat {callsign}")
        except Exception:
            self.logger.error("Fehler in send_chat_to_tak:\n" + traceback.format_exc())

    def process_meshtastic_text_message(self, packet, node=None, source_interface=None):
        chat_payload = self._extract_meshtastic_chat_payload(packet, node=node)
        if not chat_payload:
            return
        message = chat_payload["message"]

        from_id = packet.get('fromId') or packet.get('from')
        from_num = packet.get('from')
        if (from_id in self.local_node_ids or from_num in self.local_node_numbers) and self._was_seen_recently(
            self.recent_meshtastic_outbound_texts, message
        ):
            self.logger.debug("Echo einer gerade vom Gateway gesendeten Chatnachricht ignoriert.")
            return

        message_id = packet.get('id')
        message_hash = hashlib.sha256(message.encode("utf-8", errors="ignore")).hexdigest()
        dedupe_key = f"mesh:{message_id}" if message_id is not None else f"mesh:{from_id}:{message_hash}"
        if self._was_seen_recently(self.recent_meshtastic_chat_ids, dedupe_key):
            return
        self._remember_recent_chat(self.recent_meshtastic_chat_ids, dedupe_key)

        if self._handle_meshtastic_cot_chunk(message, from_id or "MESH-UNKNOWN"):
            return

        if self.relay_text_messages and len(self.interfaces) > 1:
            relay_targets = self._get_relay_targets(source_interface)
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
        lat, lon, alt = self._resolve_chat_position(sender_uid, node=node)
        self.send_chat_to_tak(sender_uid, callsign, message, lat, lon, alt, chatroom=chatroom)
        self.logger.info(f"Meshtastic-Chat nach TAK weitergeleitet: {callsign}: {message}")

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
        event_uid = root.get("uid") or ""
        event_type = str(root.get("type") or "").lower()
        has_chat_identity = event_uid.startswith(GEOCHAT_UID_PREFIX) or event_type.startswith("b-t-f")
        has_chat_elements = any(element is not None for element in (chat, chat_note, chatgrp))
        has_chat_remarks = _looks_like_tak_chat_remarks(remarks)

        message = ""
        if remarks is not None and remarks.text:
            message = _extract_latest_wintak_chat_message(remarks.text)
        note = _find_descendant_by_local_name(detail, "note")
        if not message:
            for element in (note, chat_note, chat, remarks, chatgrp):
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
            return None
        if not (has_chat_identity or has_chat_elements or has_chat_remarks):
            return None

        link = _find_descendant_by_local_name(detail, "link")
        contact = _find_descendant_by_local_name(detail, "contact")
        uid_parts = [part for part in event_uid.split(".") if part]
        sender_uid = ""
        if link is not None and link.get("uid"):
            sender_uid = link.get("uid")
        if not sender_uid and remarks is not None:
            sender_uid = remarks.get("sourceID") or remarks.get("source") or sender_uid
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
        sender_callsign = _strip_tak_sender_prefix(sender_callsign)
        if not sender_callsign:
            sender_callsign = "UNKNOWN-SENDER"

        chatroom = DEFAULT_CHATROOM_NAME
        if chat is not None:
            chatroom = (
                chat.get("chatroom")
                or chat.get("chatRoom")
                or chat.get("id")
                or chat.get("parent")
                or chatroom
            )
        if chatroom == DEFAULT_CHATROOM_NAME and chatgrp is not None:
            chatroom = chatgrp.get("id") or chatgrp.get("name") or chatroom
        # WinTAK GeoChat UIDs typically follow GeoChat.<sender>.<chatroom>.<messageId>.
        if chatroom == DEFAULT_CHATROOM_NAME and len(uid_parts) >= 3:
            chatroom = uid_parts[2] or chatroom
        if chatroom == DEFAULT_CHATROOM_NAME and remarks is not None:
            chatroom = remarks.get("to") or chatroom

        return {
            "event_uid": root.get("uid"),
            "sender_uid": sender_uid,
            "sender_callsign": sender_callsign,
            "chatroom": chatroom,
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

    def _normalize_inbound_tak_packet(self, packet_xml):
        packet_bytes = _ensure_bytes(packet_xml)
        if packet_bytes.startswith(b"\xef\xbb\xbf"):
            packet_bytes = packet_bytes[3:]
        return _extract_first_tak_event(packet_bytes)

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

    def handle_inbound_tak_packet(self, packet_xml, source_addr=None, source_protocol=None):
        metadata = self._extract_cot_event_metadata(packet_xml)
        cot_dedupe_key = None
        if metadata is not None:
            cot_dedupe_key = metadata["uid"] or self._build_cot_dedupe_key(packet_xml)
            if cot_dedupe_key and self._was_seen_recently(self.recent_cot_ids, cot_dedupe_key):
                return

        chat_payload = self._extract_tak_chat_payload(packet_xml)
        if chat_payload:
            sender_uid = chat_payload["sender_uid"] or self.gateway_uid
            if sender_uid in self.local_node_ids:
                self.logger.debug("TAK-Chat vom lokalen Meshtastic-Knoten ignoriert, um Echos zu vermeiden.")
                return

            dedupe_key = chat_payload["event_uid"] or f"tak:{sender_uid}:{chat_payload['message']}"
            if self._was_seen_recently(self.recent_tak_chat_ids, dedupe_key):
                self.logger.debug("TAK-Chat wegen Duplikat-Schutz ignoriert.")
                return

            try:
                sent_chunks = self._prepare_meshtastic_text_chunks(chat_payload["message"])
                total_sent = self._send_text_to_meshtastic(chat_payload["message"], prepared_chunks=sent_chunks)
            except Exception as exc:
                self.logger.warning(f"TAK-Chat konnte nicht ins Mesh gesendet werden: {exc}")
                self.logger.debug("Fehler beim Senden von TAK-Chat ins Mesh:\n" + traceback.format_exc())
                return
            self._remember_recent_chat(self.recent_tak_chat_ids, dedupe_key)
            for chunk in sent_chunks:
                self._remember_recent_chat(self.recent_meshtastic_outbound_texts, chunk)
            src = chat_payload["sender_callsign"]
            if len(sent_chunks) > 1:
                self.logger.info(
                    f"TAK-Chat wurde in {len(sent_chunks)} Mesh-Nachrichten über {total_sent} Interface(s) gesendet: "
                    f"{src}: {chat_payload['message']}"
                )
            else:
                self.logger.info(
                    f"TAK-Chat ins Mesh über {total_sent} Interface(s) gesendet: {src}: {chat_payload['message']}"
                )
            return

        if metadata is None:
            protocol_label = str(source_protocol or DEFAULT_SOURCE_PROTOCOL).upper()
            self.logger.debug(
                f"{protocol_label}-Paket am TAK-Listener empfangen, aber nicht als CoT erkannt"
                + (f" ({source_addr[0]}:{source_addr[1]})" if source_addr else "")
            )
            return

        try:
            sent_chunks = self._forward_cot_to_meshtastic(packet_xml)
        except Exception as exc:
            self.logger.warning(f"TAK-CoT konnte nicht ins Mesh gesendet werden: {exc}")
            self.logger.debug("Fehler beim Senden von TAK-CoT ins Mesh:\n" + traceback.format_exc())
            return
        if cot_dedupe_key:
            self._remember_recent_chat(self.recent_cot_ids, cot_dedupe_key)
        self.logger.info(
            f"TAK-CoT ins Mesh gesendet: {metadata['uid'] or 'ohne UID'} "
            f"({len(sent_chunks)} Fragment{'e' if len(sent_chunks) != 1 else ''})"
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
            normalized_packet = self._normalize_inbound_tak_packet(packet_xml)
            if not normalized_packet:
                continue
            self.handle_inbound_tak_packet(normalized_packet, source_addr=addr, source_protocol="UDP")

    def _handle_tak_tcp_client(self, conn, addr):
        buffer_text = ""
        decoder = codecs.getincrementaldecoder("utf-8")()
        conn.settimeout(TCP_SOCKET_TIMEOUT_SECONDS)
        try:
            while not self.shutdown_flag.is_set():
                try:
                    data = conn.recv(TCP_RECV_BUFFER_SIZE)
                    if not data:
                        break
                    buffer_text += decoder.decode(data)
                    events, buffer_text = self._extract_tak_events_from_stream_buffer(buffer_text)
                    for packet_xml in events:
                        normalized_packet = self._normalize_inbound_tak_packet(packet_xml)
                        if not normalized_packet:
                            continue
                        self.handle_inbound_tak_packet(normalized_packet, source_addr=addr, source_protocol="TCP")
                except socket.timeout:
                    continue
            buffer_text += decoder.decode(b"", final=True)
            events, buffer_text = self._extract_tak_events_from_stream_buffer(buffer_text)
            for packet_xml in events:
                normalized_packet = self._normalize_inbound_tak_packet(packet_xml)
                if not normalized_packet:
                    continue
                self.handle_inbound_tak_packet(normalized_packet, source_addr=addr, source_protocol="TCP")
        except OSError:
            self.logger.debug("Fehler beim Lesen einer TAK-TCP-Verbindung:\n" + traceback.format_exc())
        finally:
            try:
                conn.close()
            except Exception:
                pass

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
            if self._is_text_message_packet(packet) or self._is_atak_plugin_packet(packet):
                self.process_meshtastic_text_message(packet, node=node, source_interface=interface)
            if from_id and node:
                # Force update für Live-Events
                self.process_node(node, 0, force_update=True)
        except Exception:
            self.logger.debug("Fehler im on_any_packet:\n" + traceback.format_exc())

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
                'type': 'a-f-G-U-C',
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
            SubElement(detail, 'contact', {'callsign': callsign, 'endpoint': f"{self.tak_ip}:{self.tak_port}:udp"})
            SubElement(detail, '__group', {'name': 'Cyan', 'role': 'Team Member'})
            if position_source == "GPS":
                geopointsrc = "GPS"
            elif position_source == "LAST_KNOWN":
                geopointsrc = "ESTIMATED"
            else:
                geopointsrc = "USER"
            SubElement(detail, 'precisionlocation', {'geopointsrc': geopointsrc})
            if position_source == "LAST_KNOWN":
                SubElement(detail, 'remarks').text = "Listed (Last Known Position)"
            elif not is_real:
                SubElement(detail, 'remarks').text = "Listed (No GPS Fix)"

            # ElementTree.tostring mit UTF-8 ergibt bytes
            packet_xml = tostring(event, encoding='utf-8')
            self._send_packet_to_tak(packet_xml, callsign)
        except Exception:
            self.logger.error("Fehler in send_broadcast:\n" + traceback.format_exc())

    def _cleanup_server_socket(self):
        """
        Cleanup remote server socket
        """
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
        print("Bitte installiere sie (z.B. pip install meshtastic pypubsub pyserial colorlog pyyaml).")
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
            "--no-gui", action="store_true",
            help="GUI deaktivieren und im Terminal-Modus starten"
        )
        args = parser.parse_args()

        cfg = load_config()

        # GUI-Modus wenn Tkinter verfügbar und nicht explizit deaktiviert
        if tk is not None and not args.no_gui:
            try:
                app = GatewayApp(cfg)
                app.run()
            except (tk.TclError, RuntimeError):
                # Kein Display / Tkinter-Fehler → Terminal-Fallback
                _run_terminal_mode(cfg, args)
        else:
            _run_terminal_mode(cfg, args)

    except Exception:
        print("Unerwarteter Fehler beim Starten:")
        traceback.print_exc()
        print("Drücke Enter zum Beenden...")
        input()
        sys.exit(1)
