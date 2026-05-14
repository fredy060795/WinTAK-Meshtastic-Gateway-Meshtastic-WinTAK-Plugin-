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
import ipaddress
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
INBOUND_TAK_DEBUG_SNIPPET_CHARS = 240
UTF8_BOM_CHAR = "\ufeff"
RECENT_CHAT_CACHE_TTL_SECONDS = 30
RECENT_CHAT_CACHE_MAX_ENTRIES = 256
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


MESHTASTIC_ATAK_PLUGIN_PORTNUM = _resolve_meshtastic_portnum(
    "ATAK_PLUGIN_V2",
    72,
    "ATAK_PLUGIN",
)
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
        if local_name in _TAK_CHAT_MESSAGE_FIELD_NAMES:
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


def _normalize_meshtastic_enum_key(value):
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


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
        self._root.configure(bg="#1f2937")

        self._setup_styles()
        self._build_ui()

    # ─────────────────────────── Styles / Theming ─────────────────────────────

    def _setup_styles(self):
        style = ttk.Style(self._root)
        style.theme_use("clam")

        # ── Farben ──
        BG        = "#081311"   # Window background
        PANEL     = "#12231f"   # Header/panel background
        CARD      = "#17302a"   # Card/frame background
        BORDER    = "#2f5b4e"   # Border color
        FG        = "#effaf4"   # Main text
        FG_SUB    = "#9ec3b6"   # Subtle helper text
        ACCENT    = "#3fd48b"   # Primary accent
        ACCENT_H  = "#69e8a5"   # Hover
        SUCCESS   = "#63e6af"   # Success
        WARNING   = "#f2d16b"   # Warning
        DANGER    = "#ff7b72"   # Danger
        TEXT_BG   = "#0b1916"   # Text/log background

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
            "text_bg": TEXT_BG,
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

        header = tk.Frame(root, bg=C["panel"], bd=0, highlightthickness=1,
                          highlightbackground=C["border"], highlightcolor=C["border"])
        header.pack(fill="x", padx=10, pady=(10, 0))

        header_left = tk.Frame(header, bg=C["panel"])
        header_left.pack(side="left", fill="both", expand=True, padx=(12, 8), pady=10)
        header_right = tk.Frame(header, bg=C["panel"])
        header_right.pack(side="right", fill="y", padx=(8, 12), pady=10)

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
            text="WinTAK Meshtastic Gateway",
            bg=C["panel"],
            fg=C["fg"],
            font=("Segoe UI", 18, "bold"),
            anchor="w",
        ).pack(fill="x")
        tk.Label(
            title_block,
            text="Live gateway operations with direct mesh text and WinTAK CoT send controls.",
            bg=C["panel"],
            fg=C["fg_sub"],
            font=("Segoe UI", 10),
            anchor="w",
        ).pack(fill="x", pady=(2, 8))
        tk.Label(
            title_block,
            text="MESH <-> TAK CONTROL SURFACE",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        ).pack(fill="x")

        endpoint_items = [
            ("WinTAK UDP", f"{self.cfg.get('local_tak_ip', WINTAK_REQUIRED_HOST)}:{self.cfg.get('local_tak_port', 4242)}"),
            ("WinTAK TCP", f"0.0.0.0:{self.cfg.get('local_tak_tcp_listen_port', TCP_LISTENER_DEFAULT_PORT)}"),
            ("Remote TAK", f"{self.cfg.get('tak_server_protocol', 'TCP')} {self.cfg.get('tak_server_host', '82.165.11.84')}:{self.cfg.get('tak_server_port', 8088)}"),
        ]
        for title, value in endpoint_items:
            card = tk.Frame(
                header_right,
                bg=C["bg"],
                bd=1,
                highlightthickness=1,
                highlightbackground=C["border"],
                highlightcolor=C["border"],
                padx=10,
                pady=6,
            )
            card.pack(fill="x", pady=2)
            tk.Label(card, text=title, bg=C["bg"], fg=C["fg_sub"], font=("Segoe UI", 8, "bold")).pack(anchor="w")
            tk.Label(card, text=value, bg=C["bg"], fg=C["fg"], font=("Segoe UI", 9)).pack(anchor="w", pady=(2, 0))

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
            bg=C["text_bg"],
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

        self._relay_text_messages_var = tk.BooleanVar(value=as_bool(self.cfg.get("relay_text_messages", True)))
        ttk.Checkbutton(
            cfg_frame,
            text="Enable relay between selected COM ports",
            variable=self._relay_text_messages_var,
        ).grid(row=8, column=0, columnspan=2, sticky="w", pady=(0, 4))
        cfg_label("Relay From COM Port(s):", row=8, col=2, padx=(8, 6), pady=(0, 4))
        self._relay_text_from_ports_var = tk.StringVar(value=_format_ports_for_entry(self.cfg.get("relay_text_from_ports")))
        ttk.Entry(cfg_frame, textvariable=self._relay_text_from_ports_var, width=16).grid(
            row=8, column=3, sticky="ew", pady=(0, 4)
        )

        cfg_label("Relay To COM Port(s):", row=9, col=2, padx=(8, 6), pady=(0, 4))
        self._relay_text_to_ports_var = tk.StringVar(value=_format_ports_for_entry(self.cfg.get("relay_text_to_ports")))
        self._relay_text_to_ports_var.trace_add("write", self._on_relay_to_ports_changed)
        relay_to_frame = ttk.Frame(cfg_frame)
        relay_to_frame.grid(row=9, column=3, columnspan=2, sticky="ew", pady=(0, 4))
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
        ).grid(row=10, column=0, columnspan=5, sticky="w", pady=(0, 4))
        self._refresh_relay_to_picker_options()

        self._send_nodes_without_gps_var = tk.BooleanVar(value=as_bool(self.cfg.get("send_nodes_without_gps", True)))
        ttk.Checkbutton(
            cfg_frame,
            text="Send nodes without GPS fix",
            variable=self._send_nodes_without_gps_var,
            command=self._update_no_gps_hint,
        ).grid(row=11, column=0, columnspan=2, sticky="w", pady=(0, 4))
        cfg_label("Fallback Lat:", row=11, col=2, padx=(8, 6), pady=(0, 4))
        raw_park_lat = self.cfg.get("park_lat")
        park_lat_val = "" if raw_park_lat is None else f"{float(raw_park_lat):.6f}".rstrip("0").rstrip(".")
        self._park_lat_var = tk.StringVar(value=park_lat_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lat_var, width=16).grid(
            row=11, column=3, sticky="w", pady=(0, 4)
        )

        self._wintak_setup_var = tk.StringVar()
        ttk.Label(cfg_frame, textvariable=self._wintak_setup_var, style="Hint.TLabel").grid(
            row=12, column=0, columnspan=2, sticky="w", pady=(0, 4)
        )
        cfg_label("Fallback Lon:", row=12, col=2, padx=(8, 6), pady=(0, 4))
        raw_park_lon = self.cfg.get("park_lon")
        park_lon_val = "" if raw_park_lon is None else f"{float(raw_park_lon):.6f}".rstrip("0").rstrip(".")
        self._park_lon_var = tk.StringVar(value=park_lon_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lon_var, width=16).grid(
            row=12, column=3, sticky="w", pady=(0, 4)
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
            row=13, column=0, columnspan=6, sticky="w", pady=(0, 2)
        )
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

        status_bar = tk.Frame(root, bg=C["panel"], height=26)
        status_bar.pack(fill="x", side="bottom", padx=10, pady=(10, 10))
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
        tcp_port = self._get_wintak_tcp_port_text()
        banner_text = f"Create a local server in WinTAK: {WINTAK_REQUIRED_HOST} | Port {tcp_port} | TCP"
        self._wintak_banner_var.set(banner_text)
        self._wintak_setup_var.set(
            f"WinTAK: add a local server at {WINTAK_REQUIRED_HOST}:{tcp_port} using TCP."
        )
        self._bottom_wintak_hint_var.set(f"WinTAK local: {WINTAK_REQUIRED_HOST} | TCP | Port {tcp_port}")

    def _update_no_gps_hint(self):
        send_without_gps = bool(self._send_nodes_without_gps_var.get())
        has_park = bool(self._park_lat_var.get().strip()) and bool(self._park_lon_var.get().strip())
        if send_without_gps and not has_park:
            self._no_gps_hint_var.set(
                "Note: set park_lat and park_lon for nodes without GPS, or they will be skipped."
            )
        else:
            self._no_gps_hint_var.set("")

    def _parse_int_field(self, raw_value, field_name, min_value=MIN_PORT_NUMBER, max_value=MAX_PORT_NUMBER):
        try:
            value = int(str(raw_value).strip())
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} must be a whole number.")
        if not (min_value <= value <= max_value):
            raise ValueError(f"{field_name} must be between {min_value} and {max_value}.")
        return value

    def _apply_form_to_cfg(self, ports):
        self.cfg["log_level"] = self._log_level_var.get()
        self.cfg["meshtastic_port"] = ports[0] if len(ports) == 1 else ports
        self.cfg["tak_server_host"] = self._server_host_var.get().strip()
        self.cfg["tak_server_protocol"] = self._server_protocol_var.get().strip().upper() or "TCP"
        self.cfg["local_tak_ip"] = self._local_tak_ip_var.get().strip() or WINTAK_REQUIRED_HOST

        self.cfg["tak_server_port"] = self._parse_int_field(self._server_port_var.get(), "Remote Port")
        self.cfg["local_tak_port"] = self._parse_int_field(self._local_tak_port_var.get(), "Local TAK Port")
        self.cfg["local_tak_chat_listen_port"] = self._parse_int_field(
            self._local_tak_chat_listen_port_var.get(), "Local TAK Chat Listen Port"
        )
        self.cfg["local_tak_tcp_listen_port"] = self._parse_int_field(
            self._local_tak_tcp_listen_port_var.get(), "Local TAK TCP Listen Port"
        )
        self.cfg["sync_interval_seconds"] = self._parse_int_field(
            self._sync_interval_var.get(), "Sync Interval", min_value=1, max_value=86400
        )
        self.cfg["relay_text_messages"] = bool(self._relay_text_messages_var.get())
        relay_from_ports = _parse_ports_text(self._relay_text_from_ports_var.get())
        relay_to_choice = self._relay_text_to_picker_var.get().strip()
        if relay_to_choice == "All other selected ports":
            relay_to_ports = []
        else:
            relay_to_ports = _parse_ports_text(self._relay_text_to_ports_var.get())
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
            self.cfg["park_lat"] = lat_val
            self.cfg["park_lon"] = lon_val
        elif not park_lat_text and not park_lon_text:
            self.cfg.pop("park_lat", None)
            self.cfg.pop("park_lon", None)
        else:
            missing = "park_lon" if park_lat_text else "park_lat"
            raise ValueError(
                "Both coordinates (park_lat and park_lon) must be filled in, or both must stay empty. "
                f"Currently missing: {missing}."
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

        self.sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_udp.settimeout(self.SOCKET_TIMEOUT)  # Add timeout to prevent hanging
        self.sock_chat_listeners = []

        # Callback invoked by the TCP listener for connection events and received chat messages.
        # Signature: callback(kind, sender, message, addr)
        # kind: "connect" | "disconnect" | "chat"
        self.wintak_tcp_chat_callback = None

        # Remote server socket(s)
        self.sock_remote = None  # für TCP: persistent socket; für UDP: socket used for sendto
        self.remote_pytak_loop = None
        self.remote_pytak_queue = None
        self.remote_pytak_ready = threading.Event()
        self.remote_cot_url = self._build_remote_cot_url()

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
        self.partial_meshtastic_forwarder_messages = {}
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
        event_uid = (root.get("uid") or "").strip()
        event_type = (root.get("type") or "").strip()
        event_how = (root.get("how") or "").strip()
        if not event_uid or not event_type or not event_how:
            return None
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
            "lat": lat,
            "lon": lon,
            "hae": _coerce_cot_point_float(point.get("hae")),
            "ce": _coerce_cot_point_float(point.get("ce")),
            "le": _coerce_cot_point_float(point.get("le")),
            "has_meshtastic_marker": (
                detail is not None
                and _find_descendant_by_local_name(detail, "__meshtastic") is not None
            ),
        }

    def _normalize_generic_cot_event(self, packet_xml, add_meshtastic_marker=False):
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
            return None, None, "CoT-how fehlt"

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
        if _is_persistable_cot_type(event_type) and _find_child_by_local_name(detail, "archive") is None:
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
        if not _is_meshtastic_pli_event_type(event_type):
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

        detail = _find_child_by_local_name(root, "detail")
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
        if _is_meshtastic_pli_event_type(event_type):
            return None

        metadata = self._extract_cot_event_metadata(packet_xml)
        if metadata is None:
            return None

        detail = _find_child_by_local_name(root, "detail")
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
        packet_bytes = _ensure_bytes(packet_xml).strip()
        if not packet_bytes:
            return None
        metadata = self._extract_cot_event_metadata(packet_bytes)
        if metadata and metadata["uid"]:
            return metadata["uid"]
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
        sender_uid = _strip_tak_sender_prefix(normalize_meshtastic_uid(raw_sender_uid))
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

        team_name = MESHTASTIC_TEAM_ENUM_TO_NAME.get(
            int(group.get("team") or MESHTASTIC_DEFAULT_TEAM_ENUM),
            MESHTASTIC_TEAM_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_TEAM_ENUM, "White"),
        )
        role_name = MESHTASTIC_ROLE_ENUM_TO_NAME.get(
            int(group.get("role") or MESHTASTIC_DEFAULT_ROLE_ENUM),
            MESHTASTIC_ROLE_ENUM_TO_NAME.get(MESHTASTIC_DEFAULT_ROLE_ENUM, "Team Member"),
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
            "type": MESHTASTIC_PLI_COT_EVENT_TYPE,
            "how": "m-g",
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
            "type": MESHTASTIC_PLI_COT_EVENT_TYPE,
        })
        SubElement(detail, "__group", {"name": team_name, "role": role_name})
        SubElement(detail, "status", {"battery": str(battery)})
        SubElement(detail, "track", {"speed": str(speed), "course": str(course)})
        SubElement(detail, "precisionlocation", {"geopointsrc": "GPS"})
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

    def _send_packet_to_tak(self, packet_xml, label):
        cot_dedupe_key = self._build_cot_dedupe_key(packet_xml)
        if cot_dedupe_key:
            self._remember_recent_chat(self.recent_cot_ids, cot_dedupe_key)
        try:
            self.sock_udp.sendto(packet_xml, (self.tak_ip, self.tak_port))
            self.logger.debug(f"Lokales UDP gesendet an {self.tak_ip}:{self.tak_port} ({label})")
        except Exception as e:
            self.logger.warning(f"Fehler beim Senden an lokales TAK (UDP): {e}")

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
        message_id = f"{sender_uid}-{message_hash}-{int(time.time() * 1000)}"
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
        SubElement(detail, '__serverdestination', {
            'destination': f"0.0.0.0:4242:tcp:{sender_uid}",
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

    def _build_meshtastic_send_data_kwargs(self, iface, portnum):
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

    def _send_data_to_interfaces(self, payload, interfaces, portnum, allow_reconnect=True):
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
                kwargs = self._build_meshtastic_send_data_kwargs(iface, portnum)
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
        payload_bytes = _ensure_bytes(payload).strip()
        if not payload_bytes:
            return []
        if len(payload_bytes) <= MESHTASTIC_DATA_PAYLOAD_MAX_BYTES:
            return [payload_bytes]
        if MESHTASTIC_FORWARDER_FRAGMENT_PAYLOAD_BYTES <= 0:
            raise ValueError("Ungültige Meshtastic-Forwarder-Fragmentgröße konfiguriert.")

        total_parts = math.ceil(len(payload_bytes) / MESHTASTIC_FORWARDER_FRAGMENT_PAYLOAD_BYTES)
        if total_parts < 2 or total_parts > 255:
            raise ValueError("Meshtastic-ATAK_FORWARDER-Payload kann nicht sinnvoll fragmentiert werden.")

        message_id = uuid.uuid4().bytes[:MESHTASTIC_FORWARDER_FRAGMENT_MESSAGE_ID_BYTES]
        packets = []
        for index, offset in enumerate(
            range(0, len(payload_bytes), MESHTASTIC_FORWARDER_FRAGMENT_PAYLOAD_BYTES),
            start=1,
        ):
            chunk_payload = payload_bytes[offset:offset + MESHTASTIC_FORWARDER_FRAGMENT_PAYLOAD_BYTES]
            packets.append(
                b"".join(
                    (
                        MESHTASTIC_FORWARDER_FRAGMENT_MAGIC,
                        bytes((MESHTASTIC_FORWARDER_FRAGMENT_VERSION,)),
                        message_id,
                        bytes((index, total_parts)),
                        chunk_payload,
                    )
                )
            )
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

    def _forward_meshtastic_cot_xml_to_tak(self, packet_xml, from_id, source_label="Mesh-CoT"):
        normalized_packet, metadata, error = self._normalize_generic_cot_event(
            packet_xml,
            add_meshtastic_marker=True,
        )
        if normalized_packet is None or metadata is None:
            self.logger.warning(f"{source_label} verworfen: {error or 'ungültiges CoT-Event'}")
            self.logger.debug(f"{source_label} Rohpayload: {_build_safe_payload_snippet(packet_xml)}")
            return True
        if metadata.get("is_marker"):
            self.logger.debug(
                f"Marker-CoT nach Decode/Reassembly validiert: valid=yes source={source_label} "
                f"uid={metadata['uid']} type={metadata['type']} how={metadata['how']}"
            )
        self.logger.debug(
            f"{source_label} rekonstruiert: uid={metadata['uid']} type={metadata['type']} "
            f"how={metadata['how']} lat={metadata['lat']:.6f} lon={metadata['lon']:.6f} "
            f"payload={_build_safe_payload_snippet(normalized_packet)}"
        )
        cot_dedupe_key = self._build_cot_dedupe_key(normalized_packet)
        if cot_dedupe_key and self._was_seen_recently(self.recent_cot_ids, cot_dedupe_key):
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
        self.logger.debug(
            f"ATAK_FORWARDER-Paket aus dem Mesh empfangen: {len(payload)} Byte von {from_id}"
        )
        fragment = self._parse_meshtastic_forwarder_fragment(payload)
        if fragment is not None:
            return self._handle_meshtastic_forwarder_fragment(fragment, from_id)
        packet_xml = self._decode_meshtastic_forwarder_payload(payload)
        if not packet_xml:
            self.logger.warning("ATAK_FORWARDER-Payload aus dem Mesh konnte nicht dekodiert werden.")
            return True
        return self._forward_meshtastic_cot_xml_to_tak(packet_xml, from_id, source_label="ATAK_FORWARDER")

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
        else:
            detail_packet = self._prepare_meshtastic_detail_packet(normalized_packet)
            if detail_packet is not None:
                try:
                    transport_subject = _get_cot_subject_label(metadata)
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug(
                            f"{transport_subject} nutzt ATAK_PLUGIN-detail=7: uid={detail_packet['uid']} "
                            f"compressed={detail_packet['is_compressed']} payload_bytes={len(detail_packet['payload'])} "
                            f"serialized={_build_safe_payload_snippet(detail_packet['payload'])}"
                        )
                    self._send_data_to_interfaces(
                        detail_packet["payload"],
                        interfaces,
                        MESHTASTIC_ATAK_PLUGIN_PORTNUM,
                    )
                    return {"transport": "ATAK_PLUGIN_DETAIL", "count": 1}
                except Exception as exc:
                    self.logger.warning(
                        "ATAK_PLUGIN-detail=7-Senden fehlgeschlagen, versuche ATAK_FORWARDER/COTM-Fallback: "
                        f"{exc}"
                    )
            else:
                self.logger.debug(
                    f"{_get_cot_subject_label(metadata)} Typ {metadata['type']} "
                    "passt nicht in ATAK_PLUGIN-detail=7 und nutzt ATAK_FORWARDER."
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
            self.logger.debug(
                f"{transport_subject} via ATAK_FORWARDER verpackt: paketanzahl={len(forwarder_packets)} "
                f"modus={'direkt' if len(forwarder_packets) == 1 else 'fragmentiert'}"
            )
            for forwarder_packet in forwarder_packets:
                self._send_data_to_interfaces(
                    forwarder_packet,
                    interfaces,
                    MESHTASTIC_ATAK_FORWARDER_PORTNUM,
                )
                interfaces = self._get_interfaces_snapshot()
            transport = "ATAK_FORWARDER" if len(forwarder_packets) == 1 else "ATAK_FORWARDER_FRAGMENTS"
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
        return self._forward_meshtastic_cot_xml_to_tak(packet_xml, from_id, source_label="ATAK_PLUGIN-PLI")

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
        has_chat_message_fields = _has_tak_chat_message_fields(detail)
        generic_message_nodes = []
        for element in detail.iter():
            if element is detail:
                continue
            if _xml_local_name(element.tag).lower() in {"message", "text", "body", "content"}:
                generic_message_nodes.append(element)

        message = ""
        if remarks is not None and remarks.text:
            message = _extract_latest_wintak_chat_message(remarks.text)
        note = _find_descendant_by_local_name(detail, "note")
        if not message:
            for element in (note, chat_note, chat, remarks, chatgrp, *generic_message_nodes):
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
        if not (has_chat_identity or has_chat_elements or has_chat_remarks or has_chat_message_fields):
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
        if not recipient_uid:
            recipient_uid = recipient_callsign or DEFAULT_CHATROOM_NAME
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

    def _send_tak_chat_to_meshtastic(self, chat_payload):
        payload = self._build_meshtastic_geochat_payload(chat_payload)
        interfaces = self._ensure_meshtastic_interfaces(raise_on_empty=True)
        try:
            sent_interfaces = self._send_data_to_interfaces(payload, interfaces, MESHTASTIC_ATAK_PLUGIN_PORTNUM)
            return {
                "transport": "ATAK_PLUGIN_CHAT",
                "count": len(sent_interfaces),
                "chunks": 1,
            }
        except Exception as exc:
            if not (_is_meshtastic_payload_too_big_error(exc) or isinstance(exc, AttributeError)):
                raise
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

        cot_dedupe_key = None
        if metadata is not None:
            cot_dedupe_key = metadata["uid"] or self._build_cot_dedupe_key(packet_xml)
            if cot_dedupe_key and self._was_seen_recently(self.recent_cot_ids, cot_dedupe_key):
                self.logger.debug("TAK-CoT wegen Duplikat-Schutz ignoriert.")
                return
            if metadata.get("has_meshtastic_marker"):
                self.logger.debug(
                    f"TAK-CoT mit __meshtastic-Marker nicht erneut ins Mesh gesendet: {metadata['uid']}"
                )
                return

        if chat_payload:
            sender_uid = chat_payload["sender_uid"] or self.gateway_uid
            if sender_uid in self.local_node_ids:
                self.logger.debug("TAK-Chat vom lokalen Meshtastic-Knoten ignoriert, um Echos zu vermeiden.")
                return

            dedupe_key = chat_payload["event_uid"] or f"tak:{sender_uid}:{chat_payload['message']}"
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

    def _handle_tak_tcp_client(self, conn, addr):
        buffer_text = ""
        probe_buffer = b""
        decoder = None
        conn.settimeout(TCP_SOCKET_TIMEOUT_SECONDS)
        cb = self.wintak_tcp_chat_callback
        if cb:
            try:
                cb("connect", None, None, addr)
            except Exception:
                pass
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
                                conn.sendall(self._build_pong_xml().encode("utf-8"))
                                self.logger.debug(
                                    f"TAK-TCP-Ping von {_format_network_endpoint(addr)} "
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
                                source_protocol="TCP",
                                listener_port=self.tcp_chat_listen_port,
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
                            source_protocol="TCP",
                            listener_port=self.tcp_chat_listen_port,
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
                        source_protocol="TCP",
                        listener_port=self.tcp_chat_listen_port,
                        packet_size=len(probe_buffer_bytes),
                        was_normalized=_ensure_bytes(normalized_packet) != probe_buffer_bytes,
                    )
                else:
                    self._log_inbound_tak_diagnostics(
                        source_addr=addr,
                        source_protocol="TCP",
                        listener_port=self.tcp_chat_listen_port,
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
                        source_protocol="TCP",
                        listener_port=self.tcp_chat_listen_port,
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
                    source_protocol="TCP",
                    listener_port=self.tcp_chat_listen_port,
                    packet_size=packet_size,
                    was_normalized=was_normalized,
                )
        except (OSError, UnicodeError, ValueError):
            self.logger.debug("Fehler beim Lesen einer TAK-TCP-Verbindung:\n" + traceback.format_exc())
        finally:
            cb = self.wintak_tcp_chat_callback
            if cb:
                try:
                    cb("disconnect", None, None, addr)
                except Exception:
                    pass
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
