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
import datetime
import math
import re
import socket
import sqlite3
import time
import logging
import threading
import traceback
from xml.etree.ElementTree import Element, SubElement, tostring
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
except Exception:
    meshtastic = None
    pub = None

CFG_FILENAME = "config.yaml"
MAX_DETECTED_PORTS_DISPLAY = 6
MIN_PORT_NUMBER = 1
MAX_PORT_NUMBER = 65535


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

        ttk.Separator(cfg_frame, orient="horizontal").grid(
            row=7, column=0, columnspan=6, sticky="ew", pady=(2, 8))

        # ── Zeile 8: GPS-Optionen ──
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
        ).grid(row=8, column=0, columnspan=2, sticky="w", pady=(0, 4))
        ttk.Checkbutton(
            cfg_frame,
            text="Gateway-Position beim Start setzen",
            variable=self._set_gateway_position_var,
        ).grid(row=8, column=2, columnspan=3, sticky="w", padx=(8, 0), pady=(0, 4))

        # ── Zeile 9: park_lat / park_lon ──
        cfg_label("Fallback Lat (park_lat):", row=9, col=0, pady=(0, 4))
        raw_park_lat = self.cfg.get("park_lat")
        park_lat_val = "" if raw_park_lat is None else f"{float(raw_park_lat):.6f}".rstrip("0").rstrip(".")
        self._park_lat_var = tk.StringVar(value=park_lat_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lat_var, width=16).grid(
            row=9, column=1, sticky="w", padx=(6, 12), pady=(0, 4))
        cfg_label("Fallback Lon (park_lon):", row=9, col=2, padx=(8, 6), pady=(0, 4))
        raw_park_lon = self.cfg.get("park_lon")
        park_lon_val = "" if raw_park_lon is None else f"{float(raw_park_lon):.6f}".rstrip("0").rstrip(".")
        self._park_lon_var = tk.StringVar(value=park_lon_val)
        ttk.Entry(cfg_frame, textvariable=self._park_lon_var, width=16).grid(
            row=9, column=3, sticky="w", pady=(0, 4))
        self._park_lat_var.trace_add("write", lambda *_: self._update_no_gps_hint())
        self._park_lon_var.trace_add("write", lambda *_: self._update_no_gps_hint())

        # ── Zeile 10: Hinweis ──
        self._no_gps_hint_var = tk.StringVar()
        ttk.Label(cfg_frame, textvariable=self._no_gps_hint_var, style="Hint.TLabel").grid(
            row=10, column=0, columnspan=6, sticky="w", pady=(0, 2))
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

        # ── Eingabe / Befehlszeile ──
        input_frame = ttk.LabelFrame(root, text=" ⌨  Befehlseingabe ", padding=6)
        input_frame.pack(fill="x", padx=10, pady=(6, 0))

        self._input_var = tk.StringVar()
        input_entry = ttk.Entry(input_frame, textvariable=self._input_var)
        input_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        input_entry.bind("<Return>", lambda _e: self._on_send_command())
        ttk.Button(input_frame, text="Senden", command=self._on_send_command,
                   style="Accent.TButton").pack(side="left")

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
        self.cfg["sync_interval_seconds"] = self._parse_int_field(
            self._sync_interval_var.get(), "Sync-Intervall", min_value=1, max_value=86400
        )

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

    def _run_gateway(self, ports):
        try:
            gw = TAKMeshtasticGateway(ports, self.cfg)
            self._gateway = gw
            self._root.after(0, lambda: self._status_var.set(f"🟢 Läuft  –  {', '.join(ports)}"))
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

    def _on_gateway_stopped(self):
        if self._gui_handler:
            logging.getLogger("TAK_Meshtastic_Gateway").removeHandler(self._gui_handler)
            self._gui_handler = None
        self._gateway = None
        self._start_btn.configure(state="normal")
        self._stop_btn.configure(state="disabled")
        self._status_var.set("⬛ Gestoppt")

    def _on_manual_sync(self):
        gw = self._gateway
        if gw:
            threading.Thread(target=gw.full_sync, daemon=True).start()
            self._append_log("Manuelle Vollsynchronisation ausgelöst.", "INFO")
        else:
            self._append_log("Gateway ist nicht gestartet.", "WARNING")

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
        
        self.sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_udp.settimeout(self.SOCKET_TIMEOUT)  # Add timeout to prevent hanging

        # Remote server socket(s)
        self.sock_remote = None  # für TCP: persistent socket; für UDP: socket used for sendto

        # interne State
        self.logger = self.setup_logging()
        self.interfaces = []
        self.server_lock = threading.Lock()
        self.shutdown_flag = threading.Event()  # For graceful shutdown
        self.last_known_positions = {}

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
        if (
            self.park_lat is not None
            and self.park_lon is not None
            and not math.isnan(self.park_lat)
            and not math.isnan(self.park_lon)
            and -90.0 <= self.park_lat <= 90.0
            and -180.0 <= self.park_lon <= 180.0
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
            for port in self.ports:
                self.logger.info(f"Versuche Verbindung zum Meshtastic-Hardware-Interface an {port} ...")
                iface = meshtastic.serial_interface.SerialInterface(port)
                self.interfaces.append(iface)
                self.logger.info(f"Interface an {port} erfolgreich verbunden.")
            # subscribe to receive events (one subscription handles all interfaces via pubsub)
            if pub is not None:
                pub.subscribe(self.on_any_packet, "meshtastic.receive")
            else:
                self.logger.warning("pypubsub nicht gefunden: Live-Empfangs-Callbacks möglicherweise nicht aktiv.")
            # Start maintenance thread
            threading.Thread(target=self.maintain_server, daemon=True).start()
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
            if from_id:
                node = None
                # Suche zuerst im sendenden Interface, dann in allen anderen
                for iface in ([interface] + [i for i in self.interfaces if i is not interface]):
                    if hasattr(iface, 'nodes') and iface.nodes:
                        node = iface.nodes.get(from_id)
                        if node:
                            break
                if node:
                    # Force update für Live-Events
                    self.process_node(node, 0, force_update=True)
        except Exception:
            self.logger.debug("Fehler im on_any_packet:\n" + traceback.format_exc())

    def full_sync(self):
        """
        Schickt eine Sync über alle bekannten Nodes aller verbundenen Interfaces.
        """
        for iface in self.interfaces:
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
        for iface in self.interfaces:
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
            uid = raw_uid.replace('!', 'ID-')
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

            # Sende lokal an WinTAK (UDP)
            try:
                self.sock_udp.sendto(packet_xml, (self.tak_ip, self.tak_port))
                self.logger.debug(f"Lokales UDP gesendet an {self.tak_ip}:{self.tak_port} ({callsign})")
            except Exception as e:
                self.logger.warning(f"Fehler beim Senden an lokales TAK (UDP): {e}")

            # Sende an entfernten TAK-Server (abhängig von server_protocol)
            if self.server_protocol == "UDP":
                with self.server_lock:
                    sock = self.sock_remote
                    if sock:
                        try:
                            sock.sendto(packet_xml, (self.server_ip, self.server_port))
                            self.logger.info(f"Remote-UDP gesendet an {self.server_ip}:{self.server_port} ({callsign})")
                        except (socket.timeout, socket.error, OSError) as e:
                            self.logger.warning(f"Fehler beim Senden an Remote-UDP-Server ({type(e).__name__}): {e}")
            else:  # TCP
                with self.server_lock:
                    s = self.sock_remote
                    if s:
                        try:
                            # TCP erwartet evtl. newline-terminierte Pakete
                            s.sendall(packet_xml + b"\n")
                            self.logger.info(f"Remote-TCP gesendet an {self.server_ip}:{self.server_port} ({callsign})")
                        except (socket.timeout, socket.error, OSError) as e:
                            self.logger.warning(f"Fehler beim Senden an Remote-TCP-Server ({type(e).__name__}), Socket wird zurückgesetzt: {e}")
                            try:
                                s.close()
                            except Exception:
                                pass
                            self.sock_remote = None
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
        
        # Close remote socket
        self._cleanup_server_socket()
        
        # Close all meshtastic interfaces
        for iface in self.interfaces:
            try:
                iface.close()
            except Exception:
                pass

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
