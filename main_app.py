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
import socket
import time
import logging
import threading
import traceback
from xml.etree.ElementTree import Element, SubElement, tostring

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
    """Best-effort float conversion."""
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

        # Park coordinates wenn kein GPS-Fix (optional)
        self.park_lat = to_float_or_none(self.cfg.get("park_lat"))
        self.park_lon = to_float_or_none(self.cfg.get("park_lon"))
        self.park_coords = normalize_coordinates(self.park_lat, self.park_lon)
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
        for method_name in ("setFixedPosition", "set_fixed_position", "setPosition", "set_position"):
            method = getattr(target, method_name, None)
            if not callable(method):
                continue
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
                    continue
                except Exception:
                    self.logger.warning("Fehler beim Setzen der Gateway-Position:\n" + traceback.format_exc())
                    return False
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
                local_node_updated = self._invoke_position_setter(local_node, lat, lon, 0)
                iface_updated = False if local_node_updated else self._invoke_position_setter(iface, lat, lon, 0)
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

            final_lat, final_lon, is_real = 0.0, 0.0, False

            # Priorität: integer-Telemetrie (1e-7) -> float -> fallback park
            # NOTE: Coordinates at exactly (0,0) are treated as no GPS fix and use fallback
            # per README: "Nodes without a valid GPS fix are placed at 0.0, 0.0 by default"
            # This prevents displaying nodes at "Null Island" in the Atlantic Ocean
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

            if not is_real:
                if not self.send_nodes_without_gps:
                    self.logger.debug(f"Überspringe Node ohne gültigen GPS-Fix: {callsign}")
                    return
                if self.park_coords is None:
                    self.logger.debug(f"Überspringe Node ohne gültigen GPS-Fix (kein park_lat/park_lon): {callsign}")
                    return
                final_lat = self.park_coords[0] - (index * 0.001)
                final_lon = self.park_coords[1]

            if is_real and force_update:
                self.logger.info(f"LIVE: Position-Update empfangen von {callsign}")

            alt = pos.get('altitude', 0) or 0
            self.send_broadcast(uid, callsign, final_lat, final_lon, alt, is_real)
        except Exception:
            self.logger.error("Fehler in process_node:\n" + traceback.format_exc())

    def send_broadcast(self, uid, callsign, lat, lon, alt, is_real):
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
            SubElement(detail, 'precisionlocation', {'geopointsrc': 'GPS' if is_real else 'USER'})
            if not is_real:
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


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="TAK Meshtastic Gateway")
        parser.add_argument(
            "--all-ports", action="store_true",
            help="Alle verfügbaren seriellen USB-Ports automatisch verwenden (kein interaktiver Dialog)"
        )
        args = parser.parse_args()

        cfg = load_config()

        # Falls fehlende Abhängigkeiten -> klare Fehlermeldung
        missing = []
        if meshtastic is None:
            missing.append("meshtastic")
        if pub is None:
            missing.append("pypubsub")
        if serial is None:
            missing.append("pyserial")
        if colorlog is None:
            # colorlog ist optional; keine Aufnahme in missing zwingend
            pass
        if missing:
            print("WARNUNG: Folgende Python-Pakete fehlen oder konnten nicht importiert werden:")
            for m in missing:
                print(" - " + m)
            print("Bitte installiere sie (z.B. pip install meshtastic pypubsub pyserial colorlog pyyaml).")
            print("Fortfahren? (Enter=ja, Ctrl-C zum Abbrechen)")
            input()

        # Port-Auswahl (mit config override, unterstützt mehrere Streams)
        p_devs = choose_serial_ports(cfg, all_ports_mode=args.all_ports)
        print(f"Verwende Port(s): {', '.join(p_devs)}")

        gw = TAKMeshtasticGateway(p_devs, cfg)
        gw.run()

    except Exception as exc:
        print("Unerwarteter Fehler beim Starten:")
        traceback.print_exc()
        print("Drücke Enter zum Beenden...")
        input()
        sys.exit(1)
