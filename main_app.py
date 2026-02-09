#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TAK Meshtastic Gateway - vollständige, robuste Version
- Lädt config.yaml (optional)
- Unterstützt TCP und UDP zu entferntem TAK-Server
- Sendet CoT-XML an lokales WinTAK (UDP) und optional an entfernten TAK-Server
- Verbessertes Logging, stabile Wiederverbindung, sichere COM-Port-Auswahl
"""

import os
import sys
import datetime
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
    def __init__(self, port, cfg=None):
        self.port = port
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
        self.sock_udp.settimeout(5.0)  # Add timeout to prevent hanging

        # Remote server socket(s)
        self.sock_remote = None  # für TCP: persistent socket; für UDP: socket used for sendto

        # interne State
        self.logger = self.setup_logging()
        self.interface = None
        self.server_lock = threading.Lock()
        self.shutdown_flag = threading.Event()  # For graceful shutdown

        # Park coordinates wenn kein GPS-Fix (optional)
        self.park_lat = float(self.cfg.get("park_lat", 0.0))
        self.park_lon = float(self.cfg.get("park_lon", 0.0))

        # Start
        try:
            if meshtastic is None:
                raise RuntimeError("meshtastic-Paket nicht installiert / importierbar.")
            self.logger.info(f"Versuche Verbindung zum Meshtastic-Hardware-Interface an {self.port} ...")
            self.interface = meshtastic.serial_interface.SerialInterface(self.port)
            # subscribe to receive events if pubsub available
            if pub is not None:
                pub.subscribe(self.on_any_packet, "meshtastic.receive")
            else:
                self.logger.warning("pypubsub nicht gefunden: Live-Empfangs-Callbacks möglicherweise nicht aktiv.")
            # Start maintenance thread
            threading.Thread(target=self.maintain_server, daemon=True).start()
            self.logger.info("Gateway gestartet. Führe initiale Vollsynchronisation aus.")
            self.full_sync()
        except Exception as e:
            self.logger.error(f"Fehler beim Initialisieren: {e}")
            self.logger.debug(traceback.format_exc())

    def setup_logging(self):
        # Logger mit colorlog wenn verfügbar, sonst Standardlogging
        logger = logging.getLogger("TAK_Meshtastic_Gateway")
        logger.setLevel(logging.INFO)
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
                sock.settimeout(5.0)
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
        Callback für empfangene Packets von meshtastic
        """
        try:
            from_id = packet.get('fromId') or packet.get('from')
            if from_id:
                node = None
                if hasattr(self.interface, 'nodes') and self.interface.nodes:
                    node = self.interface.nodes.get(from_id)
                if node:
                    # Force update für Live-Events
                    self.process_node(node, 0, force_update=True)
        except Exception:
            self.logger.debug("Fehler im on_any_packet:\n" + traceback.format_exc())

    def full_sync(self):
        """
        Schickt eine Sync über alle bekannten Nodes.
        """
        try:
            nodes = []
            if hasattr(self.interface, 'nodes') and self.interface.nodes:
                nodes = sorted(self.interface.nodes.values(), key=lambda x: x.get('user', {}).get('longName', ''))
            for i, node in enumerate(nodes):
                self.process_node(node, i)
        except Exception:
            self.logger.error("Fehler während full_sync:\n" + traceback.format_exc())

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
            if lat_i is not None and lon_i is not None and lat_i != 0:
                final_lat, final_lon, is_real = lat_i * 1e-7, lon_i * 1e-7, True
            elif lat_f is not None and lon_f is not None and lat_f != 0:
                final_lat, final_lon, is_real = lat_f, lon_f, True

            if not is_real:
                final_lat = self.park_lat - (index * 0.001)
                final_lon = self.park_lon

            if is_real and force_update:
                self.logger.info(f"LIVE: {callsign} @ {final_lat:.5f}, {final_lon:.5f}")

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
                        except Exception as e:
                            self.logger.warning(f"Fehler beim Senden an Remote-UDP-Server: {e}")
            else:  # TCP
                with self.server_lock:
                    s = self.sock_remote
                    if s:
                        try:
                            # TCP erwartet evtl. newline-terminierte Pakete
                            s.sendall(packet_xml + b"\n")
                            self.logger.info(f"Remote-TCP gesendet an {self.server_ip}:{self.server_port} ({callsign})")
                        except Exception as e:
                            self.logger.warning(f"Fehler beim Senden an Remote-TCP-Server, Socket wird zurückgesetzt: {e}")
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
        
        # Close meshtastic interface
        if self.interface:
            try:
                self.interface.close()
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
                self.shutdown_flag.wait(int(self.cfg.get("sync_interval_seconds", 300)))
        except KeyboardInterrupt:
            self.logger.info("Beende auf Benutzereingabe.")
        except Exception:
            self.logger.error("Fehler in run:\n" + traceback.format_exc())
        finally:
            self.cleanup()


def choose_serial_port(cfg):
    """
    Sicherheits-Auswahl der COM-Ports. Unterstützt:
    - automatische Auswahl über cfg["meshtastic_port"] wenn vorhanden
    - ansonsten interaktive Auswahl
    Returns: devicename (z.B. 'COM3')
    """
    # config override
    cfg_port = cfg.get("meshtastic_port")
    if cfg_port:
        # wenn Port numerisch in Liste vorkommt -> validieren
        ports = []
        if serial is not None:
            try:
                ports = list(serial.tools.list_ports.comports())
            except Exception:
                ports = []
        # einfache Existenzprüfung (falls möglich)
        if not ports:
            print(f"Konfigurierter Port {cfg_port} wird verwendet (keine Portprüfung möglich).")
            return cfg_port
        for p in ports:
            if p.device == cfg_port:
                print(f"Konfigurierter Port {cfg_port} gefunden und wird verwendet.")
                return cfg_port
        print(f"Konfigurierter Port {cfg_port} wurde nicht unter den gefundenen Ports entdeckt. Weiter zur manuellen Auswahl...")

    # Liste aller Ports anzeigen
    if serial is None:
        print("pyserial nicht verfügbar; benutze Standard COM7.")
        return "COM7"

    ports = list(serial.tools.list_ports.comports())
    if not ports:
        print("Keine seriellen Ports gefunden. Drücke Enter um mit Standard COM7 fortzufahren.")
        input()
        return "COM7"

    print("Gefundene serielle Ports:")
    for i, p in enumerate(ports):
        print(f"[{i}] {p.device} - {getattr(p, 'description', '')}")

    while True:
        val = input("\nSelect Port (Index, Enter = 0): ").strip()
        if val == "":
            idx = 0
            break
        try:
            idx = int(val)
            if 0 <= idx < len(ports):
                break
        except Exception:
            pass
        print("Ungültige Auswahl. Bitte Index-Zahl eingeben.")
    return ports[idx].device


if __name__ == "__main__":
    try:
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

        # Port-Auswahl (mit config override)
        p_dev = choose_serial_port(cfg)
        print(f"Verwende Port: {p_dev}")

        gw = TAKMeshtasticGateway(p_dev, cfg)
        gw.run()

    except Exception as exc:
        print("Unerwarteter Fehler beim Starten:")
        traceback.print_exc()
        print("Drücke Enter zum Beenden...")
        input()
        sys.exit(1)