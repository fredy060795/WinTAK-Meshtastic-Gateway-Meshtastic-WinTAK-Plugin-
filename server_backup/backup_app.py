#!/usr/bin/env python3
"""Server-Backup: sichert einen kompletten Linux-Server per SSH auf diesen PC.

Bedienung im Browser unter http://127.0.0.1:5042 (start.bat startet alles).

Zwei Sicherungsarten:
  * "files"  - komplettes Dateisystem (alle Dateien, Einstellungen, Programme, Datenbanken,
               Docker usw.) als tar.gz mit Rechten, Besitzern, ACLs und xattrs. Laeuft im
               laufenden Betrieb. Wiederherstellen: Server mit demselben Betriebssystem neu
               installieren, Backup zurueckspielen, Neustart - fertig.
  * "image"  - Rohabbild der ganzen Festplatte (dd). Am konsistentesten aus dem
               Rescue-System des Hosters; Wiederherstellen ebenfalls aus dem Rescue-System.

Das Server-Passwort wird unter Windows mit DPAPI verschluesselt gespeichert (nur dieses
Windows-Konto kann es lesen). Fuer Root-Rechte wird sudo mit demselben Passwort benutzt.
"""

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import sys
import tarfile
import threading
import time
import traceback
import webbrowser
import zlib
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

try:
    import paramiko
except ImportError:  # start.bat installiert paramiko; ohne laeuft nur die Oberflaeche nicht
    paramiko = None

APP_DIR = Path(__file__).resolve().parent
HTML_FILE = APP_DIR / "backup_ui.html"
DEFAULT_HTTP_PORT = 5042


def _data_dir():
    """Einstellungen neben dem Skript, sonst (schreibgeschuetzt) im Benutzerprofil."""
    try:
        probe = APP_DIR / f".write_test_{os.getpid()}"
        probe.write_text("x")
        probe.unlink()
        return APP_DIR
    except OSError:
        base = Path(os.environ.get("LOCALAPPDATA") or Path.home() / ".local" / "share")
        path = base / "ServerBackup"
        path.mkdir(parents=True, exist_ok=True)
        return path


DATA_DIR = _data_dir()
CONFIG_FILE = DATA_DIR / "backup_config.json"
STATE_FILE = DATA_DIR / "backup_state.json"
LOG_FILE = DATA_DIR / "backup_log.txt"
KNOWN_HOSTS = DATA_DIR / "known_hosts"

# Pseudo-Dateisysteme, Caches und Swap werden nicht gesichert (werden beim Start neu erzeugt).
DEFAULT_EXCLUDES = [
    "./proc/*", "./sys/*", "./dev/*", "./run/*", "./tmp/*", "./var/tmp/*",
    "./mnt/*", "./media/*", "./lost+found",
    "./swapfile", "./swap.img", "./swap",
    "./var/cache/apt/archives/*.deb",
    "./var/lib/lxcfs/*",
    "./var/lib/docker/overlay2/*/merged/*",
    "./snap/*/[0-9]*/*",
]
# Beim Zurueckspielen nicht ueberschreiben: Mount-Punkte des laufenden Systems, die
# Partitions-UUIDs des neu installierten Systems (fstab) und dessen EFI-Partition
# (der Bootloader wird danach neu installiert).
RESTORE_EXCLUDES = ["./proc", "./sys", "./dev", "./run", "./etc/fstab", "./boot/efi"]

WEEKDAYS = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]

DEFAULT_CONFIG = {
    "host": "",
    "port": 22,
    "user": "",
    "password_enc": "",
    "key_file": "",
    "mode": "files",
    "image_disk": "",
    "exclude_extra": "",
    "pre_command": "",
    "post_command": "",
    "verify": True,
    "backup_dir": str(Path.home() / "ServerBackups"),
    "schedule": {"type": "off", "every_hours": 6, "time": "03:00", "weekday": 0, "monthday": 1},
    "retention": {"keep_count": 7, "max_age_days": 0},
    "listen_host": "127.0.0.1",
    "http_port": DEFAULT_HTTP_PORT,
}


class BackupError(Exception):
    pass


class Cancelled(Exception):
    pass


# --------------------------------------------------------------------------- Logging

_log_lines = deque(maxlen=600)
_log_lock = threading.Lock()


def log(msg):
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"{stamp}  {line}" for line in str(msg).splitlines() or [""]]
    with _log_lock:
        for line in lines:
            _log_lines.append(line)
            print(line, flush=True)
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
        except OSError:
            pass


def _load_log_tail():
    try:
        with open(LOG_FILE, encoding="utf-8", errors="replace") as fh:
            for line in deque(fh, maxlen=300):
                _log_lines.append(line.rstrip("\n"))
    except OSError:
        pass


# --------------------------------------------------------------------------- Passwort

def _dpapi(data, protect):
    import ctypes
    from ctypes import wintypes

    class Blob(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

    buf = ctypes.create_string_buffer(data, len(data))
    blob_in = Blob(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    blob_out = Blob()
    crypt32 = ctypes.windll.crypt32
    if protect:
        ok = crypt32.CryptProtectData(ctypes.byref(blob_in), "ServerBackup", None, None, None, 0,
                                      ctypes.byref(blob_out))
    else:
        ok = crypt32.CryptUnprotectData(ctypes.byref(blob_in), None, None, None, None, 0,
                                        ctypes.byref(blob_out))
    if not ok:
        raise ctypes.WinError()
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(ctypes.cast(blob_out.pbData, ctypes.c_void_p))


def encrypt_password(password):
    if not password:
        return ""
    raw = password.encode("utf-8")
    if os.name == "nt":
        return "dpapi:" + base64.b64encode(_dpapi(raw, True)).decode()
    # Ausserhalb von Windows gibt es kein DPAPI - die Datei dann nur fuer den Benutzer lesbar halten.
    return "b64:" + base64.b64encode(raw).decode()


def decrypt_password(blob):
    if not blob:
        return ""
    kind, _, data = blob.partition(":")
    raw = base64.b64decode(data)
    if kind == "dpapi":
        return _dpapi(raw, False).decode("utf-8")
    return raw.decode("utf-8")


# --------------------------------------------------------------------------- Konfiguration

_cfg_lock = threading.RLock()


def _read_json(path, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def _write_json(path, data):
    tmp = Path(str(path) + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    if os.name != "nt":
        os.chmod(path, 0o600)


def load_config():
    with _cfg_lock:
        cfg = json.loads(json.dumps(DEFAULT_CONFIG))
        stored = _read_json(CONFIG_FILE, {})
        for key, value in stored.items():
            if isinstance(value, dict) and isinstance(cfg.get(key), dict):
                cfg[key].update(value)
            else:
                cfg[key] = value
        return cfg


def save_config(cfg):
    with _cfg_lock:
        _write_json(CONFIG_FILE, cfg)


def load_state():
    return _read_json(STATE_FILE, {})


def update_state(**changes):
    with _cfg_lock:
        state = load_state()
        state.update(changes)
        _write_json(STATE_FILE, state)
        return state


def _int(value, default, lo=None, hi=None):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    if lo is not None:
        value = max(lo, value)
    if hi is not None:
        value = min(hi, value)
    return value


def apply_config_update(data):
    """Uebernimmt die Felder aus der Oberflaeche; ein leeres Passwortfeld behaelt das alte."""
    with _cfg_lock:
        cfg = load_config()
        old_schedule = dict(cfg["schedule"])
        for key in ("host", "user", "key_file", "image_disk", "exclude_extra",
                    "pre_command", "post_command", "backup_dir"):
            if key in data:
                cfg[key] = str(data[key] or "").strip()
        if "port" in data:
            cfg["port"] = _int(data["port"], 22, 1, 65535)
        if data.get("mode") in ("files", "image"):
            cfg["mode"] = data["mode"]
        if "verify" in data:
            cfg["verify"] = bool(data["verify"])
        if data.get("clear_password"):
            cfg["password_enc"] = ""
        elif data.get("password"):
            cfg["password_enc"] = encrypt_password(data["password"])
        sch = data.get("schedule") or {}
        if sch.get("type") in ("off", "hours", "daily", "weekly", "monthly"):
            cfg["schedule"]["type"] = sch["type"]
        if "every_hours" in sch:
            cfg["schedule"]["every_hours"] = _int(sch["every_hours"], 6, 1, 24)
        if "time" in sch and re.fullmatch(r"\d{1,2}:\d{2}", str(sch["time"])):
            hh, mm = str(sch["time"]).split(":")
            cfg["schedule"]["time"] = f"{min(int(hh), 23):02d}:{min(int(mm), 59):02d}"
        if "weekday" in sch:
            cfg["schedule"]["weekday"] = _int(sch["weekday"], 0, 0, 6)
        if "monthday" in sch:
            cfg["schedule"]["monthday"] = _int(sch["monthday"], 1, 1, 28)
        ret = data.get("retention") or {}
        if "keep_count" in ret:
            cfg["retention"]["keep_count"] = _int(ret["keep_count"], 7, 0, 10000)
        if "max_age_days" in ret:
            cfg["retention"]["max_age_days"] = _int(ret["max_age_days"], 0, 0, 100000)
        save_config(cfg)
        if cfg["schedule"] != old_schedule:
            # Ein neuer Zeitplan zaehlt ab jetzt - kein sofortiges "Nachholen".
            update_state(schedule_anchor=time.time())
        return cfg


# --------------------------------------------------------------------------- Zeitplan

def _slot_candidates(sch, now):
    hh, mm = (int(x) for x in sch.get("time", "03:00").split(":"))
    kind = sch.get("type")
    today = now.date()
    for offset in range(-33, 34):
        day = today + dt.timedelta(days=offset)
        if kind == "hours":
            step = max(1, int(sch.get("every_hours", 6)))
            for hour in range(0, 24, step):
                yield dt.datetime.combine(day, dt.time(hour, mm))
        elif kind == "daily":
            yield dt.datetime.combine(day, dt.time(hh, mm))
        elif kind == "weekly" and day.weekday() == int(sch.get("weekday", 0)):
            yield dt.datetime.combine(day, dt.time(hh, mm))
        elif kind == "monthly" and day.day == int(sch.get("monthday", 1)):
            yield dt.datetime.combine(day, dt.time(hh, mm))


def schedule_slots(sch, now=None):
    """(letzter faelliger Termin, naechster Termin) oder (None, None) wenn aus."""
    now = now or dt.datetime.now()
    if sch.get("type", "off") == "off":
        return None, None
    prev = nxt = None
    for slot in _slot_candidates(sch, now):
        if slot <= now:
            if prev is None or slot > prev:
                prev = slot
        elif nxt is None or slot < nxt:
            nxt = slot
    return prev, nxt


def describe_schedule(sch):
    kind = sch.get("type", "off")
    if kind == "off":
        return "Kein Zeitplan (nur manuell)"
    if kind == "hours":
        return f"Alle {sch.get('every_hours')} Stunden (Minute {sch.get('time', '03:00')[3:]})"
    if kind == "daily":
        return f"Täglich um {sch.get('time')}"
    if kind == "weekly":
        return f"Jeden {WEEKDAYS[int(sch.get('weekday', 0))]} um {sch.get('time')}"
    return f"Monatlich am {sch.get('monthday')}. um {sch.get('time')}"


# --------------------------------------------------------------------------- SSH

def _require_paramiko():
    if paramiko is None:
        raise BackupError("Python-Paket 'paramiko' fehlt. Bitte ueber start.bat starten "
                          "(installiert es automatisch) oder: pip install paramiko")


def connect(cfg):
    _require_paramiko()
    if not cfg.get("host") or not cfg.get("user"):
        raise BackupError("Server (Domain/IP) und Benutzer muessen eingetragen sein.")
    password = decrypt_password(cfg.get("password_enc", "")) or None
    key_file = (cfg.get("key_file") or "").strip()
    if key_file and not Path(key_file).expanduser().is_file():
        log(f"  Hinweis: SSH-Schluessel {key_file} nicht gefunden - melde mich nur mit Passwort an.")
        key_file = ""
    if not password and not key_file:
        log("  Hinweis: kein Passwort und kein Schluessel eingetragen - versuche Standard-Schluessel.")
    client = paramiko.SSHClient()
    KNOWN_HOSTS.touch(exist_ok=True)
    client.load_host_keys(str(KNOWN_HOSTS))
    # Beim ersten Kontakt wird der Host-Key gemerkt; aendert er sich spaeter, wird abgebrochen.
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=cfg["host"], port=int(cfg.get("port") or 22), username=cfg["user"],
            password=password,
            key_filename=str(Path(key_file).expanduser()) if key_file else None,
            look_for_keys=not key_file and not password, allow_agent=not password,
            timeout=20, banner_timeout=30, auth_timeout=30,
        )
    except paramiko.BadHostKeyException:
        raise BackupError(
            "Der Host-Key des Servers hat sich geaendert (z. B. weil er neu installiert wurde). "
            "Wenn das so gewollt ist: in der Oberflaeche 'Host-Key zuruecksetzen' klicken.")
    except paramiko.AuthenticationException:
        raise BackupError(f"Anmeldung als {cfg['user']}@{cfg['host']} abgelehnt - "
                          "Benutzer/Passwort pruefen.")
    except (OSError, paramiko.SSHException) as exc:
        raise BackupError(f"Keine SSH-Verbindung zu {cfg['host']}:{cfg.get('port')}: {exc}")
    transport = client.get_transport()
    transport.set_keepalive(30)
    transport.window_size = 8 * 1024 * 1024
    return client, password


def run_remote(client, command, stdin_text=None, timeout=120):
    chan = client.get_transport().open_session()
    chan.settimeout(timeout)
    chan.exec_command(command)
    if stdin_text is not None:
        chan.sendall(stdin_text.encode("utf-8"))
    chan.shutdown_write()
    out, err = bytearray(), bytearray()
    deadline = time.time() + timeout
    while True:
        if chan.recv_ready():
            out += chan.recv(65536)
        elif chan.recv_stderr_ready():
            err += chan.recv_stderr(65536)
        elif chan.exit_status_ready() and not chan.recv_ready() and not chan.recv_stderr_ready():
            break
        else:
            if time.time() > deadline:
                chan.close()
                raise BackupError(f"Zeitueberschreitung bei: {command[:80]}")
            time.sleep(0.05)
    code = chan.recv_exit_status()
    chan.close()
    return code, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


class Privilege:
    """Wie Befehle mit Root-Rechten laufen: direkt als root, sudo ohne oder mit Passwort."""

    def __init__(self, prefix, password=None, how=""):
        self.prefix = prefix
        self.password = password
        self.how = how

    def wrap(self, script):
        return f"{self.prefix}sh -c {shlex.quote(script)}"

    def stdin_prefix(self):
        return (self.password + "\n").encode("utf-8") if self.password else b""


def get_privilege(client, password):
    code, out, _ = run_remote(client, "id -u")
    if code == 0 and out.strip() == "0":
        return Privilege("", how="angemeldet als root")
    code, _, _ = run_remote(client, "sudo -n true")
    if code == 0:
        return Privilege("sudo -n ", how="sudo ohne Passwort")
    if not password:
        raise BackupError("Fuer ein komplettes Backup werden Root-Rechte gebraucht. Bitte das "
                          "Passwort des Benutzers eintragen (wird fuer sudo verwendet).")
    # -k: nie einen zwischengespeicherten sudo-Zugang benutzen, sonst landet das Passwort
    # im Datenstrom des Befehls.
    prefix = "sudo -k -S -p '' "
    code, _, err = run_remote(client, prefix + "true", stdin_text=password + "\n", timeout=30)
    if code != 0:
        raise BackupError("sudo hat nicht funktioniert (Passwort falsch oder Benutzer hat keine "
                          f"sudo-Rechte): {err.strip()[:200]}")
    return Privilege(prefix, password, how="sudo mit Passwort")


SERVER_INFO_SCRIPT = r"""
export PATH="$PATH:/usr/sbin:/sbin"
echo "== Betriebssystem"; cat /etc/os-release 2>/dev/null; uname -a
echo; echo "== Hostname"; hostname -f 2>/dev/null || hostname
echo; echo "== Boot"; [ -d /sys/firmware/efi ] && echo UEFI || echo BIOS
echo; echo "== Datentraeger"; lsblk -o NAME,SIZE,TYPE,FSTYPE,UUID,MOUNTPOINT 2>/dev/null
echo; echo "== Belegung"; df -hT -x tmpfs -x devtmpfs -x squashfs -x overlay 2>/dev/null
echo; echo "== Netzwerk"; ip -br addr 2>/dev/null
"""


def tar_command(cfg):
    excludes = list(DEFAULT_EXCLUDES)
    for line in (cfg.get("exclude_extra") or "").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            line = line[2:] if line.startswith("./") else line.lstrip("/")
            excludes.append("./" + line)
    exclude_args = " ".join(f"--exclude={shlex.quote(e)}" for e in excludes)
    return (
        'export PATH="$PATH:/usr/sbin:/sbin"; '
        'tar --version 2>/dev/null | grep -q "GNU tar" || { echo "GNU tar fehlt auf dem Server" >&2; exit 90; }; '
        'if command -v pigz >/dev/null 2>&1; then Z=--use-compress-program=pigz; else Z=--gzip; fi; '
        'NICE=""; command -v ionice >/dev/null 2>&1 && NICE="ionice -c3"; '
        'cd / && exec nice -n 19 $NICE tar --create $Z --file=- --directory=/ '
        '--numeric-owner --acls --xattrs "--xattrs-include=*" --sparse '
        '--warning=no-file-changed --warning=no-file-removed --warning=no-file-ignored '
        f'--ignore-failed-read --anchored {exclude_args} .'
    )


def _disk_detect_script(cfg):
    disk = (cfg.get("image_disk") or "").strip()
    if disk:
        return f"D={shlex.quote(disk)}; "
    return ('D=$(lsblk -dnrpo NAME,TYPE 2>/dev/null | awk \'$2=="disk"{print $1; exit}\'); ')


def image_command(cfg):
    return (
        'export PATH="$PATH:/usr/sbin:/sbin"; ' + _disk_detect_script(cfg) +
        '[ -b "$D" ] || { echo "Festplatte nicht gefunden: $D" >&2; exit 91; }; '
        'echo "DISK=$D" >&2; '
        'if command -v pigz >/dev/null 2>&1; then Z="pigz -1"; else Z="gzip -1"; fi; '
        '{ dd if="$D" bs=4M status=none || echo "DD_FAILED" >&2; } | $Z'
    )


# --------------------------------------------------------------------------- Jobs

class Job:
    def __init__(self, kind, label):
        self.kind = kind
        self.label = label
        self.phase = "Starte ..."
        self.bytes = 0
        self.total = None
        self.started = time.time()
        self.cancel = threading.Event()
        self._samples = deque(maxlen=12)

    def add_bytes(self, n):
        self.bytes += n
        self._samples.append((time.time(), self.bytes))

    def speed(self):
        if len(self._samples) < 2:
            return 0
        (t0, b0), (t1, b1) = self._samples[0], self._samples[-1]
        return (b1 - b0) / (t1 - t0) if t1 > t0 else 0

    def check(self):
        if self.cancel.is_set():
            raise Cancelled()

    def as_dict(self):
        return {"kind": self.kind, "label": self.label, "phase": self.phase, "bytes": self.bytes,
                "total": self.total, "started": self.started, "speed": self.speed(),
                "elapsed": time.time() - self.started}


_job = None
_job_lock = threading.Lock()


def current_job():
    return _job


def start_job(kind, label, target, *args):
    global _job
    with _job_lock:
        if _job is not None:
            raise BackupError(f"Es laeuft bereits: {_job.label}")
        job = Job(kind, label)
        _job = job

    def runner():
        global _job
        try:
            target(job, *args)
        except Exception:  # noqa: BLE001 - jeder Fehler gehoert ins Protokoll
            log("FEHLER (intern):\n" + traceback.format_exc())
        finally:
            with _job_lock:
                _job = None

    threading.Thread(target=runner, name=kind, daemon=True).start()
    return job


def _stream_stderr(chan, sink):
    buf = b""
    while True:
        try:
            data = chan.recv_stderr(65536)
        except (OSError, EOFError):
            break
        if not data:
            break
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            sink.append(line.decode("utf-8", "replace"))
    if buf:
        sink.append(buf.decode("utf-8", "replace"))


def _host_slug(host):
    return re.sub(r"[^A-Za-z0-9._-]", "_", host) or "server"


def _backup_dir(cfg):
    path = Path(cfg.get("backup_dir") or DEFAULT_CONFIG["backup_dir"]).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _run_hook(client, priv, command, label):
    if not command.strip():
        return
    log(f"  {label}: {command}")
    chan_cmd = priv.wrap(command)
    code, out, err = run_remote(client, chan_cmd, stdin_text=(priv.password + "\n") if priv.password else None,
                                timeout=3600)
    for line in (out + err).strip().splitlines()[-20:]:
        log(f"    {line}")
    if code != 0:
        raise BackupError(f"{label} endete mit Code {code}.")


def do_backup(job, trigger):
    cfg = load_config()
    host = cfg["host"]
    mode = cfg["mode"]
    started = time.time()
    update_state(last_attempt=started)
    log(f"=== Backup gestartet ({trigger}): {cfg['user']}@{host} - "
        f"{'Komplettes Dateisystem' if mode == 'files' else 'Festplatten-Image'} ===")
    partial = None
    client = None
    priv = None
    try:
        out_dir = _backup_dir(cfg)
        free = shutil.disk_usage(out_dir).free
        log(f"  Speicherort: {out_dir} (frei: {_fmt_size(free)})")
        job.phase = "Verbinde ..."
        client, password = connect(cfg)
        priv = get_privilege(client, password)
        log(f"  Verbunden, Root-Rechte: {priv.how}")
        _, info, _ = run_remote(client, SERVER_INFO_SCRIPT, timeout=60)
        job.check()
        if cfg.get("pre_command"):
            job.phase = "Befehl vor dem Backup ..."
            _run_hook(client, priv, cfg["pre_command"], "Befehl vorher")
        stamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        ext = "tar.gz" if mode == "files" else "img.gz"
        name = f"{_host_slug(host)}_{stamp}.{ext}"
        final = out_dir / name
        partial = out_dir / (name + ".partial")
        job.phase = "Sichere Server ..."
        log(f"  Schreibe {final.name} ...")
        command = priv.wrap(tar_command(cfg) if mode == "files" else image_command(cfg))
        chan = client.get_transport().open_session(window_size=8 * 1024 * 1024)
        chan.settimeout(900)
        chan.exec_command(command)
        stdin = priv.stdin_prefix()
        if stdin:
            chan.sendall(stdin)
        chan.shutdown_write()
        errors = []
        err_thread = threading.Thread(target=_stream_stderr, args=(chan, errors), daemon=True)
        err_thread.start()
        sha = hashlib.sha256()
        try:
            with open(partial, "wb") as fh:
                while True:
                    if job.cancel.is_set():
                        chan.close()
                        raise Cancelled()
                    try:
                        data = chan.recv(1024 * 1024)
                    except socket.timeout:
                        raise BackupError("Server sendet seit 15 Minuten keine Daten mehr - abgebrochen.")
                    if not data:
                        break
                    fh.write(data)
                    sha.update(data)
                    job.add_bytes(len(data))
        except OSError as exc:
            raise BackupError(f"Schreiben nach {partial} fehlgeschlagen: {exc}")
        code = chan.recv_exit_status()
        err_thread.join(5)
        chan.close()
        errors = [e for e in errors if e.strip()]
        disk = next((e[5:] for e in errors if e.startswith("DISK=")), "")
        errors = [e for e in errors if not e.startswith("DISK=")]
        warnings = []
        if mode == "image" and "DD_FAILED" in errors:
            raise BackupError("dd konnte die Festplatte nicht vollstaendig lesen: "
                              + "; ".join(errors[-5:]))
        if code == 1 and mode == "files":
            warnings.append("Einige Dateien haben sich waehrend der Sicherung geaendert.")
        elif code != 0:
            for line in errors[-15:]:
                log(f"    server: {line}")
            raise BackupError(f"Sicherung auf dem Server endete mit Code {code}.")
        for line in errors[-15:]:
            log(f"    server: {line}")
        if errors:
            warnings.extend(errors[-30:])
        if job.bytes < 1024:
            raise BackupError("Es kamen praktisch keine Daten an - Backup ist leer.")
        log(f"  Uebertragen: {_fmt_size(job.bytes)} in {_fmt_duration(time.time() - started)}")
        members = None
        if cfg.get("verify", True):
            job.phase = "Pruefe Archiv ..."
            log("  Pruefe Archiv auf Vollstaendigkeit ...")
            members = verify_archive(partial, mode, job)
            log(f"  Archiv OK{f' ({members} Eintraege)' if members is not None else ''}.")
        os.replace(partial, final)
        partial = None
        meta = {
            "file": final.name, "host": host, "user": cfg["user"], "mode": mode,
            "disk": disk, "created": dt.datetime.fromtimestamp(started).isoformat(timespec="seconds"),
            "created_ts": started, "duration_s": round(time.time() - started, 1),
            "size": final.stat().st_size, "sha256": sha.hexdigest(),
            "status": "warnung" if warnings else "ok", "warnings": warnings,
            "members": members, "verified": bool(cfg.get("verify", True)),
            "trigger": trigger, "keep": False, "server_info": info,
        }
        _write_json(final.with_name(final.name + ".json"), meta)
        update_state(last_success=time.time(),
                     last_result={"ok": True, "time": time.time(),
                                  "message": f"Backup {final.name} ({_fmt_size(meta['size'])})"})
        log(f"=== Backup fertig: {final} ===")
        job.phase = "Raeume alte Backups auf ..."
        apply_retention(cfg)
    except Cancelled:
        log("=== Backup abgebrochen (vom Benutzer). Alte Backups bleiben unveraendert ===")
        update_state(last_result={"ok": False, "time": time.time(), "message": "Abgebrochen"})
    except BackupError as exc:
        log(f"FEHLER: {exc}")
        log("=== Abgebrochen, alte Backups bleiben unveraendert ===")
        update_state(last_result={"ok": False, "time": time.time(), "message": str(exc)})
    finally:
        if client is not None and priv is not None and cfg.get("post_command"):
            try:
                _run_hook(client, priv, cfg["post_command"], "Befehl nachher")
            except Exception as exc:  # noqa: BLE001
                log(f"  Befehl nachher fehlgeschlagen: {exc}")
        if client is not None:
            client.close()
        if partial is not None and partial.exists():
            try:
                partial.unlink()
            except OSError:
                pass


def verify_archive(path, mode, job):
    try:
        if mode == "files":
            count = 0
            with tarfile.open(path, mode="r|gz") as tf:
                for _ in tf:
                    count += 1
                    if count % 2000 == 0:
                        job.check()
            if count == 0:
                raise BackupError("Archiv enthaelt keine Dateien.")
            return count
        decomp = zlib.decompressobj(16 + zlib.MAX_WBITS)
        with open(path, "rb") as fh:
            while True:
                job.check()
                chunk = fh.read(4 * 1024 * 1024)
                if not chunk:
                    break
                decomp.decompress(chunk)
                while decomp.unused_data and decomp.eof:  # mehrere gzip-Glieder (pigz)
                    rest = decomp.unused_data
                    decomp = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    decomp.decompress(rest)
        if not decomp.eof:
            raise BackupError("Image ist unvollstaendig (gzip-Ende fehlt).")
        return None
    except (tarfile.TarError, zlib.error, EOFError) as exc:
        raise BackupError(f"Archiv ist beschaedigt/unvollstaendig: {exc}")


# --------------------------------------------------------------------------- Backups verwalten

def list_backups(cfg=None):
    cfg = cfg or load_config()
    try:
        out_dir = Path(cfg.get("backup_dir") or "").expanduser()
        metas = []
        for meta_path in out_dir.glob("*.json"):
            meta = _read_json(meta_path, None)
            if not isinstance(meta, dict) or "file" not in meta:
                continue
            archive = out_dir / meta["file"]
            meta["exists"] = archive.is_file()
            if meta["exists"]:
                meta["size"] = archive.stat().st_size
            metas.append(meta)
    except OSError:
        return []
    metas.sort(key=lambda m: m.get("created_ts", 0), reverse=True)
    return metas


def _safe_backup_path(cfg, name):
    out_dir = Path(cfg.get("backup_dir") or "").expanduser().resolve()
    path = (out_dir / str(name)).resolve()
    if path.parent != out_dir or not (path.name.endswith(".tar.gz") or path.name.endswith(".img.gz")):
        raise BackupError("Ungueltiger Dateiname.")
    return path


def delete_backup(cfg, name, reason="geloescht"):
    path = _safe_backup_path(cfg, name)
    job = current_job()
    if job is not None and job.kind == "restore" and job.label.endswith(name):
        raise BackupError("Dieses Backup wird gerade zurueckgespielt.")
    for p in (path, path.with_name(path.name + ".json")):
        try:
            p.unlink()
        except FileNotFoundError:
            pass
    log(f"  Backup {reason}: {name}")


def set_keep(cfg, name, keep):
    path = _safe_backup_path(cfg, name)
    meta_path = path.with_name(path.name + ".json")
    meta = _read_json(meta_path, None)
    if meta is None:
        raise BackupError("Backup nicht gefunden.")
    meta["keep"] = bool(keep)
    _write_json(meta_path, meta)


def apply_retention(cfg):
    """Loescht alte Backups dieses Servers nach den Regeln. Geschuetzte und das neueste
    Backup werden nie geloescht."""
    keep_count = int(cfg["retention"].get("keep_count") or 0)
    max_age = int(cfg["retention"].get("max_age_days") or 0)
    backups = [b for b in list_backups(cfg) if b.get("host") == cfg.get("host")]
    if not backups:
        return 0
    newest = backups[0]["file"]
    unprotected = [b for b in backups if not b.get("keep")]
    doomed = set()
    if keep_count > 0:
        doomed.update(b["file"] for b in unprotected[keep_count:])
    if max_age > 0:
        limit = time.time() - max_age * 86400
        doomed.update(b["file"] for b in unprotected if b.get("created_ts", 0) < limit)
    doomed.discard(newest)
    for name in sorted(doomed):
        try:
            delete_backup(cfg, name, reason="automatisch geloescht (Aufbewahrungsregel)")
        except (OSError, BackupError) as exc:
            log(f"  Konnte {name} nicht loeschen: {exc}")
    return len(doomed)


def cleanup_partials(cfg):
    try:
        for p in Path(cfg.get("backup_dir") or "").expanduser().glob("*.partial"):
            p.unlink()
            log(f"  Unvollstaendige Datei entfernt: {p.name}")
    except OSError:
        pass


# --------------------------------------------------------------------------- Wiederherstellen

POST_RESTORE_SCRIPT = r"""
export PATH="$PATH:/usr/sbin:/sbin"
echo "-> initramfs neu erzeugen"
if command -v update-initramfs >/dev/null 2>&1; then update-initramfs -u -k all
elif command -v dracut >/dev/null 2>&1; then dracut --regenerate-all -f; fi
echo "-> Bootloader neu installieren"
ROOTSRC=$(findmnt -nvo SOURCE /)
DISK=$(lsblk -nsrpo NAME,TYPE "$ROOTSRC" 2>/dev/null | awk '$2=="disk"{print $1; exit}')
if command -v grub-install >/dev/null 2>&1; then
  if [ -d /sys/firmware/efi ]; then grub-install --recheck || echo "WARNUNG: grub-install (UEFI) fehlgeschlagen"
  elif [ -n "$DISK" ]; then grub-install --recheck "$DISK" || echo "WARNUNG: grub-install $DISK fehlgeschlagen"
  fi
fi
if command -v update-grub >/dev/null 2>&1; then update-grub
elif command -v grub2-mkconfig >/dev/null 2>&1; then
  grub2-mkconfig -o /boot/grub2/grub.cfg
fi
sync
echo "-> fertig"
"""


def restore_command(meta, cfg):
    if meta["mode"] == "files":
        excludes = " ".join(f"--exclude={shlex.quote(e)}" for e in RESTORE_EXCLUDES)
        return (
            'export PATH="$PATH:/usr/sbin:/sbin"; '
            'tar --version 2>/dev/null | grep -q "GNU tar" || { echo "GNU tar fehlt auf dem Server" >&2; exit 90; }; '
            'cd / && tar --extract --gzip --file=- --directory=/ --preserve-permissions '
            '--numeric-owner --acls --xattrs "--xattrs-include=*" --anchored '
            f'{excludes}'
        )
    disk = (cfg.get("image_disk") or "").strip()
    if not disk:
        raise BackupError("Fuer ein Image muss unter 'Festplatte' das Ziel eingetragen sein "
                          "(z. B. /dev/sda oder /dev/vda - im Rescue-System mit lsblk nachsehen).")
    return (
        'export PATH="$PATH:/usr/sbin:/sbin"; '
        f'D={shlex.quote(disk)}; '
        '[ -b "$D" ] || { echo "Festplatte nicht gefunden: $D" >&2; exit 91; }; '
        'if findmnt -rno SOURCE | grep -q "^$D"; then '
        'echo "$D ist eingehaengt - ein Image nur aus dem Rescue-System zurueckspielen!" >&2; exit 92; fi; '
        'gzip -dc | dd of="$D" bs=4M conv=fsync status=none && sync && '
        '(blockdev --rereadpt "$D" 2>/dev/null || true)'
    )


def do_restore(job, name, reboot):
    cfg = load_config()
    client = None
    log(f"=== Wiederherstellung gestartet: {name} -> {cfg['user']}@{cfg['host']} ===")
    try:
        path = _safe_backup_path(cfg, name)
        meta = _read_json(path.with_name(path.name + ".json"), None)
        if meta is None or not path.is_file():
            raise BackupError("Backup-Datei nicht gefunden.")
        job.total = path.stat().st_size
        job.phase = "Verbinde ..."
        client, password = connect(cfg)
        priv = get_privilege(client, password)
        log(f"  Verbunden, Root-Rechte: {priv.how}")
        command = priv.wrap(restore_command(meta, cfg))
        job.phase = "Spiele Backup ein ..."
        chan = client.get_transport().open_session(window_size=8 * 1024 * 1024)
        chan.settimeout(900)
        chan.exec_command(command)
        errors = []
        err_thread = threading.Thread(target=_stream_stderr, args=(chan, errors), daemon=True)
        err_thread.start()
        out_thread = threading.Thread(target=lambda: _drain(chan), daemon=True)
        out_thread.start()
        stdin = priv.stdin_prefix()
        if stdin:
            chan.sendall(stdin)
        try:
            with open(path, "rb") as fh:
                while True:
                    if job.cancel.is_set():
                        chan.close()
                        raise Cancelled()
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    chan.sendall(chunk)
                    job.add_bytes(len(chunk))
        except (OSError, EOFError) as exc:
            if not chan.exit_status_ready():
                raise BackupError(f"Uebertragung abgebrochen: {exc}")
            # Der Server hat vorzeitig beendet - seine Fehlermeldung folgt unten.
        try:
            chan.shutdown_write()
        except (OSError, EOFError):
            pass
        job.phase = "Server schreibt Dateien ..."
        code = chan.recv_exit_status()
        err_thread.join(5)
        chan.close()
        errors = [e for e in errors if e.strip()]
        for line in errors[-20:]:
            log(f"    server: {line}")
        ok = code == 0 or (meta["mode"] == "files" and code == 1)
        if not ok and meta["mode"] == "files" and code == 2 and job.bytes >= job.total:
            log("  WARNUNG: tar meldete Fehler bei einzelnen Dateien (siehe oben), "
                "das Backup wurde aber komplett eingespielt.")
            ok = True
        if not ok:
            raise BackupError(f"Wiederherstellung endete mit Code {code}.")
        if meta["mode"] == "files":
            job.phase = "Bootloader & initramfs ..."
            log("  Richte Bootloader und initramfs fuer diese Festplatte ein ...")
            code, out, err = run_remote(client, priv.wrap(POST_RESTORE_SCRIPT),
                                        stdin_text=(priv.password + "\n") if priv.password else None,
                                        timeout=1800)
            for line in (out + err).strip().splitlines()[-25:]:
                log(f"    {line}")
        log("=== Wiederherstellung fertig ===")
        if reboot:
            log("  Starte den Server neu ...")
            chan = client.get_transport().open_session()
            chan.exec_command(priv.wrap("sleep 2; reboot || systemctl reboot"))
            if priv.password:
                chan.sendall(priv.stdin_prefix())
            chan.shutdown_write()
            time.sleep(1)
            log("  Neustart ausgeloest. Nach dem Neustart hat der Server ggf. den alten Host-Key - "
                "dann 'Host-Key zuruecksetzen' klicken.")
        else:
            log("  Bitte den Server jetzt neu starten, damit alle Dienste den Stand des Backups haben.")
        update_state(last_result={"ok": True, "time": time.time(),
                                  "message": f"Wiederhergestellt: {name}"})
    except Cancelled:
        log("=== Wiederherstellung abgebrochen - der Server ist jetzt evtl. nur teilweise "
            "wiederhergestellt. Bitte erneut ausfuehren! ===")
        update_state(last_result={"ok": False, "time": time.time(), "message": "Wiederherstellung abgebrochen"})
    except BackupError as exc:
        log(f"FEHLER: {exc}")
        log("=== Wiederherstellung fehlgeschlagen ===")
        update_state(last_result={"ok": False, "time": time.time(), "message": str(exc)})
    finally:
        if client is not None:
            client.close()


def _drain(chan):
    try:
        while chan.recv(65536):
            pass
    except (OSError, EOFError):
        pass


def do_test(cfg):
    lines = []
    client, password = connect(cfg)
    try:
        lines.append(f"Anmeldung als {cfg['user']}@{cfg['host']} erfolgreich.")
        _, out, _ = run_remote(client, ". /etc/os-release 2>/dev/null; echo \"$PRETTY_NAME\"; "
                                       "hostname; tar --version 2>/dev/null | head -n1; "
                                       "df -h / 2>/dev/null | tail -n1")
        info = out.strip().splitlines() + ["", "", "", ""]
        lines.append(f"System: {info[0]}  (Hostname {info[1]})")
        lines.append(f"tar: {info[2] or 'nicht gefunden!'}")
        lines.append(f"Root-Dateisystem: {info[3]}")
        priv = get_privilege(client, password)
        lines.append(f"Root-Rechte: {priv.how} - komplettes Backup moeglich.")
        if "GNU tar" not in info[2]:
            lines.append("WARNUNG: Ohne GNU tar ist nur der Image-Modus moeglich.")
    finally:
        client.close()
    return "\n".join(lines)


# --------------------------------------------------------------------------- Scheduler

def scheduler_loop(stop):
    while not stop.wait(20):
        try:
            cfg = load_config()
            sch = cfg["schedule"]
            if sch.get("type") == "off" or not cfg.get("host"):
                continue
            prev, _ = schedule_slots(sch)
            if prev is None:
                continue
            state = load_state()
            slot = prev.timestamp()
            if slot <= state.get("schedule_anchor", 0):
                continue
            if current_job() is not None:
                continue
            if state.get("slot") == slot:
                attempts = state.get("slot_attempts", 1)
                last_ok = state.get("last_success", 0) >= slot
                # Fehlgeschlagen? Bis zu 3 Versuche im Abstand von 15 Minuten.
                if last_ok or attempts >= 3 or time.time() - state.get("last_attempt", 0) < 900:
                    continue
                update_state(slot_attempts=attempts + 1)
                trigger = f"Zeitplan, Versuch {attempts + 1}"
            else:
                update_state(slot=slot, slot_attempts=1)
                trigger = "Zeitplan"
            start_job("backup", "Backup", do_backup, trigger)
        except Exception:  # noqa: BLE001
            log("Zeitplan-Fehler:\n" + traceback.format_exc())


# --------------------------------------------------------------------------- Autostart

def _startup_file():
    appdata = os.environ.get("APPDATA")
    if os.name != "nt" or not appdata:
        return None
    return Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup" / "ServerBackup.vbs"


def autostart_enabled():
    path = _startup_file()
    return bool(path and path.exists())


def set_autostart(enabled):
    path = _startup_file()
    if path is None:
        raise BackupError("Autostart gibt es nur unter Windows.")
    if enabled:
        bat = APP_DIR / "start.bat"
        path.write_text(
            'Set sh = CreateObject("WScript.Shell")\r\n'
            f'sh.Run """{bat}"" --autostart", 7, False\r\n', encoding="utf-8")
    elif path.exists():
        path.unlink()


# --------------------------------------------------------------------------- Hilfen

def _fmt_size(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _fmt_duration(sec):
    sec = int(sec)
    return f"{sec // 3600}h {sec % 3600 // 60:02d}m" if sec >= 3600 else f"{sec // 60}m {sec % 60:02d}s"


def pick_folder(initial):
    try:
        import tkinter
        from tkinter import filedialog
    except ImportError:
        raise BackupError("Ordnerauswahl nicht verfuegbar - Pfad bitte von Hand eintragen.")
    root = tkinter.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        return filedialog.askdirectory(parent=root, initialdir=initial or str(Path.home()),
                                       title="Speicherort fuer Server-Backups waehlen") or ""
    finally:
        root.destroy()


def remove_host_key(cfg):
    KNOWN_HOSTS.touch(exist_ok=True)
    keys = paramiko.HostKeys(str(KNOWN_HOSTS)) if paramiko else None
    if keys is None:
        return
    port = int(cfg.get("port") or 22)
    name = cfg["host"] if port == 22 else f"[{cfg['host']}]:{port}"
    if name in keys:
        del keys[name]
        keys.save(str(KNOWN_HOSTS))
    log(f"  Host-Key fuer {name} entfernt - wird bei der naechsten Verbindung neu gemerkt.")


# --------------------------------------------------------------------------- Web-Oberflaeche

TOKEN = secrets.token_urlsafe(24)


def public_state():
    cfg = load_config()
    state = load_state()
    prev, nxt = schedule_slots(cfg["schedule"])
    job = current_job()
    free = None
    try:
        free = shutil.disk_usage(Path(cfg["backup_dir"]).expanduser()).free
    except OSError:
        pass
    shown = {k: v for k, v in cfg.items() if k != "password_enc"}
    shown["password_set"] = bool(cfg.get("password_enc"))
    return {
        "config": shown,
        "job": job.as_dict() if job else None,
        "next_run": nxt.timestamp() if nxt else None,
        "schedule_text": describe_schedule(cfg["schedule"]),
        "last_result": state.get("last_result"),
        "last_success": state.get("last_success"),
        "backups": list_backups(cfg),
        "free_space": free,
        "autostart": autostart_enabled(),
        "autostart_supported": _startup_file() is not None,
        "log": list(_log_lines)[-300:],
        "data_dir": str(DATA_DIR),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "ServerBackup"

    def log_message(self, *_):
        pass

    def _host_ok(self):
        host = (self.headers.get("Host") or "").lower()
        if self.server.loopback_only:
            return host.split(":")[0] in ("127.0.0.1", "localhost", "[::1]")
        return True

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if not self._host_ok():
            return self._send(403, {"error": "forbidden"})
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            html = HTML_FILE.read_text(encoding="utf-8").replace("__TOKEN__", TOKEN)
            return self._send(200, html.encode("utf-8"), "text/html; charset=utf-8")
        if path == "/api/state":
            if self.headers.get("X-Token") != TOKEN:
                return self._send(403, {"error": "forbidden"})
            return self._send(200, public_state())
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self._host_ok() or self.headers.get("X-Token") != TOKEN:
            return self._send(403, {"error": "forbidden"})
        try:
            length = int(self.headers.get("Content-Length") or 0)
            data = json.loads(self.rfile.read(length) or b"{}")
            result = self.route(urlparse(self.path).path, data)
            self._send(200, {"ok": True, **(result or {})})
        except BackupError as exc:
            self._send(400, {"ok": False, "error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            log("Fehler in der Oberflaeche:\n" + traceback.format_exc())
            self._send(500, {"ok": False, "error": str(exc)})

    def route(self, path, data):
        if path == "/api/config":
            apply_config_update(data)
            log("Einstellungen gespeichert.")
            return {}
        cfg = load_config()
        if path == "/api/test":
            return {"message": do_test(cfg)}
        if path == "/api/backup":
            start_job("backup", "Backup", do_backup, "manuell")
            return {}
        if path == "/api/cancel":
            job = current_job()
            if job:
                job.cancel.set()
                job.phase = "Breche ab ..."
            return {}
        if path == "/api/restore":
            name = str(data.get("file") or "")
            _safe_backup_path(cfg, name)
            if str(data.get("confirm") or "").strip() != cfg.get("host"):
                raise BackupError("Zur Bestaetigung muss der Servername exakt eingegeben werden.")
            start_job("restore", f"Wiederherstellung {name}", do_restore, name, bool(data.get("reboot")))
            return {}
        if path == "/api/delete":
            delete_backup(cfg, data.get("file"))
            return {}
        if path == "/api/keep":
            set_keep(cfg, data.get("file"), data.get("keep"))
            return {}
        if path == "/api/cleanup":
            n = apply_retention(cfg)
            log(f"Aufraeumen: {n} Backup(s) geloescht.")
            return {"deleted": n}
        if path == "/api/hostkey/reset":
            remove_host_key(cfg)
            return {}
        if path == "/api/autostart":
            set_autostart(bool(data.get("enabled")))
            log(f"Autostart mit Windows {'eingeschaltet' if data.get('enabled') else 'ausgeschaltet'}.")
            return {}
        if path == "/api/browse":
            return {"path": pick_folder(cfg.get("backup_dir"))}
        if path == "/api/open-folder":
            folder = _backup_dir(cfg)
            if os.name == "nt":
                os.startfile(str(folder))  # noqa: S606
            return {}
        if path == "/api/quit":
            if current_job() and not data.get("force"):
                raise BackupError("Es laeuft gerade ein Auftrag - erst abbrechen oder abwarten.")
            log("Programm wird beendet.")
            threading.Thread(target=self.server.shutdown, daemon=True).start()
            return {}
        raise BackupError("Unbekannte Aktion.")


def main():
    parser = argparse.ArgumentParser(description="Server-Backup mit Weboberflaeche")
    parser.add_argument("--port", type=int, help=f"Port der Oberflaeche (Standard {DEFAULT_HTTP_PORT})")
    parser.add_argument("--no-browser", action="store_true", help="Browser nicht automatisch oeffnen")
    parser.add_argument("--autostart", action="store_true", help="Start mit Windows (ohne Browser)")
    args = parser.parse_args()

    cfg = load_config()
    if not CONFIG_FILE.exists():
        save_config(cfg)
    port = args.port or int(cfg.get("http_port") or DEFAULT_HTTP_PORT)
    listen = cfg.get("listen_host") or "127.0.0.1"
    url = f"http://127.0.0.1:{port}/"
    try:
        server = ThreadingHTTPServer((listen, port), Handler)
    except OSError:
        print(f"Port {port} ist belegt - laeuft das Server-Backup schon? Oeffne {url}")
        if not args.autostart:
            webbrowser.open(url)
        return 2
    server.daemon_threads = True
    server.loopback_only = listen in ("127.0.0.1", "localhost", "::1")
    _load_log_tail()
    log(f"Server-Backup gestartet - Oberflaeche: {url}  (Einstellungen in {DATA_DIR})")
    if paramiko is None:
        log("WARNUNG: paramiko ist nicht installiert - Backups funktionieren erst nach "
            "'pip install paramiko' (start.bat erledigt das automatisch).")
    cleanup_partials(cfg)
    stop = threading.Event()
    threading.Thread(target=scheduler_loop, args=(stop,), daemon=True).start()
    if not (args.no_browser or args.autostart):
        threading.Timer(0.8, webbrowser.open, args=(url,)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        job = current_job()
        if job:
            job.cancel.set()
            time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
