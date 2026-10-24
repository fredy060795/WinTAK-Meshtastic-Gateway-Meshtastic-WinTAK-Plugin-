"""
Airtime control for CoT that the gateway sends into the Meshtastic (LoRa) mesh.

LoRa channels are slow and shared. WinTAK reports its own position every few seconds and
re-sends markers with fresh timestamps; forwarding all of that 1:1 keeps the channel busy
(high "channel utilization") and slows down every node in range. The governor decides per
event whether it is worth the airtime:

* Positions (PLI): at most every ``pli_min_interval`` seconds per UID, and only every
  ``pli_stationary_interval`` seconds if the unit moved less than ``pli_min_distance_m``.
  A suppressed position is not lost: the newest one per UID is kept and sent as soon as it
  is allowed ("latest wins").
* Markers / other CoT: only when new or changed; an unchanged marker is repeated at most
  every ``marker_repeat_interval`` seconds. Rapid edits of the same marker are coalesced.
* Channel utilization brake: the local radio reports how busy the channel is. Above
  ``chutil_limit_pli`` percent positions are held back, above ``chutil_limit_markers``
  markers as well. Chat always goes through - it is the most important traffic.
"""

import hashlib
import math
import threading
import time
from xml.etree.ElementTree import ParseError, fromstring, tostring

PRIORITY_CHAT = 0
PRIORITY_MARKER = 1
PRIORITY_PLI = 2

DECISION_SEND = "send"
DECISION_DEFER = "defer"   # held back; the newest version is sent later automatically
DECISION_DROP = "drop"     # duplicate / unchanged, nothing to send

DEFAULTS = {
    "enabled": True,
    "pli_min_interval": 60,
    "pli_stationary_interval": 300,
    "pli_min_distance_m": 25.0,
    "marker_min_interval": 10,
    "marker_repeat_interval": 1800,
    "chutil_limit_pli": 25.0,
    "chutil_limit_markers": 40.0,
}

# Attributes / elements that change on every re-send without changing what the marker means.
_VOLATILE_EVENT_ATTRS = ("time", "start", "stale")
_VOLATILE_DETAIL_TAGS = ("__serverdestination", "_flow-tags_", "precisionlocation", "status", "track")


def haversine_m(lat1, lon1, lat2, lon2):
    radius = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(min(1.0, math.sqrt(a)))


def cot_content_signature(packet_xml):
    """Hash of what a CoT event *means* - ignores timestamps and other per-send noise."""
    try:
        root = fromstring(packet_xml)
    except (ParseError, TypeError, ValueError):
        data = packet_xml if isinstance(packet_xml, bytes) else str(packet_xml).encode("utf-8")
        return hashlib.sha256(data).hexdigest()
    for attr in _VOLATILE_EVENT_ATTRS:
        root.attrib.pop(attr, None)
    for parent in list(root.iter()):
        for child in list(parent):
            if child.tag.split("}")[-1] in _VOLATILE_DETAIL_TAGS:
                parent.remove(child)
    for element in root.iter():
        if element.tag.split("}")[-1] == "point":
            # ~1 m resolution; GPS jitter of a placed marker should not count as a change.
            for key in ("lat", "lon"):
                try:
                    element.set(key, f"{float(element.get(key)):.5f}")
                except (TypeError, ValueError):
                    pass
            for key in ("hae", "ce", "le"):
                element.attrib.pop(key, None)
    return hashlib.sha256(tostring(root)).hexdigest()


# Detail elements that only matter on a TAK server / in one client and can be left out on LoRa.
MESH_STRIP_DETAIL_TAGS = ("__serverdestination", "_flow-tags_", "precisionlocation", "__forcexml")


def slim_cot_for_mesh(packet_xml):
    """Smaller XML for the mesh: no declaration/whitespace, no server-only elements, sane precision.

    Coordinates keep 7 decimals (~1 cm), hae/ce/le 1 decimal - plenty for TAK, but no
    15-digit floats. Returns the original bytes if the XML cannot be parsed.
    """
    data = packet_xml if isinstance(packet_xml, bytes) else str(packet_xml).encode("utf-8")
    try:
        root = fromstring(data)
    except (ParseError, TypeError, ValueError):
        return data.strip()
    for parent in list(root.iter()):
        for child in list(parent):
            if child.tag.split("}")[-1] in MESH_STRIP_DETAIL_TAGS:
                parent.remove(child)
    for parent in list(root.iter()):
        for child in list(parent):
            # An empty <remarks/> carries no information.
            if child.tag.split("}")[-1] == "remarks" and not (child.text or "").strip() and not len(child):
                parent.remove(child)
    for element in root.iter():
        if element.tag.split("}")[-1] == "link":
            element.attrib.pop("production_time", None)
        if element.text is not None and not element.text.strip():
            element.text = None
        if element.tail is not None and not element.tail.strip():
            element.tail = None
        if element.tag.split("}")[-1] == "point":
            for key, digits in (("lat", 7), ("lon", 7), ("hae", 1), ("ce", 1), ("le", 1)):
                value = element.get(key)
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                text = f"{number:.{digits}f}".rstrip("0").rstrip(".")
                element.set(key, text if text not in ("", "-") else "0")
    return tostring(root, encoding="utf-8", xml_declaration=False).replace(b" />", b"/>")


class MeshAirtimeGovernor:
    """Admission control for TAK -> mesh traffic. Thread-safe."""

    def __init__(self, settings=None, *, channel_utilization=None, send_deferred=None, logger=None, clock=None):
        self.settings = dict(DEFAULTS)
        self.settings.update({k: v for k, v in (settings or {}).items() if v is not None})
        self._channel_utilization = channel_utilization or (lambda: None)
        self._send_deferred = send_deferred
        self._logger = logger
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._last_sent = {}      # uid -> {"time", "lat", "lon", "signature"}
        self._pending = {}        # uid -> {"packet", "meta", "since"}
        self._stop = threading.Event()
        self._thread = None
        self.stats = {"sent": 0, "suppressed": 0, "deferred": 0, "deferred_sent": 0, "chat": 0}

    # -- configuration helpers --------------------------------------------

    @classmethod
    def settings_from_cfg(cls, cfg):
        cfg = cfg or {}

        def number(key, default):
            try:
                return float(cfg.get(key, default))
            except (TypeError, ValueError):
                return default

        return {
            "enabled": str(cfg.get("mesh_airtime_control", True)).strip().lower() not in ("0", "false", "no", "off"),
            "pli_min_interval": number("mesh_pli_min_interval_seconds", DEFAULTS["pli_min_interval"]),
            "pli_stationary_interval": number("mesh_pli_stationary_interval_seconds", DEFAULTS["pli_stationary_interval"]),
            "pli_min_distance_m": number("mesh_pli_min_distance_m", DEFAULTS["pli_min_distance_m"]),
            "marker_min_interval": number("mesh_marker_min_interval_seconds", DEFAULTS["marker_min_interval"]),
            "marker_repeat_interval": number("mesh_marker_repeat_interval_seconds", DEFAULTS["marker_repeat_interval"]),
            "chutil_limit_pli": number("mesh_chutil_limit_pli", DEFAULTS["chutil_limit_pli"]),
            "chutil_limit_markers": number("mesh_chutil_limit_markers", DEFAULTS["chutil_limit_markers"]),
        }

    # -- state --------------------------------------------------------------

    def channel_utilization(self):
        try:
            value = self._channel_utilization()
        except Exception:
            return None
        try:
            return None if value is None else float(value)
        except (TypeError, ValueError):
            return None

    def brake_level(self, utilization=None):
        """0 = free, 1 = positions held back, 2 = positions and markers held back."""
        util = self.channel_utilization() if utilization is None else utilization
        if util is None:
            return 0
        if util >= self.settings["chutil_limit_markers"]:
            return 2
        if util >= self.settings["chutil_limit_pli"]:
            return 1
        return 0

    def get_status(self):
        util = self.channel_utilization()
        with self._lock:
            stats = dict(self.stats)
            pending = len(self._pending)
        stats.update(
            enabled=bool(self.settings["enabled"]),
            channel_utilization=util,
            brake_level=self.brake_level(util),
            pending=pending,
            limits={"pli": self.settings["chutil_limit_pli"], "markers": self.settings["chutil_limit_markers"]},
        )
        return stats

    # -- decisions ----------------------------------------------------------

    @staticmethod
    def priority_for(meta, is_chat=False):
        if is_chat:
            return PRIORITY_CHAT
        if meta and meta.get("is_pli"):
            return PRIORITY_PLI
        return PRIORITY_MARKER

    def decide(self, packet_xml, meta, is_chat=False):
        """Return (decision, reason). Records the send when the decision is DECISION_SEND."""
        if is_chat:
            with self._lock:
                self.stats["chat"] += 1
            return DECISION_SEND, "Chat hat immer Vorrang"
        if not self.settings["enabled"] or not meta or not meta.get("uid"):
            return DECISION_SEND, "Airtime-Steuerung aus"
        now = self._clock()
        uid = meta["uid"]
        priority = self.priority_for(meta)
        brake = self.brake_level()
        signature = cot_content_signature(packet_xml)
        with self._lock:
            last = self._last_sent.get(uid)
            decision, reason = self._decide_locked(meta, priority, brake, signature, last, now)
            if decision == DECISION_SEND:
                self._remember_sent_locked(uid, meta, signature, now)
                self._pending.pop(uid, None)
                self.stats["sent"] += 1
            elif decision == DECISION_DEFER:
                self._pending[uid] = {"packet": packet_xml, "meta": dict(meta), "since": now}
                self.stats["deferred"] += 1
            else:
                self.stats["suppressed"] += 1
        return decision, reason

    def _decide_locked(self, meta, priority, brake, signature, last, now):
        settings = self.settings
        if priority == PRIORITY_PLI:
            if brake >= 1:
                return DECISION_DEFER, "Kanal ausgelastet – Position zurückgestellt"
            if last is None:
                return DECISION_SEND, "erste Position"
            elapsed = now - last["time"]
            if elapsed < settings["pli_min_interval"]:
                return DECISION_DEFER, f"Position gedrosselt (max. alle {settings['pli_min_interval']:.0f} s)"
            moved = None
            if last.get("lat") is not None and meta.get("lat") is not None:
                moved = haversine_m(last["lat"], last["lon"], meta["lat"], meta["lon"])
            if moved is not None and moved < settings["pli_min_distance_m"] and elapsed < settings["pli_stationary_interval"]:
                return DECISION_DEFER, f"kaum bewegt ({moved:.0f} m) – nächste Meldung spätestens nach {settings['pli_stationary_interval']:.0f} s"
            return DECISION_SEND, "Position fällig"
        # markers and other CoT
        if last is not None and last.get("signature") == signature:
            if now - last["time"] < settings["marker_repeat_interval"]:
                return DECISION_DROP, "Marker unverändert"
        if brake >= 2:
            return DECISION_DEFER, "Kanal stark ausgelastet – Marker zurückgestellt"
        if last is not None and now - last["time"] < settings["marker_min_interval"]:
            return DECISION_DEFER, "Marker wird gerade bearbeitet – Änderungen werden zusammengefasst"
        return DECISION_SEND, "neu oder geändert"

    def _remember_sent_locked(self, uid, meta, signature, now):
        self._last_sent[uid] = {"time": now, "lat": meta.get("lat"), "lon": meta.get("lon"), "signature": signature}
        if len(self._last_sent) > 5000:
            oldest = sorted(self._last_sent.items(), key=lambda item: item[1]["time"])[:1000]
            for key, _ in oldest:
                self._last_sent.pop(key, None)

    # -- deferred delivery ----------------------------------------------------

    def due_pending(self):
        """Pending events that may be sent now, highest priority first."""
        now = self._clock()
        brake = self.brake_level()
        due = []
        with self._lock:
            for uid, entry in list(self._pending.items()):
                meta = entry["meta"]
                priority = self.priority_for(meta)
                last = self._last_sent.get(uid)
                signature = cot_content_signature(entry["packet"])
                decision, _reason = self._decide_locked(meta, priority, brake, signature, last, now)
                if decision == DECISION_SEND:
                    due.append((priority, entry["since"], uid, entry))
                elif decision == DECISION_DROP:
                    self._pending.pop(uid, None)
            due.sort(key=lambda item: (item[0], item[1]))
            result = []
            for _priority, _since, uid, entry in due:
                self._pending.pop(uid, None)
                self._remember_sent_locked(uid, entry["meta"], cot_content_signature(entry["packet"]), now)
                self.stats["deferred_sent"] += 1
                self.stats["sent"] += 1
                result.append(entry)
        return result

    def flush_due(self):
        if self._send_deferred is None:
            return 0
        sent = 0
        for entry in self.due_pending():
            try:
                self._send_deferred(entry["packet"], entry["meta"])
                sent += 1
            except Exception as exc:
                if self._logger:
                    self._logger.warning(f"Zurückgestelltes CoT konnte nicht ins Mesh gesendet werden: {exc}")
        return sent

    def start(self, interval=5.0):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, args=(interval,), daemon=True, name="mesh-airtime")
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self, interval):
        while not self._stop.wait(interval):
            self.flush_due()
