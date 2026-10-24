"""Tests for the mesh airtime governor (channel utilization control)."""

import sys
import unittest

sys.path.insert(0, ".")

from mesh_airtime import (
    slim_cot_for_mesh,
    DECISION_DEFER,
    DECISION_DROP,
    DECISION_SEND,
    MeshAirtimeGovernor,
    cot_content_signature,
)


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _pli(uid="WINTAK-1", lat=48.0, lon=11.0, t="2026-01-01T00:00:00Z"):
    xml = (f'<event version="2.0" uid="{uid}" type="a-f-G-U-C" how="h-e" time="{t}" start="{t}" stale="{t}">'
           f'<point lat="{lat}" lon="{lon}" hae="0" ce="5" le="5"/><detail><contact callsign="W1"/></detail></event>')
    return xml, {"uid": uid, "lat": lat, "lon": lon, "is_pli": True}


def _marker(uid="M-1", lat=48.0, lon=11.0, t="2026-01-01T00:00:00Z", remarks="Treffpunkt"):
    xml = (f'<event version="2.0" uid="{uid}" type="a-h-G" how="h-g-i-g-o" time="{t}" start="{t}" stale="{t}">'
           f'<point lat="{lat}" lon="{lon}" hae="0" ce="5" le="5"/><detail><remarks>{remarks}</remarks>'
           f'<__serverdestination destinations="x"/></detail></event>')
    return xml, {"uid": uid, "lat": lat, "lon": lon, "is_pli": False}


class TestGovernor(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self.util = None
        self.sent = []
        self.gov = MeshAirtimeGovernor(
            {"pli_min_interval": 60, "pli_stationary_interval": 300, "pli_min_distance_m": 25,
             "marker_min_interval": 10, "marker_repeat_interval": 1800,
             "chutil_limit_pli": 25, "chutil_limit_markers": 40},
            channel_utilization=lambda: self.util,
            send_deferred=lambda packet, meta: self.sent.append(meta["uid"]),
            clock=self.clock,
        )

    def test_wintak_sa_every_few_seconds_is_throttled_latest_wins(self):
        self.assertEqual(self.gov.decide(*_pli())[0], DECISION_SEND)
        for i in range(1, 12):  # WinTAK sends every 5 s
            self.clock.now += 5
            xml, meta = _pli(lat=48.0 + i * 0.001)  # moving
            self.assertEqual(self.gov.decide(xml, meta)[0], DECISION_DEFER)
        self.clock.now += 5  # 60 s after the first one
        self.assertEqual(self.gov.flush_due(), 1, "newest held-back position is sent once allowed")
        self.assertEqual(self.gov.stats["sent"], 2)

    def test_stationary_unit_reports_rarely(self):
        self.gov.decide(*_pli())
        self.clock.now += 120
        self.assertEqual(self.gov.decide(*_pli(lat=48.00005))[0], DECISION_DEFER)  # ~5 m
        self.clock.now += 120
        self.assertEqual(self.gov.flush_due(), 0)
        self.clock.now += 70  # > 300 s since last send
        self.assertEqual(self.gov.flush_due(), 1)

    def test_unchanged_marker_with_new_timestamp_is_dropped(self):
        self.assertEqual(self.gov.decide(*_marker())[0], DECISION_SEND)
        self.clock.now += 30
        self.assertEqual(self.gov.decide(*_marker(t="2026-01-01T00:05:00Z"))[0], DECISION_DROP)
        self.clock.now += 30
        self.assertEqual(self.gov.decide(*_marker(remarks="geändert"))[0], DECISION_SEND)

    def test_rapid_marker_edits_are_coalesced(self):
        self.gov.decide(*_marker())
        self.clock.now += 2
        self.assertEqual(self.gov.decide(*_marker(lat=48.01))[0], DECISION_DEFER)
        self.clock.now += 2
        self.assertEqual(self.gov.decide(*_marker(lat=48.02))[0], DECISION_DEFER)
        self.clock.now += 10
        self.assertEqual(self.gov.flush_due(), 1)

    def test_channel_utilization_brake(self):
        self.util = 30.0
        self.assertEqual(self.gov.brake_level(), 1)
        self.assertEqual(self.gov.decide(*_pli())[0], DECISION_DEFER)
        self.assertEqual(self.gov.decide(*_marker())[0], DECISION_SEND)
        self.util = 45.0
        self.assertEqual(self.gov.decide(*_marker(uid="M-2"))[0], DECISION_DEFER)
        self.assertEqual(self.gov.decide("<x/>", None, is_chat=True)[0], DECISION_SEND)
        self.assertEqual(self.gov.flush_due(), 0)
        self.util = 10.0
        self.assertEqual(self.gov.flush_due(), 2)
        self.assertEqual(self.sent[0], "M-2", "markers before positions")

    def test_disabled_passes_everything(self):
        gov = MeshAirtimeGovernor({"enabled": False})
        for _ in range(5):
            self.assertEqual(gov.decide(*_pli())[0], DECISION_SEND)

    def test_signature_ignores_timestamps_and_server_noise(self):
        a = _marker(t="2026-01-01T00:00:00Z")[0]
        b = _marker(t="2026-02-01T00:00:00Z")[0].replace('destinations="x"', 'destinations="y"')
        self.assertEqual(cot_content_signature(a), cot_content_signature(b))
        self.assertNotEqual(cot_content_signature(a), cot_content_signature(_marker(lat=48.1)[0]))

    def test_settings_from_cfg(self):
        settings = MeshAirtimeGovernor.settings_from_cfg({"mesh_airtime_control": False, "mesh_pli_min_interval_seconds": "120"})
        self.assertFalse(settings["enabled"])
        self.assertEqual(settings["pli_min_interval"], 120)


class TestSlimCot(unittest.TestCase):
    def test_keeps_meaning_and_shrinks(self):
        import zlib
        from xml.etree.ElementTree import fromstring

        original = (
            b'<?xml version="1.0" encoding="UTF-8"?>\n<event version="2.0" uid="m1" type="a-h-G" how="h-g-i-g-o" '
            b'time="2026-10-24T10:00:00Z" start="2026-10-24T10:00:00Z" stale="2027-10-24T10:00:00Z">\n'
            b'  <point lat="48.30612345678912" lon="14.28612345678912" hae="9999999.0" ce="9999999.0" le="9999999.0"/>\n'
            b'  <detail><link uid="c" production_time="2026-10-24T10:00:00Z" relation="p-p"/><contact callsign="Feind 1"/>'
            b'<remarks/><usericon iconsetpath="COT_MAPPING_2525B/a-h/a-h-G"/><precisionlocation altsrc="DTED0"/>'
            b'<__serverdestination destinations="x"/><_flow-tags_ a="1"/></detail></event>'
        )
        slim = slim_cot_for_mesh(original)
        root = fromstring(slim)
        self.assertEqual((root.get("uid"), root.get("type"), root.get("stale")), ("m1", "a-h-G", "2027-10-24T10:00:00Z"))
        point = root.find("point")
        self.assertAlmostEqual(float(point.get("lat")), 48.3061235, places=6)
        self.assertEqual(root.find("detail/contact").get("callsign"), "Feind 1")
        self.assertIsNotNone(root.find("detail/usericon"))
        for gone in ("__serverdestination", "_flow-tags_", "precisionlocation", "remarks"):
            self.assertIsNone(root.find(f"detail/{gone}"))
        self.assertLess(len(zlib.compress(slim, 9)), len(zlib.compress(original)))

    def test_keeps_remarks_with_text(self):
        slim = slim_cot_for_mesh(b'<event uid="a" type="b"><point lat="1" lon="2"/><detail><remarks>Hallo</remarks></detail></event>')
        self.assertIn(b"<remarks>Hallo</remarks>", slim)


class TestGatewayIntegration(unittest.TestCase):
    def test_repeated_wintak_sa_reaches_mesh_once(self):
        import main_app

        gw = main_app.TAKMeshtasticGateway(["COM7"], {"tak_server_host": "", "local_tak_tcp_receiver_enabled": False})
        try:
            forwarded = []
            gw._forward_cot_to_meshtastic = lambda packet: forwarded.append(packet) or {"transport": "ATAK_PLUGIN", "count": 1}
            gw._record_service_monitor_event = lambda *a, **k: None
            for second in range(6):
                t = f"2026-01-01T00:00:0{second}Z"
                xml, _meta = _pli(t=t, lat=48.0 + second * 0.0001)
                gw.handle_inbound_tak_packet(xml.encode(), source_addr=("127.0.0.1", 1), source_protocol="TCP")
            self.assertEqual(len(forwarded), 1)
            self.assertEqual(gw.get_mesh_airtime_status()["deferred"], 5)
        finally:
            gw.shutdown_flag.set()


if __name__ == "__main__":
    unittest.main(verbosity=2)
