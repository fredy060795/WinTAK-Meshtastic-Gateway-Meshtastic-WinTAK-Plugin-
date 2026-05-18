"""
Focused tests for the WinTAK→ATAK marker send path.

Covers:
1. _prepare_meshtastic_forwarder_packets: single-packet now prepends the 0x00
   TRANSFER_TYPE_COT byte (matching reference meshtastic/ATAK-Plugin behaviour).
2. _decode_meshtastic_forwarder_payload: backward-compatible decode of both
   the new 0x00-prefixed format and the old raw-zlib format.
3. _forward_cot_to_meshtastic: marker CoT events (cot_class=="marker") bypass
   the ATAK_PLUGIN detail=7 path and are delivered via ATAK_FORWARDER so that
   real ATAK/iTAK clients can decode them.
"""

import zlib
import sys
import types
import unittest

sys.path.insert(0, ".")

from main_app import (
    MESHTASTIC_DATA_PAYLOAD_MAX_BYTES,
    MESHTASTIC_TRANSFER_TYPE_COT,
    FOUNTAIN_MAGIC,
    _ensure_bytes,
    TAKMeshtasticGateway,
)

# ---------------------------------------------------------------------------
# Helper — minimal stub so we can call instance methods without a real gateway
# ---------------------------------------------------------------------------

class _FakeLogger:
    def debug(self, *a, **kw): pass
    def info(self, *a, **kw): pass
    def warning(self, *a, **kw): pass
    def error(self, *a, **kw): pass
    def isEnabledFor(self, level): return False


def _make_stub_gateway():
    """Return an object that exposes the ATAK_FORWARDER packet helpers."""
    gw = object.__new__(TAKMeshtasticGateway)
    gw.logger = _FakeLogger()
    return gw


# ---------------------------------------------------------------------------
# 1. Single-packet ATAK_FORWARDER prefix
# ---------------------------------------------------------------------------

class TestForwarderPacketPrefix(unittest.TestCase):
    """_prepare_meshtastic_forwarder_packets now prepends 0x00 for single packets."""

    def setUp(self):
        self.gw = _make_stub_gateway()
        # A small zlib payload that comfortably fits in a single Meshtastic packet
        xml = (
            b'<event version="2.0" uid="M1" type="a-f-G" how="h-g">'
            b'<point lat="0" lon="0" hae="0" ce="1" le="1"/></event>'
        )
        self.small_zlib = zlib.compress(xml)
        # Sanity: it must fit with the 0x00 prefix
        self.assertLessEqual(len(self.small_zlib) + 1, MESHTASTIC_DATA_PAYLOAD_MAX_BYTES)

    def test_single_packet_starts_with_transfer_type_byte(self):
        packets = self.gw._prepare_meshtastic_forwarder_packets(self.small_zlib)
        self.assertEqual(len(packets), 1, "Expected exactly one packet for small payload")
        self.assertEqual(
            packets[0][0], MESHTASTIC_TRANSFER_TYPE_COT,
            "First byte must be 0x00 (TRANSFER_TYPE_COT)"
        )

    def test_single_packet_payload_after_prefix_is_original_zlib(self):
        packets = self.gw._prepare_meshtastic_forwarder_packets(self.small_zlib)
        self.assertEqual(packets[0][1:], self.small_zlib,
                         "Bytes after prefix must equal original zlib payload")

    def test_large_payload_uses_ftn_fountain(self):
        # Build a zlib payload that won't fit in a single packet by using
        # random (incompressible) bytes so zlib can't shrink it below the limit.
        import os
        large_raw = os.urandom(300)  # 300 random bytes → zlib output also ~300 bytes
        large_zlib = zlib.compress(large_raw)
        self.assertGreater(len(large_zlib) + 1, MESHTASTIC_DATA_PAYLOAD_MAX_BYTES,
                           "Large payload must exceed single-packet limit for this test to be valid")
        packets = self.gw._prepare_meshtastic_forwarder_packets(large_zlib)
        self.assertGreater(len(packets), 1, "Large payload must produce multiple FTN packets")
        # All FTN packets start with the FTN magic bytes
        for pkt in packets:
            self.assertEqual(pkt[:3], FOUNTAIN_MAGIC,
                             f"FTN packet must start with FTN magic, got {pkt[:3]!r}")


# ---------------------------------------------------------------------------
# 2. Decode backward compatibility
# ---------------------------------------------------------------------------

class TestForwarderDecodeBackwardCompat(unittest.TestCase):
    """_decode_meshtastic_forwarder_payload handles both old raw-zlib and new 0x00+zlib."""

    def setUp(self):
        self.gw = _make_stub_gateway()
        xml = b'<event version="2.0" uid="T1"><point lat="0" lon="0" hae="0" ce="1" le="1"/></event>'
        self.xml = xml
        self.zlib_bytes = zlib.compress(xml)

    def test_decode_prefixed_payload(self):
        """New format: 0x00 + zlib must decode back to original XML."""
        prefixed = bytes([MESHTASTIC_TRANSFER_TYPE_COT]) + self.zlib_bytes
        result = self.gw._decode_meshtastic_forwarder_payload(prefixed)
        self.assertIsNotNone(result)
        self.assertIn(b"event", result)

    def test_decode_raw_zlib_payload(self):
        """Old format: raw zlib (no prefix) must still decode (backward compat)."""
        result = self.gw._decode_meshtastic_forwarder_payload(self.zlib_bytes)
        self.assertIsNotNone(result)
        self.assertIn(b"event", result)

    def test_decode_returns_none_for_empty(self):
        self.assertIsNone(self.gw._decode_meshtastic_forwarder_payload(b""))
        self.assertIsNone(self.gw._decode_meshtastic_forwarder_payload(None))


# ---------------------------------------------------------------------------
# 3. _forward_cot_to_meshtastic: markers bypass ATAK_PLUGIN detail=7
# ---------------------------------------------------------------------------

class TestForwardCotMarkerBypassesDetail7(unittest.TestCase):
    """Markers must be sent via ATAK_FORWARDER, not ATAK_PLUGIN detail=7."""

    # Marker CoT XML sent by WinTAK (a-f-G-U-C = Friendly Ground)
    FRIENDLY_MARKER_XML = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<event version="2.0" uid="MARKER-F1" type="a-f-G-U-C" how="h-g"'
        ' time="2024-01-01T00:00:00.000Z" start="2024-01-01T00:00:00.000Z"'
        ' stale="2025-01-01T00:00:00.000Z">'
        '<point lat="48.0" lon="11.0" hae="500.0" ce="5.0" le="5.0"/>'
        '<detail><contact callsign="Friendly1"/><__group name="Cyan" role="Team Member"/></detail>'
        '</event>'
    )

    HOSTILE_MARKER_XML = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<event version="2.0" uid="MARKER-H1" type="a-h-G-U-C" how="h-g"'
        ' time="2024-01-01T00:00:00.000Z" start="2024-01-01T00:00:00.000Z"'
        ' stale="2025-01-01T00:00:00.000Z">'
        '<point lat="48.1" lon="11.1" hae="400.0" ce="5.0" le="5.0"/>'
        '<detail><contact callsign="Hostile1"/></detail>'
        '</event>'
    )

    def _make_mock_gateway(self):
        """Return a gateway stub that records what send calls were made."""
        import threading
        gw = object.__new__(TAKMeshtasticGateway)
        gw.logger = _FakeLogger()

        # Track interface calls
        gw._sent_ports = []

        def fake_send_data(payload, interfaces, portnum):
            gw._sent_ports.append(int(portnum))
            return [object()]  # fake "sent" result

        gw._send_data_to_interfaces = fake_send_data
        gw._get_interfaces_snapshot = lambda: [object()]

        # Minimal state needed by _forward_cot_to_meshtastic internals
        gw.gateway_uid = "GW-01"
        gw.gateway_callsign = "GW-01"

        # Cache state required by _remember_recent_cot_identity
        gw.chat_cache_lock = threading.Lock()
        gw.recent_cot_identity_by_uid = {}

        return gw

    def _call_forward(self, gw, xml):
        """Stub out _ensure_meshtastic_interfaces and call _forward_cot_to_meshtastic."""
        gw._ensure_meshtastic_interfaces = lambda raise_on_empty=False: [object()]
        return gw._forward_cot_to_meshtastic(xml)

    def test_friendly_marker_uses_atak_forwarder_port(self):
        from main_app import MESHTASTIC_ATAK_FORWARDER_PORTNUM, MESHTASTIC_ATAK_PLUGIN_PORTNUM
        gw = self._make_mock_gateway()
        result = self._call_forward(gw, self.FRIENDLY_MARKER_XML)
        transport = result.get("transport", "")
        # Must use an ATAK_FORWARDER transport variant
        self.assertIn("ATAK_FORWARDER", transport,
                      f"Friendly marker must be sent via ATAK_FORWARDER, got: {transport}")
        # Must NOT have sent on ATAK_PLUGIN port (72) as its primary path
        # (PLI port is fine if PLI detection fires, but for markers it should not)
        plugin_sends = [p for p in gw._sent_ports if p == MESHTASTIC_ATAK_PLUGIN_PORTNUM]
        forwarder_sends = [p for p in gw._sent_ports if p == MESHTASTIC_ATAK_FORWARDER_PORTNUM]
        self.assertGreater(len(forwarder_sends), 0,
                           "At least one packet must be sent on ATAK_FORWARDER port")
        self.assertEqual(len(plugin_sends), 0,
                         "No packet should be sent on ATAK_PLUGIN port for a marker")

    def test_hostile_marker_uses_atak_forwarder_port(self):
        from main_app import MESHTASTIC_ATAK_FORWARDER_PORTNUM, MESHTASTIC_ATAK_PLUGIN_PORTNUM
        gw = self._make_mock_gateway()
        result = self._call_forward(gw, self.HOSTILE_MARKER_XML)
        transport = result.get("transport", "")
        self.assertIn("ATAK_FORWARDER", transport,
                      f"Hostile marker must be sent via ATAK_FORWARDER, got: {transport}")
        forwarder_sends = [p for p in gw._sent_ports if p == MESHTASTIC_ATAK_FORWARDER_PORTNUM]
        self.assertGreater(len(forwarder_sends), 0)

    def test_marker_forwarder_packet_has_transfer_type_prefix(self):
        """The ATAK_FORWARDER packet sent for a marker must start with 0x00."""
        from main_app import MESHTASTIC_ATAK_FORWARDER_PORTNUM

        gw = self._make_mock_gateway()
        payloads_on_forwarder = []

        def fake_send_data(payload, interfaces, portnum):
            if int(portnum) == MESHTASTIC_ATAK_FORWARDER_PORTNUM:
                payloads_on_forwarder.append(bytes(payload))
            return [object()]

        gw._send_data_to_interfaces = fake_send_data
        self._call_forward(gw, self.FRIENDLY_MARKER_XML)

        self.assertGreater(len(payloads_on_forwarder), 0,
                           "Expected at least one ATAK_FORWARDER payload")
        first = payloads_on_forwarder[0]
        # Single-packet: must start with 0x00 (transfer type COT)
        # Multi-packet FTN: starts with 'FTN' magic
        is_single = first[0:1] == bytes([MESHTASTIC_TRANSFER_TYPE_COT])
        is_ftn = first[:3] == FOUNTAIN_MAGIC
        self.assertTrue(is_single or is_ftn,
                        f"ATAK_FORWARDER payload must start with 0x00 or FTN magic, "
                        f"got: {first[:4].hex()}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
