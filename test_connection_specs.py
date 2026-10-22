"""Tests for BLE/TCP/Serial Meshtastic connection entries."""

import sys
import threading
import types
import unittest
from unittest import mock

sys.path.insert(0, ".")

import main_app
from main_app import (
    TAKMeshtasticGateway,
    _parse_ports_text,
    choose_serial_ports,
    is_serial_port_spec,
    parse_meshtastic_connection_spec,
)


class _FakeLogger:
    def debug(self, *a, **kw): pass
    def info(self, *a, **kw): pass
    def warning(self, *a, **kw): pass
    def error(self, *a, **kw): pass


def _make_stub_gateway(ports):
    gw = object.__new__(TAKMeshtasticGateway)
    gw.logger = _FakeLogger()
    gw.ports = list(ports)
    gw.interfaces = []
    gw.interface_lock = threading.Lock()
    gw._meshtastic_pubsub_registered = True
    gw._register_local_nodes = lambda: None
    return gw


class _FakeBLEError(Exception):
    pass


class _FakeIface:
    def __init__(self, target):
        self.target = target
        self.isConnected = threading.Event()
        self.isConnected.set()
        self.client = object()
        self.closed = False

    def close(self):
        self.closed = True


class TestConnectionSpecParsing(unittest.TestCase):
    def test_serial_ports(self):
        self.assertEqual(parse_meshtastic_connection_spec("COM7")["kind"], "serial")
        self.assertEqual(parse_meshtastic_connection_spec("/dev/ttyUSB0")["kind"], "serial")
        self.assertTrue(is_serial_port_spec("COM3"))

    def test_ble_prefixes_and_mac(self):
        for value, target in (
            ("BLE:AA:BB:CC:DD:EE:FF", "AA:BB:CC:DD:EE:FF"),
            ("ble://Meshtastic_1a2b", "Meshtastic_1a2b"),
            ("aa-bb-cc-dd-ee-ff", "aa-bb-cc-dd-ee-ff"),
            ("BLE", None),
        ):
            spec = parse_meshtastic_connection_spec(value)
            self.assertEqual(spec["kind"], "ble", value)
            self.assertEqual(spec["target"], target, value)
        self.assertFalse(is_serial_port_spec("BLE:Node"))

    def test_tcp(self):
        spec = parse_meshtastic_connection_spec("TCP:192.168.1.50")
        self.assertEqual((spec["kind"], spec["target"], spec["tcp_port"]), ("tcp", "192.168.1.50", 4403))
        spec = parse_meshtastic_connection_spec("tcp://meshnode.local:4500")
        self.assertEqual((spec["target"], spec["tcp_port"]), ("meshnode.local", 4500))
        with self.assertRaises(ValueError):
            parse_meshtastic_connection_spec("TCP:")

    def test_parse_ports_text_keeps_ble_names_with_spaces(self):
        self.assertEqual(
            _parse_ports_text("COM7 COM8, BLE:My Node 1a2b; BLE:AA:BB:CC:DD:EE:FF"),
            ["COM7", "COM8", "BLE:My Node 1a2b", "BLE:AA:BB:CC:DD:EE:FF"],
        )


class TestOpenInterfaces(unittest.TestCase):
    def _fake_ble_module(self, scan_devices=(), fail_targets=()):
        created = []

        class FakeBLEInterface(_FakeIface):
            BLEError = _FakeBLEError

            def __init__(self, target):
                if target in fail_targets:
                    raise _FakeBLEError("not found")
                super().__init__(target)
                created.append(self)

            @staticmethod
            def scan():
                return [types.SimpleNamespace(address=a, name=n) for a, n in scan_devices]

        return types.SimpleNamespace(BLEInterface=FakeBLEInterface), created

    def test_ble_entry_connects_via_ble_interface_and_keeps_label(self):
        module, created = self._fake_ble_module()
        gw = _make_stub_gateway(["BLE:AA:BB:CC:DD:EE:FF"])
        with mock.patch.object(main_app, "meshtastic_ble_interface", module), \
                mock.patch.object(main_app, "meshtastic", types.SimpleNamespace()):
            interfaces = gw._connect_meshtastic_interfaces()
        self.assertEqual(len(interfaces), 1)
        self.assertEqual(created[0].target, "AA:BB:CC:DD:EE:FF")
        self.assertEqual(gw._get_interface_label(interfaces[0]), "BLE:AA:BB:CC:DD:EE:FF")
        self.assertEqual(gw._get_missing_configured_ports(interfaces), [])

    def test_ble_lookup_is_case_and_separator_insensitive(self):
        module, created = self._fake_ble_module(
            scan_devices=[("AA:BB:CC:DD:EE:FF", "Meshtastic_eeff")],
            fail_targets={"aa-bb-cc-dd-ee-ff"},
        )
        gw = _make_stub_gateway(["BLE:aa-bb-cc-dd-ee-ff"])
        with mock.patch.object(main_app, "meshtastic_ble_interface", module):
            iface = gw._open_meshtastic_interface("BLE:aa-bb-cc-dd-ee-ff")
        self.assertEqual(iface.target, "AA:BB:CC:DD:EE:FF")

    def test_ble_unavailable_raises(self):
        gw = _make_stub_gateway([])
        with mock.patch.object(main_app, "meshtastic_ble_interface", None):
            with self.assertRaises(RuntimeError):
                gw._open_meshtastic_interface("BLE:Node")

    def test_disconnected_interface_is_pruned_and_reconnected(self):
        module, created = self._fake_ble_module()
        gw = _make_stub_gateway(["BLE:Node"])
        with mock.patch.object(main_app, "meshtastic_ble_interface", module), \
                mock.patch.object(main_app, "meshtastic", types.SimpleNamespace()):
            first = gw._connect_meshtastic_interfaces()[0]
            first.isConnected.clear()  # library signalled link loss
            interfaces = gw._ensure_meshtastic_interfaces()
        self.assertTrue(first.closed)
        self.assertEqual(len(interfaces), 1)
        self.assertIsNot(interfaces[0], first)

    def test_foreign_interfaces_are_not_pruned(self):
        gw = _make_stub_gateway([])
        stub = types.SimpleNamespace()
        self.assertTrue(gw._is_meshtastic_interface_alive(stub))


class TestTerminalPortSelection(unittest.TestCase):
    def test_configured_ble_entry_is_not_rejected_by_com_port_check(self):
        com = types.SimpleNamespace(device="COM7", description="USB")
        fake_serial = types.SimpleNamespace(
            tools=types.SimpleNamespace(list_ports=types.SimpleNamespace(comports=lambda: [com]))
        )
        cfg = {"meshtastic_port": ["COM7", "BLE:AA:BB:CC:DD:EE:FF"]}
        with mock.patch.object(main_app, "serial", fake_serial), \
                mock.patch("builtins.input", return_value=""), \
                mock.patch("builtins.print"):
            self.assertEqual(choose_serial_ports(cfg), ["COM7", "BLE:AA:BB:CC:DD:EE:FF"])
            self.assertEqual(choose_serial_ports(cfg, all_ports_mode=True), ["COM7", "BLE:AA:BB:CC:DD:EE:FF"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
