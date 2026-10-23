"""Tests for the remote TAK server connection (TCP/TLS streaming, data packages, routing)."""

import datetime
import os
import shutil
import socket
import ssl
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
import zipfile

sys.path.insert(0, ".")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

import main_app
from main_app import TAKMeshtasticGateway, _apply_settings_payload_to_cfg, _build_browser_ui_form_state
from tak_server_client import (
    TAKServerClient,
    build_tak_ssl_context,
    enroll_tak_client_certificate,
    inspect_pkcs12_file,
    extract_cot_events,
    load_tak_data_package,
    parse_tak_server_endpoint,
)


class _Logger:
    def __init__(self):
        self.lines = []

    def _log(self, msg, *a, **kw):
        self.lines.append(str(msg))

    debug = info = warning = error = _log


SAMPLE_EVENT = (
    '<event version="2.0" uid="MESH-1" type="a-f-G-U-C" how="m-g" time="2026-01-01T00:00:00Z" '
    'start="2026-01-01T00:00:00Z" stale="2026-01-01T00:10:00Z"><point lat="48.1" lon="11.5" hae="0" '
    'ce="10" le="10"/><detail><contact callsign="NODE1"/></detail></event>'
)


def _make_cert(common_name, issuer_key=None, issuer_cert=None, is_ca=False):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer_cert.subject if issuer_cert else subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
    )
    if not is_ca:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
        )
    cert = builder.sign(issuer_key or key, hashes.SHA256())
    return key, cert


class _PKI:
    """CA + server cert + client cert, like a TAK Server installation."""

    def __init__(self, directory, ca_name="TAK-CA"):
        self.dir = directory
        self.ca_key, self.ca_cert = _make_cert(ca_name, is_ca=True)
        self.server_key, self.server_cert = _make_cert("localhost", self.ca_key, self.ca_cert)
        self.client_key, self.client_cert = _make_cert("gateway", self.ca_key, self.ca_cert)
        pem = serialization.Encoding.PEM
        self.ca_pem = self._write("ca.pem", self.ca_cert.public_bytes(pem))
        self.server_pem = self._write("server.pem", self.server_cert.public_bytes(pem))
        self.server_key_pem = self._write(
            "server.key",
            self.server_key.private_bytes(pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()),
        )
        enc = serialization.BestAvailableEncryption(b"atakatak")
        self.client_p12 = self._write(
            "gateway.p12", pkcs12.serialize_key_and_certificates(b"gateway", self.client_key, self.client_cert, [self.ca_cert], enc)
        )
        self.truststore_p12 = self._write(
            "truststore-root.p12", pkcs12.serialize_key_and_certificates(b"ca", None, self.ca_cert, None, enc)
        )

    def _write(self, name, data):
        path = os.path.join(self.dir, name)
        with open(path, "wb") as fh:
            fh.write(data)
        return path


class _FakeTakServer:
    """Minimal streaming TAK server: records what clients send, can push CoT to them."""

    def __init__(self, ssl_context=None):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(5)
        self.port = self.listener.getsockname()[1]
        self.ssl_context = ssl_context
        self.received = b""
        self.conn = None
        self.client_cert = None
        self.connected = threading.Event()
        self.stop = threading.Event()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.listener.accept()
            except OSError:
                return
            if self.ssl_context is None:
                break
            try:
                conn = self.ssl_context.wrap_socket(conn, server_side=True)
            except (ssl.SSLError, OSError):
                conn.close()
                continue  # e.g. client rejected our certificate and will retry
            self.client_cert = conn.getpeercert()
            break
        conn.settimeout(0.2)
        self.conn = conn
        self.connected.set()
        while not self.stop.is_set():
            try:
                data = conn.recv(65536)
            except (socket.timeout, ssl.SSLWantReadError):
                continue
            except OSError:
                return
            if not data:
                return
            self.received += data

    def push(self, text):
        self.conn.sendall(text.encode("utf-8"))

    def wait_for(self, needle, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if needle.encode("utf-8") in self.received:
                return True
            time.sleep(0.05)
        return False

    def close(self):
        self.stop.set()
        try:
            self.listener.close()
        except OSError:
            pass
        if self.conn is not None:
            try:
                self.conn.close()
            except OSError:
                pass


class TestEndpointParsing(unittest.TestCase):
    def test_variants(self):
        self.assertEqual(parse_tak_server_endpoint("tak.example.com", "", "SSL"), ("tak.example.com", 8089, "SSL"))
        self.assertEqual(parse_tak_server_endpoint("ssl://tak.example.com:8443"), ("tak.example.com", 8443, "SSL"))
        self.assertEqual(parse_tak_server_endpoint("tak.example.com:8089:ssl"), ("tak.example.com", 8089, "SSL"))
        self.assertEqual(parse_tak_server_endpoint("10.0.0.5:8088", 1, "TCP"), ("10.0.0.5", 8088, "TCP"))
        self.assertEqual(parse_tak_server_endpoint("tls://[2001:db8::1]:8089"), ("2001:db8::1", 8089, "SSL"))
        self.assertIsNone(parse_tak_server_endpoint("", 8087, "TCP"))
        with self.assertRaises(ValueError):
            parse_tak_server_endpoint("http://tak.example.com")

    def test_stream_splitting(self):
        events, rest = extract_cot_events('<?xml version="1.0"?>' + SAMPLE_EVENT + SAMPLE_EVENT[:40])
        self.assertEqual(events, [SAMPLE_EVENT])
        self.assertEqual(rest, SAMPLE_EVENT[:40])


class TestClientConnections(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.servers = []
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.stop(timeout=2)
        for server in self.servers:
            server.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _client(self, server, protocol="TCP", **kwargs):
        received = []
        client = TAKServerClient(
            "127.0.0.1" if protocol != "SSL" else "localhost",
            server.port,
            protocol,
            logger=_Logger(),
            on_event=received.append,
            sa_builder=lambda: '<event uid="GW-01" type="a-f-G-U-C"><point lat="0" lon="0"/><detail/></event>',
            reconnect_seconds=1,
            **kwargs,
        )
        self.clients.append(client)
        return client, received

    def test_tcp_sends_sa_first_then_queued_cot_and_receives_server_cot(self):
        server = _FakeTakServer()
        self.servers.append(server)
        client, received = self._client(server)
        client.send(SAMPLE_EVENT)  # queued before connect -> must not be lost
        client.start()
        self.assertTrue(server.wait_for('uid="MESH-1"', timeout=12))
        self.assertLess(server.received.index(b'uid="GW-01"'), server.received.index(b'uid="MESH-1"'))
        server.push('<?xml version="1.0"?>' + SAMPLE_EVENT.replace("MESH-1", "ATAK-7"))
        deadline = time.time() + 5
        while not received and time.time() < deadline:
            time.sleep(0.05)
        self.assertEqual(len(received), 1)
        self.assertIn('uid="ATAK-7"', received[0])
        self.assertEqual(client.get_status()["sent"], 1)

    def test_tls_with_p12_client_cert_and_truststore(self):
        pki = _PKI(self.tmp)
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(pki.server_pem, pki.server_key_pem)
        server_ctx.load_verify_locations(cafile=pki.ca_pem)
        server_ctx.verify_mode = ssl.CERT_REQUIRED  # like TAK Server port 8089
        server = _FakeTakServer(server_ctx)
        self.servers.append(server)
        client, _ = self._client(
            server,
            "SSL",
            ssl_context_factory=lambda: build_tak_ssl_context(
                client_cert=pki.client_p12,
                client_cert_password="atakatak",
                ca_cert=pki.truststore_p12,
                ca_cert_password="atakatak",
                check_hostname=True,
            ),
        )
        client.start()
        client.send(SAMPLE_EVENT)
        self.assertTrue(server.wait_for('uid="MESH-1"', timeout=12))
        subject = dict(x[0] for x in server.client_cert["subject"])
        self.assertEqual(subject["commonName"], "gateway")

    def test_server_requiring_client_cert_gives_clear_error_and_no_data_loss(self):
        pki = _PKI(self.tmp)
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(pki.server_pem, pki.server_key_pem)
        server_ctx.load_verify_locations(cafile=pki.ca_pem)
        server_ctx.verify_mode = ssl.CERT_REQUIRED
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(5)
        port = listener.getsockname()[1]
        stop = threading.Event()

        def serve():
            listener.settimeout(0.2)
            while not stop.is_set():
                try:
                    conn, _ = listener.accept()
                except (socket.timeout, OSError):
                    continue
                try:
                    tls = server_ctx.wrap_socket(conn, server_side=True)
                    tls.recv(10)
                except (ssl.SSLError, OSError):
                    pass
                finally:
                    conn.close()

        threading.Thread(target=serve, daemon=True).start()
        logger = _Logger()
        client = TAKServerClient(
            "localhost", port, "SSL", logger=logger, reconnect_seconds=1, has_client_certificate=False,
            ssl_context_factory=lambda: build_tak_ssl_context(ca_cert=pki.truststore_p12, ca_cert_password="atakatak"),
        )
        self.clients.append(client)
        client.send(SAMPLE_EVENT)
        client.start()
        try:
            deadline = time.time() + 10
            while "Client-Zertifikat" not in client.get_status()["last_error"] and time.time() < deadline:
                time.sleep(0.1)
            status = client.get_status()
            self.assertIn("kein", status["last_error"])
            self.assertFalse(status["connected"])
            self.assertEqual(status["queued"], 1, "queued CoT must be kept for a later, accepted session")
            self.assertFalse(any("✅" in line for line in logger.lines))
        finally:
            stop.set()
            listener.close()

    def test_public_ca_fallback_when_server_cert_not_in_truststore(self):
        pki = _PKI(self.tmp)
        other = _PKI(tempfile.mkdtemp(dir=self.tmp), ca_name="Other-CA")  # truststore of a different CA
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(pki.server_pem, pki.server_key_pem)
        server = _FakeTakServer(server_ctx)
        self.servers.append(server)
        # Simulate the OS trust store containing the server's CA (like Let's Encrypt on Windows).
        with unittest.mock.patch.dict(os.environ, {"SSL_CERT_FILE": pki.ca_pem}):
            client, _ = self._client(
                server, "SSL",
                ssl_context_factory=lambda: build_tak_ssl_context(ca_cert=other.truststore_p12),
                fallback_ssl_context_factory=lambda: build_tak_ssl_context(
                    ca_cert=other.truststore_p12, trust_system_cas=True),
            )
            client.start()
            client.send(SAMPLE_EVENT)
            self.assertTrue(server.wait_for('uid="MESH-1"', timeout=12))

    def test_stream_is_parseable_by_opentakserver(self):
        """Replicates OpenTAKServer's EudHandler.handle() split/parse loop on what the gateway sends."""
        import re
        from xml.etree.ElementTree import ParseError as XmlParseError, fromstring as xml_fromstring

        server = _FakeTakServer()
        self.servers.append(server)
        client, _ = self._client(server)
        with_declaration = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        for i in range(5):
            client.send((with_declaration + SAMPLE_EVENT.replace("MESH-1", f"MESH-{i}")).encode())
        client.start()
        self.assertTrue(server.wait_for('uid="MESH-4"', timeout=10))
        parsed = []
        cot = server.received.decode()
        for c in re.split("</event>|</auth>", cot):
            if "<event" in c:
                try:
                    xml_fromstring(c + "</event>")
                    parsed.append(c)
                except XmlParseError as exc:
                    self.fail(f"OpenTAKServer would drop this event: {exc}: {c[:60]!r}")
        self.assertEqual(len([c for c in parsed if 'uid="MESH-' in c]), 5)
        self.assertIn('uid="GW-01"', parsed[0])

    def test_reconnects_after_server_restart_and_keeps_queue(self):
        server = _FakeTakServer()
        self.servers.append(server)
        port = server.port
        client, _ = self._client(server)
        client.start()
        self.assertTrue(server.connected.wait(5))
        server.close()
        deadline = time.time() + 5
        while client.connected and time.time() < deadline:
            time.sleep(0.05)
        client.send(SAMPLE_EVENT)
        # Restart a server on the same port.
        restarted = None
        for _ in range(20):
            try:
                restarted = _FakeTakServer.__new__(_FakeTakServer)
                restarted.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                restarted.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                restarted.listener.bind(("127.0.0.1", port))
                break
            except OSError:
                time.sleep(0.1)
        restarted.listener.listen(5)
        restarted.port = port
        restarted.ssl_context = None
        restarted.received = b""
        restarted.conn = None
        restarted.connected = threading.Event()
        restarted.stop = threading.Event()
        threading.Thread(target=restarted._accept, daemon=True).start()
        self.servers.append(restarted)
        self.assertTrue(restarted.wait_for('uid="MESH-1"', timeout=10))


class TestDataPackage(unittest.TestCase):
    def test_reads_connect_string_and_certificates(self):
        tmp = tempfile.mkdtemp()
        try:
            pki = _PKI(tmp)
            pref = """<?xml version='1.0' standalone='yes'?>
<preferences>
  <preference version="1" name="cot_streams">
    <entry key="count" class="class java.lang.Integer">1</entry>
    <entry key="description0" class="class java.lang.String">Unser TAK</entry>
    <entry key="enabled0" class="class java.lang.Boolean">true</entry>
    <entry key="connectString0" class="class java.lang.String">tak.example.com:8089:ssl</entry>
  </preference>
  <preference version="1" name="com.atakmap.app_preferences">
    <entry key="caLocation" class="class java.lang.String">/storage/emulated/0/atak/cert/truststore-root.p12</entry>
    <entry key="caPassword" class="class java.lang.String">atakatak</entry>
    <entry key="clientPassword" class="class java.lang.String">atakatak</entry>
    <entry key="certificateLocation" class="class java.lang.String">/storage/emulated/0/atak/cert/gateway.p12</entry>
  </preference>
</preferences>"""
            zip_path = os.path.join(tmp, "server.zip")
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("certs/server.pref", pref)
                archive.write(pki.client_p12, "certs/gateway.p12")
                archive.write(pki.truststore_p12, "certs/truststore-root.p12")
                archive.writestr("MANIFEST/manifest.xml", "<MissionPackageManifest/>")
            package = load_tak_data_package(zip_path, os.path.join(tmp, "out"))
            self.assertEqual((package["host"], package["port"], package["protocol"]), ("tak.example.com", 8089, "SSL"))
            self.assertTrue(package["client_cert"].endswith("gateway.p12"))
            self.assertTrue(package["ca_cert"].endswith("truststore-root.p12"))
            self.assertEqual(package["client_cert_password"], "atakatak")
            # Context can be built from the extracted files.
            build_tak_ssl_context(
                client_cert=package["client_cert"],
                client_cert_password=package["client_cert_password"],
                ca_cert=package["ca_cert"],
                ca_cert_password=package["ca_cert_password"],
            )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class _FakeEnrollmentServer:
    """HTTPS endpoint behaving like OpenTAKServer's /Marti/api/tls/signClient/v2 (port 8446)."""

    def __init__(self, pki, username="alice", password="secret"):
        import base64 as b64
        import http.server
        import json

        outer = self
        self.requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _authorized(self):
                expected = "Basic " + b64.b64encode(f"{username}:{password}".encode()).decode()
                return self.headers.get("Authorization") == expected

            def do_GET(self):
                if not self._authorized():
                    self.send_response(401); self.end_headers(); return
                body = (b'<ns2:certificateConfig xmlns="http://bbn.com/marti/xml/config" xmlns:ns2="com.bbn.marti.config">'
                        b'<nameEntries><nameEntry name="O" value="TAK"/><nameEntry name="OU" value="Gateways"/>'
                        b'</nameEntries></ns2:certificateConfig>')
                self.send_response(200); self.end_headers(); self.wfile.write(body)

            def do_POST(self):
                if not self._authorized():
                    self.send_response(401); self.end_headers(); return
                data = self.rfile.read(int(self.headers["Content-Length"])).decode()
                outer.requests.append((self.path, self.headers.get("Content-Type"), data))
                # Same handling as OpenTAKServer: wrap the headerless base64 body into PEM.
                csr_pem = "-----BEGIN CERTIFICATE REQUEST-----\n" + data
                csr_pem += "" if csr_pem.endswith("\n") else "\n"
                csr_pem += "-----END CERTIFICATE REQUEST-----"
                csr = x509.load_pem_x509_csr(csr_pem.encode())
                now = datetime.datetime.now(datetime.timezone.utc)
                cert = (x509.CertificateBuilder().subject_name(csr.subject).issuer_name(pki.ca_cert.subject)
                        .public_key(csr.public_key()).serial_number(x509.random_serial_number())
                        .not_valid_before(now - datetime.timedelta(days=1))
                        .not_valid_after(now + datetime.timedelta(days=30)).sign(pki.ca_key, hashes.SHA256()))
                strip = lambda c: c.public_bytes(serialization.Encoding.PEM).decode().replace(
                    "-----BEGIN CERTIFICATE-----\n", "").replace("\n-----END CERTIFICATE-----\n", "")
                body = json.dumps({"signedCert": strip(cert), "ca0": strip(pki.ca_cert), "ca1": strip(pki.ca_cert)})
                self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
                self.wfile.write(body.encode())

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(pki.server_pem, pki.server_key_pem)
        self.httpd.socket = ctx.wrap_socket(self.httpd.socket, server_side=True)
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class TestCertificateEnrollment(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.pki = _PKI(self.tmp)
        self.server = _FakeEnrollmentServer(self.pki)

    def tearDown(self):
        self.server.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_truststore_is_detected(self):
        info = inspect_pkcs12_file(self.pki.truststore_p12)
        self.assertEqual(info["kind"], "truststore")
        self.assertEqual(info["password"], "atakatak")
        self.assertEqual(inspect_pkcs12_file(self.pki.client_p12)["kind"], "client")

    def test_enrollment_returns_usable_client_certificate(self):
        out = os.path.join(self.tmp, "out")
        result = enroll_tak_client_certificate(
            "localhost", "alice", "secret", out, port=self.server.port, client_uid="GW-01",
            ca_cert=self.pki.truststore_p12, ca_cert_password="atakatak",
        )
        path, content_type, body = self.server.requests[0]
        self.assertIn("/Marti/api/tls/signClient/v2?clientUid=GW-01", path)
        self.assertEqual(content_type, "text/plain")
        self.assertNotIn("BEGIN", body)
        self.assertIn("CN=alice", result["subject"])
        self.assertIn("O=TAK", result["subject"])
        self.assertEqual(inspect_pkcs12_file(result["client_cert"])["kind"], "client")

        # The enrolled certificate is accepted by a TAK server that requires client certs.
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(self.pki.server_pem, self.pki.server_key_pem)
        server_ctx.load_verify_locations(cafile=self.pki.ca_pem)
        server_ctx.verify_mode = ssl.CERT_REQUIRED
        tak = _FakeTakServer(server_ctx)
        client = TAKServerClient(
            "localhost", tak.port, "SSL", logger=_Logger(), reconnect_seconds=1,
            ssl_context_factory=lambda: build_tak_ssl_context(
                client_cert=result["client_cert"], client_cert_password=result["client_cert_password"],
                ca_cert=self.pki.truststore_p12, ca_cert_password="atakatak"),
        )
        try:
            client.start()
            client.send(SAMPLE_EVENT)
            self.assertTrue(tak.wait_for('uid="MESH-1"', timeout=12))
        finally:
            client.stop(timeout=2)
            tak.close()

    def test_enrollment_accepts_public_certificate_on_enrollment_port(self):
        other = _PKI(tempfile.mkdtemp(dir=self.tmp), ca_name="Other-CA")
        with unittest.mock.patch.dict(os.environ, {"SSL_CERT_FILE": self.pki.ca_pem}):
            result = enroll_tak_client_certificate(
                "localhost", "alice", "secret", os.path.join(self.tmp, "o2"), port=self.server.port,
                ca_cert=other.truststore_p12, ca_cert_password="atakatak",
            )
        self.assertIn("CN=alice", result["subject"])

    def test_wrong_port_requiring_client_cert_is_explained(self):
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(self.pki.server_pem, self.pki.server_key_pem)
        server_ctx.load_verify_locations(cafile=self.pki.ca_pem)
        server_ctx.verify_mode = ssl.CERT_REQUIRED
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]

        def serve():
            conn, _ = listener.accept()
            try:
                server_ctx.wrap_socket(conn, server_side=True).recv(10)
            except (ssl.SSLError, OSError):
                pass
            conn.close()

        threading.Thread(target=serve, daemon=True).start()
        try:
            with self.assertRaisesRegex(ValueError, "8446"):
                enroll_tak_client_certificate(
                    "localhost", "alice", "secret", os.path.join(self.tmp, "o3"), port=port,
                    ca_cert=self.pki.truststore_p12,
                )
        finally:
            listener.close()

    def test_wrong_password_is_reported(self):
        with self.assertRaisesRegex(ValueError, "401"):
            enroll_tak_client_certificate(
                "localhost", "alice", "wrong", os.path.join(self.tmp, "o"), port=self.server.port,
                ca_cert=self.pki.truststore_p12,
            )

    def test_untrusted_server_is_rejected(self):
        other = _PKI(tempfile.mkdtemp(dir=self.tmp), ca_name="Other-CA")
        with self.assertRaisesRegex(ValueError, "Truststore"):
            enroll_tak_client_certificate(
                "localhost", "alice", "secret", os.path.join(self.tmp, "o"), port=self.server.port,
                ca_cert=other.truststore_p12,
            )


class _RecordingClient:
    endpoint = "ssl://tak:8089"
    connected = True

    def __init__(self):
        self.sent = []

    def send(self, packet):
        self.sent.append(packet)


def _make_gateway(mode="chat_markers"):
    gw = object.__new__(TAKMeshtasticGateway)
    gw.logger = _Logger()
    gw.gateway_uid = "GW-01"
    gw.tak_server_client = _RecordingClient()
    gw.tak_server_to_mesh = mode
    gw.tak_server_to_local_tak = True
    gw.local_tak_to_server = True
    gw._tak_server_pli_forward_times = {}
    gw.inbound = []
    gw.local = []
    gw.handle_inbound_tak_packet = lambda packet, **kw: gw.inbound.append((packet, kw))
    gw._send_packet_to_tak = lambda packet, label, include_remote=True: gw.local.append((packet, include_remote))
    return gw


MARKER_EVENT = SAMPLE_EVENT.replace('type="a-f-G-U-C"', 'type="a-h-G"').replace('how="m-g"', 'how="h-g-i-g-o"')


class TestGatewayRouting(unittest.TestCase):
    def test_server_pli_not_flooded_into_mesh_by_default(self):
        gw = _make_gateway()
        gw._handle_tak_server_event(SAMPLE_EVENT)
        self.assertEqual(gw.inbound, [])
        self.assertEqual(len(gw.local), 1)
        self.assertFalse(gw.local[0][1], "server CoT must not be sent back to the server")

    def test_server_marker_goes_to_mesh_tagged_as_server_source(self):
        gw = _make_gateway()
        gw._handle_tak_server_event(MARKER_EVENT)
        self.assertEqual(len(gw.inbound), 1)
        self.assertEqual(gw.inbound[0][1]["source_protocol"], main_app.TAK_SERVER_SOURCE_PROTOCOL)

    def test_all_mode_throttles_pli(self):
        gw = _make_gateway("all")
        gw._handle_tak_server_event(SAMPLE_EVENT)
        gw._handle_tak_server_event(SAMPLE_EVENT)
        self.assertEqual(len(gw.inbound), 1)

    def test_pings_and_own_sa_are_ignored(self):
        gw = _make_gateway("all")
        ping = SAMPLE_EVENT.replace('type="a-f-G-U-C"', 'type="t-x-c-t-r"')
        own = SAMPLE_EVENT.replace("MESH-1", "GW-01")
        gw._handle_tak_server_event(ping)
        gw._handle_tak_server_event(own)
        self.assertEqual((gw.inbound, gw.local), ([], []))

    def test_mesh_cot_is_queued_for_remote_server(self):
        gw = object.__new__(TAKMeshtasticGateway)
        gw.logger = _Logger()
        gw.tak_server_client = _RecordingClient()
        self.assertTrue(gw._send_packet_to_remote_tak(SAMPLE_EVENT.encode(), "NODE1"))
        self.assertEqual(len(gw.tak_server_client.sent), 1)

    def test_gateway_sa_contains_callsign_and_group(self):
        gw = object.__new__(TAKMeshtasticGateway)
        gw.cfg = {}
        gw.gateway_uid = "GW-01"
        gw.gateway_callsign = "MSHT-GW"
        gw.park_coords = (48.1, 11.5)
        sa = gw._build_gateway_sa_xml().decode()
        self.assertIn('uid="GW-01"', sa)
        self.assertIn('callsign="MSHT-GW"', sa)
        self.assertIn("__group", sa)
        self.assertIn('lat="48.100000"', sa)


class TestSettings(unittest.TestCase):
    def test_hostname_url_and_ssl_are_accepted_and_passwords_not_exposed(self):
        cfg = {"tak_server_client_cert_password": "secret"}
        _apply_settings_payload_to_cfg(
            cfg,
            {"tak_server_host": "ssl://tak.example.com:8089", "tak_server_port": "", "tak_server_protocol": "SSL",
             "tak_server_client_cert_password": ""},
            ["COM7"],
        )
        self.assertEqual(cfg["tak_server_protocol"], "SSL")
        self.assertEqual(cfg["tak_server_port"], 8089)
        self.assertEqual(cfg["tak_server_client_cert_password"], "secret")
        self.assertEqual(_build_browser_ui_form_state(cfg)["tak_server_client_cert_password"], "")
        self.assertEqual(main_app.resolve_remote_tak_endpoint(cfg), ("tak.example.com", 8089, "SSL"))

    def test_uploaded_project_truststore_is_used_as_ca(self):
        tmp = tempfile.mkdtemp()
        try:
            with open("tak_certs/tak.persistentsystems.eu/truststore-root.p12", "rb") as fh:
                content = fh.read()
            cfg = {"tak_server_host": "tak.persistentsystems.eu"}
            with unittest.mock.patch.object(main_app, "_config_base_dir", return_value=tmp):
                info = main_app.store_tak_certificate_into_cfg(cfg, "truststore_root (2).p12", content)
                self.assertEqual(info["kind"], "truststore")
                self.assertEqual(cfg["tak_server_ca_cert"], os.path.join("tak_certs", "tak.persistentsystems.eu", "truststore-root.p12"))
                self.assertEqual(cfg["tak_server_protocol"], "SSL")
                context = build_tak_ssl_context(
                    ca_cert=main_app._resolve_config_path(cfg["tak_server_ca_cert"]),
                    ca_cert_password=cfg["tak_server_ca_cert_password"],
                )
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertIn("OpenTAKServer-CA", str(context.get_ca_certs()))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_invalid_host_is_reported(self):
        with self.assertRaises(ValueError):
            _apply_settings_payload_to_cfg({}, {"tak_server_host": "http://x", "tak_server_port": "8087"}, ["COM7"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
