"""
Remote TAK Server client for the WinTAK Meshtastic Gateway.

Implements the connection behaviour of ATAK/WinTAK "TAK Server" streams:

* Streaming CoT XML over TCP (``tcp``), TLS (``ssl``/``tls``, port 8089 with client
  certificates) or fire-and-forget UDP.
* Hostnames, IPv4/IPv6 and ``ssl://host:port`` style URLs as server address.
* Client certificates as PKCS#12 (``.p12`` from a TAK data package) or PEM files,
  CA/truststore as ``.p12`` or PEM.
* Import of ATAK/WinTAK server data packages (``.zip`` with ``.pref`` + certificates).
* The connection is read continuously (the server distributes other clients' CoT to us;
  a client that never reads is dropped by TAK Server as a slow consumer), keepalive
  pings are sent, the gateway announces itself with its own SA on connect and
  periodically, and the link is re-established automatically.
* Outgoing CoT is queued, so packets produced while the server is unreachable are sent
  after reconnecting instead of being lost.
"""

import collections
import datetime
import ipaddress
import os
import re
import select
import shutil
import socket
import ssl
import tempfile
import threading
import time
import zipfile
from xml.etree.ElementTree import ParseError, fromstring

DEFAULT_TAK_SERVER_PORTS = {"TCP": 8087, "UDP": 8087, "SSL": 8089}
TAK_SERVER_PROTOCOLS = ("TCP", "UDP", "SSL")
_PROTOCOL_ALIASES = {
    "TCP": "TCP",
    "STCP": "TCP",
    "UDP": "UDP",
    "SSL": "SSL",
    "TLS": "SSL",
    "TCPS": "SSL",
}
DEFAULT_TAK_CERT_PASSWORD = "atakatak"
DEFAULT_SEND_QUEUE_SIZE = 2000
DEFAULT_KEEPALIVE_SECONDS = 30
DEFAULT_SA_INTERVAL_SECONDS = 60
DEFAULT_RECONNECT_SECONDS = 10
CONFIRM_CONNECTION_SECONDS = 5
DEAD_CONNECTION_FACTOR = 3  # reconnect after this many keepalive periods without any server data
_PONG_RE = re.compile(r"""\btype=["']t-x-c-t-r["']""")
MAX_RECONNECT_SECONDS = 60
CONNECT_TIMEOUT_SECONDS = 15
SOCKET_IO_TIMEOUT_SECONDS = 1.0
SEND_TIMEOUT_SECONDS = 15
MAX_STREAM_BUFFER_CHARS = 4 * 1024 * 1024
_EVENT_END_RE = re.compile(r"</event\s*>|<event\b[^>]*/>", re.IGNORECASE)
_EVENT_START_RE = re.compile(r"<event\b", re.IGNORECASE)


def normalize_tak_server_protocol(value, default="TCP"):
    text = str(value or "").strip().upper()
    return _PROTOCOL_ALIASES.get(text, default)


def _split_host_port(text):
    """Split ``host``, ``host:port``, ``[v6]:port`` or a bare IPv6 address."""
    text = text.strip().strip("/")
    if text.startswith("["):
        closing = text.find("]")
        if closing < 0:
            raise ValueError(f"Ungültige IPv6-Adresse: {text}")
        host = text[1:closing]
        rest = text[closing + 1:]
        if rest.startswith(":") and rest[1:]:
            return host, rest[1:]
        return host, None
    if text.count(":") == 1:
        host, port = text.split(":", 1)
        return host, port or None
    return text, None


def parse_tak_server_endpoint(host_text, port=None, protocol=None):
    """Resolve the configured remote TAK server into ``(host, port, protocol)``.

    Accepts plain hostnames/IPs as well as ATAK style strings:
    ``tak.example.com``, ``10.0.0.5:8089``, ``ssl://tak.example.com:8089``,
    ``tak.example.com:8089:ssl`` (ATAK connectString) or ``[2001:db8::1]:8087``.
    A protocol/port inside the host text wins over the separate fields.
    """
    text = str(host_text or "").strip()
    if not text:
        return None
    url_protocol = None
    match = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://(.*)$", text)
    if match:
        url_protocol = normalize_tak_server_protocol(match.group(1), default=None)
        if url_protocol is None:
            raise ValueError(f"Unbekanntes TAK-Server-Protokoll in '{text}' (erlaubt: tcp, udp, ssl/tls).")
        rest = match.group(2)
        if rest.startswith("["):
            closing = rest.find("]")
            text = rest if closing < 0 else rest[:closing + 1] + rest[closing + 1:].split("/", 1)[0]
        else:
            text = rest.split("/", 1)[0]
    else:
        # ATAK connectString: host:port:proto
        parts = text.rsplit(":", 2)
        if len(parts) == 3 and parts[1].isdigit() and normalize_tak_server_protocol(parts[2], None):
            if not (":" in parts[0] and not parts[0].startswith("[")):
                url_protocol = normalize_tak_server_protocol(parts[2])
                text = f"{parts[0]}:{parts[1]}"
    try:
        ipaddress.ip_address(text)
        host, text_port = text, None  # bare IPv6 / IPv4
    except ValueError:
        host, text_port = _split_host_port(text)
    if not host:
        raise ValueError("TAK-Server-Adresse enthält keinen Host.")
    resolved_protocol = url_protocol or normalize_tak_server_protocol(protocol)
    resolved_port = text_port if text_port is not None else port
    if resolved_port in (None, ""):
        resolved_port = DEFAULT_TAK_SERVER_PORTS[resolved_protocol]
    try:
        resolved_port = int(resolved_port)
    except (TypeError, ValueError):
        raise ValueError(f"Ungültiger TAK-Server-Port: {resolved_port}")
    if not 1 <= resolved_port <= 65535:
        raise ValueError(f"TAK-Server-Port außerhalb 1-65535: {resolved_port}")
    return host, resolved_port, resolved_protocol


def format_tak_server_endpoint(host, port, protocol):
    shown_host = f"[{host}]" if ":" in str(host) else host
    return f"{str(protocol).lower()}://{shown_host}:{port}"


# ---------------------------------------------------------------------------
# Certificates
# ---------------------------------------------------------------------------

def _load_pkcs12(path, password):
    key, cert, additional, _used = _load_pkcs12_with_password(path, password)
    return key, cert, additional


def _load_pkcs12_with_password(path, password):
    """Open a .p12, trying the given password, then the TAK default "atakatak", then none."""
    try:
        from cryptography.hazmat.primitives.serialization import pkcs12
    except Exception as exc:  # pragma: no cover - depends on installation
        raise RuntimeError(
            "Für .p12-Zertifikate wird das Python-Paket 'cryptography' benötigt (pip install cryptography)."
        ) from exc
    with open(path, "rb") as fh:
        data = fh.read()
    candidates = []
    for candidate in (password, DEFAULT_TAK_CERT_PASSWORD, ""):
        if candidate is None or candidate in candidates:
            continue
        candidates.append(candidate)
    last_error = None
    for candidate in candidates:
        try:
            key, cert, additional = pkcs12.load_key_and_certificates(
                data, candidate.encode("utf-8") if candidate else None
            )
            return key, cert, additional, candidate
        except ValueError as exc:
            last_error = exc
    raise ValueError(
        f"Zertifikat {os.path.basename(path)} konnte nicht geöffnet werden (falsches Passwort?): {last_error}"
    )


def _cert_to_pem(cert):
    from cryptography.hazmat.primitives.serialization import Encoding

    return cert.public_bytes(Encoding.PEM)


def _key_to_pem(key):
    from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

    return key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())


def _is_pkcs12_path(path):
    return str(path or "").lower().endswith((".p12", ".pfx"))


def build_tak_ssl_context(
    client_cert=None,
    client_cert_password=None,
    client_key=None,
    ca_cert=None,
    ca_cert_password=None,
    verify=None,
    check_hostname=False,
    trust_system_cas=False,
):
    """Create an SSL context the way ATAK/WinTAK connect to TAK Server.

    ``verify=None`` means: verify the server against the given CA/truststore, and accept
    any server certificate when no CA was configured (common for self-signed TAK servers).
    ``trust_system_cas=True`` additionally accepts publicly trusted server certificates
    (e.g. Let's Encrypt in front of OpenTAKServer); the host name is then always checked.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    if verify is None:
        verify = bool(ca_cert)
    if verify:
        context.check_hostname = bool(check_hostname)
        context.verify_mode = ssl.CERT_REQUIRED
        if ca_cert:
            if _is_pkcs12_path(ca_cert):
                _key, cert, additional = _load_pkcs12(ca_cert, ca_cert_password)
                certs = [c for c in [cert] + list(additional or []) if c is not None]
                if not certs:
                    raise ValueError(f"Truststore {os.path.basename(ca_cert)} enthält keine Zertifikate.")
                context.load_verify_locations(cadata="".join(_cert_to_pem(c).decode("ascii") for c in certs))
            else:
                context.load_verify_locations(cafile=ca_cert)
            if trust_system_cas:
                context.load_default_certs()
                context.check_hostname = True
        else:
            context.load_default_certs()
    else:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    if client_cert:
        if _is_pkcs12_path(client_cert):
            key, cert, additional = _load_pkcs12(client_cert, client_cert_password)
            if key is None or cert is None:
                raise ValueError(
                    f"Client-Zertifikat {os.path.basename(client_cert)} enthält keinen privaten Schlüssel."
                )
            temp_dir = tempfile.mkdtemp(prefix="takgw-")
            try:
                cert_path = os.path.join(temp_dir, "client.pem")
                key_path = os.path.join(temp_dir, "client.key")
                fd = os.open(cert_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "wb") as fh:
                    fh.write(_cert_to_pem(cert))
                    for extra in additional or []:
                        fh.write(_cert_to_pem(extra))
                fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "wb") as fh:
                    fh.write(_key_to_pem(key))
                context.load_cert_chain(cert_path, key_path)
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            context.load_cert_chain(client_cert, client_key or None, password=client_cert_password or None)
    return context


def describe_server_certificate(host, port, timeout=10):
    """Who issued the server's certificate? Used to explain verification failures."""
    try:
        from cryptography import x509

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        with socket.create_connection((host, int(port)), timeout=timeout) as raw:
            with context.wrap_socket(raw, server_hostname=host) as tls:
                der = tls.getpeercert(binary_form=True)
        cert = x509.load_der_x509_certificate(der)
        return f"Serverzertifikat: {cert.subject.rfc4514_string()}, ausgestellt von {cert.issuer.rfc4514_string()}"
    except Exception as exc:
        return f"Serverzertifikat konnte nicht gelesen werden ({exc})"


def is_client_certificate_rejection(exc):
    """True when the server closed the TLS session because no/an unaccepted client certificate was sent."""
    text = str(exc).upper()
    return any(marker in text for marker in (
        "CERTIFICATE_REQUIRED", "BAD_CERTIFICATE", "UNKNOWN_CA", "CERTIFICATE_UNKNOWN",
        "CERTIFICATE_EXPIRED", "CERTIFICATE_REVOKED", "HANDSHAKE_FAILURE",
    ))


# ---------------------------------------------------------------------------
# Data packages
# ---------------------------------------------------------------------------

def _pref_entries(pref_xml):
    try:
        root = fromstring(pref_xml)
    except ParseError as exc:
        raise ValueError(f"Ungültige .pref-Datei im Datenpaket: {exc}")
    entries = {}
    for entry in root.iter():
        if entry.tag.split("}")[-1] != "entry":
            continue
        key = entry.get("key")
        if key:
            entries[key] = (entry.text or "").strip()
    return entries


def load_tak_data_package(zip_path, extract_dir):
    """Read an ATAK/WinTAK server connection data package.

    Extracts the certificates into ``extract_dir`` and returns a dict with
    ``host``, ``port``, ``protocol``, ``client_cert``, ``client_cert_password``,
    ``ca_cert``, ``ca_cert_password`` and ``description``.
    """
    with zipfile.ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        pref_names = [name for name in members if name.lower().endswith(".pref")]
        if not pref_names:
            raise ValueError("Datenpaket enthält keine .pref-Datei mit Server-Verbindung.")
        entries = {}
        for name in pref_names:
            entries.update(_pref_entries(archive.read(name)))

        connect_string = entries.get("connectString0", "")
        if not connect_string:
            raise ValueError("Datenpaket enthält keinen Server (connectString0 fehlt).")
        host, port, protocol = parse_tak_server_endpoint(connect_string)

        os.makedirs(extract_dir, exist_ok=True)
        cert_members = {os.path.basename(name).lower(): name for name in members if _is_pkcs12_path(name)}

        def extract(location):
            if not location:
                return None
            member = cert_members.get(os.path.basename(location.replace("\\", "/")).lower())
            if member is None:
                return None
            target = os.path.join(extract_dir, os.path.basename(member))
            with archive.open(member) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            return target

        ca_location = entries.get("caLocation0") or entries.get("caLocation")
        client_location = entries.get("certificateLocation0") or entries.get("certificateLocation")
        ca_cert = extract(ca_location)
        client_cert = extract(client_location)
        # Some packages only reference the files implicitly; fall back to the file names.
        if ca_cert is None or client_cert is None:
            for lower_name in cert_members:
                looks_like_ca = "trust" in lower_name or lower_name.startswith("ca")
                if ca_cert is None and looks_like_ca:
                    ca_cert = extract(lower_name)
                elif client_cert is None and not looks_like_ca:
                    client_cert = extract(lower_name)

    enroll = str(entries.get("enrollForCertificateWithTrust0", "")).lower() == "true"
    if protocol == "SSL" and client_cert is None and not enroll:
        raise ValueError("Datenpaket enthält kein Client-Zertifikat (.p12) für die SSL-Verbindung.")
    return {
        "host": host,
        "port": port,
        "protocol": protocol,
        "description": entries.get("description0", ""),
        "client_cert": client_cert,
        "client_cert_password": entries.get("clientPassword0") or entries.get("clientPassword") or DEFAULT_TAK_CERT_PASSWORD,
        "ca_cert": ca_cert,
        "ca_cert_password": entries.get("caPassword0") or entries.get("caPassword") or DEFAULT_TAK_CERT_PASSWORD,
        "requires_enrollment": enroll and client_cert is None,
    }


# ---------------------------------------------------------------------------
# Single certificate files / certificate enrollment
# ---------------------------------------------------------------------------

DEFAULT_TAK_ENROLLMENT_PORT = 8446


class _EnrollmentHTTPError(ValueError):
    """HTTP error status from the enrollment API (the connection itself worked)."""


def inspect_pkcs12_file(path, password=None):
    """Classify a .p12: ``{"kind": "client"|"truststore", "subjects": [...], "password": used}``.

    A file with a private key is a client certificate, one with certificates only is a
    truststore (CA) as handed out by TAK Server / OpenTAKServer.
    """
    key, cert, additional, used_password = _load_pkcs12_with_password(path, password)
    certs = [c for c in [cert] + list(additional or []) if c is not None]
    if not certs:
        raise ValueError(f"{os.path.basename(path)} enthält keine Zertifikate.")
    return {
        "kind": "client" if key is not None else "truststore",
        "subjects": [c.subject.rfc4514_string() for c in certs],
        "password": used_password or "",
    }


def _pem_body_to_cert(text):
    from cryptography import x509

    text = str(text or "").strip()
    if not text:
        return None
    if "BEGIN CERTIFICATE" not in text:
        text = "-----BEGIN CERTIFICATE-----\n" + text + "\n-----END CERTIFICATE-----\n"
    return x509.load_pem_x509_certificate(text.encode("ascii"))


def _parse_enrollment_response(body):
    """Signed cert + CA certs from a TAK Server/OpenTAKServer signClient/v2 response (JSON or XML)."""
    import json

    text = body.decode("utf-8", errors="replace").strip()
    signed, cas = None, []
    if text.startswith("{"):
        data = json.loads(text)
        signed = data.get("signedCert")
        cas = [value for key, value in sorted(data.items()) if key.startswith("ca") and value]
    else:
        root = fromstring(text)
        for element in root.iter():
            name = element.tag.split("}")[-1]
            if name == "signedCert":
                signed = element.text
            elif name.startswith("ca") and element.text:
                cas.append(element.text)
    if not signed:
        raise ValueError("Antwort des Servers enthält kein signiertes Zertifikat.")
    ca_certs = []
    seen = set()
    for value in cas:
        cert = _pem_body_to_cert(value)
        if cert is None:
            continue
        fingerprint = cert.fingerprint(_sha256())
        if fingerprint not in seen:
            seen.add(fingerprint)
            ca_certs.append(cert)
    return _pem_body_to_cert(signed), ca_certs


def _sha256():
    from cryptography.hazmat.primitives import hashes

    return hashes.SHA256()


def enroll_tak_client_certificate(
    host,
    username,
    password,
    out_dir,
    *,
    port=DEFAULT_TAK_ENROLLMENT_PORT,
    client_uid="GW-01",
    ca_cert=None,
    ca_cert_password=None,
    verify=None,
    cert_password=DEFAULT_TAK_CERT_PASSWORD,
    timeout=30,
    opener=None,
):
    """Request a client certificate with username/password, like ATAK's "enroll for certificate".

    Uses the TAK Server / OpenTAKServer API on the enrollment port (default 8446):
    ``GET /Marti/api/tls/config`` for the certificate name entries, then
    ``POST /Marti/api/tls/signClient/v2`` with a CSR. The truststore (``ca_cert``) is used to
    verify the server. Returns ``{"client_cert": path, "ca_cert": path or None, ...}``; the client
    certificate is stored as ``<username>.p12`` protected with ``cert_password``.
    """
    import base64
    import urllib.error
    import urllib.parse
    import urllib.request

    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    username = str(username or "").strip()
    if not username or not password:
        raise ValueError("Für die Zertifikatsanforderung werden Benutzername und Passwort benötigt.")
    context = build_tak_ssl_context(ca_cert=ca_cert, ca_cert_password=ca_cert_password, verify=verify)
    shown_host = f"[{host}]" if ":" in str(host) else host
    base_url = f"https://{shown_host}:{int(port)}"
    auth = "Basic " + base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    openers = [opener] if opener is not None else [urllib.request.build_opener(urllib.request.HTTPSHandler(context=context))]
    if opener is None and ca_cert and verify is not False:
        # The enrollment port is often served by nginx with a public certificate (Let's Encrypt)
        # instead of one from the TAK CA: then accept publicly trusted certificates for this host.
        system_context = build_tak_ssl_context(
            ca_cert=ca_cert, ca_cert_password=ca_cert_password, verify=True, trust_system_cas=True
        )
        openers.append(urllib.request.build_opener(urllib.request.HTTPSHandler(context=system_context)))

    def request(path, data=None, headers=None):
        while True:
            req = urllib.request.Request(base_url + path, data=data, method="POST" if data is not None else "GET")
            req.add_header("Authorization", auth)
            for key, value in (headers or {}).items():
                req.add_header(key, value)
            try:
                with openers[0].open(req, timeout=timeout) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                if exc.code == 401:
                    raise ValueError("Server lehnt Benutzername/Passwort ab (HTTP 401).")
                raise _EnrollmentHTTPError(f"Server antwortet mit HTTP {exc.code} auf {path}.")
            except (urllib.error.URLError, ssl.SSLError, ConnectionError) as exc:
                reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
                if isinstance(reason, ssl.SSLCertVerificationError):
                    if len(openers) > 1:
                        openers.pop(0)
                        continue
                    raise ValueError(
                        f"Serverzertifikat von {base_url} ist weder vom Truststore noch von einer öffentlichen "
                        f"Zertifizierungsstelle für {host} ausgestellt ({reason.verify_message}). "
                        f"{describe_server_certificate(host, port)}. Den passenden Truststore verwenden oder "
                        "\"Serverzertifikat prüfen\" auf \"Nicht prüfen\" stellen."
                    )
                if is_client_certificate_rejection(reason) or isinstance(reason, ConnectionError):
                    raise ValueError(
                        f"Port {port} verlangt bereits ein Client-Zertifikat – das ist nicht der Port für die "
                        f"Zertifikatsanforderung. Bei OpenTAKServer/TAK Server ist das Port "
                        f"{DEFAULT_TAK_ENROLLMENT_PORT} (Feld leer lassen)."
                    )
                raise ValueError(f"{base_url} nicht erreichbar: {reason}")

    name_entries = []
    try:
        config_root = fromstring(request("/Marti/api/tls/config"))
        for element in config_root.iter():
            if element.tag.split("}")[-1] == "nameEntry" and element.get("name") and element.get("value"):
                name_entries.append((element.get("name").upper(), element.get("value")))
    except (_EnrollmentHTTPError, ParseError):
        # Optional endpoint; enrollment also works without the extra name entries.
        name_entries = []

    oid_by_name = {
        "O": NameOID.ORGANIZATION_NAME,
        "OU": NameOID.ORGANIZATIONAL_UNIT_NAME,
        "C": NameOID.COUNTRY_NAME,
        "ST": NameOID.STATE_OR_PROVINCE_NAME,
        "L": NameOID.LOCALITY_NAME,
    }
    attributes = [x509.NameAttribute(NameOID.COMMON_NAME, username)]
    for name, value in name_entries:
        if name in oid_by_name:
            attributes.append(x509.NameAttribute(oid_by_name[name], value))
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    csr = x509.CertificateSigningRequestBuilder().subject_name(x509.Name(attributes)).sign(key, _sha256())
    csr_b64 = base64.b64encode(csr.public_bytes(serialization.Encoding.DER)).decode("ascii")
    # Base64 body without PEM headers, 64 chars per line, trailing newline (as ATAK sends it).
    csr_body = "\n".join(csr_b64[i:i + 64] for i in range(0, len(csr_b64), 64)) + "\n"
    query = urllib.parse.urlencode({"clientUid": client_uid, "version": "WinTAK-Meshtastic-Gateway"})
    response = request(
        f"/Marti/api/tls/signClient/v2?{query}",
        data=csr_body.encode("ascii"),
        headers={"Content-Type": "text/plain", "Accept": "application/json"},
    )
    signed_cert, ca_certs = _parse_enrollment_response(response)

    os.makedirs(out_dir, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", username) or "gateway"
    client_path = os.path.join(out_dir, f"{safe_name}.p12")
    encryption = serialization.BestAvailableEncryption(cert_password.encode("utf-8"))
    with open(client_path, "wb") as fh:
        fh.write(pkcs12.serialize_key_and_certificates(
            safe_name.encode("utf-8"), key, signed_cert, ca_certs or None, encryption
        ))
    truststore_path = None
    if not ca_cert and ca_certs:
        truststore_path = os.path.join(out_dir, "truststore-enrolled.p12")
        with open(truststore_path, "wb") as fh:
            fh.write(pkcs12.serialize_key_and_certificates(b"ca", None, ca_certs[0], ca_certs[1:] or None, encryption))
    return {
        "client_cert": client_path,
        "client_cert_password": cert_password,
        "ca_cert": truststore_path,
        "subject": signed_cert.subject.rfc4514_string(),
        "valid_until": signed_cert.not_valid_after_utc.isoformat()
        if hasattr(signed_cert, "not_valid_after_utc") else str(signed_cert.not_valid_after),
    }


# ---------------------------------------------------------------------------
# CoT stream helpers
# ---------------------------------------------------------------------------

def extract_cot_events(buffer_text):
    """Split a streaming CoT buffer into complete ``<event>`` documents.

    Returns ``(events, remaining_buffer)``.
    """
    events = []
    while True:
        start = _EVENT_START_RE.search(buffer_text)
        if start is None:
            # Keep a possible partial "<eve" tail only.
            return events, buffer_text[-16:] if len(buffer_text) > 16 else buffer_text
        end = _EVENT_END_RE.search(buffer_text, start.start())
        if end is None:
            remaining = buffer_text[start.start():]
            if len(remaining) > MAX_STREAM_BUFFER_CHARS:
                remaining = ""
            return events, remaining
        events.append(buffer_text[start.start():end.end()])
        buffer_text = buffer_text[end.end():]


_XML_DECLARATION_RE = re.compile(rb"^\s*<\?xml[^>]*\?>\s*", re.IGNORECASE)


def prepare_cot_for_stream(payload):
    """Bytes of one CoT event as TAK servers expect them in a stream.

    No XML declaration and no separator between events: OpenTAKServer splits the stream at
    ``</event>`` and parses each piece; a piece starting with whitespace followed by
    ``<?xml ...?>`` is a parse error there and the event (plus the rest of the read) is dropped.
    """
    data = payload if isinstance(payload, bytes) else str(payload).encode("utf-8")
    data = data.lstrip(b"\xef\xbb\xbf")
    data = _XML_DECLARATION_RE.sub(b"", data, count=1)
    return data.strip()


def _cot_time(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def build_tak_ping_xml(uid):
    now = datetime.datetime.now(datetime.timezone.utc)
    stale = now + datetime.timedelta(seconds=20)
    return (
        f'<event version="2.0" uid="{uid}-ping" type="t-x-c-t" how="h-g-i-g-o" '
        f'time="{_cot_time(now)}" start="{_cot_time(now)}" stale="{_cot_time(stale)}">'
        '<point lat="0.0" lon="0.0" hae="0.0" ce="9999999.0" le="9999999.0"/><detail/></event>'
    )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class TAKServerClient:
    """Persistent, bidirectional connection to a remote TAK Server."""

    def __init__(
        self,
        host,
        port,
        protocol="TCP",
        *,
        logger,
        ssl_context_factory=None,
        fallback_ssl_context_factory=None,
        has_client_certificate=True,
        on_event=None,
        sa_builder=None,
        sa_interval_seconds=DEFAULT_SA_INTERVAL_SECONDS,
        keepalive_seconds=DEFAULT_KEEPALIVE_SECONDS,
        reconnect_seconds=DEFAULT_RECONNECT_SECONDS,
        queue_size=DEFAULT_SEND_QUEUE_SIZE,
        uid="GW-01",
        socket_factory=None,
    ):
        self.host = host
        self.port = int(port)
        self.protocol = normalize_tak_server_protocol(protocol)
        self.logger = logger
        self.endpoint = format_tak_server_endpoint(host, self.port, self.protocol)
        self._ssl_context_factory = ssl_context_factory
        self._fallback_ssl_context_factory = fallback_ssl_context_factory
        self._use_fallback_ssl = False
        self.has_client_certificate = has_client_certificate
        self._on_event = on_event
        self._sa_builder = sa_builder
        self.sa_interval_seconds = max(0, int(sa_interval_seconds or 0))
        self.keepalive_seconds = max(5, int(keepalive_seconds or DEFAULT_KEEPALIVE_SECONDS))
        self.reconnect_seconds = max(1, int(reconnect_seconds or DEFAULT_RECONNECT_SECONDS))
        self.uid = uid
        self._socket_factory = socket_factory or socket.create_connection
        self._queue = collections.deque(maxlen=max(1, int(queue_size)))
        self._queue_cv = threading.Condition()
        self._stop = threading.Event()
        self._sock = None
        self._sock_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._thread = None
        self._connected = threading.Event()
        self._last_rx = time.monotonic()
        self._server_answers_pings = False
        self._confirmed = False
        self._dropped = 0
        self.status = {
            "endpoint": self.endpoint,
            "connected": False,
            "last_error": "",
            "sent": 0,
            "received": 0,
            "queued": 0,
            "connected_since": None,
        }

    # -- public API -------------------------------------------------------

    @property
    def connected(self):
        return self._connected.is_set()

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="tak-server-client")
        self._thread.start()

    def stop(self, timeout=5):
        self._stop.set()
        with self._queue_cv:
            self._queue_cv.notify_all()
        self._close_socket()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def send(self, packet_xml):
        """Queue a CoT event for the server. Returns False if it had to drop the oldest entry."""
        payload = packet_xml if isinstance(packet_xml, bytes) else str(packet_xml).encode("utf-8")
        payload = payload.strip()
        if not payload:
            return True
        with self._queue_cv:
            dropped = len(self._queue) == self._queue.maxlen
            if dropped:
                self._dropped += 1
            self._queue.append(payload)
            self.status["queued"] = len(self._queue)
            self._queue_cv.notify()
        if dropped and self._dropped % 100 == 1:
            self.logger.warning(
                f"TAK-Server {self.endpoint} nicht erreichbar: Sende-Puffer voll, älteste CoT werden verworfen."
            )
        return not dropped

    def get_status(self):
        status = dict(self.status)
        status["connected"] = self.connected and self._confirmed
        with self._queue_cv:
            status["queued"] = len(self._queue)
        return status

    # -- internals --------------------------------------------------------

    def _close_socket(self):
        with self._sock_lock:
            sock, self._sock = self._sock, None
        self._connected.clear()
        self.status["connected"] = False
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass

    def _open_socket(self):
        if self.protocol == "UDP":
            family = socket.AF_INET6 if ":" in self.host else socket.AF_INET
            sock = socket.socket(family, socket.SOCK_DGRAM)
            sock.connect((self.host, self.port))
            return sock
        raw = self._socket_factory((self.host, self.port), CONNECT_TIMEOUT_SECONDS)
        try:
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except Exception:
            pass
        if self.protocol == "SSL":
            factory = self._ssl_context_factory
            if self._use_fallback_ssl and self._fallback_ssl_context_factory is not None:
                factory = self._fallback_ssl_context_factory
            context = factory() if factory else build_tak_ssl_context()
            raw.settimeout(CONNECT_TIMEOUT_SECONDS)
            try:
                sock = context.wrap_socket(raw, server_hostname=self.host)
            except ssl.SSLCertVerificationError:
                raw.close()
                if self._use_fallback_ssl or self._fallback_ssl_context_factory is None:
                    raise
                # Server certificate not from the truststore: try publicly trusted CAs (with host name check).
                self._use_fallback_ssl = True
                self.logger.info(
                    f"Serverzertifikat von {self.endpoint} nicht vom Truststore – prüfe gegen öffentliche Zertifizierungsstellen."
                )
                return self._open_socket()
        else:
            sock = raw
        sock.settimeout(SOCKET_IO_TIMEOUT_SECONDS)
        return sock

    def _write(self, sock, payload):
        data = prepare_cot_for_stream(payload)
        if not data:
            return
        with self._send_lock:
            if self.protocol == "UDP":
                sock.send(data)
                return
            deadline = time.monotonic() + SEND_TIMEOUT_SECONDS
            view = memoryview(data)
            while view:
                try:
                    sent = sock.send(view)
                except (socket.timeout, ssl.SSLWantWriteError):
                    if time.monotonic() > deadline:
                        raise TimeoutError("TAK-Server nimmt keine Daten an (Sende-Timeout).")
                    continue
                view = view[sent:]

    def _send_sa(self, sock):
        if self._sa_builder is None:
            return
        try:
            sa = self._sa_builder()
        except Exception as exc:
            self.logger.debug(f"Gateway-SA konnte nicht erstellt werden: {exc}")
            return
        if sa:
            self._write(sock, sa if isinstance(sa, bytes) else sa.encode("utf-8"))

    def _run(self):
        delay = self.reconnect_seconds
        while not self._stop.is_set():
            connected_at = None
            try:
                self.logger.info(f"Verbinde mit TAK-Server {self.endpoint} ...")
                sock = self._open_socket()
                with self._sock_lock:
                    self._sock = sock
                self._connected.set()
                self._confirmed = False
                connected_at = time.monotonic()
                self.status.update(connected=True, last_error="", connected_since=time.time())
                if self.protocol == "SSL":
                    # With TLS 1.3 a missing/rejected client certificate is only reported after the
                    # handshake, so "connected" is logged once the server kept the session open.
                    self.logger.info(f"TLS-Verbindung zu {self.endpoint} aufgebaut, warte auf Annahme durch den Server ...")
                else:
                    self._confirm_connection()
                self._send_sa(sock)
                self._serve(sock)
            except Exception as exc:
                self._report_connection_error(exc, connected_at=connected_at)
            finally:
                was_confirmed = self._confirmed
                self._close_socket()
                if was_confirmed and not self._stop.is_set():
                    self.logger.warning(f"TAK-Server-Verbindung {self.endpoint} getrennt, neuer Versuch folgt.")
            if self._confirmed_once_recently():
                delay = self.reconnect_seconds
            if self._stop.wait(delay):
                break
            delay = min(delay * 2, MAX_RECONNECT_SECONDS)

    def _confirm_connection(self):
        if self._confirmed:
            return
        self._confirmed = True
        self.logger.info(f"✅ TAK-Server verbunden: {self.endpoint}")

    def _confirmed_once_recently(self):
        # Reset the back-off only after a session that was really accepted by the server.
        was = self._confirmed
        self._confirmed = False
        return was

    def _report_connection_error(self, exc, connected_at=None):
        if self._stop.is_set():
            return  # socket closed by stop(); not an error
        quick_close = connected_at is not None and time.monotonic() - connected_at < CONFIRM_CONNECTION_SECONDS
        rejected = is_client_certificate_rejection(exc) or (
            self.protocol == "SSL" and quick_close and isinstance(exc, (ConnectionError, ssl.SSLError))
        )
        if rejected and self.protocol == "SSL":
            if not self.has_client_certificate:
                message = (
                    "Server verlangt ein Client-Zertifikat, es ist aber keins eingerichtet. In der Browser-UI "
                    "\"Zertifikat anfordern\" (Benutzername/Passwort) ausführen oder eine Client-.p12 hochladen."
                )
            else:
                message = (
                    "Server hat das Client-Zertifikat abgelehnt (abgelaufen, gesperrt oder von einer anderen CA). "
                    "Neues Zertifikat anfordern bzw. passendes Datenpaket importieren."
                )
            self.status["last_error"] = message
            self.logger.warning(f"TAK-Server {self.endpoint}: {message} ({exc})")
            return
        if isinstance(exc, ssl.SSLCertVerificationError):
            message = f"Serverzertifikat nicht vertrauenswürdig: {exc.verify_message}. Truststore prüfen."
        elif isinstance(exc, ssl.SSLError):
            message = f"TLS-Fehler: {exc}"
        else:
            message = f"{type(exc).__name__}: {exc}"
        self.status["last_error"] = message
        self.logger.warning(f"TAK-Server {self.endpoint} nicht verbunden: {message}")

    def _serve(self, sock):
        """Single-threaded loop: read server traffic, send queued CoT, keep the stream alive.

        Reading and writing happen on one thread because an SSL socket must not be used
        concurrently from several threads.
        """
        last_write = time.monotonic()
        last_sa = time.monotonic()
        session_start = time.monotonic()
        self._last_rx = session_start
        self._server_answers_pings = False
        received_at_start = self.status["received"]
        buffer_text = ""
        while not self._stop.is_set():
            if self.protocol != "UDP":
                buffer_text = self._read_available(sock, buffer_text)
                if buffer_text is None:
                    return
            if not self._confirmed:
                if (
                    time.monotonic() - session_start >= CONFIRM_CONNECTION_SECONDS
                    or self.status["received"] > received_at_start
                ):
                    self._confirm_connection()
                else:
                    # Hold back queued CoT until the server accepted the session; with TLS 1.3 a
                    # rejection arrives after the handshake and data written meanwhile would be lost.
                    continue
            with self._queue_cv:
                if not self._queue and self.protocol == "UDP":
                    self._queue_cv.wait(timeout=1.0)
                payload = self._queue.popleft() if self._queue else None
                self.status["queued"] = len(self._queue)
            now = time.monotonic()
            if payload is not None:
                try:
                    self._write(sock, payload)
                except Exception:
                    # Put it back so it is delivered after reconnect.
                    with self._queue_cv:
                        self._queue.appendleft(payload)
                    raise
                self.status["sent"] += 1
                last_write = now
                continue
            if self.sa_interval_seconds and now - last_sa >= self.sa_interval_seconds:
                self._send_sa(sock)
                last_sa = last_write = now
            elif self.protocol != "UDP" and now - last_write >= self.keepalive_seconds:
                self._write(sock, build_tak_ping_xml(self.uid).encode("utf-8"))
                last_write = now
            if (
                self.protocol != "UDP"
                and self._server_answers_pings
                and now - self._last_rx > DEAD_CONNECTION_FACTOR * self.keepalive_seconds
            ):
                # The server answered pings before and has now been silent for several ping rounds:
                # the TCP session is dead (NAT timeout, Wi-Fi switch) even though no error was reported.
                raise ConnectionError(
                    f"keine Antwort vom Server seit {now - self._last_rx:.0f} s (Pong fehlt) – Verbindung wird neu aufgebaut"
                )

    def _wait_readable(self, sock, timeout):
        pending = getattr(sock, "pending", None)
        if callable(pending) and pending():
            return True
        with self._queue_cv:
            if self._queue and self._confirmed:
                timeout = 0
        try:
            readable, _, _ = select.select([sock], [], [], timeout)
        except (OSError, ValueError):
            raise ConnectionError("Socket geschlossen.")
        return bool(readable)

    def _read_available(self, sock, buffer_text):
        """Read whatever the server sent. Returns the new buffer or None when the server closed."""
        # Short wait so queued packets are not delayed; up to 0.5 s when idle.
        while self._wait_readable(sock, 0.5):
            try:
                data = sock.recv(65536)
            except (socket.timeout, ssl.SSLWantReadError):
                return buffer_text
            if not data:
                self.logger.info(f"TAK-Server {self.endpoint} hat die Verbindung geschlossen.")
                return None
            self._last_rx = time.monotonic()
            if data[:1] == b"\xbf" and not buffer_text:
                # TAK protocol (protobuf) is only used after negotiation, which we never request.
                self.logger.debug("Unerwartetes TAK-Protobuf-Paket vom Server ignoriert.")
                continue
            buffer_text += data.decode("utf-8", errors="ignore")
            events, buffer_text = extract_cot_events(buffer_text)
            for event_xml in events:
                self.status["received"] += 1
                if _PONG_RE.search(event_xml):
                    self._server_answers_pings = True
                if self._on_event is None:
                    continue
                try:
                    self._on_event(event_xml)
                except Exception as exc:
                    self.logger.debug(f"Fehler beim Verarbeiten einer TAK-Server-Nachricht: {exc}")
            with self._queue_cv:
                if self._queue and self._confirmed:
                    break
        return buffer_text
