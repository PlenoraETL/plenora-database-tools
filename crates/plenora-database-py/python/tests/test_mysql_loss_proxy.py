"""Offline TLS protocol double: no database and no network outside loopback."""
import shutil
import socket
import ssl
import subprocess
import threading

import pytest

from ._mysql_loss_proxy import MysqlCommitCutter, packet


def frame(payload, sequence=0):
    return len(payload).to_bytes(3, "little") + bytes([sequence]) + payload


@pytest.fixture
def certificate(tmp_path):
    openssl = shutil.which("openssl")
    assert openssl, "TLS fault fixture requires OpenSSL"
    for command in (
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=offline-ca",
         "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
         "-keyout", "ca.key", "-out", "ca.pem"],
        ["req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
         "-keyout", "server.key", "-out", "server.csr"],
        ["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
         "-days", "1", "-extfile", "extensions", "-out", "server.pem"],
    ):
        (tmp_path / "extensions").write_text(
            "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
        subprocess.run([openssl, *command], cwd=tmp_path, check=True, capture_output=True)
    return tmp_path


@pytest.mark.parametrize("cut, acknowledged", [(False, True), (True, True), (True, False)])
def test_tls_auth_fragmentation_and_commit_ack(certificate, cut, acknowledged):
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(5)
    failures, received = [], []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate / "server.pem", certificate / "server.key")

    def server():
        try:
            with listener.accept()[0] as raw:
                raw.settimeout(5)
                raw.sendall(frame(b"greeting"))
                assert len(packet(raw)) == 36
                with context.wrap_socket(raw, server_side=True) as tls:
                    assert packet(tls)[4:] == b"authentication"
                    tls.sendall(frame(b"\x00authenticated", 2))
                    for _ in range(3):
                        received.append(packet(tls)[4:])
                        answer = frame(b"\xfferror" if len(received) == 3 and not acknowledged else b"\x00ok", 1)
                        for fragment in (answer[:1], answer[1:3], answer[3:]):
                            tls.sendall(fragment)
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=server)
    thread.start()
    proxy = MysqlCommitCutter(f"127.0.0.1:{listener.getsockname()[1]}",
                             str(certificate / "ca.pem"), "marked_table")
    try:
        with socket.create_connection(("127.0.0.1", proxy.port), timeout=5) as raw:
            assert packet(raw)[4:] == b"greeting"
            raw.sendall(frame((0x800).to_bytes(4, "little") + bytes(28), 1))
            client_context = ssl.create_default_context(cafile=certificate / "ca.pem")
            with client_context.wrap_socket(raw, server_hostname="localhost") as client:
                client.sendall(frame(b"authentication", 2))
                assert packet(client)[4:] == b"\x00authenticated"
                # A normal COMMIT must not be cut. Mark only the last transaction.
                for index, query in enumerate((b"COMMIT", b"INSERT INTO marked_table", b"COMMIT" if cut else b"ROLLBACK")):
                    message = frame(b"\x03" + query)
                    for fragment in (message[:2], message[2:5], message[5:]):
                        client.sendall(fragment)
                    if cut and index == 2:
                        with pytest.raises(EOFError):
                            packet(client)
                    else:
                        assert packet(client)[4:] == b"\x00ok"
    finally:
        proxy.close()
        thread.join(6)
        listener.close()
    assert not thread.is_alive()
    assert not failures, [type(e).__name__ for e in failures]
    assert proxy.errors == ([] if acknowledged else [{"stage": "relay", "type": "AssertionError"}])
    assert proxy.cuts == int(cut and acknowledged)
    assert len(received) == 3


def test_tls_pending_want_states_and_partial_sends(monkeypatch):
    from . import _mysql_loss_proxy as module
    query = frame(b"\x03INSERT marked") + frame(b"\x03COMMIT")

    class TLS:
        def __init__(self, incoming):
            self.incoming = bytearray(incoming)
            self.sent = bytearray()
            self.send_calls = self.recv_calls = 0
            self.closed = False

        def setblocking(self, _value):
            pass

        def pending(self):
            return len(self.incoming)

        def recv(self, _size):
            self.recv_calls += 1
            if self.recv_calls == 1:
                raise ssl.SSLWantWriteError()
            if self is server and not self.incoming and self.sent == query:
                self.incoming.extend(frame(b"\x00ok"))
            if not self.incoming:
                raise ssl.SSLWantReadError()
            value = bytes(self.incoming[:2])
            del self.incoming[:2]
            return value

        def send(self, value):
            self.send_calls += 1
            if self.send_calls == 1:
                raise ssl.SSLWantReadError()
            if self.send_calls == 2:
                raise ssl.SSLWantWriteError()
            self.sent.extend(value[:3])
            return min(3, len(value))

        def shutdown(self, _how):
            self.closed = True

    client, server = TLS(query), TLS(b"")
    calls = []

    def select(readable, writable, exceptional, timeout):
        calls.append(timeout)
        assert len(calls) < 200, "TLS state machine stalled"
        if any(c.pending() for c in readable):
            assert timeout == 0
        # TLS plaintext remains readable even with no new OS data.
        ready = [c for c in readable if not c.pending()]
        return ready, writable, exceptional

    monkeypatch.setattr(module.select, "select", select)
    proxy = MysqlCommitCutter.__new__(MysqlCommitCutter)
    proxy.marker, proxy.cuts, proxy._lock = b"marked", 0, threading.Lock()
    proxy._pump(client, server)
    assert proxy.cuts == 1 and client.closed
    assert server.sent == query
    assert server.send_calls > 3


@pytest.mark.parametrize("surface", ["sync", "async", "database-write-ipc", "write"])
@pytest.mark.parametrize("full_auth", [False, True])
def test_real_clients_reach_auth_through_proxy(certificate, tmp_path, surface, full_auth):
    import asyncio
    import json
    import os
    import struct
    import plenora_database as p
    import pyarrow as pa

    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(5)
    seen, failures = [], []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate / "server.pem", certificate / "server.key")

    def server():
        try:
            with listener.accept()[0] as raw:
                raw.settimeout(5)
                flags = 1 | 0x200 | 0x800 | 0x2000 | 0x8000 | 0x20000 | 0x80000
                greeting = (b"\x0a9.7.2\x00" + struct.pack("<I", 1) + b"12345678\x00"
                            + struct.pack("<H", flags & 65535) + b"\x2d" + struct.pack("<H", 2)
                            + struct.pack("<H", flags >> 16) + b"\x15" + bytes(10)
                            + b"abcdefghijkl\x00caching_sha2_password\x00")
                raw.sendall(frame(greeting))
                assert len(packet(raw)) == 36
                with context.wrap_socket(raw, server_side=True) as tls:
                    authentication = packet(tls)
                    seen.append("authenticated_transport")
                    if full_auth:
                        tls.sendall(frame(b"\x01\x04", authentication[3] + 1))
                        authentication = packet(tls)
                        assert authentication[4:] == b"offline-secret\x00"
                        seen.append("full_auth")
                    # Controlled server error proves connect reached authentication.
                    tls.sendall(frame(b"\xff\x15\x04#28000controlled", authentication[3] + 1))
        except BaseException as error:
            failures.append(type(error).__name__)

    thread = threading.Thread(target=server)
    thread.start()
    proxy = MysqlCommitCutter(f"127.0.0.1:{listener.getsockname()[1]}",
                             str(certificate / "ca.pem"), "marked_table")
    try:
        if surface in ("sync", "async"):
            config = proxy.route_config(p.EngineConfig("mysql", host="unused.invalid", database="test",
                user="test", password="offline-secret", tls_ca=(certificate / "ca.pem").read_text()))
            async def connect():
                return await p.async_engine_from_url(config)
            with pytest.raises(p.PlenoraError) as raised:
                if surface == "sync":
                    p.engine_from_url(config)
                else:
                    asyncio.run(connect())
            error = {key: getattr(raised.value, key) for key in ("category", "phase", "remote_effect")}
        else:
            operation = tmp_path / "operation.json"
            operation.write_text(json.dumps({"target": {"schema": "test", "object": "marked_table"},
                "mode": "append", "mapping_policy": "strict", "transaction_profile": "single_transaction"}))
            data = pa.table({"value": [1]})
            ipc = tmp_path / "data.arrow"
            with pa.ipc.new_file(ipc, data.schema) as writer:
                writer.write_table(data)
            arguments = proxy.route_arguments(["unused.invalid", "test", "test", "1",
                                               "--tls-ca-path-env", "PLENORA_OFFLINE_CA"])
            if surface == "write":
                request = tmp_path / "request.json"
                request.write_text(json.dumps({"provider": "mysql", "secret_environment": "PLENORA_OFFLINE_SECRET",
                    "provider_arguments": arguments, "operation_path": str(operation), "accept_loss": True}))
                args = ["--input", str(request), "--data", str(ipc)]
            else:
                args = ["mysql", "PLENORA_OFFLINE_SECRET", str(operation), str(ipc), *arguments, "--accept-loss"]
            result = subprocess.run([os.environ["PLENORA_CLI_BIN"], surface, *args, "--format", "json"],
                capture_output=True, text=True, timeout=15, check=False,
                env={**os.environ, "PLENORA_OFFLINE_SECRET": "offline-secret",
                     "PLENORA_OFFLINE_CA": str(certificate / "ca.pem")})
            assert result.returncode == 5
            error = json.loads(result.stdout)["error"]
        assert {key: error[key] for key in ("category", "phase", "remote_effect")} == {
            "category": "authentication", "phase": "connect", "remote_effect": "none"}
    finally:
        proxy.close()
        thread.join(6)
        listener.close()
    assert not thread.is_alive() and not failures, failures
    assert seen == (["authenticated_transport", "full_auth"] if full_auth else ["authenticated_transport"])
    assert not proxy.errors, proxy.errors
    assert proxy.cuts == 0
