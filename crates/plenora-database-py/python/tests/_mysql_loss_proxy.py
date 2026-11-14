"""Fault TLS sul protocollo MySQL, soltanto nei test delle superfici."""
import socket
import ssl
import threading
import select
import time
from pathlib import Path


def packet(connection):
    def read(size):
        data = bytearray()
        while len(data) < size:
            part = connection.recv(size - len(data))
            if not part:
                raise EOFError
            data.extend(part)
        return bytes(data)
    header = read(4)
    return header + read(int.from_bytes(header[:3], "little"))


class MysqlCommitCutter:
    """Termina TLS sui due lati e sopprime la risposta al COMMIT marcato.

    Usa esclusivamente il certificato della fixture; entrambi i lati verificano
    TLS. La risposta del server viene letta prima di notificare il fault al
    client, cosi il test non confonde un COMMIT mai inviato con uno applicato.
    """
    def __init__(self, host, ca, marker):
        name, sep, port = host.rpartition(":")
        self.upstream = (name, int(port)) if sep else (host, 3306)
        self.marker = marker.encode()
        self.cuts = 0
        self.errors = []
        self._lock = threading.Lock()
        self._closing = threading.Event()
        self._threads = []
        self._connections = []
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self.client_tls = ssl.create_default_context(cafile=ca)
        self.server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        directory = Path(ca).parent
        self.server_tls.load_cert_chain(directory / "server.pem", directory / "server.key")
        self._acceptor = threading.Thread(target=self._accept, daemon=True)
        self._acceptor.start()

    def route_config(self, config):
        from dataclasses import replace
        return replace(config, host="127.0.0.1", port=self.port)

    def route_arguments(self, arguments):
        arguments = list(arguments)
        arguments[0] = "127.0.0.1"
        if len(arguments) > 3 and arguments[3].isdecimal():
            arguments[3] = str(self.port)
        else:
            arguments.insert(3, str(self.port))
        return arguments

    def _accept(self):
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            worker = threading.Thread(target=self._relay, args=(client,), daemon=True)
            self._threads.append(worker)
            worker.start()

    def _relay(self, client):
        server = None
        stage = "connect"
        try:
            server = socket.create_connection(self.upstream, timeout=30)
            client.settimeout(30)
            stage = "greeting"
            client.sendall(packet(server))
            request = packet(client)
            assert len(request) == 36 and int.from_bytes(request[4:8], "little") & 0x800
            server.sendall(request)
            stage = "upstream_tls"
            server = self.client_tls.wrap_socket(server, server_hostname=self.upstream[0])
            stage = "client_tls"
            client = self.server_tls.wrap_socket(client, server_side=True)
            self._connections.extend([server, client])
            # One thread owns both SSL objects. Concurrent recv/send on a single
            # SSL connection races TLS state (notably post-handshake records).
            stage = "relay"
            self._pump(client, server)

        except EOFError:
            if stage != "relay":
                self.errors.append({"stage": stage, "type": "EOFError"})
        except OSError as error:
            if not self._closing.is_set():
                self.errors.append({"stage": stage, "type": type(error).__name__,
                                    "verify_code": getattr(error, "verify_code", None)})
        except AssertionError:
            self.errors.append({"stage": stage, "type": "AssertionError"})
        finally:
            client.close()
            if server is not None:
                server.close()

    def _pump(self, client, server):
        sockets = (client, server)
        for connection in sockets:
            connection.setblocking(False)
        incoming = {connection: bytearray() for connection in sockets}
        outgoing = {connection: bytearray() for connection in sockets}
        read_needs_write, write_needs_read = set(), set()
        marked = cutting = False
        deadline = time.monotonic() + 30
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            ready, writable, _ = select.select(
                sockets, [c for c in sockets if c in read_needs_write or
                          (outgoing[c] and c not in write_needs_read)], [],
                0 if any(c.pending() for c in sockets) else remaining)
            for connection in sockets:
                if outgoing[connection] and (connection in writable or
                        (connection in write_needs_read and connection in ready)):
                    try:
                        sent = connection.send(outgoing[connection])
                        del outgoing[connection][:sent]
                        write_needs_read.discard(connection)
                    except ssl.SSLWantReadError:
                        write_needs_read.add(connection)
                    except ssl.SSLWantWriteError:
                        write_needs_read.discard(connection)
                if not (connection in ready or connection.pending() or
                        (connection in read_needs_write and connection in writable)):
                    continue
                try:
                    part = connection.recv(65536)
                    read_needs_write.discard(connection)
                except ssl.SSLWantReadError:
                    read_needs_write.discard(connection)
                    continue
                except ssl.SSLWantWriteError:
                    read_needs_write.add(connection)
                    continue
                if not part:
                    if cutting:
                        raise AssertionError("COMMIT response absent")
                    return
                deadline = time.monotonic() + 30
                buffer = incoming[connection]
                buffer.extend(part)
                while len(buffer) >= 4:
                    size = 4 + int.from_bytes(buffer[:3], "little")
                    if len(buffer) < size:
                        break
                    message = bytes(buffer[:size])
                    del buffer[:size]
                    if connection is client:
                        marked |= self.marker in message
                        cutting |= marked and message[4:] == b"\x03COMMIT"
                    elif cutting:
                        assert message[4] == 0, "COMMIT not acknowledged"
                        with self._lock:
                            self.cuts += 1
                        client.shutdown(socket.SHUT_RDWR)
                        return
                    outgoing[server if connection is client else client].extend(message)

    def close(self):
        self._closing.set()
        try:
            self._listener.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._listener.close()
        self._acceptor.join(timeout=35)
        for connection in self._connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        for thread in self._threads:
            thread.join(timeout=35)
