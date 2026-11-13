"""Fault TLS sul protocollo MySQL, soltanto nei test delle superfici."""
import socket
import ssl
import threading
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
        self._threads = []
        self._connections = []
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self.client_tls = ssl.create_default_context(cafile=ca)
        self.server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        directory = Path(ca).parent
        self.server_tls.load_cert_chain(directory / "server.pem", directory / "server.key")
        threading.Thread(target=self._accept, daemon=True).start()

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
        try:
            server = socket.create_connection(self.upstream, timeout=30)
            client.settimeout(30)
            client.sendall(packet(server))
            request = packet(client)
            assert len(request) == 36 and int.from_bytes(request[4:8], "little") & 0x800
            server.sendall(request)
            server = self.client_tls.wrap_socket(server, server_hostname=self.upstream[0])
            client = self.server_tls.wrap_socket(client, server_side=True)
            self._connections.extend([server, client])
            cutting = threading.Event()
            committed = threading.Event()

            def back():
                try:
                    while True:
                        response = packet(server)
                        if cutting.is_set():
                            assert response[4] == 0, "COMMIT della fixture non confermato"
                            committed.set()
                            return
                        client.sendall(response)
                except (EOFError, OSError):
                    pass

            reader = threading.Thread(target=back, daemon=True)
            reader.start()
            marked = False
            while True:
                request = packet(client)
                marked |= self.marker in request
                if marked and request[4:] == b"\x03COMMIT":
                    cutting.set()
                    server.sendall(request)
                    assert committed.wait(30), "risposta COMMIT non osservata"
                    with self._lock:
                        self.cuts += 1
                    client.shutdown(socket.SHUT_RDWR)
                    break
                server.sendall(request)
        except (EOFError, OSError):
            pass
        except AssertionError:
            self.errors.append("protocollo o commit non osservato")
        finally:
            client.close()
            if server is not None:
                server.close()

    def close(self):
        self._listener.close()
        for connection in self._connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        for thread in self._threads:
            thread.join(timeout=35)
