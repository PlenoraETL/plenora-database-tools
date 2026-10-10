"""Un commit senza conferma, provocato davvero, su `copy_from` e `acopy_from`.

Un proxy TCP sta fra la sessione e il riferimento PostgreSQL. Inoltra tutto
finche la connessione che ha toccato la tabella di prova non invia `COMMIT`:
allora chiude il lato client **prima** di inoltrare il comando, cosi il server
riceve il commit ma la sua risposta non arriva mai.

La scrittura usa `mode="create"`: e il percorso in cui l'adapter restituisce
l'esito ignoto come documento. `append` passa dalla diagnostica di riga, che
lo rifiuta gia come errore `quarantine`.

Un esito ignoto non e un valore di ritorno (SURF-014): un chiamante che non
legge `status` proseguirebbe su uno stato che nessuno ha verificato. Deve
arrivare come `PlenoraCommitOutcomeUnknownError`, con l'esito e la sua
`recovery` in `details["write_outcome"]`.
"""
from __future__ import annotations

import socket
import threading
import time

import pytest

import plenora_database as p

from ._harness import aconnect_postgres, connect_postgres, postgres_dsn_or_skip

pyarrow = pytest.importorskip("pyarrow")

TABLE = "_pyp_outcome_unknown"


class CommitCutter:
    """Proxy che interrompe il primo `COMMIT` di una connessione marcata."""

    def __init__(self, upstream: tuple[str, int], marker: bytes) -> None:
        self._upstream = upstream
        self._marker = marker
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self.cuts = 0
        self._lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def close(self) -> None:
        self._listener.close()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._listener.accept()
                server = socket.create_connection(self._upstream)
            except OSError:
                return
            threading.Thread(
                target=self._relay, args=(client, server), daemon=True
            ).start()

    def _relay(self, client: socket.socket, server: socket.socket) -> None:
        def back() -> None:
            try:
                while data := server.recv(65536):
                    client.sendall(data)
            except OSError:
                pass

        threading.Thread(target=back, daemon=True).start()
        marked = False
        try:
            while data := client.recv(65536):
                marked = marked or self._marker in data
                if marked and b"COMMIT" in data:
                    # Prima si chiude il client, poi si inoltra: la conferma
                    # del server non ha piu una strada per tornare indietro.
                    client.shutdown(socket.SHUT_RDWR)
                    client.close()
                    server.sendall(data)
                    with self._lock:
                        self.cuts += 1
                    time.sleep(2)
                    break
                server.sendall(data)
        except OSError:
            pass
        finally:
            server.close()


def _redirect(dsn: str, port: int) -> tuple[tuple[str, int], str]:
    """Host e porta della DSN `chiave=valore`, e la stessa DSN sul proxy."""

    assert "'" not in dsn, "DSN con valori quotati non supportata dal proxy"
    host, upstream_port, rest = "localhost", 5432, []
    for token in dsn.split():
        if token.startswith("host="):
            host = token.removeprefix("host=")
        elif token.startswith("port="):
            upstream_port = int(token.removeprefix("port="))
        else:
            rest.append(token)
    return (host, upstream_port), " ".join(
        [*rest, "host=127.0.0.1", f"port={port}"]
    )


@pytest.fixture(name="cutter")
def _cutter():
    dsn = postgres_dsn_or_skip()
    upstream, _ = _redirect(dsn, 0)
    direct = connect_postgres(dsn)
    direct.execute_sql(f"DROP TABLE IF EXISTS {TABLE}")
    cutter = CommitCutter(upstream, TABLE.encode())
    _, cutter.dsn = _redirect(dsn, cutter.port)
    try:
        yield cutter
    finally:
        cutter.close()
        try:
            direct.execute_sql(f"DROP TABLE IF EXISTS {TABLE}")
        finally:
            direct.close()


def _table() -> "pyarrow.Table":
    return pyarrow.table({"id": pyarrow.array([1, 2, 3], type=pyarrow.int64())})


def _assert_unknown(error: p.PlenoraCommitOutcomeUnknownError) -> None:
    # Il canale e stato chiuso: la categoria e `io` (ERR-001); la classe resta
    # quella del commit ignoto.
    assert error.category == "io"
    assert error.phase == "commit"
    assert error.remote_effect == "unknown"
    assert error.retry == {"kind": "requires_recovery"}
    outcome = error.details["write_outcome"]
    assert outcome["status"] == "outcome_unknown"
    assert outcome["recovery"]["automatic_retry_allowed"] is False
    assert error.execution_id == outcome["execution_id"]


def test_copy_from_with_a_lost_commit_acknowledgement_raises(cutter) -> None:
    session = connect_postgres(cutter.dsn)
    try:
        with pytest.raises(p.PlenoraCommitOutcomeUnknownError) as raised:
            session.copy_from(
                "public", TABLE, _table(), mode="create",
                mapping_policy="compatible",
            )
    finally:
        session.close()
    assert cutter.cuts == 1, "il proxy non ha interrotto il commit"
    _assert_unknown(raised.value)


@pytest.mark.asyncio
async def test_acopy_from_with_a_lost_commit_acknowledgement_raises(cutter) -> None:
    session = await aconnect_postgres(cutter.dsn)
    try:
        with pytest.raises(p.PlenoraCommitOutcomeUnknownError) as raised:
            await session.acopy_from(
                "public", TABLE, _table(), mode="create",
                mapping_policy="compatible",
            )
    finally:
        session.close()
    assert cutter.cuts == 1, "il proxy non ha interrotto il commit"
    _assert_unknown(raised.value)
