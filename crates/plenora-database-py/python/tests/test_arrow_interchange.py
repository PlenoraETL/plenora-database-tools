"""Arrow PyCapsule Interface: `copy_from` la accetta, `BatchReader` la offre.

Le prove offline usano un produttore che espone soltanto
`__arrow_c_stream__`, come una tabella di un'altra libreria; quelle live
leggono da PostgreSQL e riscrivono senza passare dai `bytes`.
"""

from __future__ import annotations

import io

import pytest

pyarrow = pytest.importorskip("pyarrow")
import pyarrow.ipc as ipc  # noqa: E402

from plenora_database._arrow_io import _to_ipc_bytes  # noqa: E402

from ._harness import connect_postgres, postgres_dsn_or_skip  # noqa: E402


class OnlyCapsule:
    """Un produttore che non e ne `pyarrow.Table` ne iterabile."""

    def __init__(self, table: "pyarrow.Table") -> None:
        self._table = table

    def __arrow_c_stream__(self, requested_schema=None):
        return self._table.__arrow_c_stream__(requested_schema)


def _decoded(buffer: bytes) -> "pyarrow.Table":
    return ipc.open_stream(io.BytesIO(buffer)).read_all()


def test_copy_from_input_accepts_a_pycapsule_producer() -> None:
    table = pyarrow.table(
        {"id": pyarrow.array([1, 2, 3], pyarrow.int64()), "nome": ["a", None, "c"]}
    )
    assert _decoded(_to_ipc_bytes(OnlyCapsule(table))).equals(table)


def test_an_empty_pycapsule_stream_keeps_its_schema() -> None:
    schema = pyarrow.schema(
        [pyarrow.field("id", pyarrow.int64(), False, metadata={"k": "v"})]
    )
    decoded = _decoded(_to_ipc_bytes(OnlyCapsule(schema.empty_table())))
    assert decoded.num_rows == 0
    assert decoded.schema.equals(schema, check_metadata=True)


class FailingProducer:
    """Un produttore che fallisce dopo il primo batch, con un dato nel messaggio."""

    def __init__(self, table: "pyarrow.Table") -> None:
        self._table = table

    def __arrow_c_stream__(self, requested_schema=None):
        def batches():
            yield from self._table.to_batches()
            raise RuntimeError("CANARINO-riga-segreta")

        reader = pyarrow.RecordBatchReader.from_batches(self._table.schema, batches())
        return reader.__arrow_c_stream__(requested_schema)


def test_a_failing_producer_is_a_typed_error_without_its_data() -> None:
    import plenora_database as p

    table = pyarrow.table({"id": pyarrow.array([1, 2], pyarrow.int64())})
    with pytest.raises(p.PlenoraDataMappingError) as raised:
        _to_ipc_bytes(FailingProducer(table))
    error = raised.value
    assert error.category == "data_mapping"
    assert error.remote_effect == "none"
    assert error.retry == {"kind": "never"}
    assert "CANARINO" not in str(error)
    assert error.__cause__ is None and error.__suppress_context__


def test_a_generator_failing_before_its_first_batch_is_translated() -> None:
    import plenora_database as p

    def batches():
        raise RuntimeError("CANARINO-riga-segreta")
        yield  # pragma: no cover

    with pytest.raises(p.PlenoraDataMappingError) as raised:
        _to_ipc_bytes(batches())
    assert "CANARINO" not in str(raised.value)
    assert raised.value.__cause__ is None and raised.value.__suppress_context__


def test_a_capsule_property_that_raises_is_translated() -> None:
    import plenora_database as p

    class Hostile:
        @property
        def __arrow_c_stream__(self):
            raise RuntimeError("CANARINO-riga-segreta")

    with pytest.raises(p.PlenoraDataMappingError) as raised:
        _to_ipc_bytes(Hostile())
    assert "CANARINO" not in str(raised.value)


def test_argument_errors_stay_argument_errors() -> None:
    with pytest.raises(ValueError):
        _to_ipc_bytes([])
    with pytest.raises(TypeError):
        _to_ipc_bytes(42)


def test_a_plenora_error_from_the_producer_is_kept() -> None:
    import plenora_database as p

    class PlenoraProducer:
        def __arrow_c_stream__(self, requested_schema=None):
            raise p.PlenoraTimeoutError("timeout: scaduto")

    with pytest.raises(p.PlenoraTimeoutError):
        _to_ipc_bytes(PlenoraProducer())


def test_a_capsule_can_be_exported_once_per_request() -> None:
    table = pyarrow.table({"id": pyarrow.array([1, 2, 3], pyarrow.int64())})
    producer = OnlyCapsule(table)
    first = _decoded(_to_ipc_bytes(producer))
    second = _decoded(_to_ipc_bytes(producer))
    assert first.equals(table) and second.equals(table)


def test_a_capsule_released_early_can_be_dropped_unconsumed() -> None:
    """Rilascio senza consumatore: non misura la memoria, prova che la
    capsula si scarta e che il produttore resta esportabile."""
    table = pyarrow.table({"id": pyarrow.array(range(10), pyarrow.int64())})
    capsule = table.__arrow_c_stream__()
    del capsule  # rilasciata senza consumatore
    assert _decoded(_to_ipc_bytes(OnlyCapsule(table))).num_rows == 10


def test_acopy_from_consumes_the_source_off_the_event_loop() -> None:
    import asyncio
    import threading

    from plenora_database._async_session import AsyncSession

    threads = []

    class Recording(OnlyCapsule):
        def __arrow_c_stream__(self, requested_schema=None):
            threads.append(threading.get_ident())
            return super().__arrow_c_stream__(requested_schema)

    class Native:
        async def acopy_from(self, *args):
            return {"status": "committed"}

    async def scenario():
        session = AsyncSession(Native())
        table = pyarrow.table({"id": pyarrow.array([1], pyarrow.int64())})
        await session.acopy_from("s", "t", Recording(table), mapping_policy="strict")
        return threading.get_ident()

    loop_thread = asyncio.run(scenario())
    assert threads and threads[0] != loop_thread


# ---------------- Live ----------------


@pytest.fixture(name="session")
def _session():
    s = connect_postgres(postgres_dsn_or_skip())
    s.execute_sql("DROP TABLE IF EXISTS _pyx_source")
    s.execute_sql("DROP TABLE IF EXISTS _pyx_target")
    s.execute_sql("CREATE TABLE _pyx_source (id BIGINT PRIMARY KEY, nome TEXT)")
    s.execute_sql(
        "INSERT INTO _pyx_source SELECT gs, 'n' || gs FROM generate_series(1, 300) gs"
    )
    try:
        yield s
    finally:
        try:
            s.execute_sql("DROP TABLE IF EXISTS _pyx_source")
            s.execute_sql("DROP TABLE IF EXISTS _pyx_target")
        finally:
            s.close()


def test_the_reader_is_a_pycapsule_stream(session) -> None:
    reader = session.read("public", "_pyx_source", order_by=[("id", "asc")])
    table = pyarrow.RecordBatchReader.from_stream(reader).read_all()
    assert table.column_names == ["id", "nome"]
    assert table.column("id").to_pylist() == list(range(1, 301))


def test_read_all_returns_a_pyarrow_table(session) -> None:
    table = session.read("public", "_pyx_source").read_all()
    assert isinstance(table, pyarrow.Table)
    assert table.num_rows == 300


def test_a_reader_writes_back_without_bytes(session) -> None:
    reader = session.read("public", "_pyx_source")
    outcome = session.copy_from(
        "public", "_pyx_target", reader, mode="create", mapping_policy="compatible"
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 300
    copied = session.read("public", "_pyx_target", order_by=[("id", "asc")]).read_all()
    assert copied.column("nome").to_pylist() == [f"n{i}" for i in range(1, 301)]


def test_the_reader_can_be_consumed_from_another_thread(session) -> None:
    """`acopy_from` consuma la sorgente in un executor: il reader non e legato
    al thread che l'ha aperto."""
    from concurrent.futures import ThreadPoolExecutor

    reader = session.read("public", "_pyx_source", order_by=[("id", "asc")])
    with ThreadPoolExecutor(max_workers=1) as pool:
        table = pool.submit(reader.read_all).result()
    assert table.column("id").to_pylist() == list(range(1, 301))


def test_reading_and_schema_from_two_threads_do_not_deadlock(session) -> None:
    """Contesa sul reader: un thread legge fino a un errore del database,
    un altro chiede lo schema in continuazione. Tradurre l'errore tenendo il
    mutex, o attendere il mutex tenendo il GIL, li bloccava a vicenda."""
    import threading

    import plenora_database as p

    session.execute_sql("DROP VIEW IF EXISTS _pyx_failing")
    session.execute_sql(
        "CREATE VIEW _pyx_failing AS "
        "SELECT gs AS id, 1 / (gs - 150000) AS x FROM generate_series(1, 200000) gs"
    )
    try:
        reader = session.read("public", "_pyx_failing")
        done = threading.Event()
        errors = []

        def consume() -> None:
            try:
                for _ in reader:
                    pass
            except p.PlenoraError as error:
                errors.append(error)
            finally:
                done.set()

        def poll() -> None:
            while not done.is_set():
                reader.schema_bytes()

        threads = [threading.Thread(target=consume), threading.Thread(target=poll)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        assert not any(thread.is_alive() for thread in threads), "stallo fra GIL e mutex"
        assert errors, "la divisione per zero doveva arrivare come PlenoraError"
    finally:
        session.execute_sql("DROP VIEW IF EXISTS _pyx_failing")
