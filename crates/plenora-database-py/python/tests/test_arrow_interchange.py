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
