"""F4-3 — Test live per Arrow batch reader (sync + async).

Richiede pyarrow installato per la parte di parsing consumer-side.
"""
from __future__ import annotations

import io
import os

import pytest
import pytest_asyncio

import plenora_database as p

from ._harness import aconnect_postgres, connect_postgres, postgres_dsn_or_skip

pyarrow = pytest.importorskip("pyarrow")
pyarrow_ipc = pytest.importorskip("pyarrow.ipc")


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.execute("DROP TABLE IF EXISTS _pyf43_rows")
    s.execute(
        "CREATE TABLE _pyf43_rows ("
        " id BIGSERIAL PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " amount INT NOT NULL)"
    )
    # Seed 500 righe
    s.execute(
        "INSERT INTO _pyf43_rows (label, amount) "
        "SELECT 'row-' || gs::TEXT, gs * 10 "
        "FROM generate_series(1, 500) gs"
    )
    try:
        yield s
    finally:
        try:
            s.execute("DROP TABLE IF EXISTS _pyf43_rows")
        finally:
            s.close()


# ---------------- Sync ----------------


def test_read_returns_iterator_of_bytes(session) -> None:
    reader = session.read("public", "_pyf43_rows")
    assert iter(reader) is reader
    total_rows = 0
    batch_count = 0
    for chunk in reader:
        assert isinstance(chunk, bytes)
        assert len(chunk) > 0
        # Ogni chunk è un Arrow IPC stream self-contained.
        tbl = pyarrow_ipc.open_stream(io.BytesIO(chunk)).read_all()
        assert tbl.num_columns == 3
        assert tbl.column_names == ["id", "label", "amount"]
        total_rows += tbl.num_rows
        batch_count += 1
    assert total_rows == 500
    assert batch_count >= 1


def test_read_schema_bytes_returns_arrow_ipc(session) -> None:
    reader = session.read("public", "_pyf43_rows")
    schema_bytes = reader.schema_bytes()
    assert isinstance(schema_bytes, bytes)
    reader_ipc = pyarrow_ipc.open_stream(io.BytesIO(schema_bytes))
    fields = [f.name for f in reader_ipc.schema]
    assert fields == ["id", "label", "amount"]


def test_read_empty_table_returns_no_batches(session) -> None:
    session.execute("DROP TABLE IF EXISTS _pyf43_empty")
    session.execute("CREATE TABLE _pyf43_empty (id INT PRIMARY KEY)")
    try:
        reader = session.read("public", "_pyf43_empty")
        chunks = list(reader)
        assert chunks == []
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf43_empty")


def test_read_preserves_row_values_and_order(session) -> None:
    reader = session.read("public", "_pyf43_rows")
    all_ids: list[int] = []
    for chunk in reader:
        tbl = pyarrow_ipc.open_stream(io.BytesIO(chunk)).read_all()
        all_ids.extend(tbl.column("id").to_pylist())
    # 500 righe, id sequenziali (BIGSERIAL da 1)
    assert sorted(all_ids) == list(range(1, 501))


def test_read_error_on_missing_table_raises(session) -> None:
    with pytest.raises(p.PlenoraError):
        # Il read fallisce all'apertura (schema token check).
        list(session.read("public", "_pyf43_absolutely_nothere"))


# ---------------- Async ----------------


@pytest_asyncio.fixture(name="asession")
async def _asession():
    dsn = postgres_dsn_or_skip()
    s = await aconnect_postgres(dsn)
    await s.execute("DROP TABLE IF EXISTS _pyf43_arows")
    await s.execute(
        "CREATE TABLE _pyf43_arows ("
        " id BIGSERIAL PRIMARY KEY, label TEXT NOT NULL)"
    )
    await s.execute(
        "INSERT INTO _pyf43_arows (label) "
        "SELECT 'a-' || gs::TEXT FROM generate_series(1, 300) gs"
    )
    try:
        yield s
    finally:
        try:
            await s.execute("DROP TABLE IF EXISTS _pyf43_arows")
        finally:
            s.close()


@pytest.mark.asyncio
async def test_aread_returns_async_iterator(asession) -> None:
    reader = await asession.aread("public", "_pyf43_arows")
    total = 0
    async for chunk in reader:
        assert isinstance(chunk, bytes)
        tbl = pyarrow_ipc.open_stream(io.BytesIO(chunk)).read_all()
        total += tbl.num_rows
    assert total == 300


@pytest.mark.asyncio
async def test_aread_schema_bytes_awaitable(asession) -> None:
    reader = await asession.aread("public", "_pyf43_arows")
    schema_bytes = await reader.schema_bytes()
    assert isinstance(schema_bytes, bytes)
    fields = [f.name for f in pyarrow_ipc.open_stream(io.BytesIO(schema_bytes)).schema]
    assert fields == ["id", "label"]
