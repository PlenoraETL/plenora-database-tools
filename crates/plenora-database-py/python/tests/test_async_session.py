"""F3-7 — Test integrazione live per AsyncSession + builder async.

Richiede pytest-asyncio.
"""
from __future__ import annotations

import os

import pytest
import pytest_asyncio

import plenora_database as p

from ._harness import aconnect_postgres, postgres_dsn_or_skip


@pytest_asyncio.fixture(name="session")
async def _session():
    dsn = postgres_dsn_or_skip()
    s = await aconnect_postgres(dsn)
    try:
        yield s
    finally:
        s.close()


# ================================ aconnect ================================


@pytest.mark.asyncio
async def test_aconnect_returns_async_session() -> None:
    dsn = postgres_dsn_or_skip()
    s = await aconnect_postgres(dsn)
    try:
        assert isinstance(s, p.AsyncSession)
        assert isinstance(s.server_version, str)
        assert any(c.isdigit() for c in s.server_version)
        # PostGIS optional
        assert s.postgis_version is None or isinstance(s.postgis_version, str)
    finally:
        s.close()


@pytest.mark.asyncio
async def test_aconnect_invalid_dsn_raises_plenora_error() -> None:
    with pytest.raises(p.PlenoraError):
        await p.aconnect(
            "host=host-inesistente.invalid user=x password=y dbname=z connect_timeout=1"
        )


@pytest.mark.asyncio
async def test_async_context_manager_closes(session) -> None:
    dsn = postgres_dsn_or_skip()
    async with await aconnect_postgres(dsn) as s2:
        assert s2.is_closed is False
    assert s2.is_closed is True


# ================================ execute raw ================================


@pytest.mark.asyncio
async def test_execute_scalar_int_literal(session) -> None:
    v = await session.execute_scalar("SELECT 42")
    assert v == 42


@pytest.mark.asyncio
async def test_execute_scalar_with_params(session) -> None:
    v = await session.execute_scalar(
        "SELECT ($1::int + $2::int)::BIGINT",
        [10, 20],
    )
    assert v == 30


@pytest.mark.asyncio
async def test_execute_returning_rows_shape(session) -> None:
    rows = await session.execute_returning_rows(
        "SELECT id, name FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name) ORDER BY id"
    )
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


@pytest.mark.asyncio
async def test_execute_dml_returns_affected(session) -> None:
    await session.execute("DROP TABLE IF EXISTS _pyf7_dml")
    await session.execute("CREATE TABLE _pyf7_dml (id INT PRIMARY KEY, x TEXT)")
    try:
        n = await session.execute(
            "INSERT INTO _pyf7_dml (id, x) VALUES ($1, $2), ($3, $4)",
            [1, "a", 2, "b"],
        )
        assert n == 2
    finally:
        await session.execute("DROP TABLE IF EXISTS _pyf7_dml")


@pytest.mark.asyncio
async def test_execute_error_maps_to_plenora_error(session) -> None:
    with pytest.raises(p.PlenoraNotFoundError):
        await session.execute_scalar("SELECT * FROM tabella_che_non_esiste_zzz")


@pytest.mark.asyncio
async def test_close_then_execute_raises(session) -> None:
    session.close()
    with pytest.raises(RuntimeError, match="chiusa"):
        await session.execute_scalar("SELECT 1")


# ================================ portable AST async ================================


@pytest_asyncio.fixture(name="items_table")
async def _items_table(session):
    await session.execute("DROP TABLE IF EXISTS _pyf7_items")
    await session.execute(
        "CREATE TABLE _pyf7_items ("
        " id BIGSERIAL PRIMARY KEY,"
        " code TEXT UNIQUE NOT NULL,"
        " qty INT NOT NULL DEFAULT 0)"
    )
    try:
        yield session
    finally:
        await session.execute("DROP TABLE IF EXISTS _pyf7_items")


@pytest.mark.asyncio
async def test_async_select_all(items_table) -> None:
    await items_table.insert("_pyf7_items").rows([
        {"code": "A", "qty": 1},
        {"code": "B", "qty": 2},
    ]).execute()
    rows = await items_table.select("_pyf7_items").columns("code", "qty").order_by("code").all()
    assert rows == [{"code": "A", "qty": 1}, {"code": "B", "qty": 2}]


@pytest.mark.asyncio
async def test_async_select_one_and_scalar(items_table) -> None:
    await items_table.insert("_pyf7_items").values(code="X", qty=42).execute()
    row = await items_table.select("_pyf7_items").columns("code").where_eq("code", "X").one()
    assert row == {"code": "X"}
    val = await items_table.select("_pyf7_items").columns("qty").where_eq("code", "X").scalar()
    assert val == 42
    # Missing
    none_row = await items_table.select("_pyf7_items").where_eq("code", "MISSING").one()
    assert none_row is None


@pytest.mark.asyncio
async def test_async_insert_returning_one(items_table) -> None:
    row = (
        await items_table.insert("_pyf7_items")
        .values(code="N", qty=99)
        .returning("id", "code", "qty")
        .one()
    )
    assert row["code"] == "N"
    assert row["qty"] == 99
    assert isinstance(row["id"], int)


@pytest.mark.asyncio
async def test_async_update_and_delete(items_table) -> None:
    await items_table.insert("_pyf7_items").values(code="U", qty=1).execute()
    n = (
        await items_table.update("_pyf7_items")
        .set(qty=100)
        .where_eq("code", "U")
        .execute()
    )
    assert n == 1
    updated = await items_table.select("_pyf7_items").columns("qty").where_eq("code", "U").scalar()
    assert updated == 100
    d = await items_table.delete("_pyf7_items").where_eq("code", "U").execute()
    assert d == 1


@pytest.mark.asyncio
async def test_async_upsert(items_table) -> None:
    # First: insert.
    row1 = (
        await items_table.upsert("_pyf7_items")
        .values(code="UP", qty=1)
        .conflict_target("code")
        .update_on_conflict(qty=999)
        .returning("id", "qty")
        .one()
    )
    assert row1["qty"] == 1
    # Second: conflict → update.
    row2 = (
        await items_table.upsert("_pyf7_items")
        .values(code="UP", qty=2)
        .conflict_target("code")
        .update_on_conflict(qty=999)
        .returning("id", "qty")
        .one()
    )
    assert row2["id"] == row1["id"]
    assert row2["qty"] == 999


@pytest.mark.asyncio
async def test_async_predicates_chain(items_table) -> None:
    await items_table.insert("_pyf7_items").rows([
        {"code": "A", "qty": 1},
        {"code": "B", "qty": 5},
        {"code": "C", "qty": 10},
    ]).execute()
    rows = (
        await items_table.select("_pyf7_items")
        .columns("code")
        .where_gte("qty", 5)
        .where_ne("code", "C")
        .all()
    )
    assert [r["code"] for r in rows] == ["B"]


@pytest.mark.asyncio
async def test_multiple_async_operations_can_interleave(items_table) -> None:
    # Verifica che il bridge asyncio non blocchi l'event loop:
    # eseguiamo 10 execute_scalar in parallelo con asyncio.gather.
    import asyncio

    async def one_query(n: int) -> int:
        return await items_table.execute_scalar("SELECT $1::int", [n])

    results = await asyncio.gather(*(one_query(i) for i in range(10)))
    assert results == list(range(10))
