"""F3-7 — Test integrazione live per AsyncTransaction."""
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
    await s.execute_sql("DROP TABLE IF EXISTS _pyf7tx")
    await s.execute_sql("CREATE TABLE _pyf7tx (id INT PRIMARY KEY, val TEXT NOT NULL)")
    try:
        yield s
    finally:
        try:
            await s.execute_sql("DROP TABLE IF EXISTS _pyf7tx")
        finally:
            s.close()


# --------------------------- lifecycle ---------------------------


@pytest.mark.asyncio
async def test_begin_returns_active_transaction(session) -> None:
    tx = await session.begin()
    assert await tx.is_active() is True
    await tx.rollback()
    assert await tx.is_active() is False


@pytest.mark.asyncio
async def test_context_manager_commits_on_normal_exit(session) -> None:
    async with await session.begin() as tx:
        await tx.execute_sql("INSERT INTO _pyf7tx (id, val) VALUES ($1, $2)", [1, "a"])
    cnt = await session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf7tx")
    assert cnt == 1


@pytest.mark.asyncio
async def test_context_manager_rolls_back_on_exception(session) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        async with await session.begin() as tx:
            await tx.execute_sql("INSERT INTO _pyf7tx (id, val) VALUES ($1, $2)", [2, "b"])
            raise RuntimeError("boom")
    cnt = await session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf7tx")
    assert cnt == 0


@pytest.mark.asyncio
async def test_explicit_commit_persists(session) -> None:
    tx = await session.begin()
    await tx.execute_sql("INSERT INTO _pyf7tx (id, val) VALUES ($1, $2)", [3, "c"])
    await tx.commit()
    assert await tx.is_active() is False
    cnt = await session.execute_scalar(
        "SELECT COUNT(*)::BIGINT FROM _pyf7tx WHERE id = $1", [3]
    )
    assert cnt == 1


@pytest.mark.asyncio
async def test_explicit_rollback_discards(session) -> None:
    tx = await session.begin()
    await tx.execute_sql("INSERT INTO _pyf7tx (id, val) VALUES ($1, $2)", [4, "d"])
    await tx.rollback()
    cnt = await session.execute_scalar(
        "SELECT COUNT(*)::BIGINT FROM _pyf7tx WHERE id = $1", [4]
    )
    assert cnt == 0


@pytest.mark.asyncio
async def test_methods_on_closed_transaction_raise(session) -> None:
    tx = await session.begin()
    await tx.commit()
    with pytest.raises(RuntimeError, match="non attiva"):
        await tx.execute_sql("SELECT 1")


# --------------------------- portable AST in tx ---------------------------


@pytest.mark.asyncio
async def test_builders_work_inside_async_transaction(session) -> None:
    async with await session.begin() as tx:
        new = await tx.insert("_pyf7tx").values(id=10, val="ten").returning("id").one()
        assert new["id"] == 10

        table = p.table("_pyf7tx", "id")
        statement = p.select(table.c.id).where(table.c.id == p.bind("identity", p.BindType.INTEGER))
        assert (await tx.execute(statement, {"identity": 10})).scalar_one() == 10

        row = await tx.select("_pyf7tx").columns("val").where_eq("id", 10).one()
        assert row.as_dict() == {"val": "ten"}

        await tx.update("_pyf7tx").set(val="TEN").where_eq("id", 10).execute()
        val = await tx.select("_pyf7tx").columns("val").where_eq("id", 10).scalar()
        assert val == "TEN"

        await tx.delete("_pyf7tx").where_eq("id", 10).execute()
        assert await tx.select("_pyf7tx").where_eq("id", 10).one() is None

        target = p.table("_pyf7tx", "id", "val")
        inserted = await tx.execute(
            p.insert(target)
            .values(id=p.bind("id", p.BindType.INTEGER), val=p.bind("value", p.BindType.STRING))
            .returning(target.c.id),
            {"id": 11, "value": "eleven"},
        )
        assert inserted.scalar_one() == 11
        updated = await tx.execute(
            p.update(target)
            .values(val=p.bind("value", p.BindType.STRING))
            .where(target.c.id == p.bind("id", p.BindType.INTEGER)),
            {"value": "ELEVEN", "id": 11},
        )
        assert updated.affected_rows == 1
        upserted = await tx.execute(
            p.upsert(target)
            .values(id=p.bind("id", p.BindType.INTEGER), val=p.bind("insert_value", p.BindType.STRING))
            .on_conflict(target.c.id)
            .set(val=p.bind("update_value", p.BindType.STRING)),
            {"id": 11, "insert_value": "ignored", "update_value": "UPSERTED"},
        )
        assert upserted.affected_rows == 1
        deleted = await tx.execute(
            p.delete(target)
            .where(target.c.id == p.bind("id", p.BindType.INTEGER))
            .returning(target.c.id),
            {"id": 11},
        )
        assert deleted.scalar_one() == 11


# --------------------------- savepoints ---------------------------


@pytest.mark.asyncio
async def test_savepoint_rollback_preserves_prior_statements(session) -> None:
    async with await session.begin() as tx:
        await tx.execute_sql("INSERT INTO _pyf7tx (id, val) VALUES ($1, $2)", [20, "kept"])
        await tx.savepoint("sp1")
        await tx.execute_sql("INSERT INTO _pyf7tx (id, val) VALUES ($1, $2)", [21, "risky"])
        await tx.rollback_to_savepoint("sp1")
        await tx.release_savepoint("sp1")
    cnt = await session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf7tx")
    assert cnt == 1


# --------------------------- isolation ---------------------------


@pytest.mark.asyncio
async def test_begin_with_serializable_isolation(session) -> None:
    async with await session.begin(isolation="serializable") as tx:
        level = await tx.execute_scalar("SHOW transaction_isolation")
        assert level == "serializable"


@pytest.mark.asyncio
async def test_begin_with_invalid_isolation_raises_value_error(session) -> None:
    with pytest.raises(ValueError, match="sconosciuto"):
        await session.begin(isolation="bogus")


@pytest.mark.asyncio
async def test_begin_with_read_only_rejects_write(session) -> None:
    with pytest.raises(p.PlenoraError):
        async with await session.begin(read_only=True) as tx:
            await tx.execute_sql("INSERT INTO _pyf7tx (id, val) VALUES ($1, $2)", [50, "x"])
