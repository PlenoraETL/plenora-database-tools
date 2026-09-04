"""Parita minima live delle due factory prima assenti dal gate del SDK."""

from __future__ import annotations

import pytest

import plenora_database as p

from ._harness import (
    aconnect_mariadb_reference,
    aconnect_sqlserver_reference,
    connect_mariadb_reference,
    connect_sqlserver_reference,
)


def test_mariadb_sync_capabilities_inspect_ddl_and_transaction() -> None:
    session = connect_mariadb_reference()
    table = "_sdk_mariadb_surface"
    try:
        assert session.provider_capabilities["provider"] == "mariadb"
        assert session.provider_capabilities["transactions"]["single_transaction"] is True
        assert isinstance(session.inspect.catalogs(), list)
        assert isinstance(session.inspect.schemas(), list)
        session.execute_ddl(f"DROP TABLE IF EXISTS {table}")
        session.execute_ddl(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, label VARCHAR(32) NOT NULL)"
        )
        inserted = session.execute_sql(
            f"INSERT INTO {table} (id, label) VALUES (?, ?)", [1, "maria"]
        )
        assert inserted.affected_rows == 1
        assert session.execute_scalar(f"SELECT label FROM {table} WHERE id = ?", [1]) == "maria"
        target = p.table(table, "id", "label")
        returned = session.execute(
            p.insert(target)
            .values(id=p.bind("id", p.BindType.INTEGER), label=p.bind("label", p.BindType.STRING))
            .returning(target.c.id),
            {"id": 2, "label": "core-v3"},
        )
        assert returned.scalar_one() == 2
        upserted = session.execute(
            p.upsert(target)
            .values(id=p.bind("id", p.BindType.INTEGER), label=p.bind("insert_label", p.BindType.STRING))
            .on_conflict(target.c.id)
            .set(label=p.bind("update_label", p.BindType.STRING)),
            {"id": 2, "insert_label": "ignored", "update_label": "UPSERTED"},
        )
        assert upserted.affected_rows is not None and upserted.affected_rows >= 1
        deleted = session.execute(
            p.delete(target).where(target.c.id == p.bind("id", p.BindType.INTEGER)), {"id": 2}
        )
        assert deleted.affected_rows == 1
        assert session.inspect.describe("dataflow_test", table)["columns"]
    finally:
        try:
            session.execute_ddl(f"DROP TABLE IF EXISTS {table}")
        finally:
            session.close()


@pytest.mark.asyncio
async def test_mariadb_async_capabilities_inspect_and_ddl() -> None:
    session = await aconnect_mariadb_reference()
    table = "_sdk_mariadb_async_surface"
    try:
        assert session.provider_capabilities["provider"] == "mariadb"
        assert isinstance(await session.inspect.catalogs(), list)
        await session.execute_ddl(f"DROP TABLE IF EXISTS {table}")
        await session.execute_ddl(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY)")
        assert await session.execute_scalar("SELECT CAST(42 AS SIGNED)") == 42
    finally:
        try:
            await session.execute_ddl(f"DROP TABLE IF EXISTS {table}")
        finally:
            session.close()


def test_sqlserver_sync_capabilities_inspect_ddl_and_transaction() -> None:
    session = connect_sqlserver_reference()
    table = "sdk_sqlserver_surface"
    qualified = f"[plenora_test].[{table}]"
    try:
        assert session.provider_capabilities["provider"] == "sqlserver"
        assert session.provider_capabilities["transactions"]["single_transaction"] is True
        assert isinstance(session.inspect.catalogs(), list)
        assert "plenora_test" in session.inspect.schemas()
        session.execute_ddl(f"DROP TABLE IF EXISTS {qualified}")
        session.execute_ddl(
            f"CREATE TABLE {qualified} ([id] bigint NOT NULL PRIMARY KEY, "
            "[label] nvarchar(32) NOT NULL)"
        )
        inserted = session.execute_sql(
            f"INSERT INTO {qualified} ([id], [label]) VALUES (@P1, @P2)",
            [1, "tds"],
        )
        assert inserted.affected_rows == 1
        assert session.execute_scalar(
            f"SELECT [label] FROM {qualified} WHERE [id] = @P1", [1]
        ) == "tds"
        target = p.table(table, "id", "label", schema="plenora_test")
        returned = session.execute(
            p.insert(target)
            .values(id=p.bind("id", p.BindType.INTEGER), label=p.bind("label", p.BindType.STRING))
            .returning(target.c.id),
            {"id": 2, "label": "core-v3"},
        )
        assert returned.scalar_one() == 2
        upserted = session.execute(
            p.upsert(target)
            .values(id=p.bind("id", p.BindType.INTEGER), label=p.bind("insert_label", p.BindType.STRING))
            .on_conflict(target.c.id)
            .set(label=p.bind("update_label", p.BindType.STRING)),
            {"id": 2, "insert_label": "ignored", "update_label": "UPSERTED"},
        )
        assert upserted.affected_rows is None
        assert session.execute_scalar(
            f"SELECT [label] FROM {qualified} WHERE [id] = @P1", [2]
        ) == "UPSERTED"
        deleted = session.execute(
            p.delete(target)
            .where(target.c.id == p.bind("id", p.BindType.INTEGER))
            .returning(target.c.id),
            {"id": 2},
        )
        assert deleted.scalar_one() == 2
        assert session.inspect.describe("plenora_test", table)["columns"]
    finally:
        try:
            session.execute_ddl(f"DROP TABLE IF EXISTS {qualified}")
        finally:
            session.close()


@pytest.mark.asyncio
async def test_sqlserver_async_capabilities_inspect_and_ddl() -> None:
    session = await aconnect_sqlserver_reference()
    table = "sdk_sqlserver_async_surface"
    qualified = f"[plenora_test].[{table}]"
    try:
        assert session.provider_capabilities["provider"] == "sqlserver"
        assert isinstance(await session.inspect.catalogs(), list)
        await session.execute_ddl(f"DROP TABLE IF EXISTS {qualified}")
        await session.execute_ddl(f"CREATE TABLE {qualified} ([id] bigint NOT NULL PRIMARY KEY)")
        assert await session.execute_scalar("SELECT CAST(42 AS bigint)") == 42
    finally:
        try:
            await session.execute_ddl(f"DROP TABLE IF EXISTS {qualified}")
        finally:
            session.close()
