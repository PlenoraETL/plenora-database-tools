"""Parita minima live delle due factory prima assenti dal gate del SDK."""

from __future__ import annotations

import pytest

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
        assert session.capabilities["provider"] == "mariadb"
        assert session.capabilities["transactions"]["single_transaction"] is True
        assert isinstance(session.inspect.catalogs(), list)
        assert isinstance(session.inspect.schemas(), list)
        session.execute_ddl(f"DROP TABLE IF EXISTS {table}")
        session.execute_ddl(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, label VARCHAR(32) NOT NULL)"
        )
        assert session.execute(
            f"INSERT INTO {table} (id, label) VALUES (?, ?)", [1, "maria"]
        ) == 1
        assert session.execute_scalar(f"SELECT label FROM {table} WHERE id = ?", [1]) == "maria"
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
        assert session.capabilities["provider"] == "mariadb"
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
        assert session.capabilities["provider"] == "sqlserver"
        assert session.capabilities["transactions"]["single_transaction"] is True
        assert isinstance(session.inspect.catalogs(), list)
        assert "plenora_test" in session.inspect.schemas()
        session.execute_ddl(f"DROP TABLE IF EXISTS {qualified}")
        session.execute_ddl(
            f"CREATE TABLE {qualified} ([id] bigint NOT NULL PRIMARY KEY, "
            "[label] nvarchar(32) NOT NULL)"
        )
        assert session.execute(
            f"INSERT INTO {qualified} ([id], [label]) VALUES (@P1, @P2)",
            [1, "tds"],
        ) == 1
        assert session.execute_scalar(
            f"SELECT [label] FROM {qualified} WHERE [id] = @P1", [1]
        ) == "tds"
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
        assert session.capabilities["provider"] == "sqlserver"
        assert isinstance(await session.inspect.catalogs(), list)
        await session.execute_ddl(f"DROP TABLE IF EXISTS {qualified}")
        await session.execute_ddl(f"CREATE TABLE {qualified} ([id] bigint NOT NULL PRIMARY KEY)")
        assert await session.execute_scalar("SELECT CAST(42 AS bigint)") == 42
    finally:
        try:
            await session.execute_ddl(f"DROP TABLE IF EXISTS {qualified}")
        finally:
            session.close()
