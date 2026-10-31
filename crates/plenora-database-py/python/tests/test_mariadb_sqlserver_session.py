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


def test_sqlserver_sync_exact_temporal_and_duplicate_names() -> None:
    session = connect_sqlserver_reference()
    try:
        rows = session.query_sql("SELECT CAST('2026-10-31T12:34:56.1234567' AS datetime2(7)) AS T")
        assert rows[0]["T"] == "2026-10-31T12:34:56.123456700"
        for offset, utc in [("+02:00", "2026-10-31T10:34:56"),
                            ("-05:30", "2026-10-31T18:04:56"),
                            ("+00:00", "2026-10-31T12:34:56"),
                            ("+14:00", "2026-10-30T22:34:56"),
                            ("-12:00", "2026-11-01T00:34:56")]:
            sql = (
                "SELECT v AS T, CONVERT(varchar(33), "
                "CAST(SWITCHOFFSET(v, '+00:00') AS datetime2(7)), 126) AS oracle_utc "
                f"FROM (VALUES (CAST('2026-10-31T12:34:56.1234567{offset}' "
                "AS datetimeoffset(7)))) AS source(v)"
            )
            rows = session.query_sql(sql)
            # Confronto testuale: datetime Python troncherebbe la settima cifra.
            assert rows[0]["T"] == utc + ".123456700+00:00"
            assert rows[0]["oracle_utc"] == utc + ".1234567"

        with pytest.raises(p.PlenoraDataMappingError, match="alias"):
            session.query_sql("SELECT 1 AS X, 2 AS X")
        assert session.query_sql("SELECT 1 AS A, 2 AS B") == [{"A": 1, "B": 2}]
    finally:
        session.close()


@pytest.mark.asyncio
async def test_sqlserver_async_exact_temporal_and_duplicate_names() -> None:
    session = await aconnect_sqlserver_reference()
    try:
        rows = await session.query_sql("SELECT CAST('2026-10-31T12:34:56.1234567' AS datetime2(7)) AS T")
        assert rows[0]["T"] == "2026-10-31T12:34:56.123456700"
        for offset, utc in [("+02:00", "2026-10-31T10:34:56"),
                            ("-05:30", "2026-10-31T18:04:56"),
                            ("+00:00", "2026-10-31T12:34:56"),
                            ("+14:00", "2026-10-30T22:34:56"),
                            ("-12:00", "2026-11-01T00:34:56")]:
            sql = (
                "SELECT v AS T, CONVERT(varchar(33), "
                "CAST(SWITCHOFFSET(v, '+00:00') AS datetime2(7)), 126) AS oracle_utc "
                f"FROM (VALUES (CAST('2026-10-31T12:34:56.1234567{offset}' "
                "AS datetimeoffset(7)))) AS source(v)"
            )
            rows = await session.query_sql(sql)
            # Confronto testuale: datetime Python troncherebbe la settima cifra.
            assert rows[0]["T"] == utc + ".123456700+00:00"
            assert rows[0]["oracle_utc"] == utc + ".1234567"

        with pytest.raises(p.PlenoraDataMappingError, match="alias"):
            await session.query_sql("SELECT 1 AS X, 2 AS X")
        assert await session.query_sql("SELECT 1 AS A, 2 AS B") == [{"A": 1, "B": 2}]
    finally:
        session.close()


def test_mariadb_sync_duplicate_result_names_require_aliases() -> None:
    from ._harness import connect_mariadb_reference
    import plenora_database as database

    session = connect_mariadb_reference()
    try:
        with pytest.raises(database.PlenoraDataMappingError, match="alias"):
            session.query_sql("SELECT 1 AS x, 2 AS x")
        rows = session.query_sql("SELECT 1 AS a, 2 AS b")
        assert list(rows[0].values()) == [1, 2]
    finally:
        session.close()


@pytest.mark.asyncio
async def test_mariadb_async_duplicate_result_names_require_aliases() -> None:
    from ._harness import aconnect_mariadb_reference
    import plenora_database as database

    session = await aconnect_mariadb_reference()
    try:
        with pytest.raises(database.PlenoraDataMappingError, match="alias"):
            await session.query_sql("SELECT 1 AS x, 2 AS x")
        rows = await session.query_sql("SELECT 1 AS a, 2 AS b")
        assert list(rows[0].values()) == [1, 2]
    finally:
        session.close()
