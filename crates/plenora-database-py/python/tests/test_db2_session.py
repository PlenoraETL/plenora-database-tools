"""Parita live delle factory Db2 sync e async, in un file dedicato."""

from __future__ import annotations

import struct

import plenora_database as p
import pytest

from ._harness import (
    aconnect_db2_reference,
    connect_db2_reference,
    db2_config_or_skip,
)


def _db2_engine_config() -> p.EngineConfig:
    host, database, user, password, port, ca_path, tls_mode = db2_config_or_skip()
    return p.EngineConfig(
        "db2",
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        tls_ca=ca_path,
        tls_mode=tls_mode,
    )


def test_db2_sync_capabilities_catalog_transaction_and_portable_select() -> None:
    session = connect_db2_reference()
    try:
        assert session.capabilities["provider"] == "db2"
        assert session.capabilities["transactions"]["single_transaction"] is True
        assert "PLENORA" in session.inspect.catalogs()
        assert "PLENORA_TEST" in session.inspect.schemas()
        assert session.inspect.describe("PLENORA_TEST", "READ_PROBE")["columns"]
        assert session.execute_scalar(
            "SELECT LABEL FROM PLENORA_TEST.READ_PROBE WHERE ID = ?", [1]
        ) == "alpha"
        assert (
            session.select("READ_PROBE", "PLENORA_TEST")
            .columns("ID", "LABEL")
            .where_eq("ID", 1)
            .one()
            == {"ID": 1, "LABEL": "alpha"}
        )
    finally:
        session.close()


def test_db2_sync_spatial_capabilities_and_portable_predicate() -> None:
    session = connect_db2_reference()
    try:
        spatial = session.capabilities["spatial"]
        assert spatial["geometry"] is True
        assert spatial["read_wkb"] is True
        assert spatial["write_wkb"] is True
        assert spatial["requires_declared_crs"] is True
        assert spatial["spatial_index"] is False
        point = b"\x01" + struct.pack("<Idd", 1, 1.0, 2.0)
        reference = p.spatial.geometry(ewkb=point, srid=4326)
        rows = (
            session.select("SPATIAL_PROBE", "PLENORA_TEST")
            .columns("ID")
            .where_spatial("SHAPE", "intersects", reference)
            .order_by("ID")
            .all()
        )
        assert rows == [{"ID": 1}, {"ID": 2}, {"ID": 4}]
    finally:
        session.close()


@pytest.mark.asyncio
async def test_db2_async_capabilities_inspect_and_scalar_query() -> None:
    session = await aconnect_db2_reference()
    try:
        assert session.capabilities["provider"] == "db2"
        assert session.capabilities["spatial"]["geometry"] is True
        assert "PLENORA_TEST" in await session.inspect.schemas()
        assert await session.execute_scalar(
            "SELECT COUNT(*) FROM PLENORA_TEST.READ_PROBE"
        ) == 2
    finally:
        session.close()


def test_db2_engine_uses_the_core_lifecycle() -> None:
    with p.engine_from_url(_db2_engine_config()) as engine:
        assert engine.provider_kind == "db2"
        with engine.session() as session:
            assert session.execute_scalar(
                "SELECT CAST(7 AS BIGINT) FROM SYSIBM.SYSDUMMY1"
            ) == 7
            assert session.execute(
                p.select(
                    p.bind("answer", p.BindType.INTEGER).label("ANSWER")
                ),
                {"answer": 7},
            ).scalar_one() == 7
            tables = p.table(
                "READ_PROBE", "ID", "LABEL", schema="PLENORA_TEST"
            )
            statement = (
                p.select(tables.c.LABEL)
                .where(tables.c.ID == p.bind("identity", p.BindType.INTEGER))
                .limit(1)
            )
            assert session.execute(statement, {"identity": 1}).scalar_one() == "alpha"
            total = p.func.count()
            aggregate = (
                p.select(tables.c.LABEL, total.label("OBJECT_COUNT"))
                .group_by(tables.c.LABEL)
                .having(total >= total)
                .limit(1)
            )
            assert session.execute(aggregate).one()["OBJECT_COUNT"] >= 1
            ranked = p.select(
                p.func.count(tables.c.LABEL).over().label("POSITION")
            ).cte("RANKED")
            assert (
                session.execute(
                    p.select(ranked.c.POSITION).select_from(ranked).limit(1)
                ).scalar_one()
                >= 1
            )
            target = p.table("READ_PROBE", "ID", "LABEL", schema="PLENORA_TEST")
            transaction = session.begin()
            try:
                inserted = transaction.execute(
                    p.insert(target).values(
                        ID=p.bind("identity", p.BindType.INTEGER), LABEL=p.bind("label", p.BindType.STRING)
                    ),
                    {"identity": 90, "label": "core-v3"},
                )
                assert inserted.affected_rows == 1
                updated = transaction.execute(
                    p.update(target)
                    .values(LABEL=p.bind("label", p.BindType.STRING))
                    .where(target.c.ID == p.bind("identity", p.BindType.INTEGER)),
                    {"label": "CORE-V3", "identity": 90},
                )
                assert updated.affected_rows == 1
                upserted = transaction.execute(
                    p.upsert(target)
                    .values(ID=p.bind("identity", p.BindType.INTEGER), LABEL=p.bind("insert_label", p.BindType.STRING))
                    .on_conflict(target.c.ID)
                    .set(LABEL=p.bind("update_label", p.BindType.STRING)),
                    {
                        "identity": 90,
                        "insert_label": "ignored",
                        "update_label": "UPSERTED",
                    },
                )
                assert (
                    upserted.affected_rows is not None
                    and upserted.affected_rows >= 1
                )
                deleted = transaction.execute(
                    p.delete(target).where(target.c.ID == p.bind("identity", p.BindType.INTEGER)),
                    {"identity": 90},
                )
                assert deleted.affected_rows == 1
            finally:
                transaction.rollback()
        assert engine.statistics()["active_sessions"] == 0


@pytest.mark.asyncio
async def test_async_db2_engine_uses_the_core_lifecycle() -> None:
    async with await p.async_engine_from_url(_db2_engine_config()) as engine:
        assert engine.provider_kind == "db2"
        async with engine.session() as session:
            assert await session.execute_scalar(
                "SELECT CAST(8 AS BIGINT) FROM SYSIBM.SYSDUMMY1"
            ) == 8
            tables = p.table(
                "READ_PROBE", "ID", "LABEL", schema="PLENORA_TEST"
            ).alias("r")
            statement = (
                p.select(tables.c.LABEL)
                .where(tables.c.ID == p.bind("identity", p.BindType.INTEGER))
                .limit(1)
            )
            assert (
                await session.execute(statement, {"identity": 1})
            ).scalar_one() == "alpha"
        assert engine.statistics()["active_sessions"] == 0
