"""Parita live delle factory Db2 sync e async, in un file dedicato."""

from __future__ import annotations

import struct

import plenora_database as p
import pytest

from ._harness import aconnect_db2_reference, connect_db2_reference


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
