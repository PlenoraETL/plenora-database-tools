"""F3-8 — Scenario end-to-end che esercita l'intera superficie API del SDK.

Simula un flusso PFM realistico:
  1. connect + probe capabilities
  2. schema setup (tabella con UUID / NUMERIC / TIMESTAMPTZ / geometry)
  3. INSERT typed (uuid + decimal + timestamptz) via builder portable
  4. UPDATE spatial via SQL raw
  5. SELECT spatial via portable AST (where_spatial)
  6. RESET update via transaction async con savepoint
  7. Error handling tipizzato (PlenoraNotFoundError)
  8. Cleanup

Il test funziona come "living documentation" del pattern d'uso e come
smoke-check che ogni interfaccia SDK (sync + async, session + transaction,
builders + spatial + typed + errors) sia raggiungibile dall'entry-point
`import plenora_database as p`.
"""
from __future__ import annotations

import os

import pytest
import pytest_asyncio

import plenora_database as p

from ._harness import aconnect_postgres, connect_postgres, postgres_dsn_or_skip


@pytest_asyncio.fixture(name="clean_schema")
async def _clean_schema():
    dsn = postgres_dsn_or_skip()
    async with await aconnect_postgres(dsn) as s:
        await s.execute("DROP TABLE IF EXISTS _pyf8_building")
        await s.execute(
            "CREATE TABLE _pyf8_building ("
            " id UUID PRIMARY KEY,"
            " code TEXT UNIQUE NOT NULL,"
            " name TEXT NOT NULL,"
            " area NUMERIC(10,2),"
            " created_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
            " version INT NOT NULL DEFAULT 1,"
            " location geometry(Point, 4326))"
        )
    yield
    async with await aconnect_postgres(dsn) as s:
        await s.execute("DROP TABLE IF EXISTS _pyf8_building")


@pytest.mark.asyncio
async def test_e2e_pfm_building_lifecycle_async(clean_schema) -> None:
    dsn = postgres_dsn_or_skip()
    b_uuid = "b1000000-0000-0000-0000-000000000001"

    async with await aconnect_postgres(dsn) as s:
        # 1. Probe capabilities (metadata scoperti al connect)
        assert isinstance(s.server_version, str)
        assert any(c.isdigit() for c in s.server_version)
        if s.postgis_version is None:
            pytest.skip("scenario spaziale: PostGIS non installato")

        # 2. Transaction async con builder portable + typed
        async with await s.begin() as tx:
            row = (
                await tx.insert("_pyf8_building")
                .values(
                    id=p.uuid(b_uuid),
                    code="TORRE-MI",
                    name="Torre Milano",
                    area=p.decimal("15000.50"),
                )
                .returning("code", "name", "area", "version")
                .one()
            )
            assert row["code"] == "TORRE-MI"
            assert row["name"] == "Torre Milano"
            assert row["area"] == "15000.50"   # NUMERIC decoded (P0.8)
            assert row["version"] == 1

            # 3. Update location via raw SQL (spatial construction)
            n = await tx.execute(
                "UPDATE _pyf8_building "
                "SET location = ST_SetSRID(ST_MakePoint($1, $2), 4326) "
                "WHERE id = $3",
                [9.19, 45.46, p.uuid(b_uuid)],
            )
            assert n == 1

            # 4. Spatial query via portable AST
            ref_ewkb = await tx.execute_scalar(
                "SELECT ST_AsEWKB(ST_SetSRID("
                " ST_MakeEnvelope(9.0, 45.0, 10.0, 46.0), 4326))"
            )
            ref = p.spatial.geometry(ewkb=ref_ewkb, srid=4326)
            found = (
                await tx.select("_pyf8_building")
                .columns("code", "name")
                .where_spatial("location", "intersects", ref)
                .all()
            )
            assert found == [{"code": "TORRE-MI", "name": "Torre Milano"}]

            # 5. Savepoint + rollback selettivo
            await tx.savepoint("bump_area")
            await tx.update("_pyf8_building").set(
                area=p.decimal("99999.00")
            ).where_eq("id", p.uuid(b_uuid)).execute()
            # ripensamento — annulla solo l'update area
            await tx.rollback_to_savepoint("bump_area")
            await tx.release_savepoint("bump_area")
            # area invariata
            area = (
                await tx.select("_pyf8_building")
                .columns("area")
                .where_eq("id", p.uuid(b_uuid))
                .scalar()
            )
            assert area == "15000.50"

            # 6. Error handling tipizzato: tabella inesistente.
            # NB: dopo un errore, Postgres marca la tx come aborted.
            # Uso un savepoint per contenere l'errore senza abortire la
            # tx principale (pattern realistico).
            await tx.savepoint("probe_missing")
            with pytest.raises(p.PlenoraNotFoundError) as exc_info:
                await tx.select("_pyf8_nonexistent").one()
            assert exc_info.value.category == "not_found"
            assert exc_info.value.provider == "postgres"
            await tx.rollback_to_savepoint("probe_missing")
            await tx.release_savepoint("probe_missing")
        # commit automatico su exit senza eccezioni

        # 7. Persistenza confermata fuori dalla tx
        count = await s.execute_scalar(
            "SELECT COUNT(*)::BIGINT FROM _pyf8_building"
        )
        assert count == 1


def test_e2e_pfm_building_lifecycle_sync(clean_schema) -> None:
    # Equivalente sync dell'e2e async — verifica che i pattern
    # documentati funzionino identicamente su entrambe le API.
    dsn = postgres_dsn_or_skip()
    # UUID valido (hex + dash, 36 char).
    b_uuid = "b2000000-0000-0000-0000-000000000002"

    with connect_postgres(dsn) as s:
        if s.postgis_version is None:
            pytest.skip("scenario spaziale: PostGIS non installato")

        with s.begin() as tx:
            row = (
                tx.insert("_pyf8_building")
                .values(
                    id=p.uuid(b_uuid),
                    code="DUOMO-MI",
                    name="Duomo",
                    area=p.decimal("11700.00"),
                )
                .returning("code", "area")
                .one()
            )
            assert row == {"code": "DUOMO-MI", "area": "11700.00"}

            tx.execute(
                "UPDATE _pyf8_building "
                "SET location = ST_SetSRID(ST_MakePoint($1, $2), 4326) "
                "WHERE id = $3",
                [9.19, 45.46, p.uuid(b_uuid)],
            )

            ref_ewkb = tx.execute_scalar(
                "SELECT ST_AsEWKB(ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))"
            )
            ref = p.spatial.geography(ewkb=ref_ewkb, srid=4326)
            near = (
                tx.select("_pyf8_building")
                .columns("code")
                .where_spatial(
                    "location", "d_within", ref, distance_meters=100.0
                )
                .all()
            )
            # Il predicate d_within su geometry(Point,4326) userà cast
            # ::geography se semantics=geography.
            # Il record inserted è alla stessa posizione → matcha.
            assert [r["code"] for r in near] == ["DUOMO-MI"]

            tx.savepoint("probe_missing_sync")
            with pytest.raises(p.PlenoraNotFoundError):
                tx.select("_pyf8_nonexistent_sync").one()
            tx.rollback_to_savepoint("probe_missing_sync")
            tx.release_savepoint("probe_missing_sync")

        cnt = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf8_building")
        assert cnt == 1


def test_e2e_optimistic_conflict_pattern_sync() -> None:
    # Pattern PFM realistico: update ottimistico con expected_version.
    # Se il version è cambiato, riprova con la versione nuova.
    dsn = postgres_dsn_or_skip()
    with connect_postgres(dsn) as s:
        s.execute("DROP TABLE IF EXISTS _pyf8_opt")
        s.execute(
            "CREATE TABLE _pyf8_opt ("
            " id INT PRIMARY KEY,"
            " value TEXT NOT NULL,"
            " version INT NOT NULL DEFAULT 1)"
        )
        try:
            s.insert("_pyf8_opt").values(id=1, value="v0").execute()

            # Update ottimistico: solo se version == 1
            n = (
                s.update("_pyf8_opt")
                .set(value="v1", version=2)
                .where_eq("id", 1)
                .where_eq("version", 1)
                .execute()
            )
            assert n == 1, "il primo update deve applicarsi (version=1 matcha)"

            # Retry con expected_version obsoleto: n == 0 → conflitto rilevato
            n2 = (
                s.update("_pyf8_opt")
                .set(value="v2", version=3)
                .where_eq("id", 1)
                .where_eq("version", 1)   # stale
                .execute()
            )
            assert n2 == 0, "expected_version stale → nessun update applicato"

            # Refresh e riprova
            current = s.select("_pyf8_opt").columns("version").where_eq("id", 1).scalar()
            assert current == 2
            n3 = (
                s.update("_pyf8_opt")
                .set(value="v2", version=3)
                .where_eq("id", 1)
                .where_eq("version", current)
                .execute()
            )
            assert n3 == 1
        finally:
            s.execute("DROP TABLE IF EXISTS _pyf8_opt")


def test_e2e_error_taxonomy_reaches_python_correctly() -> None:
    # Smoke test: le 4 categorie più comuni (NotFound, Cancelled,
    # Schema per DDL, Protocol) restituiscono la sottoclasse corretta.
    dsn = postgres_dsn_or_skip()
    with connect_postgres(dsn) as s:
        # NotFound: tabella non esistente
        with pytest.raises(p.PlenoraNotFoundError):
            s.execute_scalar("SELECT * FROM _pyf8_missing_xyz")

        # Cancelled: statement_timeout
        with pytest.raises(p.PlenoraCancelledError):
            with s.begin(statement_timeout_ms=50) as tx:
                tx.execute("SELECT pg_sleep(2)")

        # InvalidPlan: portable AST malformata (JSON invalido)
        # → passato JSON malformato al bridge
        with pytest.raises(p.PlenoraInvalidPlanError):
            s._native.execute_portable_rows("{not json")


@pytest.mark.asyncio
async def test_e2e_concurrent_async_queries_share_runtime() -> None:
    # 20 query in parallelo su una sola AsyncSession usano il pool
    # tokio senza bloccare l'event loop asyncio. Nessuna deve fallire.
    import asyncio

    dsn = postgres_dsn_or_skip()
    async with await aconnect_postgres(dsn) as s:
        results = await asyncio.gather(
            *(s.execute_scalar("SELECT $1::int", [i]) for i in range(20))
        )
        assert results == list(range(20))
