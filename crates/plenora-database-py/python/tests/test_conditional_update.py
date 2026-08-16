"""F4-2 — Test live per conditional_update (sync + async)."""
from __future__ import annotations

import os

import pytest
import pytest_asyncio

import plenora_database as p

from ._harness import aconnect_postgres, connect_postgres, postgres_dsn_or_skip


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.execute("DROP TABLE IF EXISTS _pyf42_opt")
    s.execute(
        "CREATE TABLE _pyf42_opt ("
        " id INT PRIMARY KEY, "
        " value TEXT NOT NULL, "
        " version INT NOT NULL DEFAULT 1)"
    )
    try:
        yield s
    finally:
        try:
            s.execute("DROP TABLE IF EXISTS _pyf42_opt")
        finally:
            s.close()


# ---------------- Sync ----------------


def test_conditional_update_matching_version_succeeds(session) -> None:
    session.insert("_pyf42_opt").values(id=1, value="v0", version=1).execute()
    with session.begin() as tx:
        # Nessuna eccezione = success (n == 1 matcha expected).
        tx.conditional_update(
            update_sql=(
                "UPDATE _pyf42_opt SET value = $1, version = $2 "
                "WHERE id = $3 AND version = $4"
            ),
            update_params=["v1", 2, 1, 1],
            expected_affected_rows=1,
        )
    row = session.select("_pyf42_opt").columns("value", "version").where_eq("id", 1).one()
    assert row == {"value": "v1", "version": 2}


def test_conditional_update_stale_version_raises_conflict(session) -> None:
    session.insert("_pyf42_opt").values(id=2, value="v0", version=1).execute()
    with session.begin() as tx:
        with pytest.raises(p.PlenoraConcurrentModificationError):
            # Provo update con version=99 (stale) — mismatch senza probe →
            # ConcurrentModification.
            tx.conditional_update(
                update_sql=(
                    "UPDATE _pyf42_opt SET value = $1, version = $2 "
                    "WHERE id = $3 AND version = $4"
                ),
                update_params=["v_new", 2, 2, 99],
            )


def test_conditional_update_missing_key_with_probe_raises_notfound(session) -> None:
    with session.begin() as tx:
        with pytest.raises(p.PlenoraNotFoundError):
            # ID 9999 non esiste. Con key_probe che conferma l'assenza →
            # NotFound (invece di ConcurrentModification).
            tx.conditional_update(
                update_sql=(
                    "UPDATE _pyf42_opt SET value = $1 "
                    "WHERE id = $2 AND version = $3"
                ),
                update_params=["x", 9999, 1],
                key_probe_sql="SELECT 1 FROM _pyf42_opt WHERE id = $1 LIMIT 1",
                key_probe_params=[9999],
            )


def test_conditional_update_missing_key_without_probe_defaults_to_conflict(
    session,
) -> None:
    with session.begin() as tx:
        with pytest.raises(p.PlenoraConcurrentModificationError):
            # Senza probe, il default conservativo classifica come
            # ConcurrentModification anche se la chiave è assente.
            tx.conditional_update(
                update_sql=(
                    "UPDATE _pyf42_opt SET value = $1 "
                    "WHERE id = $2 AND version = $3"
                ),
                update_params=["x", 8888, 1],
            )


def test_conditional_update_key_exists_but_stale_with_probe_raises_conflict(
    session,
) -> None:
    session.insert("_pyf42_opt").values(id=3, value="orig", version=1).execute()
    with session.begin() as tx:
        with pytest.raises(p.PlenoraConcurrentModificationError):
            tx.conditional_update(
                update_sql=(
                    "UPDATE _pyf42_opt SET value = $1, version = $2 "
                    "WHERE id = $3 AND version = $4"
                ),
                update_params=["new", 2, 3, 99],  # version stale
                key_probe_sql="SELECT 1 FROM _pyf42_opt WHERE id = $1 LIMIT 1",
                key_probe_params=[3],  # chiave esiste
            )


# ---------------- Async ----------------


@pytest_asyncio.fixture(name="asession")
async def _asession():
    dsn = postgres_dsn_or_skip()
    s = await aconnect_postgres(dsn)
    await s.execute("DROP TABLE IF EXISTS _pyf42_opt_async")
    await s.execute(
        "CREATE TABLE _pyf42_opt_async ("
        " id INT PRIMARY KEY, value TEXT NOT NULL, version INT NOT NULL DEFAULT 1)"
    )
    try:
        yield s
    finally:
        try:
            await s.execute("DROP TABLE IF EXISTS _pyf42_opt_async")
        finally:
            s.close()


@pytest.mark.asyncio
async def test_async_conditional_update_success(asession) -> None:
    await asession.insert("_pyf42_opt_async").values(id=1, value="v0", version=1).execute()
    async with await asession.begin() as tx:
        await tx.conditional_update(
            update_sql=(
                "UPDATE _pyf42_opt_async SET value=$1, version=$2 "
                "WHERE id=$3 AND version=$4"
            ),
            update_params=["v1", 2, 1, 1],
        )
    v = await asession.execute_scalar(
        "SELECT value FROM _pyf42_opt_async WHERE id=$1", [1]
    )
    assert v == "v1"


@pytest.mark.asyncio
async def test_async_conditional_update_stale_raises_conflict(asession) -> None:
    await asession.insert("_pyf42_opt_async").values(id=2, value="v0", version=1).execute()
    async with await asession.begin() as tx:
        with pytest.raises(p.PlenoraConcurrentModificationError):
            await tx.conditional_update(
                update_sql=(
                    "UPDATE _pyf42_opt_async SET value=$1 "
                    "WHERE id=$2 AND version=$3"
                ),
                update_params=["x", 2, 99],
            )


@pytest.mark.asyncio
async def test_async_conditional_update_missing_with_probe_raises_notfound(
    asession,
) -> None:
    async with await asession.begin() as tx:
        with pytest.raises(p.PlenoraNotFoundError):
            await tx.conditional_update(
                update_sql=(
                    "UPDATE _pyf42_opt_async SET value=$1 "
                    "WHERE id=$2 AND version=$3"
                ),
                update_params=["x", 9999, 1],
                key_probe_sql=(
                    "SELECT 1 FROM _pyf42_opt_async WHERE id=$1 LIMIT 1"
                ),
                key_probe_params=[9999],
            )
