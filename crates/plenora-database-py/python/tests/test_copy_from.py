"""P3 v0.1.2 — Test live per bulk write via copy_from (sync + async).

Richiede pyarrow installato per costruire l'input.
"""
from __future__ import annotations

import os

import pytest
import pytest_asyncio

import plenora_database as p

from ._harness import aconnect_postgres, connect_postgres, postgres_dsn_or_skip

pyarrow = pytest.importorskip("pyarrow")


# ---------------- Sync ----------------


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.execute("DROP TABLE IF EXISTS _pyp3_copy")
    s.execute(
        "CREATE TABLE _pyp3_copy ("
        " id BIGINT PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " amount INT NOT NULL)"
    )
    try:
        yield s
    finally:
        try:
            s.execute("DROP TABLE IF EXISTS _pyp3_copy")
        finally:
            s.close()


def _make_table(n: int) -> "pyarrow.Table":
    return pyarrow.table(
        {
            "id": pyarrow.array(list(range(1, n + 1)), type=pyarrow.int64()),
            "label": pyarrow.array([f"row-{i}" for i in range(1, n + 1)]),
            "amount": pyarrow.array(
                [i * 10 for i in range(1, n + 1)], type=pyarrow.int32()
            ),
        }
    )


def test_copy_from_append_pyarrow_table_returns_committed_outcome(session) -> None:
    tbl = _make_table(500)
    outcome = session.copy_from("public", "_pyp3_copy", tbl)
    assert outcome["status"] == "committed"
    assert outcome["provider"] == "postgres"
    assert outcome["rows"]["received"] == 500
    assert outcome["rows"]["confirmed"] == 500
    assert outcome["rows"]["inserted"] == 500
    assert outcome["rows"]["failed"] == 0
    # Un esito certo non porta `recovery`: la variante `committed` dello
    # schema ha `unevaluatedProperties: false` e non dichiara quel campo.
    # Il convertitore lo scriveva comunque, a `None`, e questa asserzione
    # consacrava la violazione.
    assert "recovery" not in outcome

    # Verifica lato DB
    count = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyp3_copy")
    assert count == 500


def test_copy_from_multiple_batches_all_land(session) -> None:
    tbl = _make_table(1000)
    # Suddivido in batches per verificare che multi-batch funziona.
    batches = tbl.to_batches(max_chunksize=250)
    assert len(batches) == 4
    outcome = session.copy_from("public", "_pyp3_copy", batches)
    assert outcome["rows"]["confirmed"] == 1000

    count = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyp3_copy")
    assert count == 1000


def test_copy_from_ipc_bytes_pass_through(session) -> None:
    import io
    import pyarrow.ipc as ipc

    tbl = _make_table(100)
    buf = io.BytesIO()
    with ipc.new_stream(buf, tbl.schema) as writer:
        for b in tbl.to_batches():
            writer.write_batch(b)
    ipc_bytes = buf.getvalue()

    outcome = session.copy_from("public", "_pyp3_copy", ipc_bytes)
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 100


def test_copy_from_mode_create_builds_table_from_arrow_schema(session) -> None:
    """v0.2.0 — mode='create' crea la tabella target dallo schema Arrow.

    Il provider Postgres genera CREATE TABLE dallo schema Arrow. Il target
    NON deve esistere già (o Conflict); il preflight `Create + !exists`
    procede, il write path esegue DDL + COPY in stessa transazione.
    """
    session.execute("DROP TABLE IF EXISTS _pyv020_create")
    try:
        tbl = pyarrow.table(
            {
                "id": pyarrow.array([1, 2, 3, 4, 5], type=pyarrow.int64()),
                "label": pyarrow.array(["a", "b", "c", "d", "e"]),
                "amount": pyarrow.array(
                    [10.5, 20.5, 30.5, 40.5, 50.5], type=pyarrow.float64()
                ),
            }
        )
        outcome = session.copy_from("public", "_pyv020_create", tbl, mode="create")
        assert outcome["status"] == "committed"
        assert outcome["rows"]["confirmed"] == 5

        # Verifica DDL applicato
        cols = session.execute_returning_rows(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='_pyv020_create' "
            "ORDER BY ordinal_position"
        )
        col_names = [c["column_name"] for c in cols]
        assert col_names == ["id", "label", "amount"]

        # Verifica dati landed
        count = session.execute_scalar(
            "SELECT COUNT(*)::BIGINT FROM _pyv020_create"
        )
        assert count == 5
    finally:
        session.execute("DROP TABLE IF EXISTS _pyv020_create")


def test_copy_from_mode_create_conflicts_if_target_exists(session) -> None:
    """mode='create' con target esistente deve restituire Conflict."""
    tbl = _make_table(3)
    # _pyp3_copy esiste (creata dalla fixture)
    with pytest.raises(p.PlenoraConflictError):
        session.copy_from("public", "_pyp3_copy", tbl, mode="create")


def test_copy_from_strict_policy_rejects_nullable_to_not_null(session) -> None:
    """Con mapping_policy='strict' il preflight boccia il pattern
    comune Arrow-nullable → PG NOT NULL (severity DataLoss).
    Il default 'compatible' invece lo tollera."""
    tbl = _make_table(3)  # pyarrow.Table con schema tutto nullable
    with pytest.raises(p.PlenoraDataMappingError):
        session.copy_from("public", "_pyp3_copy", tbl, mapping_policy="strict")


def test_copy_from_invalid_mapping_policy_raises_invalid_plan(session) -> None:
    tbl = _make_table(3)
    with pytest.raises(p.PlenoraInvalidPlanError):
        session.copy_from(
            "public", "_pyp3_copy", tbl, mapping_policy="not_a_policy"
        )


def test_copy_from_invalid_mode_raises_invalid_plan(session) -> None:
    tbl = _make_table(5)
    with pytest.raises(p.PlenoraInvalidPlanError):
        session.copy_from("public", "_pyp3_copy", tbl, mode="not_a_mode")


def test_copy_from_invalid_profile_raises_invalid_plan(session) -> None:
    tbl = _make_table(5)
    with pytest.raises(p.PlenoraInvalidPlanError):
        session.copy_from(
            "public", "_pyp3_copy", tbl, transaction_profile="not_a_profile"
        )


def test_copy_from_wrong_type_raises_type_error(session) -> None:
    with pytest.raises(TypeError):
        session.copy_from("public", "_pyp3_copy", 123)  # type: ignore[arg-type]


def test_copy_from_empty_iterable_raises_value_error(session) -> None:
    with pytest.raises(ValueError):
        session.copy_from("public", "_pyp3_copy", [])


# ---------------- Async ----------------


@pytest_asyncio.fixture(name="asession")
async def _asession():
    dsn = postgres_dsn_or_skip()
    s = await aconnect_postgres(dsn)
    await s.execute("DROP TABLE IF EXISTS _pyp3_acopy")
    await s.execute(
        "CREATE TABLE _pyp3_acopy ("
        " id BIGINT PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " amount INT NOT NULL)"
    )
    try:
        yield s
    finally:
        try:
            await s.execute("DROP TABLE IF EXISTS _pyp3_acopy")
        finally:
            s.close()


@pytest.mark.asyncio
async def test_acopy_from_append_returns_awaitable_outcome(asession) -> None:
    tbl = _make_table(300)
    outcome = await asession.acopy_from("public", "_pyp3_acopy", tbl)
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 300

    count = await asession.execute_scalar(
        "SELECT COUNT(*)::BIGINT FROM _pyp3_acopy"
    )
    assert count == 300
