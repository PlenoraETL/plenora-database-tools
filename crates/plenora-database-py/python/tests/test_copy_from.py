"""P3 v0.1.2 — Test live per bulk write via copy_from (sync + async).

Richiede pyarrow installato per costruire l'input.
"""
from __future__ import annotations

import os

import pytest
import pytest_asyncio

import plenora_database as p

pyarrow = pytest.importorskip("pyarrow")

DSN_ENV = "PLENORA_TEST_POSTGRES_DSN"


def _dsn_or_skip() -> str:
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        pytest.skip(f"live test: manca env {DSN_ENV}")
    return dsn


# ---------------- Sync ----------------


@pytest.fixture(name="session")
def _session():
    dsn = _dsn_or_skip()
    s = p.connect(dsn)
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
    assert outcome["recovery"] is None

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
    dsn = _dsn_or_skip()
    s = await p.aconnect(dsn)
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
