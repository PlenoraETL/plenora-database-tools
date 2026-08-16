"""F3-5 — Test integrazione live per Transaction context + savepoints."""
from __future__ import annotations

import os

import pytest

import plenora_database

from ._harness import connect_postgres, postgres_dsn_or_skip


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.execute("DROP TABLE IF EXISTS _pyf5_tx")
    s.execute(
        "CREATE TABLE _pyf5_tx (id INT PRIMARY KEY, val TEXT NOT NULL)"
    )
    try:
        yield s
    finally:
        try:
            s.execute("DROP TABLE IF EXISTS _pyf5_tx")
        finally:
            s.close()


# --------------------------- lifecycle ---------------------------


def test_begin_returns_active_transaction(session) -> None:
    tx = session.begin()
    assert tx.is_active is True
    tx.rollback()
    assert tx.is_active is False


def test_context_manager_commits_on_normal_exit(session) -> None:
    with session.begin() as tx:
        tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [1, "a"])
    # Dopo il with senza eccezioni: committato.
    cnt = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf5_tx")
    assert cnt == 1


def test_context_manager_rolls_back_on_exception(session) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        with session.begin() as tx:
            tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [2, "b"])
            raise RuntimeError("boom")
    # Dopo l'eccezione: rollback-ato.
    cnt = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf5_tx")
    assert cnt == 0


def test_explicit_commit_persists(session) -> None:
    tx = session.begin()
    tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [3, "c"])
    tx.commit()
    assert tx.is_active is False
    cnt = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf5_tx WHERE id = $1", [3])
    assert cnt == 1


def test_explicit_rollback_discards(session) -> None:
    tx = session.begin()
    tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [4, "d"])
    tx.rollback()
    assert tx.is_active is False
    cnt = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf5_tx WHERE id = $1", [4])
    assert cnt == 0


def test_methods_on_closed_transaction_raise(session) -> None:
    tx = session.begin()
    tx.commit()
    with pytest.raises(RuntimeError, match="non attiva"):
        tx.execute("SELECT 1")
    with pytest.raises(RuntimeError, match="non attiva"):
        tx.commit()
    with pytest.raises(RuntimeError, match="non attiva"):
        tx.rollback()


def test_context_manager_exit_after_explicit_commit_noop(session) -> None:
    # Se l'utente committa esplicitamente dentro il with, __exit__ deve
    # essere no-op (non tentare un secondo commit).
    with session.begin() as tx:
        tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [5, "e"])
        tx.commit()
        assert tx.is_active is False
    # Nessun errore, e la riga è persistita.
    cnt = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf5_tx WHERE id = $1", [5])
    assert cnt == 1


# --------------------------- portable AST in tx ---------------------------


def test_builders_work_inside_transaction(session) -> None:
    with session.begin() as tx:
        new = tx.insert("_pyf5_tx").values(id=10, val="ten").returning("id").one()
        assert new["id"] == 10

        row = tx.select("_pyf5_tx").columns("val").where_eq("id", 10).one()
        assert row == {"val": "ten"}

        tx.update("_pyf5_tx").set(val="TEN").where_eq("id", 10).execute()
        assert tx.select("_pyf5_tx").columns("val").where_eq("id", 10).scalar() == "TEN"

        tx.delete("_pyf5_tx").where_eq("id", 10).execute()
        assert tx.select("_pyf5_tx").where_eq("id", 10).one() is None


# --------------------------- savepoints ---------------------------


def test_savepoint_rollback_preserves_prior_statements(session) -> None:
    with session.begin() as tx:
        tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [20, "kept"])
        tx.savepoint("sp1")
        tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [21, "risky"])
        tx.rollback_to_savepoint("sp1")
        tx.release_savepoint("sp1")
        # kept c'è, risky no.
        rows = tx.select("_pyf5_tx").columns("id").order_by("id").all()
        assert [r["id"] for r in rows] == [20]
    # Fuori dal with: solo kept persistito.
    cnt = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM _pyf5_tx")
    assert cnt == 1


def test_savepoint_nested_semantics(session) -> None:
    with session.begin() as tx:
        tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [30, "outer"])
        tx.savepoint("outer_sp")
        tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [31, "middle"])
        tx.savepoint("inner_sp")
        tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [32, "inner"])
        tx.rollback_to_savepoint("inner_sp")
        tx.release_savepoint("inner_sp")
        tx.release_savepoint("outer_sp")
        rows = tx.select("_pyf5_tx").columns("id").order_by("id").all()
        assert [r["id"] for r in rows] == [30, 31]


# --------------------------- isolation options ---------------------------


def test_begin_with_serializable_isolation(session) -> None:
    with session.begin(isolation="serializable") as tx:
        # Verifica lato server che l'isolation sia serializable.
        level = tx.execute_scalar("SHOW transaction_isolation")
        assert level == "serializable"


def test_begin_with_read_only(session) -> None:
    with session.begin(read_only=True) as tx:
        mode = tx.execute_scalar("SHOW transaction_read_only")
        assert mode == "on"
        # Un INSERT deve fallire in una tx read-only.
        with pytest.raises(RuntimeError):
            tx.execute("INSERT INTO _pyf5_tx (id, val) VALUES ($1, $2)", [99, "x"])


def test_begin_with_invalid_isolation_raises_value_error(session) -> None:
    with pytest.raises(ValueError, match="sconosciuto"):
        session.begin(isolation="bogus")


def test_begin_with_statement_timeout(session) -> None:
    # timeout 100 ms; SELECT pg_sleep(2) deve superarlo e fallire.
    with session.begin(statement_timeout_ms=100) as tx:
        with pytest.raises(RuntimeError):
            tx.execute("SELECT pg_sleep(2)")
        # Alla fine del with, tx potrebbe non essere più committabile;
        # il context manager fa best-effort rollback.
