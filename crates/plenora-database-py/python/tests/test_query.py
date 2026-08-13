"""F3-4 — Test integrazione live per portable AST builder."""
from __future__ import annotations

import os

import pytest

import plenora_database

DSN_ENV = "PLENORA_TEST_POSTGRES_DSN"


def _dsn_or_skip() -> str:
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        pytest.skip(f"live test: manca env {DSN_ENV}")
    return dsn


@pytest.fixture(name="session")
def _session():
    dsn = _dsn_or_skip()
    s = plenora_database.connect(dsn)
    # Setup tabella condivisa: rimossa in teardown.
    s.execute("DROP TABLE IF EXISTS _pyf4_items")
    s.execute(
        "CREATE TABLE _pyf4_items ("
        " id BIGSERIAL PRIMARY KEY,"
        " code TEXT UNIQUE NOT NULL,"
        " label TEXT NOT NULL,"
        " qty INT NOT NULL DEFAULT 0)"
    )
    try:
        yield s
    finally:
        try:
            s.execute("DROP TABLE IF EXISTS _pyf4_items")
        finally:
            s.close()


def _seed(session, rows: list[dict]) -> None:
    session.insert("_pyf4_items").rows(rows).execute()


# -------------------------- SELECT --------------------------


def test_select_all_returns_list_of_dict(session) -> None:
    _seed(session, [
        {"code": "A", "label": "alpha", "qty": 1},
        {"code": "B", "label": "beta", "qty": 2},
    ])
    rows = session.select("_pyf4_items").columns("code", "qty").order_by("code").all()
    assert rows == [{"code": "A", "qty": 1}, {"code": "B", "qty": 2}]


def test_select_where_eq_narrows_result(session) -> None:
    _seed(session, [{"code": "A", "label": "x", "qty": 1}, {"code": "B", "label": "y", "qty": 2}])
    rows = session.select("_pyf4_items").columns("code").where_eq("code", "A").all()
    assert rows == [{"code": "A"}]


def test_select_one_returns_first_row_or_none(session) -> None:
    _seed(session, [{"code": "A", "label": "x", "qty": 1}])
    row = session.select("_pyf4_items").columns("code").where_eq("code", "A").one()
    assert row == {"code": "A"}
    none_row = session.select("_pyf4_items").columns("code").where_eq("code", "MISSING").one()
    assert none_row is None


def test_select_scalar_returns_first_cell(session) -> None:
    _seed(session, [{"code": "A", "label": "x", "qty": 42}])
    qty = session.select("_pyf4_items").columns("qty").where_eq("code", "A").scalar()
    assert qty == 42


def test_select_predicates_ne_lt_gte(session) -> None:
    _seed(session, [
        {"code": "A", "label": "x", "qty": 1},
        {"code": "B", "label": "y", "qty": 5},
        {"code": "C", "label": "z", "qty": 10},
    ])
    ne = session.select("_pyf4_items").columns("code").where_ne("code", "B").order_by("code").all()
    assert [r["code"] for r in ne] == ["A", "C"]
    lt = session.select("_pyf4_items").columns("code").where_lt("qty", 5).all()
    assert [r["code"] for r in lt] == ["A"]
    gte = session.select("_pyf4_items").columns("code").where_gte("qty", 5).order_by("code").all()
    assert [r["code"] for r in gte] == ["B", "C"]


def test_select_where_in_and_between(session) -> None:
    _seed(session, [
        {"code": "A", "label": "x", "qty": 1},
        {"code": "B", "label": "y", "qty": 5},
        {"code": "C", "label": "z", "qty": 10},
    ])
    in_result = session.select("_pyf4_items").columns("code").where_in("code", ["A", "C"]).order_by("code").all()
    assert [r["code"] for r in in_result] == ["A", "C"]
    between = session.select("_pyf4_items").columns("code").where_between("qty", 3, 8).all()
    assert [r["code"] for r in between] == ["B"]


def test_select_where_like_and_is_null(session) -> None:
    _seed(session, [
        {"code": "AAA", "label": "alpha", "qty": 1},
        {"code": "ABB", "label": "beta", "qty": 2},
    ])
    like = session.select("_pyf4_items").columns("code").where_like("code", "AA%").all()
    assert [r["code"] for r in like] == ["AAA"]
    # is_null: qty è NOT NULL per definizione → nessuna riga.
    empty = session.select("_pyf4_items").columns("code").where_is_null("qty").all()
    assert empty == []


def test_select_multi_where_chains_in_and(session) -> None:
    _seed(session, [
        {"code": "A", "label": "x", "qty": 1},
        {"code": "B", "label": "x", "qty": 5},
        {"code": "C", "label": "y", "qty": 10},
    ])
    rows = (
        session.select("_pyf4_items")
        .columns("code")
        .where_eq("label", "x")
        .where_lt("qty", 3)
        .all()
    )
    assert [r["code"] for r in rows] == ["A"]


def test_select_limit_bounds_the_result(session) -> None:
    _seed(session, [{"code": f"{i}", "label": "x", "qty": i} for i in range(10)])
    rows = session.select("_pyf4_items").columns("code").order_by("qty").limit(3).all()
    assert len(rows) == 3


# -------------------------- INSERT --------------------------


def test_insert_returning_id_returns_new_row(session) -> None:
    row = (
        session.insert("_pyf4_items")
        .values(code="X", label="new", qty=99)
        .returning("id", "code")
        .one()
    )
    assert row["code"] == "X"
    assert isinstance(row["id"], int) and row["id"] > 0


def test_insert_multi_row_all_returning(session) -> None:
    rows = (
        session.insert("_pyf4_items")
        .rows([
            {"code": "M1", "label": "m", "qty": 1},
            {"code": "M2", "label": "m", "qty": 2},
        ])
        .returning("code", "qty")
        .all()
    )
    assert rows == [{"code": "M1", "qty": 1}, {"code": "M2", "qty": 2}]


def test_insert_execute_without_returning_ignores_returning(session) -> None:
    # Nessun .returning() → execute() ritorna il conteggio.
    count = session.insert("_pyf4_items").values(code="E1", label="e", qty=0).execute()
    assert count == 1


# -------------------------- UPDATE --------------------------


def test_update_set_where_execute_returns_affected(session) -> None:
    _seed(session, [{"code": "A", "label": "x", "qty": 0}])
    n = session.update("_pyf4_items").set(qty=42).where_eq("code", "A").execute()
    assert n == 1
    new_qty = session.select("_pyf4_items").columns("qty").where_eq("code", "A").scalar()
    assert new_qty == 42


def test_update_returning_yields_new_row(session) -> None:
    _seed(session, [{"code": "A", "label": "x", "qty": 0}])
    row = (
        session.update("_pyf4_items")
        .set(qty=100)
        .where_eq("code", "A")
        .returning("code", "qty")
        .one()
    )
    assert row == {"code": "A", "qty": 100}


# -------------------------- DELETE --------------------------


def test_delete_where_execute_returns_affected(session) -> None:
    _seed(session, [{"code": f"D{i}", "label": "d", "qty": i} for i in range(5)])
    n = session.delete("_pyf4_items").where_gte("qty", 3).execute()
    assert n == 2
    remaining = session.select("_pyf4_items").columns("code").order_by("code").all()
    assert [r["code"] for r in remaining] == ["D0", "D1", "D2"]


def test_delete_returning_returns_deleted_rows(session) -> None:
    _seed(session, [{"code": "K", "label": "k", "qty": 1}])
    row = session.delete("_pyf4_items").where_eq("code", "K").returning("code").one()
    assert row == {"code": "K"}


# -------------------------- UPSERT --------------------------


def test_upsert_insert_or_update_on_conflict(session) -> None:
    # Prima chiamata: insert.
    row1 = (
        session.upsert("_pyf4_items")
        .values(code="U1", label="orig", qty=1)
        .conflict_target("code")
        .update_on_conflict(label="updated", qty=999)
        .returning("id", "code", "label", "qty")
        .one()
    )
    assert row1["label"] == "orig"
    assert row1["qty"] == 1
    # Seconda chiamata con stesso code: update (conflict).
    row2 = (
        session.upsert("_pyf4_items")
        .values(code="U1", label="ignored", qty=2)
        .conflict_target("code")
        .update_on_conflict(label="updated", qty=999)
        .returning("id", "code", "label", "qty")
        .one()
    )
    assert row2["id"] == row1["id"], "upsert deve conservare l'id originale"
    assert row2["label"] == "updated"
    assert row2["qty"] == 999


# -------------------------- Session wrapper --------------------------


def test_session_wrapper_forwards_attributes(session) -> None:
    assert isinstance(session.server_version, str)
    assert session.is_closed is False


def test_session_wrapper_context_manager_closes(session) -> None:
    # session è già in context via fixture; qui apriamo un nuovo Session.
    dsn = _dsn_or_skip()
    with plenora_database.connect(dsn) as s2:
        assert s2.is_closed is False
    assert s2.is_closed is True
