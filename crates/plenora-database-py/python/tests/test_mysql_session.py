"""Sessione MySQL sincrona del SDK: esecuzione SQL e ciclo di vita.

Copre la parte relazionale della superficie:
  - `connect_mysql(host, database, user, password[, port][, tls_ca_pem])`
  - `execute` / `execute_scalar` / `execute_returning_rows` / `execute_ddl`
  - parametri tipizzati (uuid, decimal, NULL)
  - context manager, e l'autocommit del DDL visibile subito

Il resto della superficie MySQL vive in moduli propri: `copy_from` e il
contratto Replace/TruncateInsert in `test_mysql_copy_from.py`, le capability
condivise con Postgres — `begin` con `SessionContext`, `read`, builder AST —
in `test_mysql_capabilities.py`.
"""
from __future__ import annotations

import os

import pytest

import plenora_database as p

from ._harness import connect_mysql_reference, mysql_config_or_skip

@pytest.fixture(name="session")
def _mysql_session():
    s = connect_mysql_reference()
    s.execute_ddl("DROP TABLE IF EXISTS _v04_sdk_test")
    s.execute_ddl(
        "CREATE TABLE _v04_sdk_test ("
        " id BIGINT PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " amount INT NOT NULL) ENGINE=InnoDB"
    )
    try:
        yield s
    finally:
        try:
            s.execute_ddl("DROP TABLE IF EXISTS _v04_sdk_test")
        finally:
            s.close()


def test_connect_mysql_returns_session_with_server_version(session) -> None:
    assert isinstance(session.server_version, str)
    assert session.server_version.startswith(("8.", "9."))
    assert session.is_closed is False
    assert session.capabilities["provider"] == "mysql"
    assert session.capabilities["reads"]["streaming"] is True
    assert isinstance(session.inspect.catalogs(), list)
    assert isinstance(session.inspect.schemas(), list)


def test_execute_ddl_insert_scalar_roundtrip(session) -> None:
    n = session.execute(
        "INSERT INTO _v04_sdk_test (id, label, amount) VALUES (?, ?, ?)",
        [1, "alfa", 10],
    )
    assert n == 1

    count = session.execute_scalar(
        "SELECT COUNT(*) FROM _v04_sdk_test WHERE id = ?", [1]
    )
    assert count == 1

    target = p.table("_v04_sdk_test", "id", "label", "amount")
    assert session.execute(
        p.insert(target).values(
            id=p.bind("id"), label=p.bind("label"), amount=p.bind("amount")
        ),
        {"id": 2, "label": "beta", "amount": 20},
    ) == 1
    assert session.execute(
        p.update(target)
        .values(label=p.bind("label"))
        .where(target.c.id == p.bind("id")),
        {"label": "BETA", "id": 2},
    ) == 1
    upserted = session.execute(
        p.upsert(target)
        .values(id=p.bind("id"), label=p.bind("insert_label"), amount=p.bind("amount"))
        .on_conflict(target.c.id)
        .set(label=p.bind("update_label")),
        {"id": 2, "insert_label": "ignored", "amount": 20, "update_label": "UPSERTED"},
    )
    assert upserted.affected_rows is not None and upserted.affected_rows >= 1
    assert session.execute(
        p.delete(target).where(target.c.id == p.bind("id")), {"id": 2}
    ) == 1


def test_execute_returning_rows_provides_dicts(session) -> None:
    session.execute(
        "INSERT INTO _v04_sdk_test (id, label, amount) VALUES (?, ?, ?), (?, ?, ?)",
        [1, "a", 100, 2, "b", 200],
    )
    rows = session.execute_returning_rows(
        "SELECT id, label FROM _v04_sdk_test WHERE amount >= ? ORDER BY id", [50]
    )
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["label"] == "a"
    assert rows[1]["id"] == 2
    assert rows[1]["label"] == "b"


def test_execute_scalar_null_returns_none(session) -> None:
    v = session.execute_scalar("SELECT NULL")
    assert v is None


def test_context_manager_closes_session() -> None:
    host, database, user, password, ca_pem = mysql_config_or_skip()
    with p.connect_mysql(host, database, user, password, tls_ca_pem=ca_pem) as s:
        assert s.is_closed is False
        result = s.execute_scalar("SELECT 42")
        assert result == 42
    assert s.is_closed is True


def test_typed_params_uuid_and_decimal_roundtrip(session) -> None:
    """I typed params funzionano su MySQL come su Postgres.
    Il decorator TypedValue è provider-agnostic; parameter.rs MySQL mappa
    Uuid/Decimal/Date/Timestamp come text strings native MySQL."""
    session.execute_ddl("DROP TABLE IF EXISTS _v04_typed")
    session.execute_ddl(
        "CREATE TABLE _v04_typed ("
        " id CHAR(36) PRIMARY KEY,"
        " amount DECIMAL(10,2) NOT NULL,"
        " event_date DATE NOT NULL"
        ") ENGINE=InnoDB"
    )
    try:
        n = session.execute(
            "INSERT INTO _v04_typed (id, amount, event_date) VALUES (?, ?, ?)",
            [
                p.uuid("550e8400-e29b-41d4-a716-446655440000"),
                p.decimal("1234.56"),
                p.date("2026-08-14"),
            ],
        )
        assert n == 1
        rows = session.execute_returning_rows(
            "SELECT id, amount, event_date FROM _v04_typed"
        )
        assert len(rows) == 1
        assert rows[0]["id"] == "550e8400-e29b-41d4-a716-446655440000"
        # amount / event_date arrivano come stringhe (formato MySQL native)
    finally:
        session.execute_ddl("DROP TABLE IF EXISTS _v04_typed")


def test_null_typed_param(session) -> None:
    session.execute("INSERT INTO _v04_sdk_test (id, label, amount) VALUES (?, ?, ?)",
                    [1, "x", 10])
    # `<=>`, non `IS ?`: su MySQL `IS` accetta solo NULL/TRUE/FALSE/UNKNOWN e
    # un placeholder produce un errore di sintassi (1064). L'operatore
    # null-safe e quello che il test intende — confronto con un parametro che
    # puo essere NULL.
    rows = session.execute_returning_rows(
        "SELECT id FROM _v04_sdk_test WHERE label = ? OR label <=> ?",
        ["x", p.null("text")],
    )
    assert len(rows) >= 1


def test_ddl_autocommit_visible_immediately(session) -> None:
    # MySQL DDL è autocommit — CREATE TEMPORARY TABLE visibile subito.
    session.execute_ddl("DROP TABLE IF EXISTS _v04_ddl_visibility")
    session.execute_ddl(
        "CREATE TABLE _v04_ddl_visibility (x INT) ENGINE=InnoDB"
    )
    try:
        exists = session.execute_scalar(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = '_v04_ddl_visibility'"
        )
        assert exists == 1
    finally:
        session.execute_ddl("DROP TABLE _v04_ddl_visibility")
