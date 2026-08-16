"""F3-3 — Test integrazione live per execute / execute_scalar / execute_returning_rows.

Skippa gracefully se `PLENORA_TEST_POSTGRES_DSN` non è settato.
"""
from __future__ import annotations

import os

import pytest

import plenora_database

from ._harness import connect_postgres, postgres_dsn_or_skip


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    try:
        yield s
    finally:
        s.close()


def test_execute_scalar_returns_int_literal(session) -> None:
    assert session.execute_scalar("SELECT 42") == 42


def test_execute_scalar_returns_none_on_no_row(session) -> None:
    result = session.execute_scalar("SELECT 1 WHERE FALSE")
    assert result is None


def test_execute_scalar_with_positional_params(session) -> None:
    result = session.execute_scalar(
        "SELECT ($1::int + $2::int)::BIGINT",
        [1, 2],
    )
    assert result == 3


def test_execute_scalar_string_roundtrip(session) -> None:
    result = session.execute_scalar("SELECT $1::text", ["hello"])
    assert result == "hello"


def test_execute_scalar_bytes_roundtrip(session) -> None:
    payload = bytes([0x01, 0x02, 0xff, 0xab])
    result = session.execute_scalar("SELECT $1::bytea", [payload])
    assert result == payload


def test_execute_scalar_bool_roundtrip(session) -> None:
    assert session.execute_scalar("SELECT $1::bool", [True]) is True
    assert session.execute_scalar("SELECT $1::bool", [False]) is False


def test_execute_scalar_float_roundtrip(session) -> None:
    result = session.execute_scalar("SELECT $1::float8", [3.14])
    assert result == pytest.approx(3.14)


def test_execute_scalar_null_binds_correctly(session) -> None:
    is_null = session.execute_scalar("SELECT $1::text IS NULL", [None])
    assert is_null is True


def test_execute_scalar_jsonb_dict_roundtrip(session) -> None:
    payload = {"name": "Ada", "score": 99, "active": True, "nested": {"x": 1}}
    result = session.execute_scalar("SELECT $1::jsonb", [payload])
    assert result == payload


def test_execute_scalar_jsonb_list_roundtrip(session) -> None:
    payload = [1, "a", True, None, {"k": "v"}]
    result = session.execute_scalar("SELECT $1::jsonb", [payload])
    assert result == payload


def test_execute_returning_rows_shape(session) -> None:
    rows = session.execute_returning_rows(
        "SELECT id, name FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name) ORDER BY id"
    )
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_execute_returning_rows_empty(session) -> None:
    rows = session.execute_returning_rows("SELECT 1 WHERE FALSE")
    assert rows == []


def test_execute_dml_creates_populates_and_counts_persistent_table(session) -> None:
    session.execute("DROP TABLE IF EXISTS _pyf3_persist")
    session.execute("CREATE TABLE _pyf3_persist (id INT PRIMARY KEY, val TEXT)")
    try:
        affected = session.execute(
            "INSERT INTO _pyf3_persist (id, val) VALUES ($1, $2), ($3, $4)",
            [1, "a", 2, "b"],
        )
        assert affected == 2

        count = session.execute_scalar(
            "SELECT COUNT(*)::BIGINT FROM _pyf3_persist"
        )
        assert count == 2

        rows = session.execute_returning_rows(
            "SELECT id, val FROM _pyf3_persist ORDER BY id"
        )
        assert rows == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]

        updated = session.execute(
            "UPDATE _pyf3_persist SET val = $1 WHERE id = $2",
            ["updated", 1],
        )
        assert updated == 1

        deleted = session.execute("DELETE FROM _pyf3_persist WHERE id = $1", [2])
        assert deleted == 1
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf3_persist")


def test_execute_on_closed_session_raises(session) -> None:
    session.close()
    with pytest.raises(RuntimeError, match="chiusa"):
        session.execute("SELECT 1")
    with pytest.raises(RuntimeError, match="chiusa"):
        session.execute_scalar("SELECT 1")
    with pytest.raises(RuntimeError, match="chiusa"):
        session.execute_returning_rows("SELECT 1")


def test_execute_error_maps_to_runtime_error_with_categorized_message(session) -> None:
    with pytest.raises(RuntimeError) as exc_info:
        session.execute_scalar("SELECT * FROM nonexistent_table_xyz")
    msg = str(exc_info.value)
    # Il messaggio ha il prefisso categoria (es. "Schema:", "Execution:").
    assert ":" in msg


def test_execute_unsupported_param_type_raises_type_error(session) -> None:
    class Custom:
        pass

    with pytest.raises(TypeError, match="tipo Python non supportato"):
        session.execute("SELECT $1::text", [Custom()])
