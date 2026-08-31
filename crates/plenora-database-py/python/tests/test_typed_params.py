"""F3-6b — Test integrazione live per typed params helpers."""
from __future__ import annotations

import os

import pytest

import plenora_database as p
from plenora_database._ast import literal_value

from ._harness import connect_postgres, postgres_dsn_or_skip


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    try:
        yield s
    finally:
        s.close()


# ------------------------------ UUID ------------------------------
# Il driver supporta bind UUID sia come text (per cast
# via `$1::uuid`) sia come binary (per colonne UUID native).


def test_uuid_typed_roundtrip_direct_cast(session) -> None:
    val = "550e8400-e29b-41d4-a716-446655440000"
    # Cast diretto: il bind funziona sia come text (dispatch text) sia
    # come uuid (dispatch binary).
    result = session.execute_scalar("SELECT ($1::uuid)::text", [p.uuid(val)])
    assert result == val


def test_uuid_in_where_via_builder(session) -> None:
    session.execute("DROP TABLE IF EXISTS _pyf6b_uid")
    session.execute("CREATE TABLE _pyf6b_uid (uid UUID PRIMARY KEY, name TEXT)")
    try:
        uid = "11111111-2222-3333-4444-555555555555"
        session.execute(
            "INSERT INTO _pyf6b_uid (uid, name) VALUES ($1, $2)",
            [p.uuid(uid), "target"],
        )
        row = (
            session.select("_pyf6b_uid")
            .columns("name")
            .where_eq("uid", p.uuid(uid))
            .one()
        )
        assert row == {"name": "target"}
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6b_uid")


# ------------------------------ Int64 ------------------------------


def test_int64_helper_keeps_small_values_typed_as_i64() -> None:
    value = p.int64(1)
    assert value._plenora_typed_kind == "i64"
    assert value._plenora_typed_value == 1
    assert literal_value(value) == {"type": "i64", "value": 1}


def test_int64_helper_rejects_invalid_values_without_echoing_them() -> None:
    for value in (True, "1", 1.0):
        with pytest.raises(TypeError, match="int64 richiede un int Python"):
            p.int64(value)
    for value in (-(1 << 63) - 1, 1 << 63):
        with pytest.raises(OverflowError, match="signed 64-bit"):
            p.int64(value)


def test_int64_small_value_roundtrip_direct_bigint_cast(session) -> None:
    assert session.execute_scalar("SELECT $1::bigint", [p.int64(1)]) == 1


def test_int64_small_value_bigint_crud_via_builders(session) -> None:
    session.execute("DROP TABLE IF EXISTS _pyf6b_int64")
    session.execute(
        "CREATE TABLE _pyf6b_int64 (id INT PRIMARY KEY, row_version BIGINT NOT NULL)"
    )
    try:
        inserted = (
            session.insert("_pyf6b_int64")
            .values(id=1, row_version=p.int64(1))
            .execute()
        )
        assert inserted == 1
        updated = (
            session.update("_pyf6b_int64")
            .set(row_version=p.int64(2))
            .where_eq("row_version", p.int64(1))
            .execute()
        )
        assert updated == 1
        row = (
            session.select("_pyf6b_int64")
            .columns("row_version")
            .where_eq("row_version", p.int64(2))
            .one()
        )
        assert row == {"row_version": 2}
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6b_int64")


# ------------------------------ Date/Timestamp ------------------------------


def test_date_typed_binds_to_date_column(session) -> None:
    result = session.execute_scalar("SELECT ($1::date)::text", [p.date("2026-08-13")])
    assert result == "2026-08-13"


def test_timestamp_typed_roundtrip(session) -> None:
    # Il driver parsea con NaiveDateTime che richiede ISO-8601
    # ('YYYY-MM-DDTHH:MM:SS').
    result = session.execute_scalar(
        "SELECT ($1::timestamp)::text",
        [p.timestamp("2026-08-13T12:30:45")],
    )
    # Postgres normalizza in "2026-08-13 12:30:45".
    assert "2026-08-13" in result and "12:30:45" in result


def test_timestamptz_typed_roundtrip(session) -> None:
    result = session.execute_scalar(
        "SELECT ($1::timestamptz AT TIME ZONE 'UTC')::text",
        [p.timestamptz("2026-08-13T10:00:00+02:00")],
    )
    # 10:00+02 = 08:00 UTC → "2026-08-13 08:00:00".
    assert "2026-08-13" in result and "08:00:00" in result


# ------------------------------ Decimal ------------------------------
# Il driver supporta bind Decimal sia come text (per
# cast `$1::text::numeric`) sia come binary (per colonne NUMERIC native).


def test_decimal_helper_produces_typed_value(session) -> None:
    v = p.decimal("1234.56")
    assert v._plenora_typed_kind == "decimal"
    assert v._plenora_typed_value == "1234.56"


def test_decimal_typed_roundtrip_preserves_precision(session) -> None:
    val = "1234567.89"
    result = session.execute_scalar(
        "SELECT ($1::numeric(20,2))::text",
        [p.decimal(val)],
    )
    assert result == val


def test_decimal_typed_accepts_precision_4(session) -> None:
    result = session.execute_scalar(
        "SELECT ($1::numeric(10,4))::text",
        [p.decimal("3.1416")],
    )
    assert result == "3.1416"


def test_decimal_in_numeric_column_roundtrip(session) -> None:
    # Bind Decimal su NUMERIC e read NUMERIC via
    # decoder OLTP entrambi funzionano nativamente. Nessun cast text
    # richiesto.
    session.execute("DROP TABLE IF EXISTS _pyf6b_dec")
    session.execute(
        "CREATE TABLE _pyf6b_dec (id INT PRIMARY KEY, bal NUMERIC(12,2))"
    )
    try:
        session.execute(
            "INSERT INTO _pyf6b_dec (id, bal) VALUES ($1, $2)",
            [1, p.decimal("999.99")],
        )
        row = (
            session.select("_pyf6b_dec")
            .columns("bal")
            .where_eq("id", 1)
            .one()
        )
        assert row == {"bal": "999.99"}
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6b_dec")


def test_decimal_invalid_format_raises_unsupported(session) -> None:
    with pytest.raises(p.PlenoraUnsupportedError, match="decimal"):
        session.execute_scalar(
            "SELECT ($1::numeric)::text",
            [p.decimal("non-numerico")],
        )


# ------------------------------ Null tipizzato ------------------------------


def test_null_typed_with_type_hint(session) -> None:
    # Un NULL con hint di tipo esplicito viene bindato correttamente.
    is_null = session.execute_scalar(
        "SELECT $1::text IS NULL",
        [p.null("text")],
    )
    assert is_null is True


def test_null_typed_various_hints(session) -> None:
    for hint in ["int4", "int8", "text", "uuid", "jsonb", "bool"]:
        is_null = session.execute_scalar(
            f"SELECT $1::{hint} IS NULL",
            [p.null(hint)],
        )
        assert is_null is True, f"null typed ({hint}) doveva essere NULL"


# ------------------------------ Roundtrip generico ------------------------------


def test_typed_value_repr(session) -> None:
    v = p.uuid("aaa")
    assert "TypedValue" in repr(v)
    assert "uuid" in repr(v)


def test_untyped_int_still_works(session) -> None:
    # I typed helpers non rompono l'auto-inference standard.
    assert session.execute_scalar("SELECT $1::int", [42]) == 42


def test_full_typed_returning_via_builder(session) -> None:
    # Insert UUID + Date + Timestamp + Decimal typed → RETURNING via builder.
    # Tutti i tipi sono supportati nativamente sia in bind
    # sia in read del decoder OLTP.
    session.execute("DROP TABLE IF EXISTS _pyf6b_types")
    session.execute(
        "CREATE TABLE _pyf6b_types ("
        " id UUID PRIMARY KEY,"
        " created DATE,"
        " ts TIMESTAMP,"
        " amount NUMERIC(12,2))"
    )
    try:
        uid = "cccccccc-dddd-eeee-ffff-000000000000"
        row = (
            session.insert("_pyf6b_types")
            .values(
                id=p.uuid(uid),
                created=p.date("2026-01-15"),
                ts=p.timestamp("2026-01-15T09:00:00"),
                amount=p.decimal("999.99"),
            )
            .returning("id", "created", "ts", "amount")
            .one()
        )
        # Ritorno: server → Python. Uuid/Date/Timestamp/Decimal mappano a str.
        assert row["id"] == uid
        assert row["created"] == "2026-01-15"
        assert row["amount"] == "999.99"
        assert "2026-01-15" in row["ts"] and "09:00:00" in row["ts"]
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6b_types")
