"""F3-6b — Test integrazione live per typed params helpers."""
from __future__ import annotations

import os

import pytest

import plenora_database as p

DSN_ENV = "PLENORA_TEST_POSTGRES_DSN"


def _dsn_or_skip() -> str:
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        pytest.skip(f"live test: manca env {DSN_ENV}")
    return dsn


@pytest.fixture(name="session")
def _session():
    dsn = _dsn_or_skip()
    s = p.connect(dsn)
    try:
        yield s
    finally:
        s.close()


# ------------------------------ UUID ------------------------------
#
# Driver limitation nota: il path OLTP invia i parametri UUID via text-of-str
# binding di tokio-postgres, che però usa binary format e Postgres si aspetta
# 16 byte per il tipo UUID. Il workaround portable è cast esplicito
# `($1::text)::uuid` per forzare il bind come text.


def test_uuid_typed_roundtrip_via_text_cast(session) -> None:
    val = "550e8400-e29b-41d4-a716-446655440000"
    # Workaround driver: cast text intermedio.
    result = session.execute_scalar(
        "SELECT (($1::text))::uuid::text",
        [p.uuid(val)],
    )
    assert result == val


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
#
# Nota: il path OLTP del driver Postgres non supporta ancora binding di
# `ParameterValue::Decimal` (Unsupported). Il valore rimane esposto via
# helper `p.decimal(...)` — verrà attivato quando il driver acquisisce
# il codec Decimal (roadmap del core Rust, non del SDK Python).


def test_decimal_helper_produces_typed_value(session) -> None:
    v = p.decimal("1234.56")
    assert v._plenora_typed_kind == "decimal"
    assert v._plenora_typed_value == "1234.56"


def test_decimal_binding_currently_unsupported_at_driver_level(session) -> None:
    with pytest.raises(p.PlenoraUnsupportedError, match="decimal"):
        session.execute_scalar("SELECT ($1::numeric)::text", [p.decimal("1.0")])


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


def test_date_and_timestamp_returning_via_builder(session) -> None:
    # Insert Date + Timestamp typed → RETURNING via builder.
    # (UUID e Decimal saltati per limitazioni driver documentate sopra.)
    session.execute("DROP TABLE IF EXISTS _pyf6b_types")
    session.execute(
        "CREATE TABLE _pyf6b_types ("
        " id INT PRIMARY KEY,"
        " created DATE,"
        " ts TIMESTAMP)"
    )
    try:
        row = (
            session.insert("_pyf6b_types")
            .values(
                id=1,
                created=p.date("2026-01-15"),
                ts=p.timestamp("2026-01-15T09:00:00"),
            )
            .returning("id", "created", "ts")
            .one()
        )
        assert row["id"] == 1
        assert row["created"] == "2026-01-15"
        # Timestamp roundtrip: Postgres normalizza in "2026-01-15 09:00:00".
        assert "2026-01-15" in row["ts"] and "09:00:00" in row["ts"]
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6b_types")
